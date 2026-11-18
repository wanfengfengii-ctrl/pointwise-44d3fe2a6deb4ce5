"""Paged segment format (version 2): header, page frames and bounded tokens.

Layout of one immutable paged object::

    magic "MLAKEPG2"          8 bytes
    header length             4 bytes, big-endian unsigned
    header                    canonical JSON, ASCII
    page 0 .. page N-1        each: length 4 bytes + SHA-256 32 bytes + content

The header is ``{"body_length":L,"page_bytes":P,"page_count":N,
"row_count":R,"version":2}`` (canonical JSON, sorted keys).  The bodies of all
pages concatenated are exactly ``L`` bytes; every page except the last carries
exactly ``P`` content bytes and the last carries the remainder.  ``N`` is
therefore ``ceil(L / P)``.  Frame length fields and per-page checksums are
structural metadata and do not count towards ``P``; every label, nesting
boundary, sample token and any raw bytes left over from a record or inner
segment that straddles a page boundary live inside a page's content.

The body is one logical token stream::

    row        := label outer
    label      := varint(byte length) utf8 bytes          (length <= 1024)
    outer      := 0x00                                   (samples is None)
                | 0x01 varint(segment count) segment*
    segment    := 0x00                                   (segment is None)
                | 0x01 varint(value count) value*
    value      := 0x00                                   (None)
                | 0x01 varint(zigzag(value))             (signed 64-bit int)

Tags are single bytes; the stream is independent of page boundaries.  The
reader keeps at most one page resident, so token decoding needs O(page_bytes)
working memory regardless of record length or batch size.
"""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import struct

from monitorlake.schema.errors import CorruptSegment

PAGED_VERSION = 2
PAGED_MAGIC = b"MLAKEPG2"
PAGED_HEADER_LIMIT = 65536
PAGE_BYTES_MIN = 4096
PAGE_BYTES_MAX = 65536
LABEL_BYTES_MAX = 1024
FRAME_LENGTH = struct.Struct(">I")
DIGEST_BYTES = 32
MAX_VARINT_BYTES = 10
MAX_COUNT_VARINT = (1 << 63) - 1
MAX_VALUE_VARINT = (1 << 64) - 1
TAG_NULL = 0
TAG_LIST = 1
_HASH_CHUNK = 4096


@dataclass(frozen=True)
class PagedLayout:
    path: Path
    row_count: int
    body_length: int
    page_bytes: int
    page_count: int
    first_page: int
    file_size: int


@dataclass(frozen=True)
class PageEntry:
    index: int
    length: int
    sha256: str
    offset: int


def require_natural(value, context):
    if type(value) is not int or value < 0:
        raise CorruptSegment(f"{context}: expected a nonnegative integer")
    return value


def encode_varint(value):
    if type(value) is not int or value < 0 or value > MAX_VALUE_VARINT:
        raise ValueError("varint value out of range")
    out = bytearray()
    while True:
        piece = value & 0x7F
        value >>= 7
        if value:
            out.append(piece | 0x80)
        else:
            out.append(piece)
            return bytes(out)


def zigzag(value):
    return (value << 1) ^ (value >> 63)


def unzigzag(value):
    return (value >> 1) ^ -(value & 1)


def page_content_length(layout, index):
    if type(index) is not int or index < 0 or index >= layout.page_count:
        raise CorruptSegment("page index out of range")
    if index < layout.page_count - 1:
        return layout.page_bytes
    return layout.body_length - layout.page_bytes * (layout.page_count - 1)


def read_header(stream):
    prefix = stream.read(len(PAGED_MAGIC) + FRAME_LENGTH.size)
    if len(prefix) != len(PAGED_MAGIC) + FRAME_LENGTH.size or prefix[:len(PAGED_MAGIC)] != PAGED_MAGIC:
        raise CorruptSegment("unsupported paged segment magic")
    header_length = FRAME_LENGTH.unpack(prefix[len(PAGED_MAGIC):])[0]
    if header_length == 0 or header_length > PAGED_HEADER_LIMIT:
        raise CorruptSegment("truncated or oversized paged segment header")
    raw = stream.read(header_length)
    if len(raw) != header_length:
        raise CorruptSegment("truncated paged segment header")
    try:
        header = json.loads(raw.decode("ascii"))
    except (UnicodeError, ValueError) as error:
        raise CorruptSegment("malformed paged segment header") from error
    if type(header) is not dict or set(header) != {
        "version", "row_count", "body_length", "page_bytes", "page_count"
    }:
        raise CorruptSegment("invalid paged segment header")
    if type(header["version"]) is not int or header["version"] != PAGED_VERSION:
        raise CorruptSegment("unsupported paged segment version")
    require_natural(header["row_count"], "header row count")
    require_natural(header["body_length"], "header body length")
    require_natural(header["page_count"], "header page count")
    page_bytes = header["page_bytes"]
    if type(page_bytes) is not int or not PAGE_BYTES_MIN <= page_bytes <= PAGE_BYTES_MAX:
        raise CorruptSegment("invalid header page budget")
    body_length = header["body_length"]
    page_count = header["page_count"]
    if page_count != (body_length + page_bytes - 1) // page_bytes:
        raise CorruptSegment("page count does not match body length")
    first_page = len(PAGED_MAGIC) + FRAME_LENGTH.size + header_length
    return header, first_page


def read_layout(path, expected_row_count=None, expected_byte_count=None):
    path = Path(path)
    try:
        with path.open("rb") as stream:
            header, first_page = read_header(stream)
            size = os.fstat(stream.fileno()).st_size
    except OSError as error:
        raise CorruptSegment("required paged object unavailable") from error
    body_length = header["body_length"]
    page_count = header["page_count"]
    frame_bytes = page_count * (FRAME_LENGTH.size + DIGEST_BYTES)
    if first_page + frame_bytes + body_length != size:
        raise CorruptSegment("paged object size does not match its header")
    if expected_row_count is not None:
        require_natural(expected_row_count, "descriptor row count")
        if header["row_count"] != expected_row_count:
            raise CorruptSegment("catalog and paged segment row counts differ")
    if expected_byte_count is not None:
        require_natural(expected_byte_count, "descriptor byte count")
        if size != expected_byte_count:
            raise CorruptSegment("paged object byte count mismatch")
    return PagedLayout(
        path, header["row_count"], body_length, header["page_bytes"],
        page_count, first_page, size,
    )


def iter_page_entries(path, layout=None):
    """Enumerate page lengths and checksums without loading page contents."""
    if layout is None:
        layout = read_layout(path)
    offset = layout.first_page
    try:
        with Path(path).open("rb") as stream:
            for index in range(layout.page_count):
                stream.seek(offset)
                frame = stream.read(FRAME_LENGTH.size + DIGEST_BYTES)
                if len(frame) != FRAME_LENGTH.size + DIGEST_BYTES:
                    raise CorruptSegment("truncated page frame")
                length = FRAME_LENGTH.unpack(frame[:FRAME_LENGTH.size])[0]
                if length != page_content_length(layout, index):
                    raise CorruptSegment("page length violates the paged layout")
                yield PageEntry(index, length, frame[FRAME_LENGTH.size:].hex(), offset)
                offset += FRAME_LENGTH.size + DIGEST_BYTES + length
    except OSError as error:
        raise CorruptSegment("required paged object unavailable") from error
    if offset != layout.file_size:
        raise CorruptSegment("unaccounted bytes after the paged pages")


def verify_page_lengths(path, page_bytes=None):
    """Return every page content length, asserting the budget and coverage."""
    layout = read_layout(path)
    if page_bytes is not None and layout.page_bytes != page_bytes:
        raise CorruptSegment("page budget differs from the expected budget")
    lengths = []
    total = 0
    for entry in iter_page_entries(path, layout):
        if entry.length <= 0 or entry.length > layout.page_bytes:
            raise CorruptSegment("page content length exceeds the page budget")
        lengths.append(entry.length)
        total += entry.length
    if len(lengths) != layout.page_count or total != layout.body_length:
        raise CorruptSegment("page enumeration does not cover the body")
    return lengths


class PageWriter:
    """Cut one logical token stream into full pages of at most page_bytes."""

    def __init__(self, stream, page_bytes):
        if type(page_bytes) is not int or not PAGE_BYTES_MIN <= page_bytes <= PAGE_BYTES_MAX:
            raise ValueError("page_bytes must be an integer in 4096..65536")
        self.stream = stream
        self.page_bytes = page_bytes
        self.buffer = bytearray()
        self.body_length = 0
        self.page_count = 0

    def _flush(self):
        content = bytes(self.buffer)
        self.stream.write(FRAME_LENGTH.pack(len(content)))
        self.stream.write(hashlib.sha256(content).digest())
        self.stream.write(content)
        self.body_length += len(content)
        self.page_count += 1
        self.buffer.clear()

    def write(self, data):
        view = memoryview(data)
        while view:
            space = self.page_bytes - len(self.buffer)
            if space == 0:
                self._flush()
                continue
            take = min(space, len(view))
            self.buffer += view[:take]
            view = view[take:]
            if len(self.buffer) == self.page_bytes:
                self._flush()

    def finish(self):
        if self.buffer:
            self._flush()


class PageReader:
    """Sequential reader keeping at most one page resident."""

    def __init__(self, stream, layout):
        self.stream = stream
        self.layout = layout
        self.page_index = 0
        self.buffer = b""
        self.position = 0
        stream.seek(layout.first_page)

    @property
    def row_count(self):
        return self.layout.row_count

    def _load_page(self):
        if self.page_index >= self.layout.page_count:
            raise CorruptSegment("paged body ended inside a record")
        header = self.stream.read(FRAME_LENGTH.size + DIGEST_BYTES)
        if len(header) != FRAME_LENGTH.size + DIGEST_BYTES:
            raise CorruptSegment("truncated page frame")
        length = FRAME_LENGTH.unpack(header[:FRAME_LENGTH.size])[0]
        if length != page_content_length(self.layout, self.page_index):
            raise CorruptSegment("page length violates the paged layout")
        content = self.stream.read(length)
        if len(content) != length:
            raise CorruptSegment("truncated page content")
        if hashlib.sha256(content).digest() != header[FRAME_LENGTH.size:]:
            raise CorruptSegment("page checksum mismatch")
        self.buffer = content
        self.position = 0
        self.page_index += 1

    def read_byte(self):
        if self.position >= len(self.buffer):
            self._load_page()
        value = self.buffer[self.position]
        self.position += 1
        return value

    def read_bytes(self, count):
        if count < 0:
            raise CorruptSegment("negative token length")
        out = bytearray()
        while len(out) < count:
            if self.position >= len(self.buffer):
                self._load_page()
            take = min(count - len(out), len(self.buffer) - self.position)
            out.extend(self.buffer[self.position:self.position + take])
            self.position += take
        return bytes(out)

    def read_varint(self, limit=MAX_COUNT_VARINT):
        value = 0
        shift = 0
        for _ in range(MAX_VARINT_BYTES):
            byte = self.read_byte()
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                if value > limit:
                    raise CorruptSegment("varint value out of range")
                return value
            shift += 7
        raise CorruptSegment("overlong varint")

    def read_label(self):
        length = self.read_varint()
        if length > LABEL_BYTES_MAX:
            raise CorruptSegment("paged label exceeds its byte budget")
        try:
            return self.read_bytes(length).decode("utf-8")
        except UnicodeError as error:
            raise CorruptSegment("paged label is not UTF-8") from error

    def finish(self):
        if self.position != len(self.buffer) or self.page_index != self.layout.page_count:
            raise CorruptSegment("unaccounted bytes after the paged rows")
        if self.stream.read(1):
            raise CorruptSegment("bytes after the declared paged body")

    def close(self):
        self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def open_reader(path, expected=None):
    """Verify a whole paged object, then return a fresh sequential reader.

    ``expected`` is a descriptor with version, row_count, byte_count and digest
    attributes (a catalog ``SegmentDescriptor``).  The whole file is hashed in
    bounded chunks before any page is exposed, so a byte-damaged or truncated
    object fails before rows are produced.
    """
    path = Path(path)
    if expected is not None:
        if type(expected.version) is not int or expected.version != PAGED_VERSION:
            raise CorruptSegment("unsupported paged descriptor version")
        require_natural(expected.byte_count, "descriptor byte count")
        require_natural(expected.row_count, "descriptor row count")
        if not isinstance(expected.digest, str):
            raise CorruptSegment("invalid paged descriptor digest")
    try:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(_HASH_CHUNK)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
    except OSError as error:
        raise CorruptSegment("required paged object unavailable") from error
    if expected is not None:
        if size != expected.byte_count:
            raise CorruptSegment("paged object size differs from the catalog")
        if digest.hexdigest() != expected.digest:
            raise CorruptSegment("paged object SHA-256 mismatch")
    layout = read_layout(
        path,
        expected_row_count=None if expected is None else expected.row_count,
        expected_byte_count=size,
    )
    try:
        stream = path.open("rb")
    except OSError as error:
        raise CorruptSegment("required paged object unavailable") from error
    return PageReader(stream, layout)


def iter_row_events(reader):
    """Yield semantic events for exactly one row, validating nesting tags."""
    label = reader.read_label()
    yield ("label", label)
    tag = reader.read_byte()
    if tag == TAG_NULL:
        yield ("outer_null",)
        return
    if tag != TAG_LIST:
        raise CorruptSegment("invalid outer samples token")
    yield ("outer_begin",)
    segments = reader.read_varint()
    for _ in range(segments):
        tag = reader.read_byte()
        if tag == TAG_NULL:
            yield ("segment_null",)
            continue
        if tag != TAG_LIST:
            raise CorruptSegment("invalid segment token")
        yield ("segment_begin",)
        values = reader.read_varint()
        for _ in range(values):
            tag = reader.read_byte()
            if tag == TAG_NULL:
                yield ("value", None)
            elif tag == TAG_LIST:
                value = unzigzag(reader.read_varint(MAX_VALUE_VARINT))
                yield ("value", value)
            else:
                raise CorruptSegment("invalid sample value token")
        yield ("segment_end",)
    yield ("outer_end",)
