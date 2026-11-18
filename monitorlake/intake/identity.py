"""Canonical semantic identity, produced and compared incrementally.

The V1 identity of a batch is the deterministic JSON text::

    {"rows":[{"label":...,"samples":...},...],"source_description":...}

(``ensure_ascii``, sorted keys, no spaces).  A paged batch keeps exactly the
same identity text semantics so that ``write`` and ``write_paged`` recognise
each other as the same business batch, but on the paged path the text is
*streamed*: it is written to a temporary file and hashed in fixed chunks, and
a stored paged batch derives the same text from its immutable paged object one
row at a time.  No full identity text is ever held in memory on that path.
"""
import hashlib

CHUNK_BYTES = 4096
_SIMPLE_ESCAPES = {
    0x22: b'\\"',
    0x5C: b"\\\\",
    0x08: b"\\b",
    0x09: b"\\t",
    0x0A: b"\\n",
    0x0C: b"\\f",
    0x0D: b"\\r",
}


def _escape_piece(character):
    code = ord(character)
    simple = _SIMPLE_ESCAPES.get(code)
    if simple is not None:
        return simple
    if 0x20 <= code < 0x7F:
        return character.encode("ascii")
    if code <= 0xFFFF:
        return ("\\u%04x" % code).encode("ascii")
    code -= 0x10000
    high = 0xD800 + (code >> 10)
    low = 0xDC00 + (code & 0x3FF)
    return ("\\u%04x\\u%04x" % (high, low)).encode("ascii")


def iter_json_string(text, flush_at=CHUNK_BYTES):
    """Yield the ``json.dumps(text, ensure_ascii=True)`` bytes in small chunks."""
    if not isinstance(text, str):
        raise TypeError("canonical JSON string requires str")
    yield b'"'
    buffer = bytearray()
    for character in text:
        buffer += _escape_piece(character)
        if len(buffer) >= flush_at:
            yield bytes(buffer)
            buffer.clear()
    if buffer:
        yield bytes(buffer)
    yield b'"'


def iter_text_chunks(text, size=CHUNK_BYTES):
    """Yield ASCII bytes of an already canonical identity text."""
    for start in range(0, len(text), size):
        yield text[start:start + size].encode("ascii")


def _chunked(pieces, size=CHUNK_BYTES):
    buffer = bytearray()
    for piece in pieces:
        buffer += piece
        if len(buffer) >= size:
            yield bytes(buffer)
            buffer.clear()
    if buffer:
        yield bytes(buffer)


class IdentityWriter:
    """Stream the canonical identity text, hashing it as it goes.

    ``stream`` may be ``None`` when only the digest is wanted; the text is
    never retained either way.
    """

    def __init__(self, stream=None, flush_at=CHUNK_BYTES):
        self.stream = stream
        self.flush_at = flush_at
        self._buffer = bytearray()
        self._digest = hashlib.sha256()
        self.length = 0
        self.rows = 0

    def _emit(self, data):
        self._buffer += data
        if len(self._buffer) >= self.flush_at:
            self._flush()

    def _flush(self):
        if not self._buffer:
            return
        chunk = bytes(self._buffer)
        self._buffer.clear()
        if self.stream is not None:
            self.stream.write(chunk)
        self._digest.update(chunk)
        self.length += len(chunk)

    def begin(self):
        self._emit(b'{"rows":[')

    def write_row(self, label, samples):
        if self.rows:
            self._emit(b",")
        self._emit(b'{"label":')
        for chunk in iter_json_string(label):
            self._emit(chunk)
        self._emit(b',"samples":')
        for piece in iter_samples_json(samples):
            self._emit(piece)
        self._emit(b"}")
        self.rows += 1

    def finish(self, source_description):
        self._emit(b'],"source_description":')
        for chunk in iter_json_string(source_description):
            self._emit(chunk)
        self._emit(b"}")
        self._flush()

    @property
    def digest(self):
        return self._digest.hexdigest()


def iter_samples_json(samples):
    """Yield the canonical JSON bytes for a validated samples value."""
    if samples is None:
        yield b"null"
        return
    yield b"["
    for index, segment in enumerate(samples):
        if index:
            yield b","
        if segment is None:
            yield b"null"
            continue
        yield b"["
        for position, value in enumerate(segment):
            if position:
                yield b","
            yield b"null" if value is None else str(value).encode("ascii")
        yield b"]"
    yield b"]"


def iter_row_identity_pieces(events):
    """Turn decoded paged row events into the canonical identity bytes."""
    from monitorlake.schema.errors import CorruptSegment

    stream = iter(events)
    kind, label = next(stream)
    if kind != "label":
        raise CorruptSegment("paged row does not start with a label")
    yield b'{"label":'
    yield from iter_json_string(label)
    yield b',"samples":'
    kind = next(stream)[0]
    if kind == "outer_null":
        yield b"null"
    elif kind == "outer_begin":
        yield b"["
        first_segment = True
        for event in stream:
            name = event[0]
            if name == "segment_null":
                if not first_segment:
                    yield b","
                yield b"null"
                first_segment = False
            elif name == "segment_begin":
                if not first_segment:
                    yield b","
                yield b"["
                first_value = True
                for inner in stream:
                    inner_name = inner[0]
                    if inner_name == "value":
                        if not first_value:
                            yield b","
                        value = inner[1]
                        yield b"null" if value is None else str(value).encode("ascii")
                        first_value = False
                    elif inner_name == "segment_end":
                        yield b"]"
                        first_segment = False
                        break
                    else:
                        raise CorruptSegment("unexpected paged event inside a segment")
            elif name == "outer_end":
                yield b"]"
                break
            else:
                raise CorruptSegment("unexpected paged event inside samples")
    else:
        raise CorruptSegment("invalid outer samples event")
    yield b"}"


def iter_paged_identity_chunks(objects, sealed, source_description):
    """Stream the canonical identity text of a stored paged batch.

    The whole object digest is verified in bounded chunks before decoding and
    each page checksum is verified as it is read.  Only one page worth of body
    bytes is resident at a time.  The generator must be exhausted (or closed)
    to release the file handle.
    """
    from monitorlake.segments.paged_format import open_reader

    return iter_paged_identity_at(
        objects.path_for(sealed.digest), sealed.descriptor, source_description
    )


def iter_paged_identity_at(path, descriptor, source_description):
    """Stream identity text from a paged object path (staged or installed)."""
    from monitorlake.segments.paged_format import iter_row_events, open_reader

    reader = open_reader(path, descriptor)

    def pieces():
        yield b'{"rows":['
        for index in range(reader.row_count):
            if index:
                yield b","
            yield from iter_row_identity_pieces(iter_row_events(reader))
        yield b'],"source_description":'
        yield from iter_json_string(source_description)
        yield b"}"

    with reader:
        yield from _chunked(pieces())
        reader.finish()


def streams_equal(left, right):
    """Compare two byte-chunk iterators without holding a whole stream."""
    left = iter(left)
    right = iter(right)
    left_chunk = right_chunk = b""
    while True:
        if not left_chunk:
            left_chunk = next(left, None)
        if not right_chunk:
            right_chunk = next(right, None)
        if left_chunk is None or right_chunk is None:
            return left_chunk is None and right_chunk is None
        size = min(len(left_chunk), len(right_chunk))
        if left_chunk[:size] != right_chunk[:size]:
            return False
        left_chunk = left_chunk[size:]
        right_chunk = right_chunk[size:]
