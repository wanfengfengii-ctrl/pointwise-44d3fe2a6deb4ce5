"""Single-pass paged intake: validate, stream tokens into pages, hash identity.

Nothing here materialises a whole record, column, page directory or identity
text: a record is validated and encoded token by token straight into the page
writer, while the canonical identity bytes are hashed in place and never kept.
The paged object itself is assembled from the already-flushed page file by a
chunked copy, then re-read page by page to re-derive and confirm the same
identity digest from the exact persisted bytes.
"""
from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path

from monitorlake.intake.identity import IdentityWriter, iter_paged_identity_at
from monitorlake.intake.source import SourceCursor
from monitorlake.schema.errors import StorageError
from monitorlake.schema.types import INT64_MAX, INT64_MIN
from monitorlake.segments.layout import SegmentDescriptor
from monitorlake.segments.paged_format import (
    FRAME_LENGTH, LABEL_BYTES_MAX, PAGED_HEADER_LIMIT, PAGED_MAGIC, PAGED_VERSION,
    PAGE_BYTES_MAX, PAGE_BYTES_MIN, PageWriter, encode_varint, zigzag,
)

COPY_CHUNK = 4096


def validate_page_bytes(page_bytes):
    if type(page_bytes) is not int or not PAGE_BYTES_MIN <= page_bytes <= PAGE_BYTES_MAX:
        raise ValueError("page_bytes must be a non-boolean integer in 4096..65536")
    return page_bytes


def identity_marker(object_digest):
    """Small catalog identity for a paged batch.

    The full semantic identity is precisely ``(source_description, ordered
    canonical rows)`` and lives in the immutable paged object; the catalog only
    pins the object digest here.  Replay comparisons derive the canonical text
    from that object and compare it byte for byte.
    """
    return json.dumps({"object": object_digest}, sort_keys=True,
                      separators=(",", ":"), ensure_ascii=True)


@dataclass(frozen=True)
class PagedCandidate:
    batch_id: str
    source_description: str
    identity_digest: str
    row_count: int
    staged: object
    catalog_identity: str

    @property
    def digest(self):
        return self.identity_digest

    def identity_stream(self):
        return iter_paged_identity_at(
            self.staged.path, self.staged.descriptor, self.source_description
        )


def _label_bytes(label):
    if not isinstance(label, str):
        raise ValueError("label must be a string")
    try:
        raw = label.encode("utf-8")
    except UnicodeError as error:
        raise ValueError("label must be encodable as UTF-8") from error
    if len(raw) > LABEL_BYTES_MAX:
        raise ValueError("label exceeds 1024 UTF-8 bytes")
    return raw


def encode_row(pages, identity, raw):
    """Validate one record and append its token stream and identity bytes."""
    if not isinstance(raw, Mapping):
        raise ValueError("record must be a mapping")
    if "label" not in raw or "samples" not in raw:
        raise ValueError("record requires label and samples")
    label = raw["label"]
    raw_label = _label_bytes(label)
    pages.write(encode_varint(len(raw_label)))
    pages.write(raw_label)
    outer = raw["samples"]
    if outer is None:
        pages.write(b"\x00")
        identity.write_row(label, None)
        return
    if type(outer) is not list:
        raise ValueError("samples must be NULL or a list")
    pages.write(b"\x01")
    pages.write(encode_varint(len(outer)))
    for segment_index, segment in enumerate(outer):
        if segment is None:
            pages.write(b"\x00")
            continue
        if type(segment) is not list:
            raise ValueError(f"segment {segment_index} must be NULL or a list")
        pages.write(b"\x01")
        pages.write(encode_varint(len(segment)))
        for sample_index, sample in enumerate(segment):
            if sample is None:
                pages.write(b"\x00")
                continue
            if type(sample) is not int or not INT64_MIN <= sample <= INT64_MAX:
                raise ValueError(
                    f"segment {segment_index}, sample {sample_index}: sample must be "
                    "NULL or a non-boolean signed 64-bit integer"
                )
            pages.write(b"\x01")
            pages.write(encode_varint(zigzag(sample)))
    identity.write_row(label, outer)


def _assemble_object(stager, body_path, body_length, page_count, row_count, page_bytes):
    header = json.dumps(
        {"version": PAGED_VERSION, "row_count": row_count,
         "body_length": body_length, "page_bytes": page_bytes,
         "page_count": page_count},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("ascii")
    if len(header) > PAGED_HEADER_LIMIT:
        raise StorageError("paged segment header exceeds its structural limit")
    path, stream = stager.create("paged-candidate-")
    digest = hashlib.sha256()
    size = 0
    try:
        with stream:
            for piece in (PAGED_MAGIC, FRAME_LENGTH.pack(len(header)), header):
                stream.write(piece)
                digest.update(piece)
                size += len(piece)
            with Path(body_path).open("rb") as body:
                while True:
                    chunk = body.read(COPY_CHUNK)
                    if not chunk:
                        break
                    stream.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        stager.discard_path(path, suppress=True)
        raise
    descriptor = SegmentDescriptor(PAGED_VERSION, row_count, size, digest.hexdigest())
    return stager.stage_path(path, descriptor)


def collect_paged_batch(batch_id, source_description, rows, page_bytes, stager):
    """Validate and persist one paged candidate, streaming every byte.

    The identity digest is computed while rows are consumed (it binds the
    source description too), then the assembled immutable object is re-read
    page by page in the *same* streaming encoder to re-derive the digest.  A
    mismatch means the assembled object does not carry the accepted rows, so
    the candidate is discarded rather than published.
    """
    validate_page_bytes(page_bytes)
    body_path = body_stream = None
    final_path = None
    identity_digest = None
    try:
        body_path, body_stream = stager.create("paged-body-")
        identity = IdentityWriter()
        identity.begin()
        pages = PageWriter(body_stream, page_bytes)
        for raw in SourceCursor(rows):
            encode_row(pages, identity, raw)
        pages.finish()
        identity.finish(source_description)
        identity_digest = identity.digest
        body_stream.flush()
        os.fsync(body_stream.fileno())
        body_stream.close()
        body_stream = None
        staged = _assemble_object(
            stager, body_path, pages.body_length, pages.page_count,
            identity.rows, page_bytes,
        )
        final_path = staged.path
        stager.discard_path(body_path, suppress=True)
        body_path = None
        descriptor = staged.descriptor
        verified = hashlib.sha256()
        for chunk in iter_paged_identity_at(
            staged.path, descriptor, source_description
        ):
            verified.update(chunk)
        if verified.hexdigest() != identity_digest:
            raise StorageError("paged candidate identity does not match its object")
        return PagedCandidate(
            batch_id, source_description, identity_digest, identity.rows, staged,
            identity_marker(descriptor.digest),
        )
    except BaseException:
        if body_stream is not None:
            try:
                body_stream.close()
            except OSError:
                pass
        for path in (body_path, final_path):
            if path is not None:
                stager.discard_path(path, suppress=True)
        raise
