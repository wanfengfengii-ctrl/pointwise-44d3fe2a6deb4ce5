from dataclasses import dataclass

from monitorlake.intake.paged_batch import validate_page_bytes


@dataclass(frozen=True)
class WriteRequest:
    batch_id: str
    source_description: str
    rows: object


@dataclass(frozen=True)
class PagedWriteRequest:
    batch_id: str
    source_description: str
    rows: object
    page_bytes: int


def prepare_write_request(batch_id, source_description, rows):
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("batch_id must be a nonempty string")
    if not isinstance(source_description, str):
        raise ValueError("source_description must be a string")
    return WriteRequest(batch_id, source_description, rows)


def prepare_paged_request(batch_id, source_description, rows, page_bytes):
    request = prepare_write_request(batch_id, source_description, rows)
    validate_page_bytes(page_bytes)
    return PagedWriteRequest(
        request.batch_id, request.source_description, request.rows, page_bytes
    )


def validate_snapshot_request(snapshot_id):
    if type(snapshot_id) is not int or not 0 <= snapshot_id <= (1 << 63) - 1:
        raise ValueError("snapshot_id must be a nonnegative SQLite integer")
    return snapshot_id
