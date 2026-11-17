from dataclasses import dataclass


@dataclass(frozen=True)
class WriteRequest:
    batch_id: str
    source_description: str
    rows: object


def prepare_write_request(batch_id, source_description, rows):
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("batch_id must be a nonempty string")
    if not isinstance(source_description, str):
        raise ValueError("source_description must be a string")
    return WriteRequest(batch_id, source_description, rows)


def validate_snapshot_request(snapshot_id):
    if type(snapshot_id) is not int or not 0 <= snapshot_id <= (1 << 63) - 1:
        raise ValueError("snapshot_id must be a nonnegative SQLite integer")
    return snapshot_id
