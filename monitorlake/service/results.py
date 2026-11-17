from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class WriteResult:
    batch_id: str
    snapshot_id: int
    row_count: int

    def as_dict(self):
        return asdict(self)


def to_write_result(publication):
    return WriteResult(publication.batch_id, publication.snapshot_id, publication.row_count)
