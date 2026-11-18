from dataclasses import dataclass
import hashlib
import json
from monitorlake.intake.identity import iter_text_chunks
from monitorlake.intake.source import SourceCursor
from monitorlake.intake.ordinal import OrdinalAllocator
from monitorlake.schema.normalization import normalize_observation


@dataclass(frozen=True)
class CandidateBatch:
    batch_id: str
    source_description: str
    records: tuple
    identity: str
    digest: str

    @property
    def row_count(self):
        return len(self.records)

    @property
    def catalog_identity(self):
        return self.identity

    def identity_stream(self):
        return iter_text_chunks(self.identity)


def candidate_digest(identity):
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def collect_batch(batch_id, source_description, rows):
    allocator = OrdinalAllocator()
    records = []
    cursor = SourceCursor(rows)
    for raw in cursor:
        records.append(allocator.assign(normalize_observation(raw)))
    identity = json.dumps(
        {"source_description": source_description,
         "rows": [record.observation.as_dict() for record in records]},
        ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    return CandidateBatch(
        batch_id, source_description, tuple(records), identity, candidate_digest(identity)
    )
