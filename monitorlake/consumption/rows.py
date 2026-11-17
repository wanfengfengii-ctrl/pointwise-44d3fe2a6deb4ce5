from dataclasses import dataclass
from monitorlake.schema.types import Observation


@dataclass(frozen=True)
class RowRecord:
    snapshot_id: int
    batch_id: str
    ordinal: int
    source_description: str
    observation: Observation

    @property
    def label(self):
        return self.observation.label

    @property
    def samples(self):
        return self.observation.samples

    def coordinates(self):
        return {
            "snapshot_id": self.snapshot_id, "batch_id": self.batch_id,
            "ordinal": self.ordinal, "source_description": self.source_description,
        }

    def as_dict(self):
        return {**self.coordinates(), **self.observation.as_dict()}


def restore_observation(block, row_index):
    if not block.outer_validity[row_index]:
        return Observation(block.labels[row_index], None)
    outer = []
    begin, end = block.row_segment_offsets[row_index:row_index + 2]
    for segment_index in range(begin, end):
        if not block.segment_validity[segment_index]:
            outer.append(None)
        else:
            start, stop = block.segment_value_offsets[segment_index:segment_index + 2]
            outer.append(list(block.sample_values[start:stop]))
    return Observation(block.labels[row_index], outer)


def iter_rows(scan):
    for batch, block in scan.iter_blocks():
        for ordinal in range(block.row_count):
            scan.require_open()
            yield RowRecord(
                scan.snapshot_id, batch.batch_id, ordinal, batch.source_description,
                restore_observation(block, ordinal),
            )
