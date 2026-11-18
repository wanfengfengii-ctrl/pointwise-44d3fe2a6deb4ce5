from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class RowSummary:
    snapshot_id: int
    batch_id: str
    ordinal: int
    source_description: str
    label: str
    outer_is_null: bool
    segment_count: int
    null_segment_count: int
    empty_segment_count: int
    non_null_sample_count: int
    sum: int | None

    def as_dict(self):
        return asdict(self)


def summarize_row(row):
    outer = row.samples
    segment_count = 0
    null_segments = 0
    empty_segments = 0
    non_null = 0
    total = 0
    if outer is not None:
        segment_count = len(outer)
        for segment in outer:
            if segment is None:
                null_segments += 1
            elif len(segment) == 0:
                empty_segments += 1
            else:
                for sample in segment:
                    if sample is not None:
                        non_null += 1
                        total += sample
    return RowSummary(
        row.snapshot_id, row.batch_id, row.ordinal, row.source_description,
        row.label, outer is None, segment_count, null_segments, empty_segments,
        non_null, total if non_null else None,
    )


def iter_summaries(scan):
    yield from scan.iter_summaries()
