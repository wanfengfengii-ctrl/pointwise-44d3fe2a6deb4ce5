"""Snapshot consumption across V1 and paged batches, one row at a time.

V1 batches keep their whole-column-block convention.  Paged batches are
decoded as a token stream: at most one page is resident, and ``summaries``
never materialises a row's nested lists at all.
"""
from monitorlake.consumption.rows import RowRecord, restore_observation
from monitorlake.consumption.summary import RowSummary, summarize_row
from monitorlake.schema.errors import CorruptSegment
from monitorlake.schema.types import Observation
from monitorlake.segments.decode import decode_v1
from monitorlake.segments.layout import V1_VERSION
from monitorlake.segments.paged_format import iter_row_events, open_reader


class SnapshotScan:
    def __init__(self, repository, objects, snapshot_id, require_open):
        self.repository = repository
        self.objects = objects
        self.require_open = require_open
        self.require_open()
        self.snapshot = repository.get_snapshot(snapshot_id)
        self.snapshot_id = snapshot_id

    def iter_blocks(self):
        """V1 convenience iterator retained for whole-column-block callers."""
        self.require_open()
        for batch in self.snapshot.batches:
            self.require_open()
            if batch.version != V1_VERSION:
                raise TypeError("iter_blocks is only defined for whole V1 segments")
            data = self.objects.read(batch.sealed)
            block = decode_v1(data, batch.sealed.descriptor)
            yield batch, block

    def _v1_block(self, batch):
        data = self.objects.read(batch.sealed)
        return decode_v1(data, batch.sealed.descriptor)

    def _paged_reader(self, batch):
        return open_reader(
            self.objects.path_for(batch.sealed.digest), batch.sealed.descriptor
        )

    def iter_rows(self):
        self.require_open()
        for batch in self.snapshot.batches:
            self.require_open()
            if batch.version == V1_VERSION:
                block = self._v1_block(batch)
                for ordinal in range(block.row_count):
                    self.require_open()
                    yield RowRecord(
                        self.snapshot_id, batch.batch_id, ordinal,
                        batch.source_description,
                        restore_observation(block, ordinal),
                    )
            else:
                reader = self._paged_reader(batch)
                try:
                    for ordinal in range(reader.row_count):
                        self.require_open()
                        observation = observation_from_events(
                            iter_row_events(reader)
                        )
                        self.require_open()
                        yield RowRecord(
                            self.snapshot_id, batch.batch_id, ordinal,
                            batch.source_description, observation,
                        )
                    reader.finish()
                finally:
                    reader.close()

    def iter_summaries(self):
        self.require_open()
        for batch in self.snapshot.batches:
            self.require_open()
            if batch.version == V1_VERSION:
                block = self._v1_block(batch)
                for ordinal in range(block.row_count):
                    self.require_open()
                    yield summarize_row(RowRecord(
                        self.snapshot_id, batch.batch_id, ordinal,
                        batch.source_description,
                        restore_observation(block, ordinal),
                    ))
            else:
                reader = self._paged_reader(batch)
                try:
                    for ordinal in range(reader.row_count):
                        self.require_open()
                        summary = summary_from_events(
                            iter_row_events(reader), self.snapshot_id,
                            batch.batch_id, ordinal, batch.source_description,
                        )
                        self.require_open()
                        yield summary
                    reader.finish()
                finally:
                    reader.close()


def _first_label(events):
    kind, label = next(events)
    if kind != "label":
        raise CorruptSegment("paged row does not start with a label")
    return label


def _begin_outer(events):
    """Consume the tag after the label; return True when samples is None."""
    kind = next(events)[0]
    if kind == "outer_null":
        return True
    if kind != "outer_begin":
        raise CorruptSegment("invalid outer samples event")
    return False


def observation_from_events(events):
    """Build one Observation; this row's full result is allowed by contract."""
    label = _first_label(events)
    if _begin_outer(events):
        return Observation(label, None)
    outer = []
    current = None
    for event in events:
        name = event[0]
        if name == "segment_null":
            outer.append(None)
        elif name == "segment_begin":
            current = []
            outer.append(current)
        elif name == "segment_end":
            current = None
        elif name == "value":
            current.append(event[1])
        elif name == "outer_end":
            return Observation(label, outer)
    raise CorruptSegment("paged row ended before its outer close")


def summary_from_events(events, snapshot_id, batch_id, ordinal, source_description):
    """Fold one row into counts and an exact integer sum, without materialising."""
    label = _first_label(events)
    if _begin_outer(events):
        return RowSummary(
            snapshot_id, batch_id, ordinal, source_description, label,
            True, 0, 0, 0, 0, None,
        )
    segments = null_segments = empty_segments = 0
    non_null = 0
    total = 0
    length = 0
    for event in events:
        name = event[0]
        if name == "segment_null":
            segments += 1
            null_segments += 1
        elif name == "segment_begin":
            segments += 1
            length = 0
        elif name == "segment_end":
            if length == 0:
                empty_segments += 1
        elif name == "value":
            length += 1
            if event[1] is not None:
                non_null += 1
                total += event[1]
        elif name == "outer_end":
            return RowSummary(
                snapshot_id, batch_id, ordinal, source_description, label,
                False, segments, null_segments, empty_segments, non_null,
                total if non_null else None,
            )
    raise CorruptSegment("paged row ended before its outer close")
