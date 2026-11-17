import io
import json
from pathlib import Path
import tempfile
import unittest
from monitorlake.service.lake import Lake
from tools.scenarios import sample_rows, reference_summary


class PortFailure(Exception):
    pass


class LimitedSink:
    def __init__(self, error, successful_lines):
        self.error = error
        self.successful_lines = successful_lines
        self.lines = []

    def write(self, text):
        if len(self.lines) == self.successful_lines:
            raise self.error
        self.lines.append(text)
        return len(text)


class TestExport(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="monitorlake-export-")
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "lake"
        self.lake = Lake(self.path)
        self.addCleanup(self.lake.close)

    def exported(self, lake, snapshot):
        sink = io.StringIO()
        count = lake.export(snapshot, sink)
        records = [json.loads(line) for line in sink.getvalue().splitlines()]
        self.assertEqual(count, len(records))
        return records

    def test_frozen_snapshot_after_reopen(self):
        rows = sample_rows()
        first = self.lake.write("first", "instrument-A", rows)
        before_rows = [r.as_dict() for r in self.lake.read_rows(first.snapshot_id)]
        before_summaries = [s.as_dict() for s in self.lake.summaries(first.snapshot_id)]
        before = self.exported(self.lake, first.snapshot_id)
        self.assertEqual([item["record"] for item in before], rows)
        self.assertEqual([item["ordinal"] for item in before], list(range(len(rows))))
        self.assertTrue(all(item["batch_id"] == "first" for item in before))
        for i, item in enumerate(before):
            self.assertEqual(item["source_description"], "instrument-A")
            self.assertEqual(item["snapshot_id"], first.snapshot_id)
            self.assertEqual(item["summary"],
                             reference_summary(rows[i], first.snapshot_id, "first", i,
                                               "instrument-A"))
        later = self.lake.write("second", "instrument-B",
                                [{"label": "A", "samples": [[999]]}])
        self.assertEqual(self.exported(self.lake, first.snapshot_id), before)
        self.lake.close()
        with Lake(self.path) as reopened:
            self.assertEqual(reopened.current_snapshot(), later.snapshot_id)
            self.assertEqual(self.exported(reopened, first.snapshot_id), before)
            self.assertEqual([r.as_dict() for r in reopened.read_rows(first.snapshot_id)],
                             before_rows)
            self.assertEqual([s.as_dict() for s in reopened.summaries(first.snapshot_id)],
                             before_summaries)
            current = self.exported(reopened, later.snapshot_id)
            self.assertEqual(len(current), 6)
            self.assertEqual(current[-1]["record"]["samples"], [[999]])
            self.assertEqual(current[-1]["batch_id"], "second")
            self.assertEqual(current[-1]["ordinal"], 0)

    def test_sink_failure(self):
        receipt = self.lake.write("day", "port-test", sample_rows())
        before = self.exported(self.lake, receipt.snapshot_id)
        error = PortFailure("downstream offline")
        port = LimitedSink(error, 2)
        with self.assertRaises(PortFailure) as raised:
            self.lake.export(receipt.snapshot_id, port)
        self.assertIs(raised.exception, error)
        self.assertEqual(len(port.lines), 2)
        self.assertEqual([json.loads(text) for text in port.lines], before[:2])
        self.assertEqual(self.lake.current_snapshot(), receipt.snapshot_id)
        self.assertEqual(self.exported(self.lake, receipt.snapshot_id), before)

    def test_reinitialize_and_closed_research_entries(self):
        first = self.lake.write("user", "unchanged", sample_rows())
        self.lake.close()
        with Lake(self.path) as reopened:
            self.assertEqual(reopened.current_snapshot(), first.snapshot_id)
            second = reopened.write("user-next", "", [])
            self.assertEqual(reopened.write("user", "unchanged", sample_rows()), first)
            self.assertEqual(reopened.current_snapshot(), second.snapshot_id)
        with Lake(self.path) as again:
            self.assertEqual(again.current_snapshot(), second.snapshot_id)
            self.assertEqual(len(list(again.read_rows(first.snapshot_id))), 5)
        for operation in (
            lambda: self.lake.read_rows(first.snapshot_id),
            lambda: self.lake.summaries(first.snapshot_id),
            lambda: self.lake.export(first.snapshot_id, io.StringIO()),
        ):
            with self.assertRaises(RuntimeError):
                operation()

    def test_short_sink_write_is_not_complete_success(self):
        receipt = self.lake.write("day", "", sample_rows())
        class ShortSink:
            def __init__(self):
                self.prefix = ""
            def write(self, text):
                self.prefix += text[:1]
                return 1
        sink = ShortSink()
        with self.assertRaises(OSError):
            self.lake.export(receipt.snapshot_id, sink)
        self.assertEqual(sink.prefix, "{")
        self.assertEqual(self.lake.current_snapshot(), receipt.snapshot_id)
