from pathlib import Path
import tempfile
import unittest
from monitorlake.service.lake import Lake
from tools.scenarios import sample_rows, long_rows, reference_summary, emit_scenario


class TestSummary(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="monitorlake-summary-")
        self.addCleanup(directory.cleanup)
        self.lake = Lake(Path(directory.name) / "lake")
        self.addCleanup(self.lake.close)

    def test_exact_counts_and_sum(self):
        rows = sample_rows()
        receipt = self.lake.write("day", "meter", rows)
        expected = [reference_summary(row, receipt.snapshot_id, "day", i, "meter")
                    for i, row in enumerate(rows)]
        actual = [s.as_dict() for s in self.lake.summaries(receipt.snapshot_id)]
        self.assertEqual(actual, expected)
        self.assertEqual(
            [actual[2][field] for field in (
                "segment_count", "null_segment_count", "empty_segment_count",
                "non_null_sample_count", "sum")],
            [3, 1, 1, 2, 0],
        )
        self.assertEqual(actual[3]["sum"], 18446744073709551614)
        self.assertIsNone(actual[0]["sum"])
        self.assertIsNone(actual[1]["sum"])
        self.assertIsNone(actual[4]["sum"])
        downstream = [r.observation.as_dict()
                      for r in self.lake.read_rows(receipt.snapshot_id)]
        self.assertEqual(downstream, rows)
        emit_scenario(
            "v3.exact_integer", rows, expected, actual, rows, downstream,
            ["monitorlake/schema/validation.py", "monitorlake/segments/encode.py",
             "monitorlake/segments/decode.py", "monitorlake/consumption/summary.py"],
            "Independent flattening comprehension and Python built-in sum",
        )

    def test_signed_extremes_nulls_and_zero(self):
        rows = [
            {"label": "min", "samples": [[-(1 << 63), -(1 << 63), None]]},
            {"label": "zero", "samples": [[None, 2, -2], []]},
            {"label": "missing", "samples": [None, [], [None]]},
            {"label": "present zero", "samples": [[0]]},
        ]
        receipt = self.lake.write("extremes", "sensor", rows)
        actual = [s.as_dict() for s in self.lake.summaries(receipt.snapshot_id)]
        self.assertEqual(actual, [
            reference_summary(row, receipt.snapshot_id, "extremes", i, "sensor")
            for i, row in enumerate(rows)
        ])
        self.assertEqual([s["sum"] for s in actual], [-(1 << 64), 0, None, 0])

    def test_deterministic_long_row(self):
        rows = list(long_rows(50000))
        receipt = self.lake.write("long", "", rows)
        actual = list(self.lake.summaries(receipt.snapshot_id))
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual[0].as_dict(),
                         reference_summary(rows[0], receipt.snapshot_id, "long", 0, ""))
