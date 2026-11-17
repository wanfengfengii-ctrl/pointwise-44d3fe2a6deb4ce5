from pathlib import Path
import tempfile
import unittest
from monitorlake.service.lake import Lake
from tools.scenarios import sample_rows, long_rows


class TestRows(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="monitorlake-rows-")
        self.addCleanup(directory.cleanup)
        self.lake = Lake(Path(directory.name) / "lake")
        self.addCleanup(self.lake.close)

    def test_nested_null_and_empty(self):
        rows = sample_rows() + [
            {"label": "", "samples": [None, [], [None], [], None]},
            {"label": "A", "samples": [[], [], [0], [], None]},
        ]
        result = self.lake.write("day", "instrument", rows)
        actual = list(self.lake.read_rows(result.snapshot_id))
        self.assertEqual([r.observation.as_dict() for r in actual], rows)
        self.assertEqual([r.ordinal for r in actual], list(range(len(rows))))
        self.assertEqual(actual[0].label, actual[1].label)
        self.assertIsNone(actual[0].samples)
        self.assertEqual(actual[1].samples, [])
        self.assertTrue(all(r.snapshot_id == result.snapshot_id for r in actual))

    def test_offset_boundaries(self):
        rows = [
            {"label": "A", "samples": [[], [1, 2], None, [], [3], [None]]},
            {"label": "A", "samples": [[4], [], None, []]},
            {"label": "B", "samples": None},
            {"label": "B", "samples": []},
            {"label": "C", "samples": [[], None, [], [5, None, 6], []]},
        ]
        result = self.lake.write("offsets", "", rows)
        self.assertEqual([r.observation.as_dict() for r in self.lake.read_rows(result.snapshot_id)],
                         rows)

    def test_snapshot_is_bound_when_iterator_is_created(self):
        first = self.lake.write("first", "", sample_rows())
        iterator = self.lake.read_rows(first.snapshot_id)
        self.lake.write("second", "", [{"label": "A", "samples": [[999]]}])
        self.assertEqual([r.observation.as_dict() for r in iterator], sample_rows())
        self.assertEqual(len(list(self.lake.read_rows(self.lake.current_snapshot()))), 6)

    def test_unknown_snapshot(self):
        for bad in [99999, -1, True, None, "0", 1 << 80]:
            for operation in (self.lake.read_rows, self.lake.summaries):
                with self.subTest(snapshot=bad, operation=operation.__name__):
                    with self.assertRaises(ValueError):
                        list(operation(bad))

    def test_long_v1_row_roundtrip(self):
        rows = list(long_rows(30000))
        result = self.lake.write("long", "", iter(rows))
        self.assertEqual([r.observation.as_dict() for r in self.lake.read_rows(result.snapshot_id)],
                         rows)

    def test_iterator_cannot_continue_after_close(self):
        receipt = self.lake.write("day", "", sample_rows())
        iterator = self.lake.read_rows(receipt.snapshot_id)
        next(iterator)
        self.lake.close()
        with self.assertRaises(RuntimeError):
            next(iterator)
