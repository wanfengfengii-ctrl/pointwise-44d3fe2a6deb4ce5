import copy
from pathlib import Path
import tempfile
import unittest
from monitorlake.catalog.database import EMPTY_SNAPSHOT_ID
from monitorlake.schema.normalization import normalize_observation
from monitorlake.service.lake import Lake
from tools.scenarios import sample_rows, SinglePass


class TestIngest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="monitorlake-ingest-")
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "lake"
        self.lake = Lake(self.path)
        self.addCleanup(self.lake.close)

    def test_publish_and_replay(self):
        rows = [{"label": "x", "samples": [[1, None]]},
                {"label": "x", "samples": [[2]]}]
        source = SinglePass(rows)
        first = self.lake.write("day", "instrument", source)
        self.assertEqual(source.iterations, 1)
        self.assertEqual(first.row_count, 2)
        self.assertEqual([r.ordinal for r in self.lake.read_rows(first.snapshot_id)], [0, 1])
        self.assertEqual(self.lake.write("day", "instrument", copy.deepcopy(rows)), first)
        later = self.lake.write("later", "", [])
        self.assertEqual(self.lake.write("day", "instrument", rows), first)
        self.assertEqual(self.lake.current_snapshot(), later.snapshot_id)
        self.assertEqual(self.lake._session.database.connection.execute(
            "SELECT COUNT(*) FROM snapshots").fetchone()[0], 3)

    def test_empty_batch_and_single_pass(self):
        self.assertEqual(self.lake.current_snapshot(), EMPTY_SNAPSHOT_ID)
        self.assertEqual(list(self.lake.read_rows(EMPTY_SNAPSHOT_ID)), [])
        source = SinglePass([])
        receipt = self.lake.write("empty", "", source)
        self.assertEqual(source.iterations, 1)
        self.assertEqual(receipt.row_count, 0)
        self.assertNotEqual(receipt.snapshot_id, EMPTY_SNAPSHOT_ID)
        self.assertEqual(list(self.lake.read_rows(receipt.snapshot_id)), [])
        self.assertEqual(list(self.lake.summaries(receipt.snapshot_id)), [])
        self.assertEqual(self.lake.write("empty", "", []), receipt)

    def test_ordinal_order_and_identity(self):
        rows = [{"label": label, "samples": [[i]]}
                for i, label in enumerate(["A", "A", "", "B", "A"])]
        receipt = self.lake.write(" spaced ID ", " source ", SinglePass(rows))
        restored = list(self.lake.read_rows(receipt.snapshot_id))
        self.assertEqual([r.ordinal for r in restored], list(range(5)))
        self.assertEqual([r.observation.as_dict() for r in restored], rows)
        self.assertTrue(all(r.batch_id == " spaced ID " for r in restored))
        separate = self.lake.write("spaced ID", " source ", rows)
        self.assertNotEqual(separate.snapshot_id, receipt.snapshot_id)

    def test_replay_identity(self):
        rows = sample_rows()
        first = self.lake.write("day", "original", rows)
        current = self.lake.write("next", "next", []).snapshot_id
        self.assertEqual(self.lake.write("day", "original", copy.deepcopy(rows)), first)
        variants = []
        swapped = copy.deepcopy(rows)
        swapped[0], swapped[1] = swapped[1], swapped[0]
        variants.append(("original", swapped))
        changed = copy.deepcopy(rows)
        changed[0]["samples"] = []
        variants.append(("original", changed))
        variants.append(("different source", rows))
        variants.append(("original", list(reversed(rows))))
        for source, values in variants:
            with self.subTest(source=source, values=values):
                with self.assertRaises(ValueError):
                    self.lake.write("day", source, values)
                self.assertEqual(self.lake.current_snapshot(), current)
        malformed = copy.deepcopy(rows) + [{"label": "bad", "samples": [[True]]}]
        with self.assertRaises(ValueError):
            self.lake.write("day", "original", malformed)
        self.assertEqual(self.lake.current_snapshot(), current)

    def test_late_invalid_record(self):
        old = self.lake.write("old", "good", sample_rows()).snapshot_id
        invalid_values = [True, False, 1 << 63, -(1 << 63) - 1, 1.5]
        invalid_rows = [{"label": "bad", "samples": [[value]]} for value in invalid_values]
        invalid_rows.extend([
            {"samples": []}, {"label": "bad"}, {"label": 7, "samples": []},
            {"label": "bad", "samples": ()},
            {"label": "bad", "samples": [()]},
            {"label": "bad", "samples": [[[1]]]},
            {"label": "bad", "samples": [1]},
        ])
        for i, invalid in enumerate(invalid_rows):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    self.lake.write(f"bad-{i}", "invalid", iter(sample_rows() + [invalid]))
                self.assertEqual(self.lake.current_snapshot(), old)
                self.assertIsNone(self.lake._session.repository.lookup_batch(f"bad-{i}"))
        endpoints = [{"label": "", "samples": [[-(1 << 63), (1 << 63) - 1]]}]
        good = self.lake.write("endpoints", "", endpoints)
        self.assertEqual(list(self.lake.read_rows(good.snapshot_id))[-1].samples,
                         endpoints[0]["samples"])

    def test_normalization_owns_values(self):
        raw = {"label": "original", "samples": [[1], [], None]}
        owned = normalize_observation(raw)
        raw["samples"][0].append(2)
        self.assertEqual(owned.samples, [[1], [], None])
        first = {"label": "first", "samples": [[10]]}
        def source():
            yield first
            first["samples"][0].append(20)
            yield {"label": "last", "samples": []}
        receipt = self.lake.write("ownership", "", source())
        rows = list(self.lake.read_rows(receipt.snapshot_id))
        self.assertEqual(rows[0].samples, [[10]])
        rows[0].samples[0].append(30)
        self.assertEqual(list(self.lake.read_rows(receipt.snapshot_id))[0].samples, [[10]])

    def test_closed_handle(self):
        receipt = self.lake.write("day", "", sample_rows())
        self.lake.close()
        self.lake.close()
        with self.assertRaises(RuntimeError):
            self.lake.write("after", "", [])
        with self.assertRaises(RuntimeError):
            self.lake.current_snapshot()
        with Lake(self.path) as reopened:
            self.assertEqual(reopened.current_snapshot(), receipt.snapshot_id)
            self.assertEqual(len(list(reopened.read_rows(receipt.snapshot_id))), 5)

    def test_request_errors_do_not_consume_source(self):
        for batch_id, source in [("", ""), (None, ""), ("day", None)]:
            rows = SinglePass(sample_rows())
            with self.assertRaises(ValueError):
                self.lake.write(batch_id, source, rows)
            self.assertEqual(rows.iterations, 0)
        self.assertEqual(self.lake.current_snapshot(), EMPTY_SNAPSHOT_ID)
