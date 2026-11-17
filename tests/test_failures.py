from pathlib import Path
import sqlite3
import tempfile
import unittest
from monitorlake.schema.errors import CorruptSegment
from monitorlake.service.lake import Lake
from tools.scenarios import sample_rows, SinglePass, SourceFailure, interrupted_rows


class TestFailures(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="monitorlake-failures-")
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "lake"
        self.lake = Lake(self.path)
        self.addCleanup(self.lake.close)
        self.old = self.lake.write("old", "prior", sample_rows())
        self.old_rows = [row.as_dict() for row in self.lake.read_rows(self.old.snapshot_id)]

    def catalog_counts(self):
        connection = self.lake._session.database.connection
        return {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("batches", "snapshots", "members")}

    def test_source_exception(self):
        error = SourceFailure("instrument cable interrupted")
        source = SinglePass(interrupted_rows(sample_rows(), error, after=2))
        before = self.catalog_counts()
        with self.assertRaises(SourceFailure) as raised:
            self.lake.write("broken", "instrument", source)
        self.assertIs(raised.exception, error)
        self.assertEqual(source.iterations, 1)
        self.assertEqual(self.catalog_counts(), before)
        self.assertIsNone(self.lake._session.repository.lookup_batch("broken"))
        self.assertEqual(self.lake.current_snapshot(), self.old.snapshot_id)
        self.assertEqual([row.as_dict() for row in self.lake.read_rows(self.old.snapshot_id)],
                         self.old_rows)
        self.assertEqual(list((self.path / "staging").iterdir()), [])
        with self.assertRaises(SourceFailure) as replay_error:
            self.lake.write("old", "prior", interrupted_rows(sample_rows(), error))
        self.assertIs(replay_error.exception, error)

    def test_publish_failure_retry(self):
        before = self.catalog_counts()
        error = SourceFailure("precommit failure")
        observed = []
        def fail_after_metadata(receipt):
            connection = self.lake._session.database.connection
            self.assertTrue(connection.in_transaction)
            self.assertEqual(self.lake._session.repository.current_snapshot(), receipt.snapshot_id)
            self.assertIsNotNone(self.lake._session.repository.lookup_batch("new"))
            with sqlite3.connect(self.path / "catalog.sqlite3") as reader:
                visible = reader.execute(
                    "SELECT current_snapshot FROM state WHERE singleton=1"
                ).fetchone()[0]
                self.assertEqual(visible, self.old.snapshot_id)
                self.assertEqual(reader.execute(
                    "SELECT COUNT(*) FROM batches WHERE batch_id='new'"
                ).fetchone()[0], 0)
            observed.append(receipt)
            raise error
        self.lake.before_commit = fail_after_metadata
        with self.assertRaises(SourceFailure) as raised:
            self.lake.write("new", "fresh", sample_rows())
        self.assertIs(raised.exception, error)
        self.assertEqual(len(observed), 1)
        self.assertEqual(self.catalog_counts(), before)
        self.assertEqual(self.lake.current_snapshot(), self.old.snapshot_id)
        self.assertIsNone(self.lake._session.repository.lookup_batch("new"))
        self.assertEqual(list((self.path / "staging").iterdir()), [])
        self.assertGreaterEqual(len(list((self.path / "objects").iterdir())), 1)
        self.lake.before_commit = None
        receipt = self.lake.write("new", "fresh", sample_rows())
        self.assertEqual(receipt.row_count, 5)
        self.assertEqual(self.catalog_counts()["batches"], before["batches"] + 1)
        self.lake.close()
        with Lake(self.path) as reopened:
            self.assertEqual(reopened.current_snapshot(), receipt.snapshot_id)
            self.assertEqual(reopened.write("new", "fresh", sample_rows()), receipt)
            self.assertEqual(len(list(reopened.read_rows(receipt.snapshot_id))), 10)
            self.assertEqual([r.as_dict() for r in reopened.read_rows(self.old.snapshot_id)],
                             self.old_rows)

    def test_existing_object_is_not_overwritten(self):
        row = [{"label": "orphan", "samples": [[91]]}]
        def fail(receipt):
            raise SourceFailure("leave sealed orphan")
        self.lake.before_commit = fail
        with self.assertRaises(SourceFailure):
            self.lake.write("orphan", "", row)
        from monitorlake.intake.batch import collect_batch
        from monitorlake.segments.encode import encode_v1
        encoded = encode_v1(collect_batch("orphan", "", row).records)
        object_path = self.lake._session.objects.path_for(encoded.descriptor.digest)
        self.assertTrue(object_path.exists())
        object_path.write_bytes(b"damaged-existing-object")
        damaged = object_path.read_bytes()
        self.lake.before_commit = None
        with self.assertRaises(CorruptSegment):
            self.lake.write("orphan", "", row)
        self.assertEqual(object_path.read_bytes(), damaged)
        self.assertEqual(self.lake.current_snapshot(), self.old.snapshot_id)
        self.assertIsNone(self.lake._session.repository.lookup_batch("orphan"))
