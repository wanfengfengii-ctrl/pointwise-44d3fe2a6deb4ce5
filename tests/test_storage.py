import json
from pathlib import Path
import struct
import tempfile
import unittest
from monitorlake.catalog.database import EMPTY_SNAPSHOT_ID
from monitorlake.schema.errors import CorruptSegment, StorageError
from monitorlake.segments.layout import V1_MAGIC
from monitorlake.service.lake import Lake


class TestStorage(unittest.TestCase):
    def make_lake(self, directory):
        lake = Lake(Path(directory) / "lake")
        self.addCleanup(lake.close)
        healthy = lake.write("healthy", "prior", [{"label": "ok", "samples": [[7]]}])
        suspect = lake.write("suspect", "later", [
            {"label": "a", "samples": [[], [1, 2], None, [None], []]},
            {"label": "b", "samples": [[3], []]},
        ])
        return lake, healthy, suspect

    def assert_bad_but_old_healthy(self, lake, healthy, suspect):
        for operation in (lake.read_rows, lake.summaries):
            with self.assertRaises(CorruptSegment):
                list(operation(suspect.snapshot_id))
        self.assertEqual([r.samples for r in lake.read_rows(healthy.snapshot_id)], [[[7]]])
        self.assertEqual([s.sum for s in lake.summaries(healthy.snapshot_id)], [7])
        self.assertEqual(list(lake.read_rows(EMPTY_SNAPSHOT_ID)), [])

    def test_corrupt_required_segment(self):
        for damage in ("missing", "body"):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory(
                prefix="monitorlake-corruption-"
            ) as directory:
                lake, healthy, suspect = self.make_lake(directory)
                batch = lake._session.repository.lookup_batch("suspect")
                path = lake._session.objects.path_for(batch.sealed.digest)
                if damage == "missing":
                    path.unlink()
                else:
                    raw = bytearray(path.read_bytes())
                    raw[-2] ^= 1
                    path.write_bytes(raw)
                self.assert_bad_but_old_healthy(lake, healthy, suspect)
                lake.close()

    def rewrite_valid_checksums(self, lake, mutate_header=None, mutate_columns=None):
        import hashlib
        def digest(data):
            return hashlib.sha256(data).hexdigest()
        batch = lake._session.repository.lookup_batch("suspect")
        raw = lake._session.objects.path_for(batch.sealed.digest).read_bytes()
        prefix = len(V1_MAGIC) + 4
        header_size = struct.unpack(">I", raw[len(V1_MAGIC):prefix])[0]
        header = json.loads(raw[prefix:prefix + header_size])
        payload = raw[prefix + header_size:]
        if mutate_columns is not None:
            columns = {
                entry["name"]: json.loads(payload[entry["offset"]:
                    entry["offset"] + entry["length"]])
                for entry in header["columns"]
            }
            mutate_columns(columns)
            chunks, offset = [], 0
            for entry in header["columns"]:
                chunk = json.dumps(columns[entry["name"]], ensure_ascii=True,
                                   separators=(",", ":")).encode("ascii")
                entry.update(offset=offset, length=len(chunk),
                             count=len(columns[entry["name"]]), sha256=digest(chunk))
                offset += len(chunk)
                chunks.append(chunk)
            payload = b"".join(chunks)
        if mutate_header is not None:
            mutate_header(header)
        encoded_header = json.dumps(header, sort_keys=True,
                                    separators=(",", ":")).encode("ascii")
        rewritten = V1_MAGIC + struct.pack(">I", len(encoded_header)) + encoded_header + payload
        new_digest = digest(rewritten)
        lake._session.objects.path_for(new_digest).write_bytes(rewritten)
        lake._session.database.connection.execute(
            "UPDATE batches SET object_digest=?,byte_count=? WHERE batch_id='suspect'",
            (new_digest, len(rewritten)),
        )

    def test_directory_and_offset_integrity(self):
        def out_of_bounds(header):
            header["columns"][0]["offset"] = 10 ** 9
        def overlap(header):
            header["columns"][1]["offset"] = 0
        def descending(columns):
            columns["segment_value_offsets"][2:4] = [2, 1]
        def wrong_endpoint(columns):
            columns["row_segment_offsets"][-1] -= 1
        def null_with_children(columns):
            columns["outer_validity"][0] = False
        def invalid_scalar(columns):
            columns["sample_values"][0] = True
        def null_segment_with_children(columns):
            columns["segment_validity"][1] = False
        cases = [
            ("bounds", out_of_bounds, None),
            ("overlap", overlap, None),
            ("descending", None, descending),
            ("endpoint", None, wrong_endpoint),
            ("outer-null-children", None, null_with_children),
            ("segment-null-children", None, null_segment_with_children),
            ("scalar", None, invalid_scalar),
        ]
        for name, header_change, column_change in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory(
                prefix="monitorlake-structure-"
            ) as directory:
                lake, healthy, suspect = self.make_lake(directory)
                self.rewrite_valid_checksums(lake, header_change, column_change)
                self.assert_bad_but_old_healthy(lake, healthy, suspect)
                lake.close()

    def test_column_checksum_is_checked_independently(self):
        def wrong_column_checksum(header):
            header["columns"][-1]["sha256"] = "0" * 64
        with tempfile.TemporaryDirectory(prefix="monitorlake-column-check-") as directory:
            lake, healthy, suspect = self.make_lake(directory)
            self.rewrite_valid_checksums(lake, mutate_header=wrong_column_checksum)
            self.assert_bad_but_old_healthy(lake, healthy, suspect)
            lake.close()

    def test_corrupt_metadata_is_not_reset(self):
        with tempfile.TemporaryDirectory(prefix="monitorlake-metadata-") as directory:
            lake, healthy, suspect = self.make_lake(directory)
            path = lake._session.root
            lake._session.database.connection.execute(
                "UPDATE state SET current_snapshot=?", (healthy.snapshot_id,)
            )
            lake.close()
            with self.assertRaises(StorageError):
                Lake(path)
            import sqlite3
            with sqlite3.connect(path / "catalog.sqlite3") as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM batches").fetchone()[0], 2)
        with tempfile.TemporaryDirectory(prefix="monitorlake-empty-catalog-") as directory:
            path = Path(directory) / "lake"
            path.mkdir()
            (path / "catalog.sqlite3").write_bytes(b"")
            with self.assertRaises(StorageError):
                Lake(path)
            self.assertEqual((path / "catalog.sqlite3").read_bytes(), b"")
