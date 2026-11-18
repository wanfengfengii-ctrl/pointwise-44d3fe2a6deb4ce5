"""Paged segment regression: boundaries, identity, failures and corruption."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from monitorlake.catalog.database import EMPTY_SNAPSHOT_ID
from monitorlake.schema.errors import CorruptSegment
from monitorlake.service.lake import Lake
from monitorlake.segments.paged_format import (
    DIGEST_BYTES, FRAME_LENGTH, PAGED_MAGIC,
    iter_page_entries, read_layout, verify_page_lengths,
)
from tools.scenarios import (
    SinglePass, SourceFailure, reference_summary, sample_rows,
)


def nested_rows():
    return [
        {"label": "A", "samples": None},
        {"label": "A", "samples": []},
        {"label": "B", "samples": [None, [], [None, 2, -2]]},
        {"label": "水" * 341, "samples": [[-(1 << 63), (1 << 63) - 1]]},
        {"label": "", "samples": [[], [0], None, [None]]},
        {"label": "dup", "samples": [[i % 7 - 3 for i in range(200)]]},
    ]


def rewrite_paged_object(lake, batch_id, mutate_body=None, mutate_header=None):
    """Rewrite a paged object with fresh page checksums and whole digest.

    Page contents and their lengths are preserved, so deliberately damaged
    structure is not silently re-chunked; only the page checksums, the whole
    object digest and the catalog reference are made consistent again.
    """
    entry = lake._session.repository.lookup_batch(batch_id)
    path = lake._session.objects.path_for(entry.sealed.digest)
    raw = path.read_bytes()
    first = len(PAGED_MAGIC) + FRAME_LENGTH.size
    header_length = FRAME_LENGTH.unpack(raw[len(PAGED_MAGIC):first])[0]
    header = json.loads(raw[first:first + header_length])
    offset = first + header_length
    contents = []
    for _ in range(header["page_count"]):
        length = FRAME_LENGTH.unpack(raw[offset:offset + FRAME_LENGTH.size])[0]
        start = offset + FRAME_LENGTH.size + DIGEST_BYTES
        contents.append(bytearray(raw[start:start + length]))
        offset = start + length
    if mutate_body is not None:
        mutate_body(contents)
    header["body_length"] = sum(len(content) for content in contents)
    header["page_count"] = len(contents)
    if mutate_header is not None:
        mutate_header(header)
    encoded = json.dumps(header, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=True).encode("ascii")
    rebuilt = bytearray(PAGED_MAGIC + FRAME_LENGTH.pack(len(encoded)) + encoded)
    for content in contents:
        rebuilt += FRAME_LENGTH.pack(len(content))
        rebuilt += hashlib.sha256(bytes(content)).digest()
        rebuilt += bytes(content)
    digest = hashlib.sha256(rebuilt).hexdigest()
    new_path = lake._session.objects.path_for(digest)
    new_path.write_bytes(bytes(rebuilt))
    marker = json.dumps({"object": digest}, sort_keys=True, separators=(",", ":"))
    lake._session.database.connection.execute(
        "UPDATE batches SET object_digest=?, byte_count=?, identity=? WHERE batch_id=?",
        (digest, len(rebuilt), marker, batch_id),
    )
    return new_path



class PagedCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="monitorlake-paged-")
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "lake"
        self.lake = Lake(self.path)
        self.addCleanup(self.lake.close)

    def snapshot_rows(self, snapshot):
        return [r.as_dict() for r in self.lake.read_rows(snapshot)]

    def snapshot_summaries(self, snapshot):
        return [s.as_dict() for s in self.lake.summaries(snapshot)]


class TestPagedRoundtrip(PagedCase):
    def test_records_and_segments_cross_pages(self):
        rows = nested_rows()
        for budget in (4096, 65536):
            with self.subTest(page_bytes=budget):
                batch_id = f"b-{budget}"
                receipt = self.lake.write_paged(
                    batch_id, "instrument", SinglePass(rows), budget
                )
                observed = [r for r in self.snapshot_rows(receipt.snapshot_id)
                            if r["batch_id"] == batch_id]
                self.assertEqual(observed, [
                    {"snapshot_id": receipt.snapshot_id, "batch_id": batch_id,
                     "ordinal": i, "source_description": "instrument", **row}
                    for i, row in enumerate(rows)
                ])
                self.assertEqual(
                    [s for s in self.snapshot_summaries(receipt.snapshot_id)
                     if s["batch_id"] == batch_id],
                    [reference_summary(row, receipt.snapshot_id, batch_id, i,
                                       "instrument")
                     for i, row in enumerate(rows)],
                )

    def test_long_record_and_many_boundaries_share_a_page_stream(self):
        for budget in (4096, 65536):
            long_row = {"label": "x" * 1024,
                        "samples": [[None if i % 11 == 0 else i for i in range(30000)]]}
            rows = [{"label": "head", "samples": []}, long_row,
                    {"label": "tail", "samples": [None] * 5000}]
            batch_id = f"long-{budget}"
            receipt = self.lake.write_paged(batch_id, "", iter(rows), budget)
            observed = [r for r in self.snapshot_rows(receipt.snapshot_id)
                        if r["batch_id"] == batch_id]
            self.assertEqual(observed[1]["samples"], long_row["samples"])
            self.assertEqual(
                [s["sum"] for s in self.snapshot_summaries(receipt.snapshot_id)
                 if s["batch_id"] == batch_id and s["label"] == "x" * 1024],
                [reference_summary(long_row, 0, "", 0, "")["sum"]],
            )
            entry = self.lake._session.repository.lookup_batch(batch_id)
            path = self.lake._session.objects.path_for(entry.sealed.digest)
            lengths = verify_page_lengths(path, budget)
            self.assertTrue(all(0 < length <= budget for length in lengths))
            self.assertEqual(sum(lengths), read_layout(path).body_length)

    def test_empty_batch_and_mixed_v1_snapshot(self):
        old = self.lake.write("v1", "source-A", sample_rows())
        old_rows = self.snapshot_rows(old.snapshot_id)
        mixed = self.lake.write_paged("paged", "source-B", nested_rows(), 4096)
        empty = self.lake.write_paged("empty", "", iter([]), 4096)
        self.assertEqual(empty.row_count, 0)
        self.assertEqual([r for r in self.snapshot_rows(mixed.snapshot_id)
                          if r["batch_id"] == "paged"], [
                             {"snapshot_id": mixed.snapshot_id, "batch_id": "paged",
                              "ordinal": i, "source_description": "source-B", **row}
                             for i, row in enumerate(nested_rows())
                         ])
        self.assertEqual(self.snapshot_rows(old.snapshot_id), old_rows)
        self.assertEqual(
            [s for s in self.snapshot_summaries(mixed.snapshot_id)
             if s["batch_id"] == "v1"],
            [reference_summary(row, mixed.snapshot_id, "v1", i, "source-A")
             for i, row in enumerate(sample_rows())],
        )
        self.assertEqual(
            [s["sum"] for s in self.snapshot_summaries(old.snapshot_id)],
            [reference_summary(row, old.snapshot_id, "v1", i, "source-A")["sum"]
             for i, row in enumerate(sample_rows())],
        )
        sink = io.StringIO()
        self.assertEqual(self.lake.export(old.snapshot_id, sink), len(old_rows))
        self.assertEqual(json.loads(sink.getvalue().splitlines()[0])["summary"],
                         reference_summary(sample_rows()[0], old.snapshot_id, "v1", 0,
                                           "source-A"))

    def test_snapshot_binding_and_closed_iterators(self):
        receipt = self.lake.write_paged("p", "", nested_rows(), 4096)
        iterator = self.lake.read_rows(receipt.snapshot_id)
        self.lake.write("later", "", [{"label": "z", "samples": []}])
        self.assertEqual(len(list(iterator)), len(nested_rows()))
        iterator = self.lake.read_rows(receipt.snapshot_id)
        next(iterator)
        self.lake.close()
        with self.assertRaises(RuntimeError):
            next(iterator)


class TestPagedIdentity(PagedCase):
    def test_cross_entry_and_budget_replay(self):
        rows = nested_rows()
        paged = self.lake.write_paged("day", "instrument", SinglePass(rows), 4096)
        expected = [
            {"snapshot_id": paged.snapshot_id, "batch_id": "day", "ordinal": i,
             "source_description": "instrument", **row}
            for i, row in enumerate(rows)
        ]
        self.assertEqual(self.snapshot_rows(paged.snapshot_id), expected)
        self.assertEqual(
            self.lake.write("day", "instrument", SinglePass(rows)), paged
        )
        self.assertEqual(
            self.lake.write_paged("day", "instrument", SinglePass(rows), 65536), paged
        )
        later = self.lake.write("later", "", [])
        self.assertEqual(
            self.lake.write_paged("day", "instrument", SinglePass(rows), 4096), paged
        )
        self.assertEqual(self.lake.current_snapshot(), later.snapshot_id)
        self.lake.close()
        with Lake(self.path) as reopened:
            self.assertEqual(
                reopened.write("day", "instrument", SinglePass(rows)), paged
            )
            self.assertEqual(
                reopened.write_paged("day", "instrument", SinglePass(rows), 8192), paged
            )
            self.assertEqual(reopened.current_snapshot(), later.snapshot_id)
            self.assertEqual([r.as_dict() for r in reopened.read_rows(paged.snapshot_id)],
                             expected)

    def test_different_identity_is_rejected(self):
        rows = nested_rows()
        first = self.lake.write_paged("day", "instrument", rows, 4096)
        variants = [
            ("instrument", rows[:-1] + [{"label": "x", "samples": []}]),
            ("instrument", list(reversed(rows))),
            ("other", rows),
        ]
        for source, values in variants:
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    self.lake.write_paged("day", source, iter(values), 4096)
                with self.assertRaises(ValueError):
                    self.lake.write("day", source, iter(values))
                self.assertEqual(self.lake.current_snapshot(), first.snapshot_id)

    def test_label_limits_only_apply_to_paged(self):
        accepted = self.lake.write_paged("ok", "", [
            {"label": "水" * 341, "samples": []},
            {"label": "a" * 1024, "samples": []},
        ], 4096)
        self.assertEqual(accepted.row_count, 2)
        before = self.lake.current_snapshot()
        for bad in ("a" * 1025, "\ud800"):
            with self.subTest(label=bad[:4]):
                with self.assertRaises(ValueError):
                    self.lake.write_paged("bad", "", iter([{"label": bad, "samples": []}]),
                                          4096)
                self.assertEqual(self.lake.current_snapshot(), before)
        long_v1 = self.lake.write("v1-long", "", [{"label": "a" * 4096, "samples": []}])
        self.assertEqual(long_v1.row_count, 1)


class TestPagedFailures(PagedCase):
    def test_invalid_budget_does_not_consume_source(self):
        rows = sample_rows()
        for bad in (0, 4095, 65537, True, 4096.0, "4096", None):
            with self.subTest(budget=bad):
                source = SinglePass(rows)
                with self.assertRaises(ValueError):
                    self.lake.write_paged("bad", "", source, bad)
                self.assertEqual(source.iterations, 0)
                self.assertEqual(self.lake.current_snapshot(), EMPTY_SNAPSHOT_ID)

    def test_source_and_tail_failures_publish_nothing(self):
        old = self.lake.write("old", "prior", sample_rows())
        old_rows = self.snapshot_rows(old.snapshot_id)
        rows = nested_rows()
        error = SourceFailure("cable cut")
        def broken():
            yield rows[0]
            raise error
        with self.assertRaises(SourceFailure) as raised:
            self.lake.write_paged("broken", "", broken(), 4096)
        self.assertIs(raised.exception, error)
        invalid = [
            {"label": "bad", "samples": [[True]]},
            {"label": "bad", "samples": [[1 << 63]]},
            {"label": "bad", "samples": [()]},
            {"label": "bad", "samples": [[[1]]]},
            {"label": "a" * 1025, "samples": []},
        ]
        for raw in invalid:
            with self.subTest(raw=repr(raw)[:30]):
                with self.assertRaises(ValueError):
                    self.lake.write_paged("bad", "", iter(rows + [raw]), 4096)
                self.assertEqual(self.lake.current_snapshot(), old.snapshot_id)
                self.assertIsNone(self.lake._session.repository.lookup_batch("bad"))
        self.assertEqual(self.snapshot_rows(old.snapshot_id), old_rows)
        self.assertEqual(list((self.path / "staging").iterdir()), [])

    def test_before_commit_rollback_and_retry(self):
        old = self.lake.write("old", "", sample_rows())
        rows = nested_rows()
        error = SourceFailure("precommit")
        def fail(receipt):
            self.assertTrue(self.lake._session.database.connection.in_transaction)
            raise error
        self.lake.before_commit = fail
        with self.assertRaises(SourceFailure) as raised:
            self.lake.write_paged("retry", "", SinglePass(rows), 4096)
        self.assertIs(raised.exception, error)
        self.assertEqual(self.lake.current_snapshot(), old.snapshot_id)
        self.assertIsNone(self.lake._session.repository.lookup_batch("retry"))
        self.assertEqual(list((self.path / "staging").iterdir()), [])
        self.lake.before_commit = None
        receipt = self.lake.write_paged("retry", "", SinglePass(rows), 4096)
        self.assertEqual(receipt.row_count, len(rows))
        self.lake.close()
        with Lake(self.path) as reopened:
            self.assertEqual(reopened.current_snapshot(), receipt.snapshot_id)
            self.assertEqual(len(list(reopened.read_rows(receipt.snapshot_id))),
                             len(sample_rows()) + len(rows))


class TestPagedStorage(PagedCase):
    def setUp(self):
        super().setUp()
        self.healthy = self.lake.write("v1", "", [{"label": "ok", "samples": [[7]]}])
        self.paged = self.lake.write_paged("paged", "later", nested_rows(), 4096)

    def assert_corrupt_but_old_healthy(self):
        for operation in (self.lake.read_rows, self.lake.summaries):
            with self.assertRaises(CorruptSegment):
                list(operation(self.paged.snapshot_id))
        self.assertEqual([r.samples for r in self.lake.read_rows(self.healthy.snapshot_id)],
                         [[[7]]])
        self.assertEqual(list(self.lake.read_rows(EMPTY_SNAPSHOT_ID)), [])

    def test_missing_and_byte_damage(self):
        entry = self.lake._session.repository.lookup_batch("paged")
        path = self.lake._session.objects.path_for(entry.sealed.digest)
        raw = path.read_bytes()
        path.write_bytes(raw[:-1])
        self.assert_corrupt_but_old_healthy()
        path.write_bytes(raw)
        damaged = bytearray(raw)
        damaged[-2] ^= 1
        path.write_bytes(damaged)
        self.assert_corrupt_but_old_healthy()
        path.unlink()
        self.assert_corrupt_but_old_healthy()

    def test_structural_damage_with_recomputed_checksums(self):
        def bad_tag(contents):
            contents[0][2] = 0x02          # outer samples tag: neither NULL nor list
        def bad_label_length(contents):
            contents[0][0] = 0xFF          # label length runs past every page
        cases = [("tag", bad_tag), ("label", bad_label_length)]
        for name, mutator in cases:
            with self.subTest(case=name):
                rewrite_paged_object(self.lake, "paged", mutate_body=mutator)
                self.assert_corrupt_but_old_healthy()
                replacement = self.lake.write_paged(
                    f"restored-{name}", "later", nested_rows(), 4096
                )
                self.paged = replacement

    def test_header_page_budget_is_enforced(self):
        def wrong_page_bytes(header):
            header["page_bytes"] = 1
        rewrite_paged_object(self.lake, "paged", mutate_header=wrong_page_bytes)
        self.assert_corrupt_but_old_healthy()


class TestPageEnumeration(PagedCase):
    def test_every_page_respects_budget(self):
        rows = [{"label": "r", "samples": [[i] for i in range(300)]} for _ in range(40)]
        for budget in (4096, 8192, 65536):
            receipt = self.lake.write_paged(f"b{budget}", "", rows, budget)
            entry = self.lake._session.repository.lookup_batch(f"b{budget}")
            path = self.lake._session.objects.path_for(entry.sealed.digest)
            layout = read_layout(path)
            self.assertEqual(layout.page_bytes, budget)
            lengths = [entry.length for entry in iter_page_entries(path, layout)]
            self.assertEqual(layout.page_count, len(lengths))
            self.assertTrue(all(0 < length <= budget for length in lengths))
            self.assertTrue(all(length == budget for length in lengths[:-1]))
            self.assertEqual(sum(lengths), layout.body_length)
            self.assertEqual(verify_page_lengths(path, budget), lengths)
            self.assertEqual(receipt.snapshot_id,
                             self.lake.current_snapshot())


if __name__ == "__main__":
    unittest.main()
