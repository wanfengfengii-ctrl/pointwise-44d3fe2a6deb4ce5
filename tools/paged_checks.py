"""Joint V1/paged business checks, page envelope checks and corruption drills.

These are delivered-implementation checks (not the independent fixture oracle):
the independent list-traversal oracle lives in ``paged_examples.py``.  Here we
exercise the production entry points end to end, enumerate real page lengths
from the delivered format, force structural damage with consistent checksums,
and emit POINTWISE_SCENARIO JSON for reconciliation.
"""
import hashlib
import io
import json
from pathlib import Path

from monitorlake.schema.errors import CorruptSegment
from monitorlake.service.lake import Lake
from monitorlake.segments.paged_format import (
    DIGEST_BYTES, FRAME_LENGTH, PAGED_MAGIC, iter_page_entries, read_layout,
    verify_page_lengths,
)
from tools.scenarios import (
    SourceFailure, emit_scenario, reference_summary, sample_rows,
)


def paged_rows(sample_count=9000):
    """Deterministic rows with records, inner segments and nesting crossings."""
    rows = sample_rows() + [
        {"label": "duplicate", "samples": [None, [], [
            None if i % 17 == 0 else i % 101 - 50 for i in range(sample_count)
        ], [], [None], None]},
        {"label": "水" * 341, "samples": [
            [None if i % 3 == 0 else i % 7 - 3 for i in range(sample_count // 3)],
            [], None,
        ]},
    ]
    return rows


def expected_rows(rows, snapshot, batch_id, source):
    return [{"snapshot_id": snapshot, "batch_id": batch_id, "ordinal": ordinal,
             "source_description": source, **row} for ordinal, row in enumerate(rows)]


def expected_summaries(rows, snapshot, batch_id, source):
    return [reference_summary(row, snapshot, batch_id, ordinal, source)
            for ordinal, row in enumerate(rows)]


def joint_checks(path):
    rows = paged_rows()
    legacy = sample_rows()
    with Lake(path) as lake:
        old = lake.write("legacy", "V1", legacy)
        old_snapshot = old.snapshot_id
        old_rows = expected_rows(legacy, old_snapshot, "legacy", "V1")
        old_summaries = expected_summaries(legacy, old_snapshot, "legacy", "V1")
        paged = lake.write_paged("joint", "paged", iter(rows), 4096)
        new_rows = expected_rows(rows, paged.snapshot_id, "joint", "paged")
        new_summaries = expected_summaries(rows, paged.snapshot_id, "joint", "paged")
        actual_rows = [r.as_dict() for r in lake.read_rows(paged.snapshot_id)]
        actual_summaries = [s.as_dict() for s in lake.summaries(paged.snapshot_id)]
        assert actual_rows == expected_rows(
            legacy, paged.snapshot_id, "legacy", "V1"
        ) + new_rows
        assert actual_summaries == expected_summaries(
            legacy, paged.snapshot_id, "legacy", "V1"
        ) + new_summaries
        # old snapshot is unchanged by the new paged publication
        assert [r.as_dict() for r in lake.read_rows(old_snapshot)] == old_rows
        assert [s.as_dict() for s in lake.summaries(old_snapshot)] == old_summaries
        sink = io.StringIO()
        assert lake.export(paged.snapshot_id, sink) == len(rows) + len(legacy)
        exported = [json.loads(line) for line in sink.getvalue().splitlines()]
        assert [item["record"] for item in exported] == [
            {"label": row["label"], "samples": row["samples"]}
            for row in legacy + rows
        ]
        # replay across entries, budgets and after the pointer moved
        assert lake.write("joint", "paged", iter(rows)) == paged
        assert lake.write_paged("joint", "paged", iter(rows), 65536) == paged
        assert lake.write_paged("legacy", "V1", iter(legacy), 4096) == old
        later = lake.write_paged("later", "", iter([{"label": "z", "samples": []}]), 4096)
        assert lake.write_paged("joint", "paged", iter(rows), 8192) == paged
        # source reuse: a record mutated after its row was accepted
        shared = {"label": "reuse", "samples": [[9], [], None]}
        def reuse():
            yield shared
            shared["samples"][0].append(77)
            yield {"label": "reuse", "samples": []}
        reused = lake.write_paged("reuse", "", reuse(), 4096)
        reuse_rows = [
            {"label": "reuse", "samples": [[9], [], None]},
            {"label": "reuse", "samples": []},
        ]
        reuse_snapshot = reused.snapshot_id
        assert [r.as_dict() for r in lake.read_rows(reuse_snapshot)] == (
            expected_rows(legacy, reuse_snapshot, "legacy", "V1")
            + expected_rows(rows, reuse_snapshot, "joint", "paged")
            + expected_rows([{"label": "z", "samples": []}], reuse_snapshot,
                            "later", "")
            + expected_rows(reuse_rows, reuse_snapshot, "reuse", "")
        )
        # failure preserves the current snapshot and cleans staging
        current = lake.current_snapshot()
        error = SourceFailure("after accepted rows")
        def broken():
            yield rows[0]
            raise error
        try:
            lake.write_paged("broken", "", broken(), 4096)
        except SourceFailure as raised:
            assert raised is error
        else:
            raise AssertionError("source failure was accepted")
        assert lake.current_snapshot() == current
        assert list((path / "staging").iterdir()) == []
        latest = lake.write_paged("empty", "", iter([]), 65536)
        assert latest.row_count == 0
    with Lake(path) as reopened:
        assert reopened.current_snapshot() == latest.snapshot_id
        assert [r.as_dict() for r in reopened.read_rows(paged.snapshot_id)] == (
            expected_rows(legacy, paged.snapshot_id, "legacy", "V1") + new_rows
        )
        assert [s.as_dict() for s in reopened.summaries(old_snapshot)] == old_summaries
        assert reopened.write("joint", "paged", iter(rows)) == paged
    emit_scenario(
        "paged.joint", {"page_bytes": 4096, "rows": len(rows)},
        {"rows": len(rows) + len(legacy), "summaries": new_summaries[:2]},
        {"rows": len(actual_rows), "summaries": actual_summaries[-2:]},
        exported[:1], exported[-1:],
        ["monitorlake/intake/paged_batch.py", "monitorlake/segments/paged_format.py",
         "monitorlake/consumption/scan.py", "monitorlake/catalog/publication.py"],
        "Production entries end to end; independent list oracle in paged_examples.py",
    )
    print(json.dumps({"joint": "matched", "current_snapshot": latest.snapshot_id,
                      "legacy_snapshot": old_snapshot, "paged_snapshot": paged.snapshot_id},
                     sort_keys=True))
    return rows, legacy


def _paged_entry(lake, batch_id):
    entry = lake._session.repository.lookup_batch(batch_id)
    return entry, lake._session.objects.path_for(entry.sealed.digest)


def page_envelope_checks(path):
    """Enumerate real page content lengths for several budgets and shapes."""
    rows = paged_rows(6000)
    reports = []
    with Lake(path) as lake:
        shapes = {
            "one_long_record": [{"label": "long", "samples": [[i for i in range(12000)]]}],
            "many_empty_segments": [{"label": "segments",
                                     "samples": [None, [], [1]] * 4000}],
            "many_records": [{"label": "r", "samples": [[i]]} for i in range(2000)],
        }
        budget_shapes = [(4096, "one_long_record"), (4096, "many_empty_segments"),
                         (65536, "one_long_record"), (65536, "many_empty_segments"),
                         (4096, "many_records"), (65536, "many_records")]
        for budget, shape in budget_shapes:
            batch_id = f"{shape}-{budget}"
            lake.write_paged(batch_id, "envelope", iter(shapes[shape]), budget)
            entry, object_path = _paged_entry(lake, batch_id)
            layout = read_layout(object_path)
            lengths = verify_page_lengths(object_path, budget)
            entries = list(iter_page_entries(object_path, layout))
            assert len(lengths) == layout.page_count == len(entries)
            assert all(0 < length <= budget for length in lengths)
            assert all(length == budget for length in lengths[:-1])
            assert sum(lengths) == layout.body_length
            assert layout.page_bytes == budget
            assert entries[-1].offset + FRAME_LENGTH.size + DIGEST_BYTES + lengths[-1] \
                == layout.file_size
            assert entry.sealed.descriptor.byte_count == layout.file_size
            reports.append({
                "shape": shape, "page_bytes": budget, "pages": layout.page_count,
                "max_content": max(lengths), "body_bytes": layout.body_length,
                "row_count": layout.row_count,
            })
            # rows still round-trip independently of the enumeration
            observed = [r.observation.as_dict() for r in lake.read_rows(
                lake.current_snapshot()) if r.batch_id == batch_id]
            assert observed == shapes[shape]
    for report in reports:
        print("PAGE_ENVELOPE " + json.dumps(report, sort_keys=True))
    emit_scenario(
        "paged.page_lengths", {"budgets": [4096, 65536]},
        {"rule": "every page content length <= page_bytes; only last page is short"},
        reports, None,
        {"pages": [report["pages"] for report in reports]},
        ["monitorlake/segments/paged_format.py"],
        "Enumerated frame lengths and checksums of the real paged objects",
    )
    print("PAGE_ENVELOPE_OK")
    return reports


def _rebuild(lake, batch_id, mutate_body, mutate_header=None):
    """Rewrite a paged object keeping page lengths; refresh all checksums."""
    entry, object_path = _paged_entry(lake, batch_id)
    raw = object_path.read_bytes()
    start = len(PAGED_MAGIC) + FRAME_LENGTH.size
    header_length = FRAME_LENGTH.unpack(raw[len(PAGED_MAGIC):start])[0]
    header = json.loads(raw[start:start + header_length])
    offset = start + header_length
    contents = []
    for _ in range(header["page_count"]):
        length = FRAME_LENGTH.unpack(raw[offset:offset + FRAME_LENGTH.size])[0]
        body = offset + FRAME_LENGTH.size + DIGEST_BYTES
        contents.append(bytearray(raw[body:body + length]))
        offset = body + length
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
        "UPDATE batches SET object_digest=?,byte_count=?,identity=? WHERE batch_id=?",
        (digest, len(rebuilt), marker, batch_id),
    )
    return new_path


def structural_damage_checks(path):
    rows = [{"label": "A", "samples": None},
            {"label": "B", "samples": [None, [], [None, 2, -2]]}]
    results = []
    with Lake(path) as lake:
        healthy = lake.write("healthy", "", [{"label": "ok", "samples": [[7]]}])
        for case in ("bad-tag", "bad-count", "bad-budget"):
            batch_id = f"damaged-{case}"
            lake.write_paged(batch_id, "damage", iter(rows), 4096)
            if case == "bad-tag":
                _rebuild(lake, batch_id, lambda c: c[0].__setitem__(2, 0x02))
            elif case == "bad-count":
                def bad_count(contents):
                    # first row label is 1 byte: offset 1 holds the outer tag;
                    # rewrite the segment-count varint to an impossible value
                    contents[0][3] = 0x7F
                _rebuild(lake, batch_id, bad_count)
            else:
                _rebuild(lake, batch_id, lambda c: None,
                         lambda h: h.__setitem__("page_bytes", 1))
            snapshot = lake.current_snapshot()
            for operation in (lake.read_rows, lake.summaries):
                try:
                    list(operation(snapshot))
                except CorruptSegment:
                    pass
                else:
                    raise AssertionError(f"{case}: corruption was not reported")
            healthy_rows = [r.samples for r in lake.read_rows(healthy.snapshot_id)
                            if r.batch_id == "healthy"]
            assert healthy_rows == [[[7]]]
            healthy_summaries = [s.sum for s in lake.summaries(healthy.snapshot_id)
                                 if s.batch_id == "healthy"]
            assert healthy_summaries == [7]
            results.append({"case": case, "raised": "CorruptSegment",
                            "healthy_still_readable": True})
    for result in results:
        print("PAGED_DAMAGE " + json.dumps(result, sort_keys=True))
    emit_scenario(
        "paged.structural_damage", {"cases": [r["case"] for r in results]},
        {"error": "CorruptSegment", "healthy_prefix": "readable"},
        results, None, None,
        ["monitorlake/segments/paged_format.py", "monitorlake/catalog/objects.py"],
        "Page checksums and whole-object digest recomputed before the drill",
    )
    print("PAGED_DAMAGE_OK")
    return results


def verify_checks(path):
    joint_checks(path)
    page_envelope_checks(path)
    structural_damage_checks(path)


def demo_checks(path, sample_count):
    """Long row crossing pages, reopen, and per-row summaries."""
    long_row = {"label": "long-row",
                "samples": [None, [], [
                    None if index % 17 == 0 else (index % 101) - 50
                    for index in range(sample_count)
                ], [], [None], [-(1 << 63), (1 << 63) - 1]]}
    rows = sample_rows() + [long_row]
    with Lake(path) as lake:
        receipt = lake.write_paged("demo", "instrument", iter(rows), 4096)
        snapshot = receipt.snapshot_id
        entry, object_path = _paged_entry(lake, "demo")
        layout = read_layout(object_path)
        lengths = verify_page_lengths(object_path, 4096)
        sink = io.StringIO()
        exported = lake.export(snapshot, sink)
        exported_lines = sink.getvalue().splitlines()
    with Lake(path) as reopened:
        assert reopened.current_snapshot() == receipt.snapshot_id
        restored = [r.as_dict() for r in reopened.read_rows(snapshot)]
        summaries = [s.as_dict() for s in reopened.summaries(snapshot)]
        assert restored == expected_rows(rows, snapshot, "demo", "instrument")
        assert summaries == expected_summaries(rows, snapshot, "demo", "instrument")
        expected_lines = [
            json.dumps({**coordinates, "record": {"label": raw["label"],
                                                  "samples": raw["samples"]},
                        "summary": summary},
                       ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            for coordinates, raw, summary in zip(
                ({"snapshot_id": snapshot, "batch_id": "demo", "ordinal": ordinal,
                  "source_description": "instrument"} for ordinal in range(len(rows))),
                rows, summaries)
        ]
        assert exported_lines == expected_lines
    assert exported == len(rows)
    print(json.dumps({
        "demo": "paged-long-row", "snapshot": receipt.snapshot_id,
        "pages": layout.page_count, "page_bytes": layout.page_bytes,
        "max_page_content": max(lengths), "long_row_sum": summaries[-1]["sum"],
        "long_row_samples": summaries[-1]["non_null_sample_count"],
        "reopened": True, "exported": exported,
    }, sort_keys=True))
    print("PAGED_DEMO_OK")
