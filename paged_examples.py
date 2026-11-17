"""Public paging fixtures and independent business oracle; no page codec."""
import argparse
import gc
import io
import json
from pathlib import Path
import tempfile
import tracemalloc

from monitorlake.service.lake import Lake
from tools.scenarios import SinglePass, SourceFailure, reference_summary, sample_rows


def cases(n=18000):
    return sample_rows() + [
        {"label": "duplicate", "samples": [None, [], [
            None if i % 17 == 0 else i % 101 - 50 for i in range(n)
        ], [], [None], None]},
        {"label": "duplicate", "samples": [
            None if i % 3 == 0 else [] if i % 3 == 1 else [None, 0]
            for i in range(n)
        ]},
        {"label": "水" * 341, "samples": [[-(1 << 63), (1 << 63) - 1]]},
    ]


def check(lake, receipt, groups):
    expected = []
    expected_summaries = []
    for batch, source, rows in groups:
        for ordinal, raw in enumerate(rows):
            expected.append({
                "snapshot_id": receipt.snapshot_id, "batch_id": batch,
                "ordinal": ordinal, "source_description": source, **raw,
            })
            expected_summaries.append(reference_summary(
                raw, receipt.snapshot_id, batch, ordinal, source
            ))
    assert [r.as_dict() for r in lake.read_rows(receipt.snapshot_id)] == expected
    assert [s.as_dict() for s in lake.summaries(receipt.snapshot_id)] == expected_summaries
    sink = io.StringIO()
    assert lake.export(receipt.snapshot_id, sink) == len(expected)
    exported = [json.loads(line) for line in sink.getvalue().splitlines()]
    assert [e["summary"] for e in exported] == expected_summaries
    assert [e["record"] for e in exported] == [
        {"label": r["label"], "samples": r["samples"]} for r in expected
    ]
    return expected_summaries


def expect_same_error(operation, error):
    try:
        operation()
    except type(error) as actual:
        assert actual is error
    else:
        raise AssertionError("source or hook failure was accepted")


def fixture_check():
    raw = {"label": "B", "samples": [None, [], [None, 2, -2]]}
    result = reference_summary(raw, 1, "b", 0, "")
    assert [result[k] for k in (
        "segment_count", "null_segment_count", "empty_segment_count",
        "non_null_sample_count", "sum"
    )] == [3, 1, 1, 2, 0]
    assert len(cases(7)) == 8
    print("PAGED_FIXTURES_OK")


def verify():
    rows = cases()
    for budget in (4096, 65536):
        with tempfile.TemporaryDirectory(prefix="paged-business-") as directory:
            path = Path(directory) / "lake"
            old_rows = sample_rows()
            with Lake(path) as lake:
                first = lake.write("old", "V1", SinglePass(old_rows))
                second = lake.write_paged("new", "paged", SinglePass(rows), budget)
                check(lake, first, [("old", "V1", old_rows)])
                groups = [("old", "V1", old_rows), ("new", "paged", rows)]
                check(lake, second, groups)
                assert lake.write_paged("new", "paged", SinglePass(rows),
                                        69632 - budget) == second
                assert lake.write("new", "paged", SinglePass(rows)) == second
                assert lake.write_paged("old", "V1", SinglePass(old_rows), budget) == first
                assert lake.current_snapshot() == second.snapshot_id

                shared = {"label": "reuse", "samples": [[9], [], None]}
                def reuse():
                    yield shared
                    shared["samples"][0].append(77)
                    yield {"label": "reuse", "samples": []}
                third = lake.write_paged("reuse", "", reuse(), budget)
                reuse_expected = [
                    {"label": "reuse", "samples": [[9], [], None]},
                    {"label": "reuse", "samples": []},
                ]
                groups.append(("reuse", "", reuse_expected))
                check(lake, third, groups)

                current = lake.current_snapshot()
                for bad in (0, 4095, 65537, True, 4096.0):
                    source = SinglePass(rows)
                    try:
                        lake.write_paged("bad-budget", "", source, bad)
                    except ValueError:
                        pass
                    else:
                        raise AssertionError("invalid budget accepted")
                    assert source.iterations == 0
                invalid = [
                    {"label": "bad", "samples": [[True]]},
                    {"label": "bad", "samples": [[1 << 63]]},
                    {"label": "bad", "samples": [()]},
                    {"label": "x" * 1025, "samples": []},
                ]
                for raw in invalid:
                    try:
                        lake.write_paged("bad-record", "", iter(rows + [raw]), budget)
                    except ValueError:
                        pass
                    else:
                        raise AssertionError("invalid tail accepted")
                    assert lake.current_snapshot() == current
                changed = list(rows)
                changed[-1] = {"label": "different", "samples": []}
                for source, values in (("paged", changed), ("other", rows)):
                    try:
                        lake.write_paged("new", source, SinglePass(values), budget)
                    except ValueError:
                        pass
                    else:
                        raise AssertionError("different semantic identity replay accepted")

                error = SourceFailure("after long row")
                def broken():
                    yield rows[5]
                    raise error
                expect_same_error(
                    lambda: lake.write_paged("broken", "", broken(), budget), error
                )
                assert lake.current_snapshot() == current
                hook_error = SourceFailure("before commit")
                def fail(receipt):
                    assert receipt.batch_id == "retry"
                    raise hook_error
                lake.before_commit = fail
                expect_same_error(
                    lambda: lake.write_paged("retry", "", SinglePass(rows), budget),
                    hook_error,
                )
                assert lake.current_snapshot() == current
                lake.before_commit = None
                last = lake.write_paged("retry", "", SinglePass(rows), budget)
                groups.append(("retry", "", rows))
                check(lake, last, groups)
                assert list((path / "staging").iterdir()) == []
                empty = lake.write_paged("empty", "", iter([]), budget)
                assert empty.row_count == 0
                groups.append(("empty", "", []))
            with Lake(path) as reopened:
                check(reopened, first, [("old", "V1", old_rows)])
                check(reopened, empty, groups)
                assert reopened.write_paged("new", "paged", SinglePass(rows), budget) == second
                assert reopened.current_snapshot() == empty.snapshot_id
            print(json.dumps({"page_bytes": budget, "reopened_snapshot": empty.snapshot_id,
                              "rows_in_new_batch": second.row_count, "business": "matched"}))
    print("PAGED_BUSINESS_OK")


def peak(operation):
    gc.collect()
    tracemalloc.start()
    try:
        result = operation()
        _, high = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return result, high


def consume_summaries(lake, snapshot):
    count = 0
    total = 0
    for item in lake.summaries(snapshot):
        count += 1
        total += item.sum or 0
    return count, total


def memory():
    for shape in ("one_segment", "many_segments", "many_rows"):
        for n in (8192, 131072):
            if shape == "one_segment":
                rows = [{"label": "long", "samples": [[1] * n]}]
                expected = (1, n)
            elif shape == "many_segments":
                rows = [{"label": "long", "samples": [[], None, [1]] * n}]
                expected = (1, n)
            else:
                rows = [{"label": "short", "samples": [[1]]} for _ in range(n)]
                expected = (n, n)
            with tempfile.TemporaryDirectory(prefix="paged-memory-") as directory:
                path = Path(directory) / "lake"
                with Lake(path) as lake:
                    receipt, write_peak = peak(lambda: lake.write_paged(
                        "load", "", iter(rows), 4096
                    ))
                    _, replay_peak = peak(lambda: lake.write_paged(
                        "load", "", iter(rows), 65536
                    ))
                reopened, open_peak = peak(lambda: Lake(path))
                try:
                    actual, summary_peak = peak(lambda: consume_summaries(
                        reopened, receipt.snapshot_id
                    ))
                    assert actual == expected
                finally:
                    reopened.close()
                print(json.dumps({
                    "shape": shape, "n": n, "page_bytes": 4096,
                    "write_peak": write_peak, "replay_peak_65536": replay_peak,
                    "open_peak": open_peak, "summary_peak": summary_peak,
                    "count_and_sum": actual,
                }, sort_keys=True))
            del rows
    print("Peaks are measurements; assess the growth and documented native buffers.")


def demo():
    rows = cases(50000)
    with tempfile.TemporaryDirectory(prefix="paged-demo-") as directory:
        path = Path(directory) / "lake"
        with Lake(path) as lake:
            result = lake.write_paged("demo", "instrument", SinglePass(rows), 4096)
        with Lake(path) as lake:
            actual = check(lake, result, [("demo", "instrument", rows)])
            print(json.dumps({"receipt": result.as_dict(), "summary": actual[5],
                              "reopened": True}, ensure_ascii=True, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("fixtures", "verify", "memory", "demo"))
    command = parser.parse_args().command
    {"fixtures": fixture_check, "verify": verify, "memory": memory, "demo": demo}[command]()


if __name__ == "__main__":
    main()
