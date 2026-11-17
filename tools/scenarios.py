import argparse
import io
import json
from pathlib import Path
import tempfile
import unittest
from monitorlake.service.lake import Lake

ROOT = Path(__file__).resolve().parents[1]


def sample_rows():
    return json.loads((ROOT / "examples.json").read_text(encoding="utf-8"))


def long_rows(sample_count=20000):
    if type(sample_count) is not int or sample_count < 0:
        raise ValueError("sample_count must be nonnegative")
    yield {
        "label": "long-instrument-run",
        "samples": [None, [], [
            None if index % 17 == 0 else (index % 101) - 50
            for index in range(sample_count)
        ], [], [None]],
    }


def reference_summary(raw, snapshot_id, batch_id, ordinal, source_description):
    outer = raw["samples"]
    segments = [] if outer is None else outer
    scalars = [
        scalar for segment in segments if segment is not None
        for scalar in segment if scalar is not None
    ]
    return {
        "snapshot_id": snapshot_id, "batch_id": batch_id, "ordinal": ordinal,
        "source_description": source_description, "label": raw["label"],
        "outer_is_null": outer is None,
        "segment_count": len(segments),
        "null_segment_count": sum(segment is None for segment in segments),
        "empty_segment_count": sum(segment == [] for segment in segments),
        "non_null_sample_count": len(scalars),
        "sum": sum(scalars) if scalars else None,
    }


class SinglePass:
    def __init__(self, rows):
        self.rows = rows
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        if self.iterations != 1:
            raise AssertionError("source iterated more than once")
        return iter(self.rows)


class SourceFailure(Exception):
    pass


def interrupted_rows(rows, error, after=2):
    for index, row in enumerate(rows):
        if index == after:
            raise error
        yield row
    raise error


def emit_scenario(case_id, input_value, expected, actual,
                  downstream_expected, downstream_actual, code_paths, oracle):
    print("POINTWISE_SCENARIO " + json.dumps({
        "case_id": case_id, "input": input_value,
        "expected": expected, "actual": actual,
        "downstream_expected": downstream_expected,
        "downstream_actual": downstream_actual,
        "code_paths": code_paths, "oracle": oracle,
    }, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


def check_business(path, sample_count=20000):
    rows = sample_rows()
    expected_summaries = None
    with Lake(path) as lake:
        result = lake.write("daily-001", "instrument-export", SinglePass(rows))
        snapshot = result.snapshot_id
        actual_rows = [row.observation.as_dict() for row in lake.read_rows(snapshot)]
        assert actual_rows == rows
        expected_summaries = [
            reference_summary(row, snapshot, "daily-001", i, "instrument-export")
            for i, row in enumerate(rows)
        ]
        actual_summaries = [summary.as_dict() for summary in lake.summaries(snapshot)]
        assert actual_summaries == expected_summaries
        sink = io.StringIO()
        assert lake.export(snapshot, sink) == len(rows)
        fixed_export = [json.loads(line) for line in sink.getvalue().splitlines()]
        assert [item["record"] for item in fixed_export] == rows
        assert [item["summary"] for item in fixed_export] == expected_summaries
        replay = lake.write("daily-001", "instrument-export", SinglePass(sample_rows()))
        assert replay == result
        long_input = list(long_rows(sample_count))
        long_result = lake.write("long-001", "continuous-run", iter(long_input))
        assert long_result.row_count == 1
        expected_long = reference_summary(
            long_input[0], long_result.snapshot_id, "long-001", 0, "continuous-run"
        )
        last = list(lake.summaries(long_result.snapshot_id))[-1].as_dict()
        assert last == expected_long
        newer = lake.current_snapshot()
        lake.export(snapshot, io.StringIO())
    with Lake(path) as reopened:
        assert reopened.current_snapshot() == newer
        sink = io.StringIO()
        reopened.export(snapshot, sink)
        after_reopen = [json.loads(line) for line in sink.getvalue().splitlines()]
        assert after_reopen == fixed_export
        assert reopened.write("daily-001", "instrument-export", sample_rows()) == result
        assert reopened.current_snapshot() == newer
    emit_scenario(
        "v3.null_zero", rows, expected_summaries, actual_summaries,
        fixed_export, after_reopen,
        ["monitorlake/intake/batch.py", "monitorlake/segments/encode.py",
         "monitorlake/segments/decode.py", "monitorlake/consumption/rows.py",
         "monitorlake/consumption/summary.py", "monitorlake/consumption/export.py"],
        "Direct list traversal, Python built-in sum and exact parsed JSON comparison",
    )
    print(json.dumps({
        "format": "V1 whole column block", "sample_count": sample_count,
        "long_row_summary": last, "current_snapshot": newer,
        "reopened_old_snapshot": snapshot,
    }, sort_keys=True))


def verify():
    suite = unittest.defaultTestLoader.discover(
        str(ROOT / "tests"), pattern="test_*.py", top_level_dir=str(ROOT)
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    with tempfile.TemporaryDirectory(prefix="monitorlake-verify-") as directory:
        check_business(Path(directory) / "lake")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Monitoring lake V1 business checks")
    subcommands = parser.add_subparsers(dest="command", required=True)
    initialize = subcommands.add_parser("init")
    initialize.add_argument("path", type=Path)
    demo = subcommands.add_parser("demo")
    demo.add_argument("--samples", type=int, default=20000)
    subcommands.add_parser("verify")
    args = parser.parse_args(argv)
    if args.command == "verify":
        return verify()
    if args.command == "init":
        with Lake(args.path) as lake:
            print(json.dumps({"current_snapshot": lake.current_snapshot()}))
        return 0
    with tempfile.TemporaryDirectory(prefix="monitorlake-demo-") as directory:
        check_business(Path(directory) / "lake", args.samples)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
