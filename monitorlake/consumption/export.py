import json
from monitorlake.consumption.rows import iter_rows
from monitorlake.consumption.summary import summarize_row


def make_export_record(row):
    return {
        **row.coordinates(),
        "record": row.observation.as_dict(),
        "summary": summarize_row(row).as_dict(),
    }


def export_snapshot(scan, sink):
    delivered = 0
    for row in iter_rows(scan):
        record = make_export_record(row)
        text = json.dumps(
            record, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
        ) + "\n"
        written = sink.write(text)
        if written is not None and written != len(text):
            raise OSError("export sink performed a short write")
        delivered += 1
    return delivered
