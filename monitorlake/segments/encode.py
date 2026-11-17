from dataclasses import dataclass
import json
import struct
from monitorlake.segments.layout import (
    V1_MAGIC, V1_VERSION, COLUMN_NAMES, V1ColumnBlock, SegmentDescriptor,
    validate_column_block,
)
from monitorlake.segments.checksum import sha256_bytes


@dataclass(frozen=True)
class EncodedSegment:
    data: bytes
    descriptor: SegmentDescriptor


def build_column_block(records):
    labels, outer_validity, row_offsets = [], [], [0]
    segment_validity, value_offsets, values = [], [0], []
    for expected_ordinal, record in enumerate(records):
        if type(record.ordinal) is not int or record.ordinal != expected_ordinal:
            raise ValueError("candidate ordinals must be continuous from zero")
        observation = record.observation
        labels.append(observation.label)
        outer = observation.samples
        outer_validity.append(outer is not None)
        if outer is not None:
            for segment in outer:
                segment_validity.append(segment is not None)
                if segment is not None:
                    values.extend(segment)
                value_offsets.append(len(values))
        row_offsets.append(len(segment_validity))
    block = V1ColumnBlock(
        labels, outer_validity, row_offsets, segment_validity, value_offsets, values
    )
    validate_column_block(block)
    return block


def encode_v1(records):
    block = build_column_block(records)
    chunks, directory, offset = [], [], 0
    columns = block.columns()
    for name in COLUMN_NAMES:
        column = columns[name]
        chunk = json.dumps(
            column, ensure_ascii=True, separators=(",", ":"), allow_nan=False
        ).encode("ascii")
        directory.append({
            "name": name, "offset": offset, "length": len(chunk),
            "count": len(column), "sha256": sha256_bytes(chunk),
        })
        chunks.append(chunk)
        offset += len(chunk)
    header = json.dumps(
        {"version": V1_VERSION, "row_count": block.row_count, "columns": directory},
        sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("ascii")
    data = V1_MAGIC + struct.pack(">I", len(header)) + header + b"".join(chunks)
    descriptor = SegmentDescriptor(V1_VERSION, block.row_count, len(data), sha256_bytes(data))
    return EncodedSegment(data, descriptor)
