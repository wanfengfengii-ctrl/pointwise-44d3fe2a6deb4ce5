from dataclasses import dataclass
from monitorlake.schema.errors import CorruptSegment
from monitorlake.schema.validation import validate_sample_value

V1_VERSION = 1
V1_MAGIC = b"MLAKEV1\n"
COLUMN_NAMES = (
    "labels", "outer_validity", "row_segment_offsets", "segment_validity",
    "segment_value_offsets", "sample_values",
)


@dataclass(frozen=True)
class V1ColumnBlock:
    labels: list
    outer_validity: list
    row_segment_offsets: list
    segment_validity: list
    segment_value_offsets: list
    sample_values: list

    @property
    def row_count(self):
        return len(self.labels)

    def columns(self):
        return {name: getattr(self, name) for name in COLUMN_NAMES}


@dataclass(frozen=True)
class SegmentDescriptor:
    version: int
    row_count: int
    byte_count: int
    digest: str


def require_natural(value, context):
    if type(value) is not int or value < 0:
        raise CorruptSegment(f"{context}: expected a nonnegative integer")


def validate_directory(entries, payload_size):
    if type(entries) is not list or len(entries) != len(COLUMN_NAMES):
        raise CorruptSegment("directory must describe exactly the required columns")
    cursor = 0
    for name, entry in zip(COLUMN_NAMES, entries):
        if type(entry) is not dict or set(entry) != {
            "name", "offset", "length", "count", "sha256"
        }:
            raise CorruptSegment("invalid column directory entry")
        if entry["name"] != name:
            raise CorruptSegment("missing, duplicate or unordered column")
        for field in ("offset", "length", "count"):
            require_natural(entry[field], f"{name}.{field}")
        start, length = entry["offset"], entry["length"]
        if start != cursor or length == 0 or start + length > payload_size:
            raise CorruptSegment("column directory overlaps, has gaps or exceeds payload")
        cursor = start + length
    if cursor != payload_size:
        raise CorruptSegment("unaccounted bytes after columns")


def validate_offsets(offsets, count, endpoint, context):
    if type(offsets) is not list or len(offsets) != count + 1:
        raise CorruptSegment(f"{context}: incorrect offset count")
    previous = 0
    for offset in offsets:
        require_natural(offset, context)
        if offset < previous or offset > endpoint:
            raise CorruptSegment(f"{context}: descending or out-of-range offset")
        previous = offset
    if offsets[0] != 0 or offsets[-1] != endpoint:
        raise CorruptSegment(f"{context}: incorrect first or final offset")


def validate_column_block(block):
    for name, column in block.columns().items():
        if type(column) is not list:
            raise CorruptSegment(f"{name}: column must be a list")
    if any(not isinstance(label, str) for label in block.labels):
        raise CorruptSegment("labels: non-string value")
    rows = block.row_count
    segments = len(block.segment_validity)
    values = len(block.sample_values)
    if len(block.outer_validity) != rows:
        raise CorruptSegment("outer validity length differs from row count")
    if any(type(flag) is not bool for flag in block.outer_validity):
        raise CorruptSegment("outer validity must contain booleans")
    if any(type(flag) is not bool for flag in block.segment_validity):
        raise CorruptSegment("segment validity must contain booleans")
    validate_offsets(block.row_segment_offsets, rows, segments, "row offsets")
    validate_offsets(block.segment_value_offsets, segments, values, "segment offsets")
    for row, valid in enumerate(block.outer_validity):
        if not valid and block.row_segment_offsets[row] != block.row_segment_offsets[row + 1]:
            raise CorruptSegment("NULL outer list owns child segments")
    for segment, valid in enumerate(block.segment_validity):
        if not valid and block.segment_value_offsets[segment] != block.segment_value_offsets[segment + 1]:
            raise CorruptSegment("NULL segment owns child values")
    for value in block.sample_values:
        try:
            validate_sample_value(value)
        except ValueError as error:
            raise CorruptSegment("invalid sample value in column") from error
