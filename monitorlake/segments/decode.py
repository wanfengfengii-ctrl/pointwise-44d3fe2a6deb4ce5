import json
import struct
from monitorlake.schema.errors import CorruptSegment
from monitorlake.segments.checksum import verify_digest
from monitorlake.segments.layout import (
    V1_MAGIC, V1_VERSION, V1ColumnBlock, validate_directory,
    validate_column_block, require_natural,
)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CorruptSegment("duplicate JSON object key")
        result[key] = value
    return result


def reject_constant(value):
    raise CorruptSegment(f"non-finite JSON number: {value}")


def parse_json(data):
    return json.loads(
        data.decode("utf-8"), object_pairs_hook=unique_object,
        parse_constant=reject_constant,
    )


def decode_v1(data, descriptor):
    try:
        if not isinstance(data, bytes):
            raise CorruptSegment("segment bytes required")
        require_natural(descriptor.byte_count, "object byte count")
        require_natural(descriptor.row_count, "descriptor row count")
        if type(descriptor.version) is not int or descriptor.version != V1_VERSION:
            raise CorruptSegment("unsupported descriptor version")
        if len(data) != descriptor.byte_count:
            raise CorruptSegment("segment byte count mismatch")
        verify_digest(data, descriptor.digest)
        prefix = len(V1_MAGIC) + 4
        if len(data) < prefix or data[:len(V1_MAGIC)] != V1_MAGIC:
            raise CorruptSegment("unsupported segment magic")
        header_size = struct.unpack(">I", data[len(V1_MAGIC):prefix])[0]
        if header_size == 0 or prefix + header_size > len(data):
            raise CorruptSegment("truncated segment header")
        header = parse_json(data[prefix:prefix + header_size])
        if type(header) is not dict or set(header) != {"version", "row_count", "columns"}:
            raise CorruptSegment("invalid segment header")
        if type(header["version"]) is not int or header["version"] != V1_VERSION:
            raise CorruptSegment("unsupported segment version")
        require_natural(header["row_count"], "header row count")
        if header["row_count"] != descriptor.row_count:
            raise CorruptSegment("catalog and segment row counts differ")
        payload = data[prefix + header_size:]
        validate_directory(header["columns"], len(payload))
        columns = {}
        for entry in header["columns"]:
            start = entry["offset"]
            chunk = payload[start:start + entry["length"]]
            verify_digest(chunk, entry["sha256"], entry["name"])
            column = parse_json(chunk)
            if type(column) is not list or len(column) != entry["count"]:
                raise CorruptSegment("column item count mismatch")
            columns[entry["name"]] = column
        block = V1ColumnBlock(**columns)
        validate_column_block(block)
        if block.row_count != descriptor.row_count:
            raise CorruptSegment("decoded row count mismatch")
        return block
    except CorruptSegment:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, struct.error) as error:
        raise CorruptSegment("malformed V1 segment") from error
