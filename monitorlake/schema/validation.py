from collections.abc import Mapping
from monitorlake.schema.types import INT64_MIN, INT64_MAX


def validate_sample_value(value):
    if value is not None and (
        type(value) is not int or not INT64_MIN <= value <= INT64_MAX
    ):
        raise ValueError("sample must be NULL or a non-boolean signed 64-bit integer")


def validate_observation(value):
    if not isinstance(value, Mapping):
        raise ValueError("record must be a mapping")
    if "label" not in value or "samples" not in value:
        raise ValueError("record requires label and samples")
    if not isinstance(value["label"], str):
        raise ValueError("label must be a string")
    outer = value["samples"]
    if outer is None:
        return
    if type(outer) is not list:
        raise ValueError("samples must be NULL or a list")
    for segment_index, segment in enumerate(outer):
        if segment is None:
            continue
        if type(segment) is not list:
            raise ValueError(f"segment {segment_index} must be NULL or a list")
        for sample_index, sample in enumerate(segment):
            try:
                validate_sample_value(sample)
            except ValueError as error:
                raise ValueError(
                    f"segment {segment_index}, sample {sample_index}: {error}"
                ) from error
