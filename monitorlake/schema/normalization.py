from monitorlake.schema.types import Observation
from monitorlake.schema.validation import validate_observation


def normalize_observation(value):
    validate_observation(value)
    outer = value["samples"]
    owned = None if outer is None else [
        None if segment is None else list(segment) for segment in outer
    ]
    return Observation(value["label"], owned)
