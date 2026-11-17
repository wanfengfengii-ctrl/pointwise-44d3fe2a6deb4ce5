from dataclasses import dataclass

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Observation:
    label: str
    samples: list | None

    def as_dict(self):
        return {"label": self.label, "samples": self.samples}
