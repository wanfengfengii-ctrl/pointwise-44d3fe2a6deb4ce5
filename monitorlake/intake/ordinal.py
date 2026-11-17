from dataclasses import dataclass
from monitorlake.schema.types import Observation


@dataclass(frozen=True)
class OrdinalRecord:
    ordinal: int
    observation: Observation


class OrdinalAllocator:
    def __init__(self):
        self.next_ordinal = 0

    def assign(self, observation):
        if not isinstance(observation, Observation):
            raise TypeError("ordinal allocation requires a normalized observation")
        record = OrdinalRecord(self.next_ordinal, observation)
        self.next_ordinal += 1
        return record
