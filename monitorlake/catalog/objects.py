from dataclasses import dataclass
from pathlib import Path
import os
import re
from monitorlake.schema.errors import CorruptSegment, StorageError
from monitorlake.segments.checksum import verify_digest
from monitorlake.segments.decode import decode_v1


@dataclass(frozen=True)
class SealedObject:
    digest: str
    descriptor: object


class ObjectStore:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, digest):
        if not isinstance(digest, str) or re.fullmatch("[0-9a-f]{64}", digest) is None:
            raise StorageError("invalid object reference")
        return self.directory / (digest + ".v1")

    def read(self, sealed):
        path = self.path_for(sealed.digest)
        try:
            data = path.read_bytes()
        except OSError as error:
            raise CorruptSegment(f"required object unavailable: {sealed.digest}") from error
        if sealed.descriptor.digest != sealed.digest:
            raise CorruptSegment("object and descriptor digests disagree")
        if len(data) != sealed.descriptor.byte_count:
            raise CorruptSegment("object size differs from catalog")
        verify_digest(data, sealed.digest, "required object")
        return data

    def install(self, staged):
        descriptor = staged.descriptor
        try:
            data = staged.path.read_bytes()
        except OSError as error:
            raise StorageError("candidate staging material unavailable") from error
        decode_v1(data, descriptor)
        sealed = SealedObject(descriptor.digest, descriptor)
        target = self.path_for(descriptor.digest)
        try:
            os.link(staged.path, target)
        except FileExistsError:
            existing = self.read(sealed)
            decode_v1(existing, descriptor)
            if existing != data:
                raise CorruptSegment("existing object differs from candidate")
        except OSError as error:
            raise StorageError("cannot install immutable object") from error
        return sealed
