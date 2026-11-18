from dataclasses import dataclass
from pathlib import Path
import os
import re
from monitorlake.schema.errors import CorruptSegment, StorageError
from monitorlake.segments.checksum import verify_digest
from monitorlake.segments.decode import decode_v1
from monitorlake.segments.layout import V1_VERSION
from monitorlake.segments.paged_format import (
    PAGED_VERSION, open_reader, iter_row_events,
)

COPY_CHUNK = 4096


@dataclass(frozen=True)
class SealedObject:
    digest: str
    descriptor: object


def _files_equal(left, right):
    try:
        with Path(left).open("rb") as a, Path(right).open("rb") as b:
            while True:
                first = a.read(COPY_CHUNK)
                second = b.read(COPY_CHUNK)
                if first != second:
                    return False
                if not first:
                    return True
    except OSError as error:
        raise CorruptSegment("required object unavailable") from error


def validate_paged_object(path, descriptor):
    """Structurally validate a whole paged object with bounded memory.

    The file is hashed in chunks, the header and page lengths are checked and
    every page checksum and token stream is decoded one row at a time; no full
    body, row or page directory is retained.
    """
    if type(descriptor.version) is not int or descriptor.version != PAGED_VERSION:
        raise CorruptSegment("unsupported paged descriptor version")
    reader = open_reader(path, descriptor)
    try:
        for _ in range(reader.row_count):
            for _event in iter_row_events(reader):
                pass
        reader.finish()
    finally:
        reader.close()


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
        version = descriptor.version
        data = None
        if version == V1_VERSION:
            try:
                data = staged.path.read_bytes()
            except OSError as error:
                raise StorageError("candidate staging material unavailable") from error
            decode_v1(data, descriptor)
        elif version == PAGED_VERSION:
            validate_paged_object(staged.path, descriptor)
        else:
            raise CorruptSegment("unsupported segment version")
        sealed = SealedObject(descriptor.digest, descriptor)
        target = self.path_for(descriptor.digest)
        try:
            os.link(staged.path, target)
        except FileExistsError:
            if version == V1_VERSION:
                existing = self.read(sealed)
                decode_v1(existing, descriptor)
                if existing != data:
                    raise CorruptSegment("existing object differs from candidate")
            else:
                validate_paged_object(target, descriptor)
                if not _files_equal(target, staged.path):
                    raise CorruptSegment("existing object differs from candidate")
        except OSError as error:
            raise StorageError("cannot install immutable object") from error
        return sealed
