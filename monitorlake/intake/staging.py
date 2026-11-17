from dataclasses import dataclass
from pathlib import Path
import os
import tempfile
from monitorlake.schema.errors import StorageError
from monitorlake.segments.checksum import verify_digest


@dataclass
class StagedBatch:
    path: Path
    descriptor: object
    published: bool = False


class BatchStager:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._owned = set()

    def stage(self, encoded):
        verify_digest(encoded.data, encoded.descriptor.digest, "encoded candidate")
        if len(encoded.data) != encoded.descriptor.byte_count:
            raise StorageError("encoded candidate length mismatch")
        fd, name = tempfile.mkstemp(prefix="candidate-", suffix=".tmp", dir=self.directory)
        path = Path(name)
        self._owned.add(path)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded.data)
                stream.flush()
                os.fsync(stream.fileno())
            return StagedBatch(path, encoded.descriptor)
        except BaseException:
            self._discard_path(path, suppress=True)
            raise

    def _discard_path(self, path, suppress=False):
        if path not in self._owned:
            return
        try:
            path.unlink(missing_ok=True)
        except OSError:
            if not suppress:
                raise
        else:
            self._owned.discard(path)

    def discard(self, staged, suppress=False):
        self._discard_path(staged.path, suppress=suppress)

    def mark_published(self, staged):
        if staged.path not in self._owned:
            raise StorageError("cannot publish unowned staging material")
        staged.published = True

    def close(self):
        for path in tuple(self._owned):
            self._discard_path(path, suppress=True)
