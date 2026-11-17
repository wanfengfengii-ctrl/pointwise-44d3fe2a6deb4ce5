class StorageError(RuntimeError):
    """The persistent warehouse cannot satisfy its storage contract."""


class CorruptSegment(StorageError):
    """A required immutable segment is absent, damaged or malformed."""
