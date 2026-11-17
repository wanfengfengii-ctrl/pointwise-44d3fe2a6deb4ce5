import hashlib
import hmac
import re
from monitorlake.schema.errors import CorruptSegment


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def verify_digest(data, expected, context="segment"):
    if not isinstance(expected, str) or re.fullmatch("[0-9a-f]{64}", expected) is None:
        raise CorruptSegment(f"{context}: invalid SHA-256 descriptor")
    actual = sha256_bytes(data)
    if not hmac.compare_digest(actual, expected):
        raise CorruptSegment(f"{context}: SHA-256 mismatch")
