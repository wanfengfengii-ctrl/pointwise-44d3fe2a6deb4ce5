from dataclasses import dataclass
import json
import os
import re
import sqlite3
from monitorlake.catalog.database import EMPTY_SNAPSHOT_ID
from monitorlake.catalog.objects import SealedObject
from monitorlake.intake.batch import candidate_digest
from monitorlake.schema.errors import StorageError
from monitorlake.schema.validation import validate_observation
from monitorlake.segments.layout import SegmentDescriptor, V1_VERSION
from monitorlake.segments.paged_format import PAGED_VERSION

COPY_CHUNK = 4096
_KNOWN_VERSIONS = (V1_VERSION, PAGED_VERSION)


@dataclass(frozen=True)
class PublicationRecord:
    batch_id: str
    snapshot_id: int
    row_count: int


@dataclass(frozen=True)
class BatchEntry:
    batch_id: str
    source_description: str
    identity: str
    semantic_digest: str
    sealed: SealedObject
    first_snapshot: int

    @property
    def row_count(self):
        return self.sealed.descriptor.row_count

    @property
    def version(self):
        return self.sealed.descriptor.version

    def receipt(self):
        return PublicationRecord(self.batch_id, self.first_snapshot, self.row_count)


@dataclass(frozen=True)
class SnapshotEntry:
    snapshot_id: int
    batches: tuple


@dataclass(frozen=True)
class BatchHeader:
    """Catalog metadata needed for replay, without the stored identity text."""

    batch_id: str
    source_description: str
    semantic_digest: str
    sealed: SealedObject
    first_snapshot: int
    row_id: int

    @property
    def row_count(self):
        return self.sealed.descriptor.row_count

    @property
    def version(self):
        return self.sealed.descriptor.version

    def receipt(self):
        return PublicationRecord(self.batch_id, self.first_snapshot, self.row_count)


COPY_CHUNK = 4096


def _is_digest(value):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{64}", value) is not None


def _paged_marker(identity, object_digest):
    try:
        marker = json.loads(identity)
    except (ValueError, TypeError) as error:
        raise ValueError("invalid paged identity marker") from error
    if type(marker) is not dict or set(marker) != {"object"}:
        raise ValueError("noncanonical paged identity marker")
    if marker["object"] != object_digest:
        raise ValueError("paged identity marker does not pin its object")
    canonical = json.dumps(marker, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    if canonical != identity:
        raise ValueError("paged identity marker is not canonical")
    return marker


class CatalogRepository:
    def __init__(self, database):
        self.database = database

    def _execute(self, query, values=()):
        try:
            return self.database.require_open().execute(query, values)
        except sqlite3.Error as error:
            raise StorageError("catalog operation failed") from error

    def _batch_entry(self, row):
        try:
            if not isinstance(row["batch_id"], str) or not row["batch_id"]:
                raise ValueError("invalid batch ID")
            if not isinstance(row["source_description"], str):
                raise ValueError("invalid source")
            for field in ("semantic_digest", "object_digest"):
                if not _is_digest(row[field]):
                    raise ValueError("invalid digest")
            if type(row["row_count"]) is not int or row["row_count"] < 0:
                raise ValueError("invalid row count")
            if type(row["byte_count"]) is not int or row["byte_count"] <= 0:
                raise ValueError("invalid byte count")
            if type(row["version"]) is not int or row["version"] not in _KNOWN_VERSIONS:
                raise ValueError("unsupported segment version")
            if type(row["first_snapshot"]) is not int or row["first_snapshot"] <= 0:
                raise ValueError("invalid first receipt")
            identity = row["identity"]
            if row["version"] == V1_VERSION:
                self._validate_v1_identity(identity, row)
            else:
                _paged_marker(identity, row["object_digest"])
            descriptor = SegmentDescriptor(
                row["version"], row["row_count"], row["byte_count"], row["object_digest"]
            )
            return BatchEntry(
                row["batch_id"], row["source_description"], identity,
                row["semantic_digest"], SealedObject(descriptor.digest, descriptor),
                row["first_snapshot"],
            )
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as error:
            raise StorageError("invalid batch metadata") from error

    @staticmethod
    def _validate_v1_identity(identity, row):
        if candidate_digest(identity) != row["semantic_digest"]:
            raise ValueError("semantic identity digest mismatch")
        material = json.loads(identity)
        if type(material) is not dict or set(material) != {"source_description", "rows"}:
            raise ValueError("invalid semantic identity")
        if material["source_description"] != row["source_description"]:
            raise ValueError("identity source mismatch")
        if type(material["rows"]) is not list or len(material["rows"]) != row["row_count"]:
            raise ValueError("identity row count mismatch")
        for observation in material["rows"]:
            validate_observation(observation)
            if set(observation) != {"label", "samples"}:
                raise ValueError("noncanonical identity record")
        canonical = json.dumps(
            material, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        if canonical != identity:
            raise ValueError("identity is not canonical")

    def current_snapshot(self):
        row = self._execute("SELECT current_snapshot FROM state WHERE singleton=1").fetchone()
        if row is None or type(row[0]) is not int:
            raise StorageError("missing current snapshot pointer")
        if self._execute("SELECT 1 FROM snapshots WHERE snapshot_id=?", (row[0],)).fetchone() is None:
            raise StorageError("current snapshot does not exist")
        return row[0]

    def lookup_batch(self, batch_id):
        row = self._execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        return None if row is None else self._batch_entry(row)

    def lookup_header(self, batch_id):
        """Replay metadata only; never loads the stored identity text."""
        row = self._execute(
            "SELECT rowid,batch_id,source_description,semantic_digest,object_digest,"
            "byte_count,row_count,version,first_snapshot FROM batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            if not isinstance(row["batch_id"], str) or not row["batch_id"]:
                raise ValueError("invalid batch ID")
            if not isinstance(row["source_description"], str):
                raise ValueError("invalid source")
            for field in ("semantic_digest", "object_digest"):
                if not _is_digest(row[field]):
                    raise ValueError("invalid digest")
            if type(row["row_count"]) is not int or row["row_count"] < 0:
                raise ValueError("invalid row count")
            if type(row["byte_count"]) is not int or row["byte_count"] <= 0:
                raise ValueError("invalid byte count")
            if type(row["version"]) is not int or row["version"] not in _KNOWN_VERSIONS:
                raise ValueError("unsupported segment version")
            if type(row["first_snapshot"]) is not int or row["first_snapshot"] <= 0:
                raise ValueError("invalid first receipt")
            descriptor = SegmentDescriptor(
                row["version"], row["row_count"], row["byte_count"], row["object_digest"]
            )
            return BatchHeader(
                row["batch_id"], row["source_description"], row["semantic_digest"],
                SealedObject(descriptor.digest, descriptor), row["first_snapshot"],
                row["rowid"],
            )
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as error:
            raise StorageError("invalid batch metadata") from error

    def iter_stored_identity(self, header):
        """Yield the stored catalog identity text of a V1 batch in chunks.

        SQLite incremental BLOB I/O keeps only ``COPY_CHUNK`` bytes resident, so
        replay comparison never materialises a whole legacy identity.
        """
        connection = self.database.require_open()
        try:
            blob = connection.blobopen("batches", "identity", header.row_id, readonly=True)
        except sqlite3.Error as error:
            raise StorageError("cannot open stored semantic identity") from error
        try:
            while True:
                chunk = blob.read(COPY_CHUNK)
                if not chunk:
                    return
                yield chunk
        finally:
            blob.close()

    def get_snapshot(self, snapshot_id):
        snapshot = self._execute(
            "SELECT snapshot_id FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
        ).fetchone()
        if snapshot is None:
            raise ValueError("unknown snapshot")
        rows = list(self._execute(
            "SELECT m.position,b.* FROM members m LEFT JOIN batches b ON b.batch_id=m.batch_id "
            "WHERE m.snapshot_id=? ORDER BY m.position", (snapshot_id,)
        ))
        batches = []
        for expected, row in enumerate(rows):
            if row["position"] != expected or row["batch_id"] is None:
                raise StorageError("snapshot has missing or unordered members")
            batches.append(self._batch_entry(row))
        if snapshot_id == EMPTY_SNAPSHOT_ID and batches:
            raise StorageError("empty snapshot has members")
        return SnapshotEntry(snapshot_id, tuple(batches))

    def validate(self):
        snapshots = list(self._execute("SELECT * FROM snapshots ORDER BY snapshot_id"))
        if not snapshots or snapshots[0]["snapshot_id"] != EMPTY_SNAPSHOT_ID:
            raise StorageError("empty snapshot is missing")
        previous_id, previous_members = None, ()
        seen_batches = set()
        for row in snapshots:
            snapshot_id = row["snapshot_id"]
            if row["parent_id"] != previous_id:
                raise StorageError("snapshot parent chain is broken")
            snapshot = self.get_snapshot(snapshot_id)
            members = tuple(batch.batch_id for batch in snapshot.batches)
            if previous_id is not None:
                if len(members) != len(previous_members) + 1 or members[:-1] != previous_members:
                    raise StorageError("snapshot is not an immutable append")
                appended = snapshot.batches[-1]
                if appended.first_snapshot != snapshot_id or appended.batch_id in seen_batches:
                    raise StorageError("first publication receipt is inconsistent")
                seen_batches.add(appended.batch_id)
            previous_id, previous_members = snapshot_id, members
        if self.current_snapshot() != previous_id:
            raise StorageError("current pointer is not the latest publication")
        total = self._execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        if total != len(seen_batches):
            raise StorageError("unreferenced catalog batch")

    def record_publication(self, candidate, sealed):
        if not self.database.require_open().in_transaction:
            raise StorageError("publication requires an active transaction")
        previous = self.current_snapshot()
        parent = self.get_snapshot(previous)
        cursor = self._execute("INSERT INTO snapshots(parent_id) VALUES(?)", (previous,))
        snapshot_id = cursor.lastrowid
        descriptor = sealed.descriptor
        if descriptor.row_count != candidate.row_count:
            raise StorageError("candidate and sealed row counts disagree")
        self._execute(
            "INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?)",
            (candidate.batch_id, candidate.source_description, candidate.catalog_identity,
             candidate.digest, sealed.digest, descriptor.byte_count, descriptor.row_count,
             descriptor.version, snapshot_id),
        )
        self._execute(
            "INSERT INTO members(snapshot_id,position,batch_id) "
            "SELECT ?,position,batch_id FROM members WHERE snapshot_id=? ORDER BY position",
            (snapshot_id, previous),
        )
        self._execute(
            "INSERT INTO members VALUES(?,?,?)",
            (snapshot_id, len(parent.batches), candidate.batch_id),
        )
        self._execute("UPDATE state SET current_snapshot=? WHERE singleton=1", (snapshot_id,))
        return PublicationRecord(candidate.batch_id, snapshot_id, candidate.row_count)
