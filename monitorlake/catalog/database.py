from contextlib import contextmanager
from pathlib import Path
import sqlite3
from monitorlake.schema.errors import StorageError

EMPTY_SNAPSHOT_ID = 0
CATALOG_VERSION = 1

SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER REFERENCES snapshots(snapshot_id)
);
CREATE TABLE batches (
    batch_id TEXT PRIMARY KEY,
    source_description TEXT NOT NULL,
    identity TEXT NOT NULL,
    semantic_digest TEXT NOT NULL,
    object_digest TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK(byte_count > 0),
    row_count INTEGER NOT NULL CHECK(row_count >= 0),
    version INTEGER NOT NULL,
    first_snapshot INTEGER NOT NULL UNIQUE REFERENCES snapshots(snapshot_id)
);
CREATE TABLE members (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
    position INTEGER NOT NULL CHECK(position >= 0),
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    PRIMARY KEY(snapshot_id, position),
    UNIQUE(snapshot_id, batch_id)
);
CREATE TABLE state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    current_snapshot INTEGER NOT NULL REFERENCES snapshots(snapshot_id)
);
INSERT INTO snapshots(snapshot_id,parent_id) VALUES(0,NULL);
INSERT INTO state(singleton,current_snapshot) VALUES(1,0);
PRAGMA user_version=1;
COMMIT;
"""


class CatalogDatabase:
    def __init__(self, path):
        self.path = Path(path)
        existed = self.path.exists()
        self.connection = None
        try:
            if existed and self.path.stat().st_size == 0:
                raise StorageError("existing catalog is empty or truncated")
            self.connection = sqlite3.connect(self.path, isolation_level=None)
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA busy_timeout=5000")
            self.initialize(new=not existed)
        except (sqlite3.Error, OSError) as error:
            self.close()
            raise StorageError("cannot open catalog") from error
        except BaseException:
            self.close()
            raise

    def require_open(self):
        if self.connection is None:
            raise RuntimeError("catalog is closed")
        return self.connection

    def initialize(self, new=False):
        connection = self.require_open()
        try:
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if new:
                if tables:
                    raise StorageError("new catalog unexpectedly contains tables")
                connection.executescript(SCHEMA)
            elif tables != {"snapshots", "batches", "members", "state"}:
                raise StorageError("catalog schema is incomplete or incompatible")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version != CATALOG_VERSION:
                raise StorageError("unsupported catalog version")
            checks = [row[0] for row in connection.execute("PRAGMA quick_check")]
            if checks != ["ok"] or list(connection.execute("PRAGMA foreign_key_check")):
                raise StorageError("catalog integrity check failed")
            states = list(connection.execute("SELECT * FROM state"))
            if len(states) != 1 or states[0]["singleton"] != 1:
                raise StorageError("catalog must contain exactly one current pointer")
            current = connection.execute(
                "SELECT 1 FROM snapshots WHERE snapshot_id=?", (states[0]["current_snapshot"],)
            ).fetchone()
            genesis = connection.execute(
                "SELECT parent_id FROM snapshots WHERE snapshot_id=0"
            ).fetchone()
            if current is None or genesis is None or genesis[0] is not None:
                raise StorageError("invalid current or empty snapshot")
        except sqlite3.Error as error:
            raise StorageError("invalid catalog metadata") from error

    @contextmanager
    def transaction(self):
        connection = self.require_open()
        if connection.in_transaction:
            raise StorageError("nested publication transaction")
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except sqlite3.Error as error:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise StorageError("catalog transaction failed") from error
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def close(self):
        connection, self.connection = self.connection, None
        if connection is not None:
            connection.close()
