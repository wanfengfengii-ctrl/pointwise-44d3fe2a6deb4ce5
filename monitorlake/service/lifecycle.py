from pathlib import Path
from monitorlake.catalog.database import CatalogDatabase
from monitorlake.catalog.repository import CatalogRepository
from monitorlake.catalog.objects import ObjectStore
from monitorlake.intake.staging import BatchStager
from monitorlake.schema.errors import StorageError


class LakeSession:
    def __init__(self, root, database, repository, objects, stager):
        self.root = root
        self.database = database
        self.repository = repository
        self.objects = objects
        self.stager = stager
        self.closed = False

    @classmethod
    def open(cls, path):
        root = Path(path)
        database = None
        try:
            root.mkdir(parents=True, exist_ok=True)
            catalog_path = root / "catalog.sqlite3"
            if not catalog_path.exists() and any(root.iterdir()):
                raise StorageError("nonempty repository has no catalog; refusing to reset it")
            database = CatalogDatabase(catalog_path)
            repository = CatalogRepository(database)
            repository.validate()
            objects = ObjectStore(root / "objects")
            stager = BatchStager(root / "staging")
            return cls(root, database, repository, objects, stager)
        except OSError as error:
            if database is not None:
                database.close()
            raise StorageError("cannot initialize repository resources") from error
        except BaseException:
            if database is not None:
                database.close()
            raise

    def require_open(self):
        if self.closed:
            raise RuntimeError("lake handle is closed")

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.stager.close()
        finally:
            self.database.close()
