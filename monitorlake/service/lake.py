from monitorlake.service.requests import prepare_write_request, validate_snapshot_request
from monitorlake.service.results import to_write_result
from monitorlake.service.lifecycle import LakeSession
from monitorlake.intake.batch import collect_batch
from monitorlake.catalog.publication import PublicationCoordinator
from monitorlake.consumption.scan import SnapshotScan
from monitorlake.consumption.rows import iter_rows
from monitorlake.consumption.summary import iter_summaries
from monitorlake.consumption.export import export_snapshot


class Lake:
    def __init__(self, path, *, before_commit=None):
        self._session = LakeSession.open(path)
        self.before_commit = before_commit
        self._publication = PublicationCoordinator(
            self._session.database, self._session.repository,
            self._session.objects, self._session.stager,
        )

    def write(self, batch_id, source_description, rows):
        self._session.require_open()
        request = prepare_write_request(batch_id, source_description, rows)
        candidate = collect_batch(
            request.batch_id, request.source_description, request.rows
        )
        self._session.require_open()
        publication = self._publication.publish(candidate, self.before_commit)
        return to_write_result(publication)

    def current_snapshot(self):
        self._session.require_open()
        return self._session.repository.current_snapshot()

    def _scan(self, snapshot_id):
        self._session.require_open()
        validate_snapshot_request(snapshot_id)
        return SnapshotScan(
            self._session.repository, self._session.objects,
            snapshot_id, self._session.require_open,
        )

    def read_rows(self, snapshot_id):
        return iter_rows(self._scan(snapshot_id))

    def summaries(self, snapshot_id):
        return iter_summaries(self._scan(snapshot_id))

    def export(self, snapshot_id, sink):
        return export_snapshot(self._scan(snapshot_id), sink)

    def close(self):
        self._session.close()

    def __enter__(self):
        self._session.require_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
