from monitorlake.segments.decode import decode_v1


class SnapshotScan:
    def __init__(self, repository, objects, snapshot_id, require_open):
        self.repository = repository
        self.objects = objects
        self.require_open = require_open
        self.require_open()
        self.snapshot = repository.get_snapshot(snapshot_id)
        self.snapshot_id = snapshot_id

    def iter_blocks(self):
        self.require_open()
        for batch in self.snapshot.batches:
            self.require_open()
            data = self.objects.read(batch.sealed)
            block = decode_v1(data, batch.sealed.descriptor)
            yield batch, block
