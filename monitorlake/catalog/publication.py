from monitorlake.segments.encode import encode_v1


class PublicationCoordinator:
    def __init__(self, database, repository, objects, stager):
        self.database = database
        self.repository = repository
        self.objects = objects
        self.stager = stager

    def resolve_replay(self, candidate):
        existing = self.repository.lookup_batch(candidate.batch_id)
        if existing is None:
            return None
        if (
            existing.identity != candidate.identity
            or existing.semantic_digest != candidate.digest
            or existing.source_description != candidate.source_description
        ):
            raise ValueError("batch ID already published with different content")
        return existing.receipt()

    def publish(self, candidate, before_commit=None):
        receipt = self.resolve_replay(candidate)
        if receipt is not None:
            return receipt
        staged = self.stager.stage(encode_v1(candidate.records))
        try:
            sealed = self.objects.install(staged)
            with self.database.transaction():
                receipt = self.resolve_replay(candidate)
                if receipt is None:
                    receipt = self.repository.record_publication(candidate, sealed)
                    if before_commit is not None:
                        before_commit(receipt)
            self.stager.mark_published(staged)
            return receipt
        finally:
            self.stager.discard(staged, suppress=True)
