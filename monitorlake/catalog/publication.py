from contextlib import closing

from monitorlake.intake.identity import iter_paged_identity_chunks, streams_equal
from monitorlake.intake.paged_batch import PagedCandidate
from monitorlake.segments.encode import encode_v1


class PublicationCoordinator:
    def __init__(self, database, repository, objects, stager):
        self.database = database
        self.repository = repository
        self.objects = objects
        self.stager = stager

    def stored_identity(self, header):
        """Canonical identity bytes of one published batch, streamed."""
        if header.version == 1:
            return self.repository.iter_stored_identity(header)
        return iter_paged_identity_chunks(
            self.objects, header.sealed, header.source_description
        )

    def resolve_replay(self, candidate):
        existing = self.repository.lookup_header(candidate.batch_id)
        if existing is None:
            return None
        if (
            existing.source_description != candidate.source_description
            or existing.semantic_digest != candidate.digest
        ):
            raise ValueError("batch ID already published with different content")
        # A digest is necessary but never sufficient: compare the full ordered
        # canonical identity, streamed, before returning the first receipt.
        with closing(self.stored_identity(existing)) as stored, \
                closing(candidate.identity_stream()) as supplied:
            if not streams_equal(stored, supplied):
                raise ValueError("batch ID already published with different content")
        return existing.receipt()

    def _stage(self, candidate):
        if isinstance(candidate, PagedCandidate):
            return candidate.staged
        return self.stager.stage(encode_v1(candidate.records))

    def publish(self, candidate, before_commit=None):
        receipt = self.resolve_replay(candidate)
        if receipt is not None:
            return receipt
        staged = self._stage(candidate)
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
