"""The document store: what version of each document is current, on RocksDB.

Only the state is kept, not the page body. The body lives in the changelog,
which is the system of record, and storing it twice would add a gigabyte to the
corpus without touching anything the claim is about.

The writer opens the store as primary. Searchers run in their own processes and
open it as secondary, which gives a read-only view that advances when it is told
to. That is deliberate: a searcher that falls behind the writer serves fewer
results, never stale ones, because the visibility rule treats a version it has
not heard of as superseded.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import cast

from rocksdict import AccessType, Options, Rdict, WriteBatch

from indexpulse.core import DocumentState, Revision, apply_revision, document_key


def _tuned_options() -> Options:
    opts = Options()
    opts.create_if_missing(True)
    # The ingest run writes 180k small records in bursts between seals, so the
    # default 64 MB buffer would flush mid-burst and stall the seal it is racing.
    opts.set_write_buffer_size(128 * 1024 * 1024)
    opts.set_max_write_buffer_number(4)
    opts.increase_parallelism(4)
    return opts


def _passthrough(db: Rdict) -> None:
    """Store the JSON bytes as given instead of pickling them.

    Pickle would work and would also make the on-disk format depend on the
    Python version that wrote it, which is a poor trade for a store the audit
    has to read back.
    """
    db.set_dumps(lambda value: value)
    db.set_loads(lambda value: value)


class DocumentStore:
    def __init__(self, path: Path, *, secondary_path: Path | None = None) -> None:
        self.path = path
        self.read_only = secondary_path is not None
        if secondary_path is None:
            self._db = Rdict(str(path), _tuned_options())
        else:
            secondary_path.mkdir(parents=True, exist_ok=True)
            self._db = Rdict(
                str(path),
                _tuned_options(),
                access_type=AccessType.secondary(str(secondary_path)),
            )
        _passthrough(self._db)

    def catch_up(self) -> None:
        """Pull in what the writer has flushed since the last call.

        A secondary does not follow the primary on its own. Calling this is how a
        searcher chooses its own staleness bound, and the freshness benchmark
        measures the result rather than assuming it.
        """
        if self.read_only:
            self._db.try_catch_up_with_primary()

    def get(self, doc_id: int) -> DocumentState | None:
        raw = self._db.get(document_key(doc_id))
        return None if raw is None else DocumentState.from_bytes(raw)

    def get_many(self, doc_ids: Sequence[int]) -> list[DocumentState | None]:
        if not doc_ids:
            return []
        raws = cast("list[bytes | None]", self._db.get([document_key(d) for d in doc_ids]))
        return [None if raw is None else DocumentState.from_bytes(raw) for raw in raws]

    def apply(self, revisions: Sequence[Revision]) -> tuple[list[Revision], int]:
        """Fold a run of revisions in, returning the ones that took effect.

        Revisions that lost to a newer version already in the store are counted
        and dropped. After a crash the replay re-delivers a stretch of the
        changelog, and that count is how the benchmark shows replay was in fact
        idempotent rather than merely assumed to be.
        """
        if not revisions:
            return [], 0

        stored = self.get_many([revision.doc_id for revision in revisions])
        current = {state.doc_id: state for state in stored if state is not None}
        batch = WriteBatch()
        batch.set_dumps(lambda value: value)

        accepted: list[Revision] = []
        ignored = 0
        for revision in revisions:
            state = apply_revision(current.get(revision.doc_id), revision)
            if state is None:
                ignored += 1
                continue
            # Carried forward so two revisions of one document inside a single
            # batch resolve against each other, not against what was on disk
            # before the batch started.
            current[revision.doc_id] = state
            batch.put(document_key(state.doc_id), state.to_bytes())
            accepted.append(revision)

        self._db.write(batch)
        return accepted, ignored

    def seed(self, states: Iterable[DocumentState], batch_size: int = 50_000) -> int:
        """Load the initial corpus without the read-modify-write above.

        The corpus is built once from documents that are known not to exist yet,
        so checking each one against the store would double the cost of the
        slowest setup step for no result.
        """
        written = 0
        batch = WriteBatch()
        batch.set_dumps(lambda value: value)
        for state in states:
            batch.put(document_key(state.doc_id), state.to_bytes())
            written += 1
            if written % batch_size == 0:
                self._db.write(batch)
                batch = WriteBatch()
                batch.set_dumps(lambda value: value)
        self._db.write(batch)
        return written

    def scan(self) -> Iterator[DocumentState]:
        it = self._db.iter()
        it.seek_to_first()
        while it.valid():
            yield DocumentState.from_bytes(it.value())
            it.next()

    def counts(self) -> tuple[int, int]:
        """Total documents seen and how many are live, for the audit."""
        total = 0
        live = 0
        for state in self.scan():
            total += 1
            live += 0 if state.deleted else 1
        return total, live

    def flush(self) -> None:
        self._db.flush()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> DocumentStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
