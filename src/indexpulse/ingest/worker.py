"""The ingest worker: changelog in, durable index state out.

The ordering in `_drain` is the whole crash-safety argument, and it is worth
stating plainly because every other arrangement of these four steps is wrong:

1. Apply the revisions to the document store.
2. Build the segment holding their vectors, complete, on disk.
3. Publish a manifest naming that segment and the changelog position past it.
4. Only now is the batch durable.

A kill between any two of them is survivable. Before step 3 the manifest still
points at the old position, so the batch is read again and reapplied, which the
store ignores because the versions are no longer newer. After step 3 the batch
is in the index and the position has moved past it. There is no window in which
the position has advanced past work the index does not hold, which is the window
`Safety.offsets_after_durable` opens on purpose for the negative control.

Nothing here handles its own death. A worker that caught the kill and tidied up
would be proving something about the handler rather than about the format on
disk, so the chaos driver sends a signal that cannot be caught.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from indexpulse.core import Manifest, Revision, Safety, SegmentKind, content_vector
from indexpulse.core.content import topic_basis
from indexpulse.index.compactor import compact, needs_compaction
from indexpulse.index.manifest_store import ManifestStore
from indexpulse.index.segment import write_segment
from indexpulse.ingest.changelog import REVISION_TOPIC, ChangelogConsumer
from indexpulse.store import DocumentStore


@dataclass(frozen=True)
class IngestConfig:
    root: Path
    store_path: Path
    bootstrap: str = "localhost:9092"
    topic: str = REVISION_TOPIC
    partitions: int = 4
    group: str = "indexpulse-ingest"
    dim: int = 128
    seal_size: int = 2_000
    seal_interval: float = 1.0
    poll_timeout: float = 0.25
    ef_search: int = 64
    compaction: bool = True
    threads: int = 0


@dataclass
class PendingBatch:
    """Vectors applied to the store but not yet in any published segment."""

    vectors: list[np.ndarray] = field(default_factory=list)
    doc_ids: list[int] = field(default_factory=list)
    versions: list[int] = field(default_factory=list)

    def add(self, doc_id: int, version: int, vector: np.ndarray) -> None:
        self.doc_ids.append(doc_id)
        self.versions.append(version)
        self.vectors.append(vector)

    def clear(self) -> None:
        self.vectors.clear()
        self.doc_ids.clear()
        self.versions.clear()

    def __len__(self) -> int:
        return len(self.doc_ids)


class IngestWorker:
    def __init__(self, config: IngestConfig, safety: Safety | None = None) -> None:
        self.config = config
        self.safety = safety or Safety()
        self.manifests = ManifestStore(config.root, self.safety)
        self.store = DocumentStore(config.store_path)
        self.consumer = ChangelogConsumer(
            config.bootstrap, config.topic, config.group, config.partitions
        )
        self.events = config.root / "worker-events.jsonl"
        self._basis = topic_basis(config.dim)
        self._pending = PendingBatch()
        self._offsets: dict[int, int] = {}
        self._applied = 0
        self._ignored = 0

    def run(self, until: float | None = None) -> None:
        manifest = self.manifests.load()
        self._offsets = dict(manifest.offsets)
        self.consumer.resume_from(self._offsets)
        self._record("start", resumed_from=dict(self._offsets), generation=manifest.generation)

        deadline = None if until is None else time.time() + until
        last_seal = time.time()
        try:
            while deadline is None or time.time() < deadline:
                drained = self._drain()
                due = len(self._pending) >= self.config.seal_size
                stale = self._pending and time.time() - last_seal >= self.config.seal_interval
                if due or stale:
                    self._seal()
                    last_seal = time.time()
                elif not drained:
                    # Nothing arrived and nothing is waiting to be sealed, so
                    # the next poll can afford to block instead of spinning.
                    continue
        finally:
            self.close()

    def _drain(self) -> int:
        records = self.consumer.poll(self.config.seal_size, self.config.poll_timeout)
        if not records:
            return 0

        revisions = [revision for _, _, revision in records]
        current, ignored = self.store.apply(revisions)
        self._applied += len(current)
        self._ignored += ignored

        # Deletes never come back from apply. A delete adds no vector, and the
        # store saying the page is gone already keeps every older vector of it
        # out of a result; compaction is what eventually removes them.
        for revision in current:
            self._pending.add(
                revision.doc_id,
                revision.version,
                content_vector(revision.content, self.config.dim, self._basis),
            )

        for partition, offset, _ in records:
            self._offsets[partition] = offset + 1

        if not self.safety.offsets_after_durable:
            self._publish_offsets_early()
        return len(records)

    def _publish_offsets_early(self) -> None:
        """Record a position the index has not reached, for the negative control.

        Whatever is pending when the worker is killed is never read again and
        never reaches a segment, so the store holds a version that no segment
        can answer for.
        """
        manifest = self.manifests.load()
        self.manifests.publish(
            manifest.advanced(
                segments=manifest.segments,
                offsets=self._offsets,
                written_at=time.time(),
            )
        )

    def _seal(self) -> None:
        pending = self._pending
        # Flushed before the segment is named anywhere, so a searcher that
        # catches its store up on seeing the new generation finds the versions
        # the segment refers to already there.
        self.store.flush()

        segment_id = f"delta-{uuid.uuid4().hex[:12]}"
        vectors = (
            np.vstack(pending.vectors)
            if pending.vectors
            else np.empty((0, self.config.dim), dtype=np.float32)
        )
        ref = write_segment(
            self.manifests.root,
            segment_id,
            vectors=vectors,
            doc_ids=np.array(pending.doc_ids, dtype=np.uint64),
            versions=np.array(pending.versions, dtype=np.uint64),
            kind=SegmentKind.DELTA,
            published_at=time.time(),
            threads=self.config.threads,
        )

        manifest = self.manifests.load()
        published = manifest.advanced(
            segments=(*manifest.segments, ref),
            offsets=self._offsets,
            written_at=time.time(),
        )
        self.manifests.publish(published)
        self._record(
            "seal",
            segment=segment_id,
            count=ref.count,
            generation=published.generation,
            offsets=dict(self._offsets),
            applied=self._applied,
            ignored=self._ignored,
        )
        pending.clear()

        if self.config.compaction and needs_compaction(published):
            self._compact(published)

    def _compact(self, manifest: Manifest) -> None:
        started = time.time()
        after, dropped = compact(
            self.manifests,
            self.store,
            manifest,
            ef_search=self.config.ef_search,
            threads=self.config.threads,
        )
        self._record(
            "compact",
            generation=after.generation,
            segments_before=len(manifest.segments),
            segments_after=len(after.segments),
            dropped=dropped,
            seconds=round(time.time() - started, 3),
        )

    def _record(self, kind: str, **fields: object) -> None:
        """Append one line, which has to survive the kill that may follow it.

        Opened and closed per event rather than held open, because a buffered
        handle in a process that is about to be killed loses exactly the events
        the audit most wants.
        """
        event = {"at": time.time(), "kind": kind, "pid": os.getpid(), **fields}
        with open(self.events, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event) + "\n")

    def close(self) -> None:
        self.consumer.close()
        self.store.close()


def seed_corpus(
    config: IngestConfig,
    revisions: list[Revision],
    *,
    safety: Safety | None = None,
) -> Manifest:
    """Build the starting index directly, without going through the changelog.

    The 1.2 million pages that are already there when the run begins are not
    what the claim is about, and pushing them through the broker would add an
    hour to every run to exercise the same path the 180 thousand revisions do.
    """
    manifests = ManifestStore(config.root, safety or Safety())
    basis = topic_basis(config.dim)
    with DocumentStore(config.store_path) as store:
        current, _ = store.apply(revisions)
        store.flush()
        vectors = np.vstack([content_vector(r.content, config.dim, basis) for r in current])
        ref = write_segment(
            manifests.root,
            f"base-{uuid.uuid4().hex[:12]}",
            vectors=vectors,
            doc_ids=np.array([r.doc_id for r in current], dtype=np.uint64),
            versions=np.array([r.version for r in current], dtype=np.uint64),
            kind=SegmentKind.BASE,
            published_at=time.time(),
            threads=config.threads,
        )
    published = Manifest(generation=0).advanced(segments=(ref,), offsets={}, written_at=time.time())
    manifests.publish(published)
    return published
