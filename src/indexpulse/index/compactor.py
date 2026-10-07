"""Merging segments, which is the only thing that ever removes a vector.

Every edit adds a vector and every delete adds none, so without this the index
grows with the changelog and a query fans out across more and more segments to
find the same number of live documents. Compaction reads a set of segments,
keeps only the vectors whose version the store still considers current, and
writes one segment in their place.

The merged segment is written in full before any manifest names it, and the
segments it replaces are retired rather than deleted, so a compaction that is
killed half way through costs disk space and nothing else.
"""

from __future__ import annotations

import time
import uuid

import numpy as np

from indexpulse.core import Manifest, SegmentKind, SegmentRef, classify
from indexpulse.index.manifest_store import ManifestStore
from indexpulse.index.segment import LoadedSegment, write_segment
from indexpulse.store import DocumentStore

# Fanning out across more than this many segments is what compaction exists to
# prevent, so it is also what triggers it.
MAX_SEGMENTS = 10
MERGE_WIDTH = 8


def needs_compaction(manifest: Manifest) -> bool:
    return len(manifest.segments) >= MAX_SEGMENTS


def choose(manifest: Manifest) -> tuple[SegmentRef, ...]:
    """Merge the smallest segments first.

    Size-tiered rather than levelled, because the small delta segments are both
    the cheapest to rewrite and the ones multiplying the per-query fan-out. The
    large base segment is only pulled in once enough others have grown to meet
    it, which is what keeps the expensive full rebuild rare.
    """
    by_size = sorted(manifest.segments, key=lambda ref: ref.count)
    return tuple(by_size[:MERGE_WIDTH])


def compact(
    manifests: ManifestStore,
    store: DocumentStore,
    manifest: Manifest,
    *,
    ef_search: int,
    threads: int = 0,
) -> tuple[Manifest, int]:
    """Replace a set of segments with one holding only what is still current.

    Returns the published manifest and how many vectors were dropped, which is
    the backlog the tombstones and superseded versions had built up.
    """
    chosen = choose(manifest)
    if len(chosen) < 2:
        return manifest, 0

    kept_vectors: list[np.ndarray] = []
    kept_ids: list[int] = []
    kept_versions: list[int] = []
    dropped = 0

    for ref in chosen:
        segment = LoadedSegment(manifests.root, ref, ef_search)
        if ref.count == 0:
            continue
        doc_ids = segment.identity[:, 0].tolist()
        versions = segment.identity[:, 1].tolist()
        states = store.get_many(doc_ids)
        live_labels = []
        for label, (doc_id, version, state) in enumerate(
            zip(doc_ids, versions, states, strict=True)
        ):
            if classify(int(version), state) is not None:
                dropped += 1
                continue
            live_labels.append(label)
            kept_ids.append(int(doc_id))
            kept_versions.append(int(version))
        if live_labels:
            # Cosine space stores unit vectors, and these went in as unit
            # vectors, so reading them back and writing them into the merged
            # segment loses nothing.
            kept_vectors.append(np.asarray(segment.index.get_items(live_labels), dtype=np.float32))

    dim = chosen[0].dim
    vectors = np.vstack(kept_vectors) if kept_vectors else np.empty((0, dim), dtype=np.float32)
    merged = write_segment(
        manifests.root,
        f"base-{uuid.uuid4().hex[:12]}",
        vectors=vectors,
        doc_ids=np.array(kept_ids, dtype=np.uint64),
        versions=np.array(kept_versions, dtype=np.uint64),
        kind=SegmentKind.BASE,
        published_at=time.time(),
        threads=threads,
    )

    replaced = {ref.segment_id for ref in chosen}
    survivors = tuple(ref for ref in manifest.segments if ref.segment_id not in replaced)
    published = manifest.advanced(
        segments=(*survivors, merged),
        offsets=manifest.offsets,
        written_at=time.time(),
    )
    manifests.publish(published, retired=replaced)
    return published, dropped
