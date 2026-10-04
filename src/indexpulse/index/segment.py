"""Immutable vector segments on disk.

A segment is an hnswlib graph plus the document identity of every vector in it,
written once and never modified. Edits and deletes are not applied to a segment;
they arrive as new segments and are resolved at query time by the visibility
rule, and only compaction ever drops anything.

That is the trade this repository is built on. It makes a segment safe to share
between processes with no locking, and it means the index grows with every edit
until compaction runs, which is what the lookup benchmark has to pay for.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import hnswlib
import numpy as np

from indexpulse.core import SegmentKind, SegmentRef

# M and ef_construction are the hnswlib defaults for a reason: the graph quality
# knobs are not what is under test, and moving them would make the lookup
# numbers a statement about tuning rather than about compaction.
GRAPH_DEGREE = 16
EF_CONSTRUCTION = 100
SPACE = "cosine"


def _paths(root: Path, segment_id: str) -> tuple[Path, Path]:
    return root / f"{segment_id}.hnsw", root / f"{segment_id}.npy"


def write_segment(
    root: Path,
    segment_id: str,
    *,
    vectors: np.ndarray,
    doc_ids: np.ndarray,
    versions: np.ndarray,
    kind: SegmentKind,
    published_at: float,
    threads: int = 0,
) -> SegmentRef:
    """Build a segment and leave it on disk, complete, before it is named anywhere.

    Nothing points at these files until a manifest naming the segment is
    published, so a worker killed part way through writing them leaves litter
    rather than a corrupt index.
    """
    root.mkdir(parents=True, exist_ok=True)
    count, dim = vectors.shape
    index = hnswlib.Index(space=SPACE, dim=dim)
    index.init_index(max_elements=max(count, 1), ef_construction=EF_CONSTRUCTION, M=GRAPH_DEGREE)
    if count:
        index.add_items(vectors, np.arange(count), num_threads=threads)

    graph_path, identity_path = _paths(root, segment_id)
    index.save_index(str(graph_path))
    identity = np.empty((count, 2), dtype=np.uint64)
    identity[:, 0] = doc_ids
    identity[:, 1] = versions
    np.save(identity_path, identity)

    return SegmentRef(
        segment_id=segment_id,
        kind=kind,
        count=count,
        dim=dim,
        published_at=published_at,
    )


@dataclass(frozen=True)
class Hit:
    doc_id: int
    version: int
    distance: float


class LoadedSegment:
    """One segment held open by a searcher, cached by id because ids never repeat."""

    def __init__(self, root: Path, ref: SegmentRef, ef_search: int) -> None:
        graph_path, identity_path = _paths(root, ref.segment_id)
        self.ref = ref
        self.identity = np.load(identity_path)
        self.index = hnswlib.Index(space=SPACE, dim=ref.dim)
        self.index.load_index(str(graph_path), max_elements=max(ref.count, 1))
        self.index.set_ef(ef_search)

    def search(self, query: np.ndarray, k: int) -> list[Hit]:
        if self.ref.count == 0:
            return []
        depth = min(k, self.ref.count)
        labels, distances = self.index.knn_query(query, k=depth, num_threads=1)
        rows = self.identity[labels[0]]
        return [
            Hit(doc_id=int(row[0]), version=int(row[1]), distance=float(distance))
            for row, distance in zip(rows, distances[0], strict=True)
        ]
