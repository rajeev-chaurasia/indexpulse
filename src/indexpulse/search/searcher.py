"""The read path.

A query goes to every live segment, the candidates are merged by distance, and
only then are they resolved against the document store. Resolving last is what
keeps the cost bounded: one multi-get of the few dozen candidates that survived
the merge, rather than a store lookup for every hit every segment returned.

The searcher reloads when the manifest's generation moves. Catching the store up
and loading new segments are two separate steps, and a reader that has done one
but not the other is safe either way round, because a version the store has not
heard of and a version the segments have not caught up to are both rejected. The
cost of that safety is a short window where a freshly edited document is in no
result at all, which is exactly what the freshness benchmark measures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from indexpulse.core import Rejection, Safety, classify
from indexpulse.index.manifest_store import ManifestStore, TornManifest
from indexpulse.index.segment import Hit, LoadedSegment
from indexpulse.store import DocumentStore

DEFAULT_EF_SEARCH = 64


@dataclass
class SearchResult:
    hits: list[Hit]
    generation: int
    segments_searched: int
    rejected: dict[Rejection, int] = field(default_factory=dict)

    @property
    def rejected_total(self) -> int:
        return sum(self.rejected.values())


class Searcher:
    def __init__(
        self,
        root: Path,
        store: DocumentStore,
        *,
        safety: Safety | None = None,
        ef_search: int = DEFAULT_EF_SEARCH,
    ) -> None:
        self.root = root
        self.store = store
        self.safety = safety or Safety()
        self.ef_search = ef_search
        self.manifests = ManifestStore(root, self.safety)
        self._segments: dict[str, LoadedSegment] = {}
        self._generation = -1
        self.torn_manifest_reads = 0
        self.missing_segment_reads = 0

    @property
    def generation(self) -> int:
        return self._generation

    def refresh(self) -> bool:
        """Adopt the newest published generation, returning whether anything moved."""
        try:
            manifest = self.manifests.load()
        except TornManifest:
            # Only reachable with the atomic swap off. Counted rather than
            # raised so the unsafe run produces a number next to the safe run's
            # zero instead of an exception trace.
            self.torn_manifest_reads += 1
            return False

        self.store.catch_up()
        if manifest.generation == self._generation:
            return False

        # Built to one side and swapped in by rebinding, so reader threads
        # calling search() never observe a half-updated segment set and need no
        # lock to be safe against this one.
        loaded: dict[str, LoadedSegment] = {}
        for ref in manifest.segments:
            cached = self._segments.get(ref.segment_id)
            if cached is not None:
                loaded[ref.segment_id] = cached
                continue
            try:
                loaded[ref.segment_id] = LoadedSegment(self.root, ref, self.ef_search)
            except FileNotFoundError:
                # A segment the manifest names has already been deleted, which
                # only happens when retirement skips the grace period.
                self.missing_segment_reads += 1

        self._segments = loaded
        self._generation = manifest.generation
        return True

    def indexed_versions(self) -> set[tuple[int, int]]:
        """Every document version the live segments can answer for.

        The audit compares this against the store to count revisions that were
        applied but never reached a segment, which is a loss a query-based
        check could only guess at.
        """
        held: set[tuple[int, int]] = set()
        for segment in self._segments.values():
            for doc_id, version in segment.identity:
                held.add((int(doc_id), int(version)))
        return held

    def search(self, query: np.ndarray, k: int = 10) -> SearchResult:
        depth = max(k * 4, k + 32)
        segments = self._segments
        candidates: list[Hit] = []
        for segment in segments.values():
            candidates.extend(segment.search(query, depth))
        candidates.sort(key=lambda hit: hit.distance)

        if not self.safety.visibility:
            return SearchResult(
                hits=self._best_per_document(candidates)[:k],
                generation=self._generation,
                segments_searched=len(segments),
            )

        rejected: dict[Rejection, int] = {}
        shortlist = candidates[:depth]
        states = self.store.get_many([hit.doc_id for hit in shortlist])
        kept: list[Hit] = []
        for hit, state in zip(shortlist, states, strict=True):
            reason = classify(hit.version, state)
            if reason is None:
                kept.append(hit)
            else:
                rejected[reason] = rejected.get(reason, 0) + 1

        return SearchResult(
            hits=self._best_per_document(kept)[:k],
            generation=self._generation,
            segments_searched=len(segments),
            rejected=rejected,
        )

    @staticmethod
    def _best_per_document(hits: list[Hit]) -> list[Hit]:
        """Collapse a document that several segments answered for.

        With the visibility rule on, at most one version of a document survives
        the filter, so this only ever folds duplicates of the same version. With
        it off, it is what lets the unsafe run return one stale row per document
        rather than a pile of them, which keeps the two runs comparable.
        """
        seen: set[int] = set()
        unique: list[Hit] = []
        for hit in hits:
            if hit.doc_id in seen:
                continue
            seen.add(hit.doc_id)
            unique.append(hit)
        return unique
