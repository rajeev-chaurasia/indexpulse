"""The corpus and the revision stream run against it.

Generated rather than downloaded, and generated from a seed so that a rerun on
another machine produces the same documents, the same edits and the same
deletes. The distribution is the one part worth arguing about: edits are aimed
disproportionately at a small hot set, because a uniform stream over 1.2 million
documents would almost never edit the same page twice and the superseded
versions compaction exists to clear would never pile up.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np

from indexpulse.core import (
    CORPUS_TOPICS,
    DocumentState,
    Operation,
    Revision,
    content_vector,
    make_content,
)
from indexpulse.core.content import topic_basis


@dataclass(frozen=True)
class CorpusSpec:
    documents: int = 1_200_000
    revisions: int = 180_000
    # Share of revisions aimed at the hot set, and how small that set is.
    hot_share: float = 0.35
    hot_fraction: float = 0.01
    delete_fraction: float = 0.12
    topic_change_fraction: float = 0.35
    seed: int = 20261102

    @property
    def hot_documents(self) -> int:
        return max(1, int(self.documents * self.hot_fraction))


def base_content(doc_id: int) -> str:
    return make_content(doc_id % CORPUS_TOPICS, doc_id)


def base_states(spec: CorpusSpec, at: float) -> Iterator[DocumentState]:
    for doc_id in range(spec.documents):
        yield DocumentState(doc_id=doc_id, version=1, deleted=False, updated_at=at)


def base_vectors(spec: CorpusSpec, dim: int) -> np.ndarray:
    """Fill one array rather than stacking a list of 1.2 million small ones."""
    basis = topic_basis(dim)
    vectors = np.empty((spec.documents, dim), dtype=np.float32)
    for doc_id in range(spec.documents):
        vectors[doc_id] = content_vector(base_content(doc_id), dim, basis)
    return vectors


def edit_content(spec: CorpusSpec, doc_id: int, version: int) -> str:
    """Rewrite a page, sometimes moving it to a different topic.

    A topic change is what makes a stale hit visible rather than merely wrong in
    the bookkeeping: the old vector keeps answering queries for a subject the
    page no longer covers.
    """
    rng = np.random.default_rng((doc_id << 20) ^ (version << 3) ^ spec.seed)
    if rng.random() < spec.topic_change_fraction:
        topic = int(rng.integers(CORPUS_TOPICS))
    else:
        topic = doc_id % CORPUS_TOPICS
    return make_content(topic, int(rng.integers(1 << 62)))


def edit_stream(spec: CorpusSpec) -> Iterator[Revision]:
    """The revisions published to the changelog, in order.

    Versions are assigned here so the stream is self-consistent before anything
    consumes it, which is what lets the audit grade the index against the
    changelog without consulting the index.
    """
    rng = np.random.default_rng(spec.seed + 1)
    versions: dict[int, int] = {}
    deleted: set[int] = set()
    hot = spec.hot_documents

    produced = 0
    while produced < spec.revisions:
        if rng.random() < spec.hot_share:
            doc_id = int(rng.integers(hot))
        else:
            doc_id = int(rng.integers(spec.documents))
        if doc_id in deleted:
            continue

        version = versions.get(doc_id, 1) + 1
        versions[doc_id] = version
        delete = rng.random() < spec.delete_fraction
        if delete:
            deleted.add(doc_id)

        yield Revision(
            doc_id=doc_id,
            version=version,
            operation=Operation.DELETE if delete else Operation.UPSERT,
            content="" if delete else edit_content(spec, doc_id, version),
            published_at=time.time(),
        )
        produced += 1
