"""Deterministic vectors derived from page content.

Standing in for an embedding model, because the claim here is about freshness and
correctness under churn, not retrieval quality. Two properties are what the
benchmarks actually need, and a trained model would only make them harder to
reason about:

Re-ingesting the same revision must produce the same vector, so that replay after
a crash cannot be detected as a change. And an edit must move the page, so that a
segment still holding the old vector puts the document in a neighbourhood its
current content no longer belongs to. That is what makes a stale hit visible to
the audit rather than merely wrong by bookkeeping.

Pages sit in topic clusters so a nearest-neighbour query returns a related set
rather than noise, which keeps segment fan-out and the rerank depth honest.
"""

from __future__ import annotations

import hashlib

import numpy as np

CORPUS_TOPICS = 64

_BASIS_SEED = 20261102
_TOPIC_SPREAD = 0.35


def _digest_seed(text: str) -> int:
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest(), "big")


def topic_basis(dim: int) -> np.ndarray:
    """Unit centroids, one per topic, stable for a given dimension.

    Drawn once and cached by the caller rather than recomputed per document; at
    64 topics in 128 dimensions random directions are near enough to orthogonal
    that forcing orthogonality would add a QR decomposition for nothing.
    """
    rng = np.random.default_rng(_BASIS_SEED + dim)
    basis = rng.standard_normal((CORPUS_TOPICS, dim), dtype=np.float32)
    return basis / np.linalg.norm(basis, axis=1, keepdims=True)


def make_content(topic: int, body_seed: int) -> str:
    """Build a page body whose topic is recoverable from the string itself.

    The topic travels in the content rather than in a separate field so that a
    revision carries everything needed to vectorise it, and the ingest worker
    never has to consult the store to index a page.
    """
    return f"topic={topic % CORPUS_TOPICS} body={body_seed:016x}"


def parse_topic(content: str) -> int:
    head, _, _ = content.partition(" ")
    return int(head.removeprefix("topic=")) % CORPUS_TOPICS


def content_vector(content: str, dim: int, basis: np.ndarray | None = None) -> np.ndarray:
    """Place a page near its topic centroid, offset by a hash of its body."""
    centroids = topic_basis(dim) if basis is None else basis
    rng = np.random.default_rng(_digest_seed(content))
    offset = rng.standard_normal(dim, dtype=np.float32) * _TOPIC_SPREAD
    vector = centroids[parse_topic(content)] + offset
    unit: np.ndarray = (vector / np.linalg.norm(vector)).astype(np.float32)
    return unit


def topic_query(topic: int, variant: int, dim: int, basis: np.ndarray | None = None) -> np.ndarray:
    """A query aimed at one topic, tighter than the pages scattered around it."""
    centroids = topic_basis(dim) if basis is None else basis
    rng = np.random.default_rng(_digest_seed(f"query={topic}:{variant}"))
    offset = rng.standard_normal(dim, dtype=np.float32) * (_TOPIC_SPREAD / 2)
    vector = centroids[topic % CORPUS_TOPICS] + offset
    unit: np.ndarray = (vector / np.linalg.norm(vector)).astype(np.float32)
    return unit
