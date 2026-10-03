"""The rule that decides whether a search hit may be served.

Every claim this repository makes rests on this file. A segment is an immutable
snapshot of some past moment, so it will hold vectors for documents that have
since been edited or deleted. Filtering those out against the document store at
query time, rather than trying to keep every segment current, is what lets
segments stay immutable and still never serve a stale version.
"""

from __future__ import annotations

from enum import StrEnum

from indexpulse.core.revision import DocumentState


class Rejection(StrEnum):
    """Why a candidate was dropped, kept separate so each cause can be counted.

    A rule that never fires looks identical to a rule that is correct, so the
    benchmarks report these counts rather than only the absence of staleness.
    """

    UNKNOWN_DOCUMENT = "unknown_document"
    DELETED = "deleted"
    SUPERSEDED = "superseded"


def classify(hit_version: int, state: DocumentState | None) -> Rejection | None:
    """Return None when the hit may be served, otherwise why it may not.

    A missing state is not a tolerable oddity. It means a segment holds a vector
    for a document the store has never seen, which happens when a segment is
    published from a worker that wrote the index before the store, so it is
    rejected rather than served on the assumption that the store will catch up.
    """
    if state is None:
        return Rejection.UNKNOWN_DOCUMENT
    if state.deleted:
        return Rejection.DELETED
    if hit_version != state.version:
        return Rejection.SUPERSEDED
    return None
