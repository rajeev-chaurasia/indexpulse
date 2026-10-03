from __future__ import annotations

from indexpulse.core import DocumentState, Rejection, classify


def state(version: int, *, deleted: bool = False) -> DocumentState:
    return DocumentState(doc_id=1, version=version, deleted=deleted, updated_at=0.0)


def test_current_version_is_served() -> None:
    assert classify(4, state(4)) is None


def test_superseded_version_is_rejected() -> None:
    assert classify(3, state(4)) is Rejection.SUPERSEDED


def test_deleted_document_is_rejected_at_any_version() -> None:
    assert classify(4, state(4, deleted=True)) is Rejection.DELETED


def test_unknown_document_is_rejected() -> None:
    """A segment ahead of the store is a failure, not a document to serve."""
    assert classify(1, None) is Rejection.UNKNOWN_DOCUMENT


def test_version_ahead_of_the_store_is_rejected() -> None:
    assert classify(5, state(4)) is Rejection.SUPERSEDED
