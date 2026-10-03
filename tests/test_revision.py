from __future__ import annotations

from indexpulse.core import DocumentState, Operation, Revision, apply_revision, document_key
from tests.conftest import revision


def test_first_revision_creates_state() -> None:
    state = apply_revision(None, revision(7, 1))
    assert state is not None
    assert (state.doc_id, state.version, state.deleted) == (7, 1, False)


def test_newer_revision_supersedes() -> None:
    first = apply_revision(None, revision(7, 1))
    second = apply_revision(first, revision(7, 2))
    assert second is not None
    assert second.version == 2


def test_replayed_revision_is_ignored() -> None:
    """Replay after a kill re-delivers what was already applied."""
    state = apply_revision(None, revision(7, 5))
    assert apply_revision(state, revision(7, 5)) is None
    assert apply_revision(state, revision(7, 4)) is None


def test_delete_marks_state_rather_than_removing_it() -> None:
    """A removed key would read as a document the store has never seen.

    The visibility rule treats those differently, and losing the distinction
    would hide a worker indexing ahead of the store.
    """
    state = apply_revision(apply_revision(None, revision(7, 1)), revision(7, 2, delete=True))
    assert state is not None
    assert state.deleted is True


def test_document_key_orders_by_id() -> None:
    assert document_key(2) < document_key(10)


def test_state_survives_a_round_trip() -> None:
    state = DocumentState(doc_id=3, version=9, deleted=False, updated_at=1.5)
    assert DocumentState.from_bytes(state.to_bytes()) == state


def test_revision_survives_a_round_trip() -> None:
    original = Revision(
        doc_id=3, version=9, operation=Operation.UPSERT, content="topic=1 body=ff", published_at=2.0
    )
    assert Revision.from_bytes(original.to_bytes()) == original
