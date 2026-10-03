from __future__ import annotations

from pathlib import Path

from indexpulse.core import DocumentState
from indexpulse.store import DocumentStore
from tests.conftest import revision


def test_apply_returns_only_what_took_effect(store: DocumentStore) -> None:
    accepted, ignored = store.apply([revision(1, 1), revision(2, 1)])
    assert len(accepted) == 2
    assert ignored == 0


def test_replayed_batch_is_idempotent(store: DocumentStore) -> None:
    batch = [revision(1, 1), revision(2, 1)]
    store.apply(batch)
    accepted, ignored = store.apply(batch)
    assert accepted == []
    assert ignored == 2


def test_two_revisions_of_one_document_resolve_inside_a_batch(store: DocumentStore) -> None:
    """Without carrying state forward, the second would be graded against disk."""
    accepted, ignored = store.apply([revision(1, 1), revision(1, 2)])
    assert [rev.version for rev in accepted] == [1, 2]
    assert ignored == 0
    state = store.get(1)
    assert state is not None and state.version == 2


def test_out_of_order_revisions_do_not_move_a_document_backwards(store: DocumentStore) -> None:
    store.apply([revision(1, 5)])
    store.apply([revision(1, 3)])
    state = store.get(1)
    assert state is not None and state.version == 5


def test_get_many_keeps_the_order_it_was_asked_in(store: DocumentStore) -> None:
    store.apply([revision(1, 1), revision(3, 1)])
    states = store.get_many([3, 99, 1])
    assert [None if s is None else s.doc_id for s in states] == [3, None, 1]


def test_counts_separate_live_from_deleted(store: DocumentStore) -> None:
    store.apply([revision(1, 1), revision(2, 1), revision(3, 1)])
    store.apply([revision(2, 2, delete=True)])
    assert store.counts() == (3, 2)


def test_seed_loads_without_reading_first(store: DocumentStore) -> None:
    states = [DocumentState(doc_id=i, version=1, deleted=False, updated_at=0.0) for i in range(100)]
    assert store.seed(states) == 100
    assert store.counts() == (100, 100)


def test_secondary_sees_the_writer_only_after_catching_up(tmp_path: Path) -> None:
    """A searcher chooses its own staleness bound, which freshness then measures."""
    path = tmp_path / "docs"
    with DocumentStore(path) as primary:
        primary.apply([revision(1, 1)])
        primary.flush()
        with DocumentStore(path, secondary_path=tmp_path / "sec") as reader:
            assert reader.get(1) is not None
            primary.apply([revision(2, 1)])
            primary.flush()
            assert reader.get(2) is None
            reader.catch_up()
            assert reader.get(2) is not None
