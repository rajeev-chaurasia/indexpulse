from __future__ import annotations

from pathlib import Path

from indexpulse.core import DocumentState
from indexpulse.store import DocumentStore
from tests.conftest import revision


def test_apply_returns_the_revisions_now_current(store: DocumentStore) -> None:
    current, ignored = store.apply([revision(1, 1), revision(2, 1)])
    assert sorted(rev.doc_id for rev in current) == [1, 2]
    assert ignored == 0


def test_a_replayed_revision_is_still_reported_as_current(store: DocumentStore) -> None:
    """A worker killed before sealing must index these again on replay.

    The store already holds them, so nothing advances, but their vectors may
    never have reached a segment. Reporting only what advanced is what lost
    twenty two revisions in the first smoke run.
    """
    batch = [revision(1, 1), revision(2, 1)]
    store.apply(batch)
    current, ignored = store.apply(batch)
    assert sorted(rev.doc_id for rev in current) == [1, 2]
    assert ignored == 2


def test_a_replayed_revision_that_is_no_longer_current_is_not_reported(
    store: DocumentStore,
) -> None:
    """Indexing a superseded version again would only add work for compaction."""
    store.apply([revision(1, 1)])
    store.apply([revision(1, 2)])
    current, ignored = store.apply([revision(1, 1)])
    assert current == []
    assert ignored == 1


def test_a_replayed_delete_is_not_reported_as_current(store: DocumentStore) -> None:
    store.apply([revision(1, 1)])
    store.apply([revision(1, 2, delete=True)])
    current, ignored = store.apply([revision(1, 2, delete=True)])
    assert current == []
    assert ignored == 1


def test_two_revisions_of_one_document_resolve_inside_a_batch(store: DocumentStore) -> None:
    """Only the later one needs a vector, and the earlier must not get one."""
    current, ignored = store.apply([revision(1, 1), revision(1, 2)])
    assert [rev.version for rev in current] == [2]
    assert ignored == 0
    state = store.get(1)
    assert state is not None and state.version == 2


def test_a_delete_later_in_a_batch_withdraws_the_upsert(store: DocumentStore) -> None:
    current, _ = store.apply([revision(1, 1), revision(1, 2, delete=True)])
    assert current == []


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
