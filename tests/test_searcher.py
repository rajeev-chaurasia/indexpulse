from __future__ import annotations

import time
from pathlib import Path

import numpy as np

from indexpulse.core import (
    Rejection,
    Revision,
    Safety,
    SegmentKind,
    SegmentRef,
    content_vector,
    make_content,
    topic_query,
)
from indexpulse.index.manifest_store import ManifestStore
from indexpulse.index.segment import write_segment
from indexpulse.search import Searcher
from indexpulse.store import DocumentStore
from tests.conftest import DIM, revision


def build_segment(root: Path, segment_id: str, revisions: list[Revision]) -> SegmentRef:
    vectors = np.vstack([content_vector(rev.content, DIM) for rev in revisions])
    return write_segment(
        root,
        segment_id,
        vectors=vectors,
        doc_ids=np.array([rev.doc_id for rev in revisions], dtype=np.uint64),
        versions=np.array([rev.version for rev in revisions], dtype=np.uint64),
        kind=SegmentKind.DELTA,
        published_at=time.time(),
    )


def publish(root: Path, *refs: SegmentRef) -> None:
    store = ManifestStore(root)
    previous = store.load()
    store.publish(previous.advanced(segments=refs, offsets={}, written_at=0.0))


def test_a_superseded_version_in_a_segment_is_not_served(
    tmp_path: Path, store: DocumentStore
) -> None:
    """The whole claim, at the smallest scale that can show it.

    The segment still holds version one because segments are never edited. The
    store has moved to version two. The hit must not come back.
    """
    old = revision(1, 1, topic=3)
    store.apply([old])
    publish(tmp_path, build_segment(tmp_path, "s1", [old]))

    store.apply([revision(1, 2, topic=3)])

    searcher = Searcher(tmp_path, store, ef_search=16)
    searcher.refresh()
    result = searcher.search(content_vector(old.content, DIM), k=5)

    assert result.hits == []
    assert result.rejected == {Rejection.SUPERSEDED: 1}


def test_a_deleted_document_is_not_served(tmp_path: Path, store: DocumentStore) -> None:
    live = revision(1, 1, topic=3)
    store.apply([live])
    publish(tmp_path, build_segment(tmp_path, "s1", [live]))
    store.apply([revision(1, 2, delete=True)])

    searcher = Searcher(tmp_path, store, ef_search=16)
    searcher.refresh()
    result = searcher.search(content_vector(live.content, DIM), k=5)

    assert result.hits == []
    assert result.rejected == {Rejection.DELETED: 1}


def test_the_current_version_is_served(tmp_path: Path, store: DocumentStore) -> None:
    current = revision(1, 1, topic=3)
    store.apply([current])
    publish(tmp_path, build_segment(tmp_path, "s1", [current]))

    searcher = Searcher(tmp_path, store, ef_search=16)
    searcher.refresh()
    result = searcher.search(content_vector(current.content, DIM), k=5)

    assert [hit.doc_id for hit in result.hits] == [1]
    assert result.rejected == {}


def test_turning_the_rule_off_serves_the_stale_version(
    tmp_path: Path, store: DocumentStore
) -> None:
    """The negative control, as a unit test.

    If this ever stops returning the stale hit, the benchmark's unsafe run
    cannot be trusted to prove the audit sees anything.
    """
    old = revision(1, 1, topic=3)
    store.apply([old])
    publish(tmp_path, build_segment(tmp_path, "s1", [old]))
    store.apply([revision(1, 2, topic=3)])

    searcher = Searcher(tmp_path, store, safety=Safety(visibility=False), ef_search=16)
    searcher.refresh()
    result = searcher.search(content_vector(old.content, DIM), k=5)

    assert [(hit.doc_id, hit.version) for hit in result.hits] == [(1, 1)]


def test_a_document_in_two_segments_is_returned_once(tmp_path: Path, store: DocumentStore) -> None:
    first = revision(1, 1, topic=3)
    second = revision(1, 2, topic=3)
    store.apply([first])
    store.apply([second])
    publish(
        tmp_path,
        build_segment(tmp_path, "s1", [first]),
        build_segment(tmp_path, "s2", [second]),
    )

    searcher = Searcher(tmp_path, store, ef_search=16)
    searcher.refresh()
    result = searcher.search(content_vector(second.content, DIM), k=5)

    assert [(hit.doc_id, hit.version) for hit in result.hits] == [(1, 2)]
    assert result.rejected == {Rejection.SUPERSEDED: 1}


def test_refresh_reports_only_real_generation_changes(tmp_path: Path, store: DocumentStore) -> None:
    current = revision(1, 1, topic=3)
    store.apply([current])
    publish(tmp_path, build_segment(tmp_path, "s1", [current]))

    searcher = Searcher(tmp_path, store, ef_search=16)
    assert searcher.refresh() is True
    assert searcher.refresh() is False


def test_a_retired_segment_leaves_the_searcher(tmp_path: Path, store: DocumentStore) -> None:
    current = revision(1, 1, topic=3)
    store.apply([current])
    first = build_segment(tmp_path, "s1", [current])
    second = build_segment(tmp_path, "s2", [current])
    publish(tmp_path, first)
    searcher = Searcher(tmp_path, store, ef_search=16)
    searcher.refresh()

    publish(tmp_path, second)
    searcher.refresh()
    assert searcher.search(topic_query(3, 0, DIM), k=5).segments_searched == 1


def test_an_empty_index_answers_without_hits(tmp_path: Path, store: DocumentStore) -> None:
    searcher = Searcher(tmp_path, store, ef_search=16)
    searcher.refresh()
    result = searcher.search(content_vector(make_content(1, 1), DIM), k=5)
    assert result.hits == []
