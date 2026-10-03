from __future__ import annotations

import numpy as np

from indexpulse.core import CORPUS_TOPICS, content_vector, make_content, topic_query

DIM = 32


def test_same_content_gives_the_same_vector() -> None:
    """Replay must not look like a change."""
    content = make_content(3, 99)
    assert np.array_equal(content_vector(content, DIM), content_vector(content, DIM))


def test_an_edit_moves_the_vector() -> None:
    first = content_vector(make_content(3, 1), DIM)
    second = content_vector(make_content(3, 2), DIM)
    assert not np.allclose(first, second)


def test_vectors_are_unit_length() -> None:
    vector = content_vector(make_content(5, 7), DIM)
    assert np.isclose(np.linalg.norm(vector), 1.0, atol=1e-6)


def test_a_topic_change_moves_a_page_further_than_an_edit() -> None:
    """The audit needs a stale hit to land in the wrong neighbourhood."""
    base = content_vector(make_content(3, 1), DIM)
    edited = content_vector(make_content(3, 2), DIM)
    moved = content_vector(make_content(40, 2), DIM)
    assert np.dot(base, moved) < np.dot(base, edited)


def test_a_query_is_closer_to_its_own_topic() -> None:
    query = topic_query(11, 0, DIM)
    same = content_vector(make_content(11, 123), DIM)
    other = content_vector(make_content(44, 123), DIM)
    assert np.dot(query, same) > np.dot(query, other)


def test_topics_wrap_rather_than_raise() -> None:
    assert make_content(CORPUS_TOPICS + 2, 1).startswith("topic=2 ")
