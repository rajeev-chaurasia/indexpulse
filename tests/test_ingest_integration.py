"""End to end against a real broker, skipped when there is not one.

These are the tests that would have caught every bug worth catching here, so
they run against Kafka rather than a stand-in. CI starts a broker for exactly
this reason.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from indexpulse.core import Safety, content_vector
from indexpulse.index.manifest_store import ManifestStore
from indexpulse.ingest import ChangelogPublisher, IngestConfig, IngestWorker
from indexpulse.ingest.changelog import ensure_topic
from indexpulse.ingest.worker import seed_corpus
from indexpulse.search import Searcher
from indexpulse.store import DocumentStore
from tests.conftest import revision

BOOTSTRAP = "localhost:9092"
DIM = 32


def broker_is_up() -> bool:
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
    except Exception:
        return False
    return True


pytestmark = pytest.mark.skipif(not broker_is_up(), reason="no broker on localhost:9092")


@pytest.fixture
def config(tmp_path: Path) -> IngestConfig:
    topic = f"indexpulse-test-{uuid.uuid4().hex[:8]}"
    ensure_topic(BOOTSTRAP, topic, 2)
    return IngestConfig(
        root=tmp_path / "index",
        store_path=tmp_path / "docs",
        bootstrap=BOOTSTRAP,
        topic=topic,
        partitions=2,
        group=f"g-{uuid.uuid4().hex[:8]}",
        dim=DIM,
        seal_size=50,
        seal_interval=0.2,
        poll_timeout=0.2,
        compaction=False,
    )


def searcher_for(config: IngestConfig, safety: Safety | None = None) -> Searcher:
    reader = DocumentStore(config.store_path, secondary_path=config.store_path.parent / "sec")
    found = Searcher(config.root, reader, safety=safety, ef_search=32)
    found.refresh()
    return found


def test_an_edit_becomes_searchable_at_its_new_content(config: IngestConfig) -> None:
    seed_corpus(config, [revision(i, 1, topic=1) for i in range(20)])

    edited = revision(3, 2, topic=1)
    publisher = ChangelogPublisher(BOOTSTRAP, config.topic)
    publisher.publish(edited)
    publisher.flush()

    IngestWorker(config).run(until=4.0)

    searcher = searcher_for(config)
    hits = searcher.search(content_vector(edited.content, DIM), k=5).hits
    assert (hits[0].doc_id, hits[0].version) == (3, 2)


def test_the_old_version_is_gone_from_results(config: IngestConfig) -> None:
    originals = [revision(i, 1, topic=1) for i in range(20)]
    seed_corpus(config, originals)

    publisher = ChangelogPublisher(BOOTSTRAP, config.topic)
    publisher.publish(revision(3, 2, topic=40))
    publisher.flush()
    IngestWorker(config).run(until=4.0)

    searcher = searcher_for(config)
    stale_query = content_vector(originals[3].content, DIM)
    served = searcher.search(stale_query, k=10)
    assert all(hit.version == 2 for hit in served.hits if hit.doc_id == 3)


def test_a_delete_stops_the_document_coming_back(config: IngestConfig) -> None:
    originals = [revision(i, 1, topic=1) for i in range(20)]
    seed_corpus(config, originals)

    publisher = ChangelogPublisher(BOOTSTRAP, config.topic)
    publisher.publish(revision(3, 2, delete=True))
    publisher.flush()
    IngestWorker(config).run(until=4.0)

    searcher = searcher_for(config)
    served = searcher.search(content_vector(originals[3].content, DIM), k=10)
    assert 3 not in {hit.doc_id for hit in served.hits}


def test_a_restarted_worker_resumes_from_the_manifest(config: IngestConfig) -> None:
    """The position in the manifest is the one that survives, not the broker's."""
    seed_corpus(config, [revision(i, 1, topic=1) for i in range(20)])

    publisher = ChangelogPublisher(BOOTSTRAP, config.topic)
    for doc_id in range(20):
        publisher.publish(revision(doc_id, 2, topic=1))
    publisher.flush()

    IngestWorker(config).run(until=3.0)
    first = ManifestStore(config.root).load()
    assert first.offsets

    IngestWorker(config).run(until=2.0)
    second = ManifestStore(config.root).load()
    assert second.offsets == first.offsets

    with DocumentStore(config.store_path) as store:
        assert all(store.get(doc_id).version == 2 for doc_id in range(20))  # type: ignore[union-attr]


def test_every_live_document_has_a_segment_holding_its_version(config: IngestConfig) -> None:
    """Lost revisions, measured directly rather than inferred from queries."""
    seed_corpus(config, [revision(i, 1, topic=1) for i in range(30)])
    publisher = ChangelogPublisher(BOOTSTRAP, config.topic)
    for doc_id in range(0, 30, 2):
        publisher.publish(revision(doc_id, 2, topic=2))
    publisher.flush()
    IngestWorker(config).run(until=4.0)

    indexed = searcher_for(config).indexed_versions()

    with DocumentStore(config.store_path) as store:
        live = {(s.doc_id, s.version) for s in store.scan() if not s.deleted}
    assert live - indexed == set()
