"""Driving one measured run.

Revisions are published at a steady rate rather than dumped into the changelog
up front. A backlog would make update-to-search freshness a measure of how fast
the worker drains a queue, which is not the number anybody cares about; at a
steady rate it measures what it claims to, the delay between a page changing and
a search reflecting the change.

Readers, the publisher and the kill driver all run against one index at the same
time, because every interesting failure here comes from that overlap and a run
that serialises them proves nothing.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import queue
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from bench.corpus import CorpusSpec, base_states, base_vectors, edit_stream
from indexpulse.core import (
    CORPUS_TOPICS,
    Manifest,
    Revision,
    Safety,
    SegmentKind,
    content_vector,
    topic_query,
)
from indexpulse.core.content import topic_basis
from indexpulse.index.manifest_store import ManifestStore, TornManifest
from indexpulse.index.segment import write_segment
from indexpulse.ingest.changelog import ChangelogPublisher, ensure_topic
from indexpulse.search import Searcher
from indexpulse.store import DocumentStore

PROBE_TIMEOUT = 90.0


@dataclass(frozen=True)
class RunSpec:
    name: str
    out: Path
    root: Path
    store_path: Path
    corpus: CorpusSpec
    safety: Safety
    topic: str
    bootstrap: str = "localhost:9092"
    partitions: int = 4
    dim: int = 128
    readers: int = 100
    reader_qps: float = 1.0
    kills: int = 60
    publish_rate: float = 300.0
    probe_every: int = 60
    refresh_interval: float = 0.2
    drain_timeout: float = 900.0
    seal_size: int = 2000
    seal_interval: float = 1.0

    @property
    def publish_seconds(self) -> float:
        return self.corpus.revisions / self.publish_rate


@dataclass
class Collected:
    served: list[dict[str, object]] = field(default_factory=list)
    freshness: list[dict[str, object]] = field(default_factory=list)
    kills: list[dict[str, object]] = field(default_factory=list)
    published: int = 0
    torn_manifest_reads: int = 0
    missing_segment_reads: int = 0


class BenchRun:
    def __init__(self, spec: RunSpec) -> None:
        self.spec = spec
        self.collected = Collected()
        self.stop = threading.Event()
        self.publishing_done = threading.Event()
        self.probe_queue: queue.Queue[Revision] = queue.Queue(maxsize=4096)
        self.basis = topic_basis(spec.dim)
        self.searcher: Searcher | None = None
        self._reader_records: list[list[dict[str, object]]] = []
        self._probe_records: list[dict[str, object]] = []
        self._probe_lock = threading.Lock()

    # Setup ------------------------------------------------------------------

    def build_base_index(self) -> Manifest:
        """Create the index as it stands before the run begins.

        Built directly rather than through the changelog. The 1.2 million pages
        already present are not what is being tested, and pushing them through
        the broker would add an hour to every run to exercise a path the
        revisions exercise anyway.
        """
        spec = self.spec
        now = time.time()
        with DocumentStore(spec.store_path) as store:
            store.seed(base_states(spec.corpus, now))
            store.flush()

        vectors = base_vectors(spec.corpus, spec.dim)
        doc_ids = np.arange(spec.corpus.documents, dtype=np.uint64)
        ref = write_segment(
            spec.root,
            "base-initial",
            vectors=vectors,
            doc_ids=doc_ids,
            versions=np.ones(spec.corpus.documents, dtype=np.uint64),
            kind=SegmentKind.BASE,
            published_at=now,
        )
        del vectors
        manifests = ManifestStore(spec.root, spec.safety)
        published = Manifest(generation=0).advanced(
            segments=(ref,), offsets={}, written_at=time.time()
        )
        manifests.publish(published)
        return published

    # Threads ----------------------------------------------------------------

    def _publish(self) -> None:
        spec = self.spec
        publisher = ChangelogPublisher(spec.bootstrap, spec.topic)
        path = spec.out / "revisions.jsonl.gz"
        started = time.time()
        count = 0
        with gzip.open(path, "wt", encoding="utf-8") as log:
            for revision in edit_stream(spec.corpus):
                if self.stop.is_set():
                    break
                # Stamped at the moment it is produced, because the audit grades
                # served hits against when a revision entered the changelog.
                sent = revision.model_copy(update={"published_at": time.time()})
                publisher.publish(sent)
                log.write(
                    json.dumps(
                        {
                            "doc_id": sent.doc_id,
                            "version": sent.version,
                            "operation": sent.operation.value,
                            "published_at": sent.published_at,
                        }
                    )
                    + "\n"
                )
                count += 1
                if sent.content and count % spec.probe_every == 0:
                    # Dropped rather than blocking the publisher behind the
                    # probers, which would distort the rate being measured.
                    with contextlib.suppress(queue.Full):
                        self.probe_queue.put_nowait(sent)

                target = started + count / spec.publish_rate
                drift = target - time.time()
                if drift > 0:
                    time.sleep(drift)
        publisher.flush()
        self.collected.published = count
        self.publishing_done.set()

    def _refresh(self) -> None:
        assert self.searcher is not None
        while not self.stop.is_set():
            with contextlib.suppress(TornManifest):
                self.searcher.refresh()
            time.sleep(self.spec.refresh_interval)

    def _read(self, slot: int) -> None:
        assert self.searcher is not None
        records = self._reader_records[slot]
        rng = np.random.default_rng(self.spec.corpus.seed + 1000 + slot)
        interval = 1.0 / self.spec.reader_qps
        while not self.stop.is_set():
            cycle = time.time()
            topic = int(rng.integers(CORPUS_TOPICS))
            query = topic_query(topic, int(rng.integers(1_000_000)), self.spec.dim, self.basis)
            started = time.perf_counter()
            result = self.searcher.search(query, k=10)
            elapsed = (time.perf_counter() - started) * 1000.0
            records.append(
                {
                    "at": time.time(),
                    "latency_ms": round(elapsed, 3),
                    "generation": result.generation,
                    "segments": result.segments_searched,
                    "hits": [[hit.doc_id, hit.version] for hit in result.hits],
                    "rejected": {reason.value: n for reason, n in result.rejected.items()},
                }
            )
            delay = interval - (time.time() - cycle)
            if delay > 0:
                time.sleep(delay)

    def _probe(self) -> None:
        """Measure how long an edit takes to become searchable at its new content.

        Queried with the document's own new vector, so the probe asks exactly
        what the claim promises: is this page findable at what it now says.
        """
        assert self.searcher is not None
        while not self.stop.is_set():
            try:
                revision = self.probe_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            query = content_vector(revision.content, self.spec.dim, self.basis)
            deadline = revision.published_at + PROBE_TIMEOUT
            visible_at = None
            outcome = "timeout"
            while time.time() < deadline and not self.stop.is_set():
                result = self.searcher.search(query, k=5)
                mine = [hit for hit in result.hits if hit.doc_id == revision.doc_id]
                if any(hit.version == revision.version for hit in mine):
                    visible_at = time.time()
                    outcome = "visible"
                    break
                if any(hit.version > revision.version for hit in mine):
                    # A later edit of the same page landed first, so this
                    # version can never appear and waiting for it would be
                    # recorded as a freshness failure it is not.
                    outcome = "superseded"
                    break
                time.sleep(0.05)
            delay = None if visible_at is None else round(visible_at - revision.published_at, 4)
            record: dict[str, object] = {
                "doc_id": revision.doc_id,
                "version": revision.version,
                "published_at": revision.published_at,
                "visible_at": visible_at,
                "outcome": outcome,
                "seconds": delay,
            }
            with self._probe_lock:
                self._probe_records.append(record)

    def _spawn_worker(self) -> subprocess.Popen[bytes]:
        spec = self.spec
        command = [
            sys.executable,
            "-m",
            "indexpulse.cli",
            "ingest",
            "--root",
            str(spec.root),
            "--store",
            str(spec.store_path),
            "--bootstrap",
            spec.bootstrap,
            "--topic",
            spec.topic,
            "--partitions",
            str(spec.partitions),
            "--dim",
            str(spec.dim),
            "--seal-size",
            str(spec.seal_size),
            "--seal-interval",
            str(spec.seal_interval),
        ]
        disabled = ",".join(spec.safety.disabled_names)
        if disabled:
            command += ["--unsafe", disabled]
        return subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _chaos(self) -> None:
        """Kill the worker outright, repeatedly, at points it cannot anticipate.

        SIGKILL rather than a request to shut down, because a worker that gets
        to tidy up proves something about its shutdown path and nothing about
        the format it leaves on disk.
        """
        spec = self.spec
        interval = spec.publish_seconds / max(spec.kills, 1)
        rng = np.random.default_rng(spec.corpus.seed + 7)
        worker = self._spawn_worker()
        for index in range(spec.kills):
            wait = interval * float(rng.uniform(0.6, 1.4))
            if self.stop.wait(wait):
                break
            started = time.time()
            worker.kill()
            worker.wait()
            self.collected.kills.append(
                {"at": started, "index": index, "pid": worker.pid, "waited": round(wait, 3)}
            )
            worker = self._spawn_worker()
        self._final_worker = worker

    # Orchestration ----------------------------------------------------------

    def run(self) -> None:
        spec = self.spec
        spec.out.mkdir(parents=True, exist_ok=True)
        ensure_topic(spec.bootstrap, spec.topic, spec.partitions)
        self.build_base_index()

        reader_store = DocumentStore(spec.store_path, secondary_path=spec.root / "secondary")
        self.searcher = Searcher(spec.root, reader_store, safety=spec.safety, ef_search=64)
        self.searcher.refresh()

        self._reader_records = [[] for _ in range(spec.readers)]
        threads = [threading.Thread(target=self._refresh, daemon=True)]
        threads += [
            threading.Thread(target=self._read, args=(slot,), daemon=True)
            for slot in range(spec.readers)
        ]
        threads += [threading.Thread(target=self._probe, daemon=True) for _ in range(4)]
        publisher = threading.Thread(target=self._publish, daemon=True)
        chaos = threading.Thread(target=self._chaos, daemon=True)

        for thread in threads:
            thread.start()
        publisher.start()
        chaos.start()

        self.publishing_done.wait(timeout=spec.publish_seconds * 3)
        chaos.join(timeout=60)
        self._drain()

        self.stop.set()
        for thread in threads:
            thread.join(timeout=5)
        publisher.join(timeout=10)

        worker = getattr(self, "_final_worker", None)
        if worker is not None:
            worker.kill()
            worker.wait()

        self.collected.torn_manifest_reads = self.searcher.torn_manifest_reads
        self.collected.missing_segment_reads = self.searcher.missing_segment_reads
        reader_store.close()
        self._write_records()
        self._write_final_state()

    def _write_final_state(self) -> None:
        state = _lost_revisions(self.spec.root, self.spec.store_path, self.spec.safety)
        (self.spec.out / "lost.json").write_text(
            json.dumps(state, indent=2) + "\n", encoding="utf-8"
        )

    def _drain(self) -> None:
        """Wait for the worker to catch up with everything that was published."""
        manifests = ManifestStore(self.spec.root, self.spec.safety)
        deadline = time.time() + self.spec.drain_timeout
        target = self.collected.published
        while time.time() < deadline:
            try:
                consumed = sum(manifests.load().offsets.values())
            except TornManifest:
                consumed = 0
            if target and consumed >= target:
                return
            time.sleep(1.0)

    def _write_records(self) -> None:
        out = self.spec.out
        served = [record for records in self._reader_records for record in records]
        served.sort(key=lambda record: float(record["at"]))  # type: ignore[arg-type]
        _write_gz(out / "served.jsonl.gz", served)
        _write_gz(out / "freshness.jsonl.gz", self._probe_records)
        _write_gz(out / "kills.jsonl.gz", self.collected.kills)

        events = self.spec.root / "worker-events.jsonl"
        if events.exists():
            with gzip.open(out / "worker-events.jsonl.gz", "wt", encoding="utf-8") as handle:
                handle.write(events.read_text(encoding="utf-8"))

        (out / "run.json").write_text(
            json.dumps(
                {
                    "name": self.spec.name,
                    "documents": self.spec.corpus.documents,
                    "revisions_published": self.collected.published,
                    "readers": self.spec.readers,
                    "kills": len(self.collected.kills),
                    "defences_disabled": list(self.spec.safety.disabled_names),
                    "torn_manifest_reads": self.collected.torn_manifest_reads,
                    "missing_segment_reads": self.collected.missing_segment_reads,
                    "queries": len(served),
                    "probes": len(self._probe_records),
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


def _lost_revisions(root: Path, store_path: Path, safety: Safety) -> dict[str, object]:
    """Live documents whose current version no segment can answer for.

    Measured against the index rather than inferred from queries, because a
    document can be missing from a result for want of ranking as easily as for
    want of a vector, and only one of those is a lost revision.
    """
    with DocumentStore(store_path) as store:
        searcher = Searcher(root, store, safety=safety, ef_search=64)
        searcher.refresh()
        indexed = searcher.indexed_versions()
        live = [(state.doc_id, state.version) for state in store.scan() if not state.deleted]
    missing = sorted(pair for pair in live if pair not in indexed)
    return {
        "live_documents": len(live),
        "indexed_versions": len(indexed),
        "lost_revisions": len(missing),
        "examples": [{"doc_id": doc, "version": version} for doc, version in missing[:50]],
    }


def _write_gz(path: Path, records: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
