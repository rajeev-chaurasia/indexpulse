"""Tests for the grader, which has to be shown to fail before its passes count.

Every one of these plants a fault in the raw records and checks the audit sees
it. A grader that cannot be made to report staleness would report none on a
broken index just as happily as on a correct one.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

from bench.audit import (
    STALENESS_TOLERANCE,
    Timeline,
    audit,
    compaction_windows,
    grade_staleness,
    inside,
    percentiles,
)

BASE = 1_000_000.0


def write(path: Path, records: list[dict[str, object]]) -> Path:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    return path


def changelog(tmp_path: Path, *entries: tuple[int, int, str, float]) -> Timeline:
    path = write(
        tmp_path / "revisions.jsonl.gz",
        [
            {"doc_id": doc, "version": version, "operation": op, "published_at": BASE + offset}
            for doc, version, op, offset in entries
        ],
    )
    return Timeline.build(path)


def served(tmp_path: Path, *hits: tuple[float, int, int]) -> Path:
    return write(
        tmp_path / "served.jsonl.gz",
        [
            {"at": BASE + offset, "latency_ms": 1.0, "hits": [[doc, version]], "rejected": {}}
            for offset, doc, version in hits
        ],
    )


def test_a_version_superseded_long_ago_is_stale(tmp_path: Path) -> None:
    timeline = changelog(tmp_path, (7, 2, "upsert", 0.0))
    hits = served(tmp_path, (60.0, 7, 1))
    assert grade_staleness(hits, timeline, STALENESS_TOLERANCE).stale_hits == 1


def test_a_version_superseded_moments_ago_is_within_tolerance(tmp_path: Path) -> None:
    """The index is allowed to lag, which is what the tolerance is for."""
    timeline = changelog(tmp_path, (7, 2, "upsert", 0.0))
    hits = served(tmp_path, (5.0, 7, 1))
    assert grade_staleness(hits, timeline, STALENESS_TOLERANCE).stale_hits == 0


def test_the_strict_reading_counts_the_same_hit(tmp_path: Path) -> None:
    timeline = changelog(tmp_path, (7, 2, "upsert", 0.0))
    hits = served(tmp_path, (5.0, 7, 1))
    assert grade_staleness(hits, timeline, 0.0).stale_hits == 1


def test_the_current_version_is_not_stale(tmp_path: Path) -> None:
    timeline = changelog(tmp_path, (7, 2, "upsert", 0.0))
    hits = served(tmp_path, (60.0, 7, 2))
    assert grade_staleness(hits, timeline, STALENESS_TOLERANCE).stale_hits == 0


def test_a_document_never_edited_is_not_stale_at_version_one(tmp_path: Path) -> None:
    """Documents present before the run have no changelog entry of their own."""
    timeline = changelog(tmp_path, (99, 2, "upsert", 0.0))
    hits = served(tmp_path, (60.0, 7, 1))
    assert grade_staleness(hits, timeline, STALENESS_TOLERANCE).stale_hits == 0


def test_a_deleted_document_is_counted_separately(tmp_path: Path) -> None:
    timeline = changelog(tmp_path, (7, 2, "delete", 0.0))
    hits = served(tmp_path, (60.0, 7, 2))
    graded = grade_staleness(hits, timeline, STALENESS_TOLERANCE)
    assert (graded.deleted_hits, graded.stale_hits) == (1, 0)


def test_a_delete_still_in_flight_is_tolerated(tmp_path: Path) -> None:
    timeline = changelog(tmp_path, (7, 2, "delete", 0.0))
    hits = served(tmp_path, (3.0, 7, 1))
    assert grade_staleness(hits, timeline, STALENESS_TOLERANCE).deleted_hits == 0


def test_a_version_newer_than_required_is_not_stale(tmp_path: Path) -> None:
    """A searcher ahead of the tolerance window is early, not wrong."""
    timeline = changelog(tmp_path, (7, 2, "upsert", 0.0), (7, 3, "upsert", 55.0))
    hits = served(tmp_path, (60.0, 7, 3))
    assert grade_staleness(hits, timeline, STALENESS_TOLERANCE).stale_hits == 0


def test_examples_are_captured_for_every_fault(tmp_path: Path) -> None:
    timeline = changelog(tmp_path, (7, 4, "upsert", 0.0))
    hits = served(tmp_path, (60.0, 7, 1))
    graded = grade_staleness(hits, timeline, STALENESS_TOLERANCE)
    assert graded.examples[0]["required_version"] == 4
    assert graded.examples[0]["served_version"] == 1


def test_percentiles_are_computed_not_approximated() -> None:
    """Interpolated between samples, which is what makes a rerun reproduce them."""
    measured = percentiles([float(n) for n in range(1, 101)])
    assert (measured["p50"], measured["p95"], measured["max"]) == (50.5, 95.05, 100.0)


def test_percentiles_of_nothing_report_nothing() -> None:
    assert percentiles([]) == {"count": 0}


def test_compaction_windows_span_backwards_from_the_event(tmp_path: Path) -> None:
    """A compaction is recorded when it ends, so its window runs back from there."""
    path = write(
        tmp_path / "worker-events.jsonl.gz",
        [
            {"at": BASE + 100.0, "kind": "compact", "seconds": 30.0},
            {"at": BASE + 10.0, "kind": "seal"},
        ],
    )
    windows = compaction_windows(path)
    assert windows == [(BASE + 70.0, BASE + 100.0)]
    assert inside(windows, BASE + 80.0) is True
    assert inside(windows, BASE + 50.0) is False
    assert inside(windows, BASE + 120.0) is False


def minimal_run(tmp_path: Path, freshness: list[dict[str, object]]) -> Path:
    run = tmp_path / "run"
    run.mkdir()
    write(run / "revisions.jsonl.gz", [])
    write(run / "served.jsonl.gz", [])
    write(run / "freshness.jsonl.gz", freshness)
    write(run / "kills.jsonl.gz", [])
    write(run / "worker-events.jsonl.gz", [])
    (run / "lost.json").write_text(json.dumps({"lost_revisions": 0, "live_documents": 1}))
    (run / "run.json").write_text(
        json.dumps(
            {
                "name": "t",
                "defences_disabled": [],
                "documents": 1,
                "revisions_published": 0,
                "kills": 0,
                "readers": 1,
                "queries": 0,
                "torn_manifest_reads": 0,
                "missing_segment_reads": 0,
            }
        )
    )
    return run


def probe(outcome: str, seconds: float | None) -> dict[str, object]:
    return {"doc_id": 1, "version": 2, "outcome": outcome, "seconds": seconds}


def test_a_probe_that_waited_for_a_prober_is_not_freshness(tmp_path: Path) -> None:
    """The bug that made the first full run report 86 seconds.

    A probe queued behind busy probers carries that wait in its wall clock.
    Counting it measures the harness, so it is excluded and reported on its own.
    """
    run = minimal_run(
        tmp_path,
        [probe("visible", 1.0), probe("visible", 2.0), probe("late_start", None)],
    )
    summary = audit(run)
    assert summary["freshness_seconds"]["count"] == 2
    assert summary["freshness_seconds"]["max"] == 2.0
    assert summary["probe_outcomes"]["late_start"] == 1


def test_a_timed_out_probe_is_counted_but_not_averaged(tmp_path: Path) -> None:
    run = minimal_run(tmp_path, [probe("visible", 1.0), probe("timeout", None)])
    summary = audit(run)
    assert summary["freshness_seconds"]["count"] == 1
    assert summary["probe_outcomes"]["timeout"] == 1


def test_a_superseded_probe_is_not_a_freshness_failure(tmp_path: Path) -> None:
    run = minimal_run(tmp_path, [probe("visible", 1.0), probe("superseded", None)])
    summary = audit(run)
    assert summary["freshness_seconds"]["count"] == 1
    assert summary["probe_outcomes"]["superseded"] == 1
