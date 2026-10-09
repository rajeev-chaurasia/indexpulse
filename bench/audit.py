"""Grading a run from its raw records, without asking the index anything.

The visibility rule is not allowed to mark its own work. Everything here is
recomputed from two files that the search path never touches: the changelog as
it was published, and the hits as they were served. If the two disagree, the
index is wrong, and no amount of agreement between the index and itself would
have shown it.
"""

from __future__ import annotations

import bisect
import gzip
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

# Matches the freshness threshold on purpose. An index may lag; it may not still
# be serving old content once the lag it is allowed has passed.
STALENESS_TOLERANCE = 12.0
BASE_VERSION = 1


def read_gz(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


@dataclass
class Timeline:
    """When each version of each document entered the changelog.

    Built from the changelog alone. Every document starts at version one,
    present from before the run, which is why a document with no revisions still
    has an answer here.
    """

    times: dict[int, list[float]]
    versions: dict[int, list[int]]
    deleted_at: dict[int, float]

    @classmethod
    def build(cls, revisions: Path) -> Timeline:
        times: dict[int, list[float]] = {}
        versions: dict[int, list[int]] = {}
        deleted_at: dict[int, float] = {}
        for record in read_gz(revisions):
            doc_id = int(record["doc_id"])
            at = float(record["published_at"])
            times.setdefault(doc_id, []).append(at)
            versions.setdefault(doc_id, []).append(int(record["version"]))
            if record["operation"] == "delete":
                deleted_at[doc_id] = at
        return cls(times=times, versions=versions, deleted_at=deleted_at)

    def settled(self, doc_id: int, at: float, tolerance: float) -> tuple[int, bool]:
        """The version a correct index must be serving by `at`, and whether it is deleted.

        Revisions published within the tolerance of `at` are still in flight, so
        they are ignored: an index is not faulted for not having caught up yet.
        """
        cutoff = at - tolerance
        published = self.times.get(doc_id)
        if not published:
            return BASE_VERSION, False
        position = bisect.bisect_right(published, cutoff)
        if position == 0:
            return BASE_VERSION, False
        version = max(self.versions[doc_id][:position])
        removed = doc_id in self.deleted_at and self.deleted_at[doc_id] <= cutoff
        return version, removed


@dataclass
class Staleness:
    stale_hits: int
    deleted_hits: int
    hits_checked: int
    examples: list[dict[str, Any]]


def grade_staleness(served: Path, timeline: Timeline, tolerance: float) -> Staleness:
    stale = 0
    deleted = 0
    checked = 0
    examples: list[dict[str, Any]] = []
    for record in read_gz(served):
        at = float(record["at"])
        for doc_id, version in record["hits"]:
            checked += 1
            required, removed = timeline.settled(int(doc_id), at, tolerance)
            if removed:
                deleted += 1
            elif int(version) >= required:
                continue
            else:
                stale += 1
            if len(examples) < 50:
                examples.append(
                    {
                        "doc_id": int(doc_id),
                        "served_version": int(version),
                        "required_version": required,
                        "deleted": removed,
                        "at": at,
                    }
                )
    return Staleness(
        stale_hits=stale, deleted_hits=deleted, hits_checked=checked, examples=examples
    )


def percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0}
    array = np.asarray(values, dtype=np.float64)
    return {
        "count": int(array.size),
        "p50": round(float(np.percentile(array, 50)), 3),
        "p95": round(float(np.percentile(array, 95)), 3),
        "p99": round(float(np.percentile(array, 99)), 3),
        "max": round(float(array.max()), 3),
    }


def compaction_windows(events: Path) -> list[tuple[float, float]]:
    windows = []
    for record in read_gz(events):
        if record.get("kind") != "compact":
            continue
        ended = float(record["at"])
        windows.append((ended - float(record["seconds"]), ended))
    return sorted(windows)


def inside(windows: list[tuple[float, float]], at: float) -> bool:
    position = bisect.bisect_right([start for start, _ in windows], at)
    return position > 0 and windows[position - 1][1] >= at


def audit(run_dir: Path) -> dict[str, Any]:
    timeline = Timeline.build(run_dir / "revisions.jsonl.gz")
    served = run_dir / "served.jsonl.gz"

    tolerated = grade_staleness(served, timeline, STALENESS_TOLERANCE)
    strict = grade_staleness(served, timeline, 0.0)

    windows = compaction_windows(run_dir / "worker-events.jsonl.gz")
    during: list[float] = []
    idle: list[float] = []
    blocked = 0
    for record in read_gz(served):
        latency = float(record["latency_ms"])
        rejected = record.get("rejected") or {}
        blocked += sum(int(count) for count in rejected.values())
        if inside(windows, float(record["at"])):
            during.append(latency)
        else:
            idle.append(latency)

    probes = [
        float(record["seconds"])
        for record in read_gz(run_dir / "freshness.jsonl.gz")
        if record.get("seconds") is not None
    ]
    outcomes: dict[str, int] = {}
    for record in read_gz(run_dir / "freshness.jsonl.gz"):
        outcome = str(record.get("outcome", "visible" if record.get("seconds") else "timeout"))
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

    run = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    lost = json.loads((run_dir / "lost.json").read_text(encoding="utf-8"))

    return {
        "name": run["name"],
        "defences_disabled": run["defences_disabled"],
        "documents": run["documents"],
        "revisions_published": run["revisions_published"],
        "kills": run["kills"],
        "readers": run["readers"],
        "queries": run["queries"],
        "hits_checked": tolerated.hits_checked,
        "staleness": {
            "tolerance_seconds": STALENESS_TOLERANCE,
            "stale_hits": tolerated.stale_hits,
            "deleted_hits": tolerated.deleted_hits,
            "examples": tolerated.examples,
        },
        "staleness_strict": {
            "tolerance_seconds": 0.0,
            "stale_hits": strict.stale_hits,
            "deleted_hits": strict.deleted_hits,
        },
        "blocked_by_visibility_rule": blocked,
        "lost_revisions": lost["lost_revisions"],
        "live_documents": lost["live_documents"],
        "freshness_seconds": percentiles(probes),
        "probe_outcomes": outcomes,
        "lookup_ms_during_compaction": percentiles(during),
        "lookup_ms_idle": percentiles(idle),
        "compactions": len(windows),
        "torn_manifest_reads": run["torn_manifest_reads"],
        "missing_segment_reads": run["missing_segment_reads"],
    }
