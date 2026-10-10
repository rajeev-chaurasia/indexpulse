"""Recompute every published number from the raw records, and refuse a mismatch.

A summary file is a convenience. It is also the easiest thing in the repository
to edit into saying something nicer, so nothing here trusts one. Each run's
summary is recomputed from the changelog and the served hits that sit beside it,
and a single field that does not match fails the build.

The claim is then checked against the safe run, and the negative control against
the unsafe ones. A safe run reporting no staleness proves nothing on its own; it
only means something next to an unsafe run, graded by the same code, that
reports plenty.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from bench.audit import audit

EVIDENCE = Path("evidence")
RAW_FILES = (
    "revisions.jsonl.gz",
    "served.jsonl.gz",
    "freshness.jsonl.gz",
    "kills.jsonl.gz",
    "worker-events.jsonl.gz",
    "lost.json",
    "run.json",
)
# Measured rather than derived, so recomputing them from the raw records would
# only be comparing a number to itself.
NOT_RECOMPUTED = {"wall_seconds"}

# Below this the audit has not been shown to see the fault it rules out, and
# the safe run's zero carries no weight. Fixed in PLAN.md before the run.
CONTROL_FLOOR = 1_000


def runs() -> list[Path]:
    return sorted(path for path in EVIDENCE.iterdir() if (path / "summary.json").exists())


def compare(published: dict[str, Any], recomputed: dict[str, Any], label: str) -> list[str]:
    problems = []
    for key, value in published.items():
        if key in NOT_RECOMPUTED:
            continue
        if key not in recomputed:
            problems.append(f"{label}: {key} is published but the audit does not produce it")
        elif recomputed[key] != value:
            problems.append(f"{label}: {key} published as {value}, recomputed as {recomputed[key]}")
    return problems


def check_claim(summary: dict[str, Any], label: str) -> list[str]:
    problems = []
    staleness = summary["staleness"]
    if staleness["stale_hits"]:
        problems.append(f"{label}: served {staleness['stale_hits']} superseded versions")
    if staleness["deleted_hits"]:
        problems.append(f"{label}: served {staleness['deleted_hits']} deleted documents")
    if summary["lost_revisions"]:
        problems.append(f"{label}: {summary['lost_revisions']} revisions never reached a segment")
    if summary["torn_manifest_reads"]:
        problems.append(f"{label}: {summary['torn_manifest_reads']} torn manifest reads")
    if summary["missing_segment_reads"]:
        problems.append(f"{label}: {summary['missing_segment_reads']} segments read after deletion")
    return problems


def check_control(summaries: dict[str, dict[str, Any]]) -> list[str]:
    problems = []
    visibility = [s for s in summaries.values() if "visibility" in s["defences_disabled"]]
    if not visibility:
        return ["no run with the visibility rule switched off, so nothing grades the audit"]
    served = max(s["staleness"]["stale_hits"] + s["staleness"]["deleted_hits"] for s in visibility)
    if served < CONTROL_FLOOR:
        problems.append(
            f"the control served {served} stale hits, below the {CONTROL_FLOOR} the audit "
            "needs to have been shown to see them"
        )
    offsets = [s for s in summaries.values() if "offsets" in s["defences_disabled"]]
    if offsets and not any(s["lost_revisions"] for s in offsets):
        problems.append("committing offsets early lost nothing, so that control shows nothing")
    return problems


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(paths: list[Path]) -> str:
    lines = [f"{digest(path)}  {path.as_posix()}" for path in sorted(paths)]
    return "\n".join(lines) + "\n"


def main() -> int:
    if not EVIDENCE.exists() or not runs():
        print("no evidence to validate", file=sys.stderr)
        return 1

    problems: list[str] = []
    summaries: dict[str, dict[str, Any]] = {}
    hashed: list[Path] = []

    for run_dir in runs():
        label = run_dir.name
        published = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        summaries[label] = published
        problems += compare(published, audit(run_dir), label)
        if not published["defences_disabled"]:
            problems += check_claim(published, label)
        for name in RAW_FILES:
            path = run_dir / name
            if path.exists():
                hashed.append(path)
            else:
                problems.append(f"{label}: {name} is missing from the raw records")

    problems += check_control(summaries)

    manifest = EVIDENCE / "manifest.sha256"
    computed = write_manifest(hashed)
    if manifest.exists():
        if manifest.read_text(encoding="utf-8") != computed:
            problems.append("the raw records do not match evidence/manifest.sha256")
    else:
        manifest.write_text(computed, encoding="utf-8")
        print(f"wrote {manifest} over {len(hashed)} files")

    if problems:
        print("\n".join(problems), file=sys.stderr)
        print(f"\n{len(problems)} evidence problem(s)", file=sys.stderr)
        return 1

    print(f"evidence ok over {len(summaries)} run(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
