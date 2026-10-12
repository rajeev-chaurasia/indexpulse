"""Run one measurement and grade it.

Two scales. `smoke` exists so the whole path, broker to audit, can be exercised
in under two minutes before committing an hour to the real thing, and it is the
scale CI runs.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from bench.audit import audit
from bench.corpus import CorpusSpec
from bench.run import BenchRun, RunSpec
from indexpulse.cli import parse_safety


@dataclass(frozen=True)
class Scale:
    corpus: CorpusSpec
    readers: int
    kills: int
    publish_rate: float
    probe_every: int
    seal_size: int


SCALES = {
    "smoke": Scale(
        corpus=CorpusSpec(documents=20_000, revisions=3_000),
        readers=10,
        kills=5,
        publish_rate=150.0,
        probe_every=10,
        seal_size=200,
    ),
    "full": Scale(
        corpus=CorpusSpec(documents=1_200_000, revisions=180_000),
        readers=100,
        kills=60,
        publish_rate=300.0,
        probe_every=60,
        seal_size=2_000,
    ),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and grade one measurement.")
    parser.add_argument("--scale", choices=sorted(SCALES), default="smoke")
    parser.add_argument("--name", default=None)
    parser.add_argument("--unsafe", default="", help="Defences to switch off, comma separated.")
    parser.add_argument("--evidence", type=Path, default=Path("evidence"))
    parser.add_argument("--work", type=Path, default=Path("var"))
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument(
        "--publish-rate",
        type=float,
        default=None,
        help="Revisions per second, overriding the scale default.",
    )
    parser.add_argument("--keep-work", action="store_true")
    args = parser.parse_args()

    scale = SCALES[args.scale]
    safety = parse_safety(args.unsafe)
    label = args.name or (
        f"{args.scale}-" + ("-".join(safety.disabled_names) if safety.disabled_names else "safe")
    )

    work = args.work / label
    if work.exists():
        shutil.rmtree(work)
    out = args.evidence / label
    if out.exists():
        shutil.rmtree(out)

    spec = RunSpec(
        name=label,
        out=out,
        root=work / "index",
        store_path=work / "docs",
        corpus=scale.corpus,
        safety=safety,
        topic=f"indexpulse-{label}-{uuid.uuid4().hex[:6]}",
        bootstrap=args.bootstrap,
        readers=scale.readers,
        kills=scale.kills,
        publish_rate=args.publish_rate or scale.publish_rate,
        probe_every=scale.probe_every,
        seal_size=scale.seal_size,
    )

    started = time.time()
    print(f"running {label}: {spec.corpus.documents} documents, {spec.corpus.revisions} revisions")
    BenchRun(spec).run()
    print(f"run finished in {time.time() - started:.0f}s, grading")

    summary = audit(out)
    summary["wall_seconds"] = round(time.time() - started, 1)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    if not args.keep_work:
        shutil.rmtree(work, ignore_errors=True)

    print(json.dumps({k: v for k, v in summary.items() if k != "staleness"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
