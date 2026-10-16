# Checking the evidence

Every number published in the README is recomputed from the files in
`evidence/` by `script/validate_evidence.py`, which runs in CI. This page
describes those files well enough that the recomputation can be done
independently, by someone who does not trust either the index or the validator.

## What a run leaves behind

Each run writes one directory under `evidence/`, named for the scale and for
whichever defences were switched off.

| file | one line per | holds |
| --- | --- | --- |
| `revisions.jsonl.gz` | changelog record | `doc_id`, `version`, `operation`, `published_at` |
| `served.jsonl.gz` | query | `at`, `latency_ms`, `generation`, `segments`, `hits`, `rejected` |
| `freshness.jsonl.gz` | probe | `doc_id`, `version`, `published_at`, `visible_at`, `outcome`, `seconds` |
| `kills.jsonl.gz` | injected failure | `at`, `index`, `pid`, `waited` |
| `worker-events.jsonl.gz` | worker event | `start`, `seal` or `compact`, with offsets, counts and durations |
| `lost.json` | run | live documents, indexed versions, and any revision no segment holds |
| `run.json` | run | configuration and the counters the harness kept |
| `summary.json` | run | every published number, all of it derived from the files above |

`summary.json` is a convenience and nothing trusts it. The validator recomputes
each field and fails the build on a single mismatch, so editing it to say
something nicer breaks CI rather than the claim.

## Recomputing the staleness count

This is the one worth doing by hand, because it is the claim.

1. Read `revisions.jsonl.gz` and group it by `doc_id`. Every document begins at
   version 1, present before the run, so a document with no entries is at
   version 1 throughout.
2. For a hit served at time `t`, find the highest version of that document
   published at or before `t - 12`. That is the version a correct index must be
   serving by then. The twelve seconds are the stated tolerance, fixed in
   [PLAN.md](../PLAN.md) before the run; an index may lag, but not past the lag
   it is allowed.
3. Read `served.jsonl.gz`. For each `[doc_id, version]` in each record's `hits`,
   compare against step 2. A served version below the required one is stale. A
   document whose delete settled before `t - 12` is a deleted document served.
4. The same walk with a tolerance of zero gives the strict figure, which counts
   hits served while an edit was still in flight. Those are not violations, and
   they are published separately rather than folded in or left out.

Nothing in that procedure consults the index, the document store, or the
visibility rule. It compares what was published against what was served, which
is the only comparison that can catch the index being wrong about itself.

## Why the unsafe runs are here

A run with every defence on, reporting no staleness, is consistent with an audit
that cannot detect staleness at all. The unsafe runs rule that out: same corpus,
same queries, same kill schedule, graded by the same code, with one defence
removed. `--unsafe visibility` should produce stale hits in quantity,
`--unsafe offsets` should lose revisions, and the validator fails if they do
not.

The atomic swap is demonstrated differently. Its failure window is a couple of
milliseconds wide, so a run of minutes may never sample it and come back clean
while proving nothing. `tests/test_manifest.py` instead reads a manifest
continuously while it is republished, with and without the rename, under the
same workload.

## What is measured rather than recomputed

The lost-revision count in `lost.json` is produced at the end of a run by
comparing the document store against the live segments. Neither is committed,
because together they run to gigabytes, so that one number is trusted in a way
the others are not. What can be said for it is that the check is unit tested,
and that the unsafe run reports losses where the safe run reports none.

The other gaps in the evidence are listed in
[known-misses.md](known-misses.md), written against this repository rather than
for it.
