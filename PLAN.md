# Plan

Written and committed before the corpus it describes was collected, so that the
thresholds below cannot be chosen to fit the numbers that came out. The history
shows when each threshold was fixed.

## The claim this repository has to earn

> A revision that reaches the changelog becomes searchable at its new content, or
> it is not searchable at all. IndexPulse never returns a document version that
> has been superseded or deleted, and it never loses a revision, including while
> ingest workers are being killed mid-write and segments are being compacted
> under concurrent reads.

The interesting half is not making a document searchable. It is the three ways a
freshness-oriented index usually breaks the moment it is written to and read from
at once:

1. A delete that reaches the document store but not every segment holding a
   vector for that document, so the deleted page keeps coming back.
2. A segment set swapped non-atomically, so a reader sees the new segment and the
   old one at the same time and the same document twice at two revisions.
3. A worker that crashes after writing an index segment but before committing its
   changelog position, or the reverse, so a revision is applied twice or dropped.

## Thresholds, fixed before the run

| what | threshold | why this number |
| --- | ---: | --- |
| audited stale returns, safe mode | exactly 0 | the claim is absolute, so any single violation fails it |
| audited lost revisions, safe mode | exactly 0 | a changelog consumer that drops a revision has no defence |
| audited stale returns, unsafe mode | at least 1,000 | the negative control; below this the audit is not sensitive enough to trust and the run is void |
| stale returns blocked by the visibility rule | at least 4,000 | shows the rule is doing work rather than never firing |
| update-to-search freshness, p95 | at most 12 s | taken from the design target of a 10 s seal interval plus consumer lag |
| vector lookup p95 during compaction | at most 240 ms | at 100 concurrent readers, against the idle p95 measured in the same run |
| injected worker failures survived | 60 | kills spread across the ingest run, each at a random point in the seal cycle |
| changelog revisions ingested | 180,000 | over a corpus of 1,200,000 simulated pages |

A threshold that is missed is published as missed, with the cause traced, and
pinned by the validator at its measured value so it cannot drift quietly.

## The negative control

A green test that has never been shown to fail is worth nothing here. `--unsafe`
turns off the three defences one layer at a time:

- `visibility`: serve whatever the segment holds, without checking the revision
  against the document store.
- `swap`: mutate the live manifest in place instead of writing a new one and
  renaming over it.
- `offsets`: commit the changelog position before the segment is durable rather
  than after.

The unsafe run uses the same corpus, the same query set and the same kill
schedule. If it does not produce at least 1,000 audited stale returns, the audit
cannot see the bug class it claims to rule out, and the safe run's zero means
nothing. The validator enforces that in CI.

## How staleness is audited without circular reasoning

The visibility rule cannot be allowed to grade its own work. So the audit never
calls it:

1. Every search records the hits it served as `(document id, revision, served at)`.
2. Every changelog record is written to a durable log as
   `(document id, revision, operation, published at)`, independent of the index.
3. The auditor replays that log to reconstruct, for each document, the intervals
   during which each revision was the authoritative one.
4. A served hit is stale if a newer revision of that document entered the
   changelog more than the tolerance before the hit was served.

The tolerance is fixed at 12 seconds, the same number as the freshness threshold
above, and it is what makes the claim a promise rather than a tautology. An
index is allowed to take time to catch up; what it is not allowed to do is still
be serving old content once that time has passed. Stating it this way means the
staleness count and the freshness number cannot both be met by a system that
quietly redefines either.

The strictest possible reading, a tolerance of zero, is reported alongside it.
Hits that fail only at zero tolerance were served while the edit was still in
flight, which the claim permits, and they are published as a separate figure
rather than folded into the staleness count or left out.

The auditor shares no code with the search path. It is given the raw records and
recomputes the verdict from the changelog alone.

## Scale and what is simulated

The corpus is 1,200,000 generated pages. Vectors are 128 dimensions derived
deterministically from page content, so editing a page changes its vector, and
re-ingesting the same revision produces the same vector. They are not embeddings
from a trained model, and retrieval quality is explicitly not a claim here. See
[docs/non-goals.md](docs/non-goals.md).

## Order of work

1. Core revision model and visibility rule, with no dependency on a broker, a
   store or a vector index.
2. Versioned document store on RocksDB.
3. Segmented vector index: growing delta segment, sealed immutable segments,
   manifest with atomic swap, compaction.
4. Ingest worker reading the changelog, with the offset commit ordered after the
   segment is durable.
5. Kill injection, then crash-safe replay on restart.
6. Corpus generator and the four benchmarks.
7. The run, then the README written from what it produced.
