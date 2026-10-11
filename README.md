# indexpulse

[![CI](https://github.com/rajeev-chaurasia/indexpulse/actions/workflows/ci.yml/badge.svg)](https://github.com/rajeev-chaurasia/indexpulse/actions/workflows/ci.yml)

A search index fed by a changelog, built so that a page being edited, deleted or
re-indexed while the index is being read cannot produce a result showing what
the page used to say.

> A revision that reaches the changelog becomes searchable at its new content,
> or it is not searchable at all. IndexPulse never returns a document version
> that has been superseded or deleted, and never loses a revision, including
> while ingest workers are being killed mid-write and segments are being
> compacted under concurrent reads.

Making a document searchable is the easy half. The half this repository is
about is what happens when writes and reads overlap, which is all the time, and
where a freshness-oriented index usually breaks:

- A delete reaches the document store but not every segment holding a vector
  for that document, so the deleted page keeps coming back.
- A segment set is swapped non-atomically, so a reader sees the old segment and
  the new one at once, and the same document at two versions.
- A worker crashes after writing a segment but before recording how far it
  read, or the reverse, so a revision is applied twice or dropped entirely.

The claim, the thresholds and the negative control were fixed in
[PLAN.md](PLAN.md) and committed before the corpus was collected, which the
history shows.

## How it stays correct

Three decisions carry the whole thing, and each is checked rather than trusted.

**Segments are never edited.** A segment is an HNSW graph plus the document
identity of every vector in it, written once. Edits and deletes do not reach
back into it. That is what makes a segment safe to share between processes with
no locking, and it means the index holds stale vectors by design.

**Stale vectors are filtered at query time, not prevented.** Candidates are
merged across segments by distance, and only the survivors are resolved against
the document store. A hit is served only if its version is the one the store
currently holds and the document is not tombstoned. The whole guarantee lives in
[`core/visibility.py`](src/indexpulse/core/visibility.py), which is thirty lines
and depends on nothing.

**One file records both the segment set and the changelog position.** Kafka will
happily remember how far a consumer group has read, and relying on that gives
two records of the same fact which disagree after a kill: the broker knows what
was delivered, the manifest knows what was made durable. Only the second is
true. So partitions are assigned explicitly, seeks come from the manifest, and
the manifest is published by writing a complete file and renaming it over the
old one.

```mermaid
flowchart TB
    subgraph changelog["Changelog"]
        direction LR
        edits["Page edits<br/>and deletes"] --> topic[("Kafka topic<br/>four partitions,<br/>keyed by document")]
    end

    subgraph ingest["Ingest worker, killed and restarted throughout"]
        direction LR
        consume["Read from the<br/>manifest position"] --> apply["Apply to the<br/>document store"]
        apply --> build["Build a delta<br/>segment, complete,<br/>on disk"]
        build --> publish["Publish a manifest<br/>by rename"]
    end

    subgraph state["Durable state"]
        direction LR
        store[("RocksDB<br/>versions and<br/>tombstones")]
        segs[("Immutable segments<br/>graph plus identity")]
        man[("manifest.json<br/>segment set and<br/>changelog position")]
    end

    subgraph read["Search, in its own process"]
        direction LR
        fan["Fan out across<br/>live segments"] --> merge["Merge by distance"]
        merge --> filter["Visibility rule:<br/>resolve against<br/>the store"]
        filter --> out["Results"]
    end

    compact["Compactor<br/>merge smallest first,<br/>drop superseded<br/>and tombstoned"]

    topic --> consume
    apply --> store
    build --> segs
    publish --> man
    man --> fan
    segs --> fan
    store --> filter
    man --> compact
    compact --> segs

    classDef cl fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef ing fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef st fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef rd fill:#ede9fe,stroke:#7c3aed,color:#4c1d95
    classDef cp fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
    class edits,topic cl
    class consume,apply,build,publish ing
    class store,segs,man st
    class fan,merge,filter,out rd
    class compact cp
```

## Surviving the kill

The ordering inside the worker is the entire crash-safety argument, and every
other arrangement of these steps is wrong:

```mermaid
flowchart LR
    s1["1. Apply revisions<br/>to the store"] --> s2["2. Write the segment,<br/>complete, on disk"]
    s2 --> s3["3. Publish the manifest<br/>naming it, and the<br/>position past it"]
    s3 --> s4["4. Durable"]

    k1["Killed here:<br/>position unchanged,<br/>batch read again,<br/>store ignores the<br/>versions it has"]
    k2["Killed here:<br/>segment is litter,<br/>nothing names it,<br/>batch read again"]
    k3["Killed here:<br/>batch is in the index<br/>and the position<br/>is past it"]

    s1 -.-> k1
    s2 -.-> k2
    s3 -.-> k3

    classDef step fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef kill fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
    class s1,s2,s3,s4 step
    class k1,k2,k3 kill
```

There is no window in which the recorded position has moved past work the index
does not hold. Nothing catches the signal: a worker that got to tidy up would be
proving something about its shutdown path rather than about the format it leaves
on disk, so the kill is one that cannot be caught.

Replay then has to be harmless, and getting that wrong is subtle. A revision
re-delivered after a kill does not advance the store, because the store already
holds that version. The first smoke run lost twenty two revisions to exactly
that: the worker indexed only what advanced the store, so a revision applied
just before a kill was dropped by the replay that should have rescued it. The
store now reports what is *current* rather than what *advanced*, and the
regression is pinned by a test.

## The negative control

A green test that has never been shown to fail is worth nothing, so the same
corpus, query set and kill schedule run with each defence switched off in turn:

| flag | what it removes | what it should break |
| --- | --- | --- |
| `--unsafe visibility` | the version check before serving a hit | stale and deleted documents come back |
| `--unsafe offsets` | recording the position only after the segment is durable | revisions never reach a segment |
| `--unsafe swap` | atomic rename, and the grace period before deleting retired segments | torn manifest reads, segments read after deletion |

If the unsafe runs do not fail, the audit cannot see the bug class it claims to
rule out, and the safe run's zero means nothing.
[`script/validate_evidence.py`](script/validate_evidence.py) fails the build on
that, not just on the claim.

## How staleness is audited

The visibility rule is not allowed to mark its own work, so the audit shares no
code with the search path. It is given two files the index never touches, the
changelog as published and the hits as served, and recomputes the verdict from
the changelog alone.

A hit is stale if a newer revision of that document entered the changelog more
than the tolerance before the hit was served. The tolerance is twelve seconds,
the same number as the freshness threshold, and it is what makes the claim a
promise rather than a tautology: an index may lag, but it may not still be
serving old content once the lag it is allowed has passed. The strictest
reading, a tolerance of zero, is published alongside it; hits that fail only
there were served while the edit was still in flight, which the claim permits.

## Running it

```bash
docker compose up -d --wait
uv sync --all-groups
uv run pytest
```

A full measurement, which takes about twenty minutes a run on a laptop:

```bash
./script/collect_evidence.sh
```

A single run at a scale that finishes in under a minute:

```bash
uv run python -m bench --scale smoke
```

## Layout

| path | what is in it |
| --- | --- |
| `src/indexpulse/core/` | revision model, visibility rule, manifest, vectors; standard library, numpy and pydantic only |
| `src/indexpulse/store/` | versioned document state on RocksDB, read by searchers as a secondary |
| `src/indexpulse/index/` | segment format, manifest publishing, compaction |
| `src/indexpulse/search/` | fan-out and the read path |
| `src/indexpulse/ingest/` | changelog client and the worker |
| `bench/` | corpus generator, run harness, auditor |
| `script/` | layer check, prose check, evidence validator |
| `evidence/` | raw records and the summary recomputed from them |

The boundary around `core` is enforced by
[`script/check_layers.py`](script/check_layers.py) in CI. Its value is that the
rule deciding whether a hit may be served can be read and tested without a
broker, a key-value store or a vector index in the way, and that property decays
the first time someone reaches for a convenience from an outer layer.

## What this is not

Vectors are generated from page content by a hash rather than a trained model,
so nothing here says anything about retrieval quality, and no recall number is
published. The failure model is process death, not host power loss. Everything
runs on one machine with one broker. See [docs/non-goals.md](docs/non-goals.md)
for the full list and [docs/known-misses.md](docs/known-misses.md) for the gaps
in the evidence, written against the repository rather than for it.
