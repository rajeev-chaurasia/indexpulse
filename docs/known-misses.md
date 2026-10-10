# Known misses

Written against the repository rather than for it. Everything here is a gap in
the evidence that a careful reader would find, listed so they do not have to.

## The lost-revision count is measured, not recomputed

Staleness, freshness and the lookup percentiles are all recomputed from the raw
records in CI, so a hand-edited summary fails the build. The lost-revision
count is not. It is produced at the end of a run by comparing the document
store against the live segments, and neither of those is committed, because
together they run to gigabytes.

So that one number is trusted in a way the others are not. The check that
produces it is unit tested, and the unsafe run shows it reporting losses while
the safe run reports none, which is the best that can be said without
committing the index itself.

## The clock is one machine's clock

Publisher, worker and searcher share a process clock, so the staleness audit
compares timestamps that cannot drift apart. That is a convenience of running
on one machine, not a property of the design. Across machines the tolerance
would have to absorb clock skew as well as propagation, and the audit as
written would quietly attribute skew to the index.

## Compaction policy is the simplest one that works

Segments are merged smallest first, eight at a time, once ten exist. There is
no cost model, no tiering, no accounting for how much of a segment is dead
before deciding to rewrite it. A policy that looked at the dead fraction would
rewrite less and would almost certainly show a better lookup percentile under
compaction than the one published here.

## A query is a vector, not a workload

Readers issue nearest-neighbour queries at a fixed rate against a topic
centroid. Real traffic is bursty, skewed towards a few popular queries, and
mixed with writes that are not uniform in size. The lookup percentiles describe
the load that was applied, which is steadier than anything real.

## Segment loading is not bounded

A searcher loads every live segment into memory and keeps it there. At the
scale measured that is under two gigabytes, so nothing here exercises eviction,
memory pressure, or what happens when the index no longer fits. An index that
had to page would behave differently under compaction, which is exactly where
the lookup numbers are taken.
