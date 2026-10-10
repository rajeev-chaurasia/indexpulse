# Non-goals

What this repository deliberately does not attempt, so that the claim it does
make can be read narrowly and checked.

## Retrieval quality

Vectors are generated from page content by a hash, not by a trained model.
Nothing here says anything about whether the right documents come back for a
query, and no recall or ranking number is published, because none would mean
anything. What the vectors have to provide is that an edit moves a page and a
replay does not, which is all the freshness and staleness claims rest on.

A real embedding model would change the numbers in one direction only: vectors
would take longer to compute, so freshness would get worse. It would not change
what the index does with them.

## Durability against power loss

The failure model is process death. Workers are killed with a signal they
cannot catch, which is what the 60 injected failures test. A host losing power
is a different failure, and the document store is not configured to survive it:
RocksDB writes go through the write-ahead log without an fsync per batch, so a
kernel that dies with the page cache unflushed loses recent writes.

The manifest is fsynced, and so is the directory it is renamed into, because
that is where the segment set and the changelog position have to agree. The
store is not, and the evidence should not be read as saying otherwise.

## One machine, one broker

Single node throughout: one broker with one partition replica, one worker
process at a time, one searcher process. Nothing here tests replication, leader
election, partition rebalancing between workers, or two workers writing to one
index. The last of those is not merely untested; the manifest has one writer by
construction and a second would corrupt it.

## Everything a search engine otherwise has

No text query, no filtering, no hybrid retrieval, no pagination, no access
control, no multi-tenancy, no metrics endpoint. Document bodies are not even
stored: the changelog is the system of record and the store keeps only the
version state that the visibility rule consults.
