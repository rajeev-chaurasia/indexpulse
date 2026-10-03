# indexpulse

A changelog-fed search index that never serves a superseded or deleted document
version, audited while its ingest workers are being killed and its segments are
being compacted under concurrent reads.

The claim, the thresholds and the negative control are fixed in
[PLAN.md](PLAN.md). The measurements are not collected yet, and this file is
written from them once they are, so nothing here states a number the repository
has not produced.
