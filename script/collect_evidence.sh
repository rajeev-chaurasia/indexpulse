#!/usr/bin/env bash
# Collect every run the published numbers come from, in one sitting on one
# machine. Run sequentially and never alongside anything else: the lookup
# percentiles are a measurement of a loaded machine, and another process on the
# same cores makes them a measurement of that instead.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose up -d --wait

# The safe run first, so a failure in the thing being claimed shows up before
# an hour goes into the controls that only give it meaning.
uv run python -m bench --scale full
for defence in visibility offsets swap; do
  uv run python -m bench --scale full --unsafe "$defence"
done

# A second regime at a rate the worker sustains without queueing, so the
# freshness figure can be read apart from the load that produced it.
uv run python -m bench --scale full --publish-rate 100 --revisions 60000 \
  --name full-safe-unsaturated

uv run python script/validate_evidence.py
