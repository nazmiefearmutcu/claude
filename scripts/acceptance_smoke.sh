#!/usr/bin/env bash
# Acceptance smoke runner.
# Demonstrates BODY backfill (integration test) + TAIL lifecycle + dedup +
# range query against the live, real corpus.
#
# Run from project root: ./scripts/acceptance_smoke.sh

set -euo pipefail
cd "$(dirname "$0")/.."

VENV=".venv/bin"
echo ">>> 1. unit + integration + smoke tests"
"$VENV/python" -m pytest -q

echo ">>> 2. CLI init"
"$VENV/awareness" init >/dev/null

echo ">>> 3. CLI health"
"$VENV/awareness" health

echo ">>> 4. live tail for 25s against public RSS feeds"
"$VENV/awareness" tail start --duration 25 2>&1 | tail -10

echo ">>> 5. dedup stats"
"$VENV/awareness" dedup-stats

echo ">>> 6. range query"
"$VENV/awareness" counts --start 2024-01-01 --end now | tail -20

echo ">>> 7. recent jobs"
"$VENV/awareness" status

echo ">>> DONE"
