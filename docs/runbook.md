# Runbook

Operational procedures for the awareness engine.

## First-time setup

```bash
cd awareness/
uv venv --python 3.13 --seed
uv pip install -e '.[dev]'
# optional: HF adapters
uv pip install -e '.[hf]'

awareness init      # creates data/ tree + Iceberg table + state DB
awareness health    # liveness check
```

## Submitting a BODY backfill

```bash
# Wide-net backfill — defaults to Common Crawl WET, FineWeb, GDELT.
awareness backfill submit --start 2024-06-01 --end 2024-06-14

# Narrow to a specific source.
awareness backfill submit --start 2024-06-01 --end now \
  --source common_crawl_wet --source gdelt --max-tasks 5

# Domain-narrowed backfill activates the CC index adapter.
awareness backfill submit --start 2024-01-01 --end 2024-03-31 \
  --domain example.com --domain news.example.org

# Run the worker pool against a submitted job in-process.
awareness backfill run <JOB_ID>

# Or run via the standalone worker script (picks up any non-tail RUNNING/PENDING job):
awareness-worker
```

## Running the TAIL

```bash
# Edit configs/tail_seeds.yaml first; add the public RSS/Atom/sitemap URLs you want.

# Foreground: Ctrl-C stops cleanly.
awareness tail start

# Bounded (auto-stop after N seconds).
awareness tail start --duration 600

# Background daemon (one process):
awareness-tail &

# Cooperative stop (asks any running tail to stop on its next poll).
awareness tail stop

# Live status.
awareness tail status
```

The tail does the following each `tail_poll_seconds` (default 60s):
1. Refreshes each seed (RSS/Atom/Sitemap).
2. New URLs are turned into `tail_recrawl` sub-partitions.
3. Workers fetch (robots.txt + politeness applies), extract text, dedupe,
   and write.

## Inspecting the corpus

```bash
# By date range.
awareness inspect --start 2024-06-01 --end now --limit 50

# Filter.
awareness inspect --start 2024-06-01 --end now --domain bbc.com --source tail_recrawl

# Counts (JSON).
awareness counts --start 2024-06-01 --end now

# Dedup stats.
awareness dedup-stats

# Process-level metrics (counters & histograms).
awareness metrics

# Recent jobs & tail state.
awareness status
```

DuckDB SQL directly:

```bash
duckdb data/duckdb/metadata.duckdb -c "
  SELECT source_type, count(*) AS n
  FROM read_json_auto('data/jsonl/captures/**/*.jsonl', union_by_name=true)
  GROUP BY source_type ORDER BY n DESC
"
```

## HTTP API

```bash
awareness-api                # http://127.0.0.1:8085
# Override:
AW_API_HOST=0.0.0.0 AW_API_PORT=9000 awareness-api
```

```bash
curl -s localhost:8085/healthz | jq
curl -s localhost:8085/status | jq
curl -s -XPOST localhost:8085/backfill -H 'content-type: application/json' \
  -d '{"start":"2024-06-01","end":"2024-06-14","sources":["common_crawl_wet"],"max_tasks":2}' | jq
curl -s -XPOST localhost:8085/backfill/JOB_ID/run | jq
curl -s "localhost:8085/inspect?start=2024-06-01&limit=20" | jq
curl -s -XPOST localhost:8085/tail/start | jq
curl -s -XPOST localhost:8085/tail/stop | jq
```

## Resume after crash

The worker pool is restart-safe:
- Tasks in `RUNNING` are atomically re-claimed when a worker picks up `PENDING`
  (the worker reclaims by status filter, not by worker id, so a stale RUNNING
  task is left as-is until you manually flip it; see "Stuck task" below).
- Tasks have per-adapter checkpoints (`row_index` for HF, `seen_urls` for
  feeds, etc.). Adapters use them on next run to resume.
- JSONL chunks are atomic — re-runs append new chunks, never corrupt
  existing ones.
- The state DB is the planner's source of truth.

Just restart:

```bash
awareness backfill run <JOB_ID>     # resumes
awareness tail start                # starts a new tail job (each `tail start` makes one)
```

## Compaction & cleanup

`scripts/compact_staging.py` (planned) lifts old JSONL chunks into Iceberg
and marks the manifest rows compacted. For now, JSONL is the source of truth
and Iceberg is written eagerly per batch — compaction is mostly a future
optimization.

To prune the WARC cache:

```bash
find data/warc -type f -mtime +7 -delete
```

To drop the WET shard cache:

```bash
rm -rf data/warc/*
```

## Stuck task / DLQ

```bash
sqlite3 data/state/awareness.sqlite <<SQL
  -- find stuck tasks (RUNNING > 1 hour, or repeatedly failed)
  SELECT task_id, source_type, partition_key, status, attempts, last_error
  FROM tasks
  WHERE (status = 'running' AND started_at < datetime('now','-1 hour'))
     OR attempts > 3
  ORDER BY created_at DESC LIMIT 50;

  -- force a RUNNING task back to PENDING for retry
  UPDATE tasks SET status='pending', started_at=NULL
  WHERE task_id='<TASK_ID>';

  -- inspect DLQ
  SELECT * FROM dlq ORDER BY id DESC LIMIT 20;
SQL
```

## Switching to the production stack

```bash
docker compose -f ops/compose/docker-compose.yml up -d

# Wait ~10s for everything to come up, then re-run awareness with:
export AW_STATE_DB_URL='postgresql+psycopg://awareness:awareness@localhost:5432/awareness'
# Iceberg over MinIO (S3) — PyIceberg picks up env-style AWS creds:
export AWS_ACCESS_KEY_ID=awareness
export AWS_SECRET_ACCESS_KEY=awareness12345
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL_S3=http://localhost:9000
export AW_ICEBERG_WAREHOUSE=s3a://awareness/warehouse

awareness init     # creates the Iceberg table in MinIO
awareness backfill submit --start 2024-06-01 --end now
```

Then in ClickHouse (port 8123):

```bash
docker exec -i $(docker compose -f ops/compose/docker-compose.yml ps -q clickhouse) \
  clickhouse-client --user awareness --password awareness < ops/clickhouse/captures.sql

# Query
docker exec -it $(docker compose -f ops/compose/docker-compose.yml ps -q clickhouse) \
  clickhouse-client --user awareness --password awareness --query="
    SELECT count(), uniqExact(domain) FROM awareness.captures_s3
  "
```

## Backups

- The state DB (SQLite or Postgres) is the only thing that can't be
  reconstructed from data files. Back it up.
- The JSONL staging directory is the corpus source of truth — back up
  `data/jsonl/`.
- The Iceberg warehouse can be rebuilt from JSONL via a future compaction
  job.
