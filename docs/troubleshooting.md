# Troubleshooting

## Tail captures nothing

1. `awareness tail status` — confirm `running: true`.
2. `awareness status` — check `tasks_completed` / `tasks_failed` for the tail job.
3. Look at `data/logs/awareness.log` for `feed_fetch_failed`, `tail_fetch_failed`,
   `robots_disallowed`.
4. Check `configs/tail_seeds.yaml` is valid YAML.
5. Try the seed URL with curl using the same User-Agent:
   ```bash
   curl -s -H "User-Agent: $(awareness health | jq -r '.user_agent // empty')" "<URL>" | head
   ```

## "pyiceberg_core needs to be installed"

```bash
uv pip install 'pyiceberg-core>=0.6'
```

This is required from PyIceberg 0.10+ to write data files. JSONL staging
continues to work without it; only the Iceberg sink fails.

## "Python int too large to convert to C long" during Iceberg append

The simhash is 64-bit unsigned; Arrow's `int64` expects signed values.
Fixed in `awareness.storage.iceberg._to_arrow` by folding values >= 2^63
to negative. If it reappears, ensure you're on the latest code.

## DuckDB "No files found that match the pattern"

The JSONL writer puts chunks at `data/jsonl/captures/Y/M/D/`. The DuckDB
reader enumerates `*.jsonl` files explicitly rather than using a glob, so
this should only surface when the data dir was moved or pruned mid-query.
Restart the CLI / refresh the index.

## Worker pool isn't draining

- Look at the `tasks` table: `SELECT status, count(*) FROM tasks GROUP BY 1`.
- If many tasks are `PENDING` but workers are idle, you may have a deadlock
  from a sub-partition adapter (CC index, feeds, GDELT). Bump
  `worker_concurrency` so discovery + recrawl run concurrently.
- If many tasks are stuck `RUNNING`, see "Stuck task / DLQ" in the runbook.

## Robots.txt false-disallow

The robots cache is per-process and TTL'd at `robots_cache_ttl_sec`
(default 3600). To clear it, restart the worker. If the site truly
disallows your User-Agent string, change `AW_USER_AGENT` to something
identifiable and verify in their robots.txt.

## FineWeb adapter is a no-op

It only activates when the `datasets` package is installed:

```bash
uv pip install -e '.[hf]'
```

It also short-circuits when the requested CC crawl id has no corresponding
FineWeb dump.

## Common Crawl shards return 404

Some crawl ids in the planner's enumerated set were never published. The WET
adapter logs `cc_wet_paths_not_found` and skips them. Use `--source
common_crawl_wet --max-tasks N` to bound retries during smoke runs.

## State DB grows large

The state DB is small per row but grows linearly with task and capture
counts. Periodically:

```bash
sqlite3 data/state/awareness.sqlite "DELETE FROM tasks WHERE status='completed' AND completed_at < datetime('now','-30 day');"
sqlite3 data/state/awareness.sqlite "VACUUM;"
```

## API doesn't respond

```bash
# Verify uvicorn is up.
curl -s localhost:8085/healthz

# Run in foreground for live logs.
AW_LOG_JSON=false awareness-api

# If port conflict:
AW_API_PORT=9090 awareness-api
```
