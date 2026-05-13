# Awareness

**Public text internet awareness engine.** Backfill ("BODY") any historical
date range from the public text web up to now, and run a live tail ("TAIL")
that captures newly published public text until you stop it.

Stores **text and text-oriented metadata only**. No images, no binary media,
no login-gated content, no paywall bypass. Robots.txt is respected; per-domain
politeness applies to live fetches.

---

## Architecture

```
                 ┌──────────────────────────┐
   user/CLI ────►│        Planner          │── partitions ──┐
                 └──────────────────────────┘                │
                                                             ▼
                 ┌──────────────────────────┐      ┌────────────────────┐
                 │  Worker Engine (asyncio) │◄─────│  Tasks (state DB)  │
                 └─────────┬────────────────┘      └────────────────────┘
                           │ async runs partition
                           ▼
                 ┌──────────────────────────┐
                 │      Source Adapters     │  Common Crawl WET / CDX / WARC
                 │                          │  HF FineWeb / FineWeb2
                 │                          │  Sitemap / RSS / Atom
                 │                          │  Tail recrawl (politeness)
                 │                          │  GDELT
                 └─────────┬────────────────┘
                           │ DocCapture
                           ▼
                 ┌──────────────────────────┐
                 │  Normalize → Dedup       │  xxhash + 64-bit simhash
                 └─────────┬────────────────┘  pigeonhole near-dup index
                           │
                           ▼
                 ┌──────────────────────────┐
                 │  JSONL staging (atomic)  │  data/jsonl/captures/Y/M/D/*.jsonl
                 └─────────┬────────────────┘
                           │ optional copy
                           ▼
                 ┌──────────────────────────┐
                 │  Iceberg (PyIceberg)     │  data/iceberg/awareness/captures/
                 └─────────┬────────────────┘
                           │ query
                           ▼
                 ┌──────────────────────────┐
                 │   DuckDB ── range query  │  CLI: `awareness inspect / counts`
                 └──────────────────────────┘
```

### Layers

| Layer | Module | Purpose |
| --- | --- | --- |
| Config | `awareness.config.settings` | env + YAML overrides |
| Schemas | `awareness.schemas.{doc,jobs}` | canonical doc envelope + job state |
| Util | `awareness.util.*` | URLs, time, hashing, robots, ratelimit |
| Sources | `awareness.sources.*` | one adapter per data tier |
| Normalize | `awareness.normalize.{text,html}` | trafilatura wrapper + cleanup |
| Dedup | `awareness.dedup.engine` | exact + canonical-URL + simhash |
| Storage | `awareness.storage.{jsonl,iceberg,duckdb_index,state}` | staging + durable + query + state DB |
| Planner | `awareness.planner.planner` | request → partitions → tasks |
| Workers | `awareness.workers.engine` | async pool, backpressure, dedup, flush |
| Tail | `awareness.tail.engine` | live capture lifecycle |
| API/CLI | `awareness.{cli,api}` | user surface |

### Data model

The single durable schema is `DocCapture` (see [src/awareness/schemas/doc.py](src/awareness/schemas/doc.py)). Every adapter
produces it. Iceberg writes the same fields (see [iceberg_schema.py](src/awareness/storage/iceberg_schema.py)). All
timestamps are UTC. Provenance lives in `source_*` columns; identity in
`doc_id`/`capture_id`; dedup grouping in `parent_doc_or_dup_group`.

## Install

```bash
cd /path/to/awareness
uv venv --python 3.13 --seed
uv pip install -e '.[dev]'
# Optional: HuggingFace adapters
uv pip install -e '.[hf]'
# Optional: Postgres state DB
uv pip install -e '.[postgres]'
```

## Run

### Initialize storage

```bash
awareness init
```

### BODY — historical backfill

```bash
# Submit, then run in-process (CLI also has a separate worker entry).
awareness backfill submit --start 2024-06-01 --end 2024-06-14 --max-tasks 5
# → emits JOB_ID
awareness backfill run JOB_ID
awareness backfill status JOB_ID
```

### TAIL — live capture

```bash
# Edit configs/tail_seeds.yaml (RSS/Atom/sitemaps you want to watch).
awareness tail start            # foreground; Ctrl-C stops cleanly
# Alt: `awareness-tail` runs the same loop with signal-based shutdown.

awareness tail status
awareness tail stop             # cooperative stop request
```

### Inspect & metrics

```bash
awareness status
awareness inspect --start 2024-06-01 --end now --limit 25
awareness counts --start 2024-06-01 --end now
awareness dedup-stats
awareness metrics
```

### HTTP API

```bash
awareness-api                   # listens on 127.0.0.1:8085
# Endpoints: /healthz /status /metrics /backfill /tail /inspect /counts ...
```

## What this is and isn't

| Yes | No (despite earlier docs / commit messages) |
| --- | --- |
| Local-only ingestion: SQLite state, JSONL on disk, Iceberg on disk via PyIceberg | **There is no cloud storage**. Nothing leaves your machine. The `ops/compose` Postgres + MinIO + Redpanda + ClickHouse stack is opt-in scaffolding; it's not running by default and the code does not write to it. |
| Polling-based live updates: dashboard refreshes every 4–5s; tail view every 2s | **No Server-Sent Events / WebSocket push.** The "live activity feed" pulses when new captures land, but it polls; if the tail is idle (nothing new to discover) the UI shows the same numbers. |
| `tail_poll_seconds` cadence re-polls every configured RSS/Atom/Sitemap seed and re-arms the discovery task | Until commit `<bug-fix>` the reseed loop crashed silently on a UNIQUE constraint after the first iteration. If you were running an older build, your "tail" effectively ran *once* and then sat idle. |
| Robots.txt + per-domain politeness are enforced in `tail_recrawl` | We don't yet expose per-fetch robots outcomes in the UI. |
| `max_tasks` caps the *planner's* initial output | Sub-partitions enqueued by discovery adapters (CC index → WARC repair, GDELT slot → tail recrawl) are **not** capped by `max_tasks`. A single GDELT 15-minute slot can fan out into 1000+ sub-fetches. |

## Configuration

`configs/awareness.yaml` is the default config file; values set in YAML take
precedence over env vars (a quirk we'll fix). To set an env var, also remove
the corresponding line from `configs/awareness.yaml`.

Examples:

| Env | Meaning |
| --- | --- |
| `AW_PROJECT_ROOT` | base dir (default: this repo) |
| `AW_DATA_DIR` | where Iceberg + JSONL + state live |
| `AW_STATE_DB_URL` | SQLAlchemy URL (SQLite default; PG works) |
| `AW_USER_AGENT` | the bot identifier for HTTP fetches |
| `AW_PER_DOMAIN_CONCURRENCY` | live-fetch concurrency cap per domain |
| `AW_TAIL_POLL_SECONDS` | feed re-poll interval |
| `AW_ENABLE_ICEBERG` | toggle Iceberg writes (JSONL always on) |

## Storage layout

```
data/
├── jsonl/captures/YYYY/MM/DD/captures-*.jsonl  ← atomic staging (source of truth)
├── iceberg/                                    ← PyIceberg warehouse + catalog
├── state/awareness.sqlite                      ← jobs/tasks/manifests/dedup
├── checkpoints/                                ← reserved for adapters
├── dlq/                                        ← dead-letter task payloads
├── cache/                                      ← robots cache & helpers
├── warc/                                       ← cached WET/WARC bytes (TTLable)
└── logs/awareness.log
```

## Compliance

- Robots.txt: enforced via `RobotsCache` before any live fetch.
- Politeness: per-domain semaphore + min inter-request delay; crawl-delay
  honored if present in robots.txt.
- Public-only: adapters target publicly reachable corpora and surfaces.
- Text-only durable: HTML is converted to text and discarded; binary media
  is never persisted.

## Optional production stack (Docker)

`ops/compose/docker-compose.yml` runs Postgres + Redpanda + MinIO +
ClickHouse for those who want the analytics-grade environment. The same
Awareness binary points at it via env vars; no code change required.

```bash
docker compose -f ops/compose/docker-compose.yml up -d
```

See [docs/runbook.md](docs/runbook.md) for the operational handbook.

## Testing

```bash
pytest                  # all tests
pytest -m smoke         # smoke only
pytest -m integration   # integration only
```
