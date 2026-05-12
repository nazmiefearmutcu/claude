# Architecture

## Goal

Build a **public text internet awareness engine** with two modes:

- **BODY** — backfill historical public text from a chosen start date up to now.
- **TAIL** — capture newly published public text from `start_time` until you stop it.

Storage is text-only. Everything else (URLs, timestamps, hashes, provenance,
language) is metadata.

## Why a layered tier strategy

A monolithic crawler is the wrong shape for the public text web because:
- It scales linearly with politeness, not with hardware.
- It cannot meaningfully backfill years of history within a session.
- It duplicates work that organizations like Common Crawl and HuggingFace
  have already done at very high cost.

We use three tiers:

| Tier | Adapters | Role |
| --- | --- | --- |
| **A — Historical bulk text** | `commoncrawl_wet`, `cc_index`, `fineweb` | Cheap, parallel, partitioned text-first corpora |
| **B — Live discovery surfaces** | `feeds` (RSS/Atom/Sitemap), `tail_recrawl`, `gdelt` | URL-level new-content discovery + polite fetch |
| **C — Targeted repair** | `warc_repair` | Byte-range WARC fetches for specific records |

Each tier produces the **same `DocCapture` envelope**, so the downstream
pipeline (dedup, storage, query) is unified.

## Control flow

```
                     ┌───────────────┐
              CLI ──►│   Planner     │     translates BackfillRequest
              API ──►│               │     into source-native partitions
                     └──────┬────────┘
                            │  TaskState rows in state DB
                            ▼
                     ┌───────────────┐
                     │ Worker Engine │     pool of asyncio workers
                     │  (bounded)    │     dedup → flush → checkpoint
                     └──────┬────────┘
                            │  PartitionSpec
                            ▼
                     ┌───────────────┐
                     │ Source        │     each yields DocCapture
                     │ Adapter       │     and may enqueue sub-partitions
                     └──────┬────────┘
                            │  DocCapture
                            ▼
                     ┌───────────────┐
                     │ Dedup Engine  │     content_hash + simhash + url
                     └──────┬────────┘
                            │
                            ▼
                ┌───────────────┐    ┌────────────────┐
                │ JSONL staging │ ─► │ Iceberg (Parq) │
                │ (atomic)      │    │ optional sink  │
                └───────┬───────┘    └────────────────┘
                        │
                        ▼
                ┌───────────────┐
                │   DuckDB      │   range query + counts + inspect
                └───────────────┘
```

### Sub-partitioning

Discovery adapters (CC index, feeds, GDELT) don't yield text directly. Their
`run_partition()` populates `context.extras["enqueue"]` with new
`PartitionSpec` records. After the task completes, the worker's
`enqueue_subpartitions()` call adds them as new pending tasks. This keeps the
data plane simple — only one queue, one worker pool, one schema.

### Resume and idempotence

- Tasks are uniquely keyed on `(job_id, partition_key)`.
- Adapters' `run_partition()` accept a `context.checkpoint` dict; they may
  read it (e.g. `row_index`, `seen_urls`) and write to it during the run.
- On task completion, the checkpoint is persisted in the state DB.
- Failed tasks are re-queued with the same partition_key; on `attempts >=
  max_retries`, the task is dead-lettered to the DLQ table.

## Storage layers

| Layer | Where | Used for |
| --- | --- | --- |
| Staging | `data/jsonl/captures/Y/M/D/*.jsonl` | Atomic source-of-truth |
| Durable | `data/iceberg/awareness/captures/` | Iceberg table for analytics |
| State | `data/state/awareness.sqlite` | Jobs, tasks, manifests, dedup |
| Query | `data/duckdb/metadata.duckdb` | DuckDB view over JSONL + (Iceberg) |
| Cache | `data/warc/` | WARC/WET shards while parsing |
| DLQ | `data/dlq/` and `dlq` table | Repeatedly-failing payloads |

The JSONL staging is written first **and is always consistent on disk** even
if PyIceberg fails. The compaction path can lift JSONL → Iceberg later.

## Identity & dedup

- `doc_id = xxhash3_128(canonical_url + content_hash)`
- `capture_id = xxhash3_128(doc_id + observed_ts + source_locator)`
- `content_hash = xxhash3_64(normalize(text))`
- `near_dup_hash = simhash64(text)` — used with a 4×16-bit segment index
  (Manku/Jain pigeonhole) for O(1)-per-segment lookup of near-duplicates.

Dedup decisions don't drop captures (provenance is sacred). They label them:
`NEW`, `REVISION`, `EXACT_DUP`, `NEAR_DUP`. Downstream readers fold
captures into canonical documents with
`WHERE doc_id = parent_doc_or_dup_group`.

## Parallelism

- **Source-level**: one adapter per source kind.
- **Shard-level**: each adapter emits multiple PartitionSpecs (e.g. one per
  CC shard, one per feed, one per GDELT 15-min slot).
- **Time-partition**: backfills enumerate ISO weeks → CC crawl IDs and 15-min
  GDELT slots.
- **Task-level**: the worker engine runs N partitions concurrently via an
  `asyncio.Semaphore(concurrency)`.
- **Per-domain politeness**: `PerDomainLimiter` enforces concurrency and
  spacing per registered domain. robots.txt crawl-delay overrides the global
  default when present.
- **Pipeline-stage**: extraction runs inside `loop.run_in_executor` so the
  event loop is never blocked by heavy parsing.

## Compliance boundaries

- **Public-only**: every adapter targets public, openly-accessible surfaces
  (Common Crawl, FineWeb, public RSS/Atom, public sitemaps, GDELT).
- **Robots.txt**: enforced before live fetches via `RobotsCache`. Disallowed
  URLs return `RobotsDecision.DISALLOWED` and skip persistence.
- **No login / no paywall**: there is no credential store; nothing
  authenticates.
- **No private APIs**: only public, documented endpoints.
- **No binary persistence**: HTML / WARC bytes live only in transient caches;
  durable storage is text + metadata.

## Failure model

| Failure | Where | Behavior |
| --- | --- | --- |
| Adapter exception | `_run_task` | task → PENDING (retry), DLQ at max_retries |
| JSONL write fail | `_flush` | logged warning; in-memory buffer cleared |
| Iceberg append fail | `_flush` | logged warning; JSONL remains source of truth |
| HTTP timeout/5xx | adapter | per-task retry with backoff |
| robots.txt disallow | adapter | capture skipped, counter incremented |
| Stop signal | engine | drain buffer → close writers → exit |

## Optional production stack

The compose file `ops/compose/docker-compose.yml` runs:
- **Postgres** → state DB (swap `AW_STATE_DB_URL`)
- **MinIO** → S3-compatible warehouse (swap `AW_ICEBERG_WAREHOUSE`)
- **Redpanda** → event bus for multi-process workers (future)
- **ClickHouse** → analytics over the same Parquet files

Code paths are identical; only env vars differ.
