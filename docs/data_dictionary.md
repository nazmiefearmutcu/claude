# Data Dictionary

Every adapter, regardless of source, produces a `DocCapture` row with these
fields. The Iceberg schema in [src/awareness/storage/iceberg_schema.py](../src/awareness/storage/iceberg_schema.py)
mirrors this exactly.

## Identity

| Field | Type | Notes |
| --- | --- | --- |
| `doc_id` | string (req) | `xxhash3_128(canonical_url + content_hash)` — stable across re-captures of the same doc |
| `capture_id` | string (req) | `xxhash3_128(doc_id + observed_ts + source_locator)` — unique per capture |
| `parent_doc_or_dup_group` | string | Self-reference if the doc is unique; points to a sibling doc_id for EXACT_DUP / NEAR_DUP / REVISION |

## Provenance

| Field | Type | Notes |
| --- | --- | --- |
| `source_type` | string (req) | enum: `common_crawl_wet`, `common_crawl_index`, `common_crawl_warc`, `fineweb`, `fineweb_2`, `sitemap`, `rss`, `atom`, `tail_recrawl`, `gdelt`, `local_fixture` |
| `source_name` | string (req) | e.g. `CC-MAIN-2024-26`, `HuggingFaceFW/fineweb`, `tail`, `fixture` |
| `source_locator` | string | Fetchable URI to the originating shard, e.g. `https://data.commoncrawl.org/...` |
| `source_shard` | string | Shard/file identifier inside the corpus |
| `source_offset_or_record_id` | string | WARC record id, row index, etc. |
| `discovery_channel` | string (req) | How we learned about this URL: `cc-wet:<crawl>`, `rss:<url>`, `sitemap:<url>`, `gdelt:<slot>`, `tail`, `fixture` |
| `job_id` | string | Awareness job id that produced this capture |
| `batch_id` | string | One per worker task; groups captures emitted together |
| `ingest_version` | string (req) | Schema version, e.g. `0.1.0` |

## URL / domain

| Field | Type | Notes |
| --- | --- | --- |
| `url` | string | Raw URL as observed |
| `canonical_url` | string | scheme/host lowercased, default ports dropped, tracking params stripped, query sorted, fragment dropped |
| `domain` | string | eTLD+1 of `canonical_url` (e.g. `bbc.co.uk`) |

## Timestamps (all UTC tz-aware)

| Field | Type | Notes |
| --- | --- | --- |
| `fetch_ts` | timestamptz (req) | When the source originally fetched the content (WARC-Date, feed pubDate, etc.) |
| `observed_ts` | timestamptz (req) | When the Awareness adapter saw this capture (now) |
| `published_ts` | timestamptz | When the content claims it was published (page metadata or feed) |
| `last_modified` | timestamptz | HTTP `Last-Modified` header |

## HTTP / response metadata

| Field | Type | Notes |
| --- | --- | --- |
| `content_type` | string | e.g. `text/html`, `text/plain` |
| `http_status` | int32 | HTTP status code where applicable |
| `etag` | string | HTTP `ETag` |

## Text payload

| Field | Type | Notes |
| --- | --- | --- |
| `title` | string | Trafilatura-extracted title, or first text line if missing |
| `text` | string (req) | UTF-8, normalized (whitespace collapsed, control chars stripped, NFKC) |
| `language` | string | BCP-47 / ISO-639-1 code from `langdetect` |

## Hashes & fingerprints

| Field | Type | Notes |
| --- | --- | --- |
| `content_hash` | string (req) | `xxhash3_64_hexdigest` over `normalize_for_hash(text)` (lowercased, punctuation-stripped) |
| `near_dup_hash` | int64 | 64-bit simhash. Stored as **signed** int64; values >= 2^63 are folded to negative |

## Compliance & notes

| Field | Type | Notes |
| --- | --- | --- |
| `robots_decision` | string (req) | `not_applicable` (bulk corpus), `allowed`, `disallowed`, `unknown`, `no_robots` |
| `terms_note_if_relevant` | string | Free-form note for adapters that want to record source-specific terms compliance |

## Partitioning

Iceberg table is partitioned by:
- `day(fetch_ts)` — primary historical pivot
- `source_type` — isolates per-tier scans

## Identity rules summary

```
Two captures with the same canonical_url AND identical normalized text
  → same doc_id (and same content_hash)

Same canonical_url, different content
  → same canonical_url, different content_hash, different doc_id

Different canonical_url, identical normalized text
  → different doc_id (URL is in the hash), but they share content_hash;
    dedup marks the second one EXACT_DUP and sets parent_doc_or_dup_group
    to the first one's doc_id
```

## Folding captures into canonical docs

```sql
-- Canonical doc per dup_group, latest capture wins.
SELECT *
FROM captures c
WHERE c.fetch_ts = (
  SELECT MAX(c2.fetch_ts)
  FROM captures c2
  WHERE c2.parent_doc_or_dup_group = c.parent_doc_or_dup_group
);
```
