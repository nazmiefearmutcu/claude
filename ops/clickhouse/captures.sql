-- ClickHouse DDL for analytics over the Iceberg captures table.
-- Use this when running the optional Docker Compose stack: it exposes the
-- captures Parquet files (written by PyIceberg) as a ClickHouse table.
--
-- Once MinIO is up and the awareness writer has populated the table:
--   1) Run this script against ClickHouse.
--   2) ``SELECT count() FROM awareness.captures_s3 WHERE fetch_ts >= now() - INTERVAL 1 DAY``.

CREATE DATABASE IF NOT EXISTS awareness;

-- S3 (MinIO) view onto the Iceberg data files. Adjust the path if you change
-- the warehouse location or table name.
CREATE TABLE IF NOT EXISTS awareness.captures_s3
ENGINE = S3(
    'http://minio:9000/awareness/warehouse/awareness/captures/data/**/*.parquet',
    'awareness',
    'awareness12345',
    'Parquet'
)
SETTINGS
    s3_truncate_on_insert = 0,
    input_format_parquet_allow_missing_columns = 1;

-- Local cache (optional, faster for repeat queries).
CREATE TABLE IF NOT EXISTS awareness.captures
(
    doc_id String,
    capture_id String,
    parent_doc_or_dup_group String,
    source_type LowCardinality(String),
    source_name LowCardinality(String),
    url Nullable(String),
    canonical_url Nullable(String),
    domain LowCardinality(Nullable(String)),
    fetch_ts DateTime64(3, 'UTC'),
    observed_ts DateTime64(3, 'UTC'),
    published_ts Nullable(DateTime64(3, 'UTC')),
    title Nullable(String),
    text String,
    language LowCardinality(Nullable(String)),
    content_hash String,
    near_dup_hash Int64,
    discovery_channel String,
    job_id Nullable(String),
    ingest_version LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMMDD(fetch_ts), source_type)
ORDER BY (fetch_ts, domain, doc_id)
TTL fetch_ts + INTERVAL 5 YEAR;
