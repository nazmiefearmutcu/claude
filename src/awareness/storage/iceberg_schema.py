"""Iceberg / PyArrow schema for the durable captures table.

We keep the schema flat (no nested struct) because:
- It maps cleanly to ClickHouse later.
- DuckDB's Parquet/Iceberg reader handles it directly.
- We can rebuild near-dup clusters without nested fields.

Partitioning: ``day(fetch_ts)`` + ``source_type``.
The day partition is the primary historical pivot; source_type lets you isolate
expensive scans (e.g. FineWeb-only).
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)


# Field IDs are stable; never renumber.
ICEBERG_SCHEMA = Schema(
    NestedField(1, "doc_id", StringType(), required=True),
    NestedField(2, "capture_id", StringType(), required=True),
    NestedField(3, "parent_doc_or_dup_group", StringType(), required=False),
    NestedField(4, "source_type", StringType(), required=True),
    NestedField(5, "source_name", StringType(), required=True),
    NestedField(6, "source_locator", StringType(), required=False),
    NestedField(7, "source_shard", StringType(), required=False),
    NestedField(8, "source_offset_or_record_id", StringType(), required=False),
    NestedField(9, "discovery_channel", StringType(), required=True),
    NestedField(10, "job_id", StringType(), required=False),
    NestedField(11, "batch_id", StringType(), required=False),
    NestedField(12, "ingest_version", StringType(), required=True),
    NestedField(13, "url", StringType(), required=False),
    NestedField(14, "canonical_url", StringType(), required=False),
    NestedField(15, "domain", StringType(), required=False),
    NestedField(16, "fetch_ts", TimestamptzType(), required=True),
    NestedField(17, "observed_ts", TimestamptzType(), required=True),
    NestedField(18, "published_ts", TimestamptzType(), required=False),
    NestedField(19, "last_modified", TimestamptzType(), required=False),
    NestedField(20, "content_type", StringType(), required=False),
    NestedField(21, "http_status", IntegerType(), required=False),
    NestedField(22, "etag", StringType(), required=False),
    NestedField(23, "title", StringType(), required=False),
    NestedField(24, "text", StringType(), required=True),
    NestedField(25, "language", StringType(), required=False),
    NestedField(26, "content_hash", StringType(), required=True),
    NestedField(27, "near_dup_hash", LongType(), required=False),
    NestedField(28, "robots_decision", StringType(), required=True),
    NestedField(29, "terms_note_if_relevant", StringType(), required=False),
)


ICEBERG_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=16, field_id=1000, transform=DayTransform(), name="fetch_day"),
    PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="source_type"),
)


def pyarrow_schema() -> pa.Schema:
    """PyArrow schema mirroring the Iceberg schema; used for writes & validation."""
    ts = pa.timestamp("us", tz="UTC")
    return pa.schema(
        [
            pa.field("doc_id", pa.string(), nullable=False),
            pa.field("capture_id", pa.string(), nullable=False),
            pa.field("parent_doc_or_dup_group", pa.string()),
            pa.field("source_type", pa.string(), nullable=False),
            pa.field("source_name", pa.string(), nullable=False),
            pa.field("source_locator", pa.string()),
            pa.field("source_shard", pa.string()),
            pa.field("source_offset_or_record_id", pa.string()),
            pa.field("discovery_channel", pa.string(), nullable=False),
            pa.field("job_id", pa.string()),
            pa.field("batch_id", pa.string()),
            pa.field("ingest_version", pa.string(), nullable=False),
            pa.field("url", pa.string()),
            pa.field("canonical_url", pa.string()),
            pa.field("domain", pa.string()),
            pa.field("fetch_ts", ts, nullable=False),
            pa.field("observed_ts", ts, nullable=False),
            pa.field("published_ts", ts),
            pa.field("last_modified", ts),
            pa.field("content_type", pa.string()),
            pa.field("http_status", pa.int32()),
            pa.field("etag", pa.string()),
            pa.field("title", pa.string()),
            pa.field("text", pa.string(), nullable=False),
            pa.field("language", pa.string()),
            pa.field("content_hash", pa.string(), nullable=False),
            pa.field("near_dup_hash", pa.int64()),
            pa.field("robots_decision", pa.string(), nullable=False),
            pa.field("terms_note_if_relevant", pa.string()),
        ]
    )


CAPTURES_TABLE_IDENTIFIER = ("awareness", "captures")
