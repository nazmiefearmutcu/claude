"""Canonical document envelope — the unified schema every source maps into.

Design rules:
- Text-only durable persistence. The ``text`` field is the canonical body.
- Provenance is always preserved (source_locator + source_shard + source_offset).
- All timestamps are UTC tz-aware. Persisted as microsecond ints in Iceberg.
- Identity layer:
    * ``doc_id`` is deterministic from canonical_url + content_hash (per-version).
    * ``capture_id`` is unique per fetch (doc_id + observed_ts).
    * ``parent_doc_or_dup_group`` links near-dupes/recaptures to a stable group.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RobotsDecision(str, Enum):
    """Outcome of robots.txt evaluation for a URL."""

    NOT_APPLICABLE = "not_applicable"  # Source is a corpus shard, not live fetch.
    ALLOWED = "allowed"
    DISALLOWED = "disallowed"
    UNKNOWN = "unknown"
    NO_ROBOTS = "no_robots"  # robots.txt absent / 404.


class SourceKind(str, Enum):
    """Identifies which tier/adapter produced a capture."""

    COMMON_CRAWL_WET = "common_crawl_wet"
    COMMON_CRAWL_INDEX = "common_crawl_index"
    COMMON_CRAWL_WARC = "common_crawl_warc"
    FINEWEB = "fineweb"
    FINEWEB_2 = "fineweb_2"
    SITEMAP = "sitemap"
    RSS = "rss"
    ATOM = "atom"
    TAIL_RECRAWL = "tail_recrawl"
    GDELT = "gdelt"
    LOCAL_FIXTURE = "local_fixture"  # tests/smoke


class SourceRef(BaseModel):
    """Pointer back to the originating shard/record.

    Captures enough metadata to re-read the original byte range:
    - ``source_type``: enum
    - ``source_name``: e.g. ``CC-MAIN-2024-26``, ``HuggingFaceFW/fineweb``
    - ``source_locator``: URI to the shard (s3://, https://, file://)
    - ``source_shard``: file/shard identifier inside the corpus
    - ``source_offset_or_record_id``: WARC record id, row index, etc.
    """

    model_config = ConfigDict(extra="forbid")

    source_type: SourceKind
    source_name: str
    source_locator: str | None = None
    source_shard: str | None = None
    source_offset_or_record_id: str | None = None


class DocCapture(BaseModel):
    """A single capture of a document at a point in time.

    Every successful adapter run produces one ``DocCapture``. The dedup engine
    later assigns ``parent_doc_or_dup_group`` based on the cluster the document
    lands in.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # Identity
    doc_id: str
    capture_id: str
    parent_doc_or_dup_group: str | None = None

    # Provenance
    source: SourceRef
    discovery_channel: str  # e.g. "sitemap:/sitemap.xml", "rss:feed_url", "cc:CC-MAIN-..."
    job_id: str | None = None
    batch_id: str | None = None
    ingest_version: str

    # URL / domain
    url: str | None = None
    canonical_url: str | None = None
    domain: str | None = None

    # Timestamps (UTC)
    fetch_ts: datetime
    observed_ts: datetime
    published_ts: datetime | None = None
    last_modified: datetime | None = None

    # HTTP / response metadata
    content_type: str | None = None
    http_status: int | None = None
    etag: str | None = None

    # Text payload
    title: str | None = None
    text: str
    language: str | None = None

    # Hashes / fingerprints
    content_hash: str  # xxhash64 of normalized text
    near_dup_hash: int | None = None  # simhash 64-bit unsigned, stored as signed int

    # Compliance / notes
    robots_decision: RobotsDecision = RobotsDecision.NOT_APPLICABLE
    terms_note_if_relevant: str | None = None

    @field_validator("fetch_ts", "observed_ts", "published_ts", "last_modified", mode="before")
    @classmethod
    def _ensure_utc(cls, v: Any) -> Any:
        if v is None or isinstance(v, str):
            return v
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        return v

    @field_validator("text")
    @classmethod
    def _text_nonempty(cls, v: str) -> str:
        if v is None or not v.strip():
            raise ValueError("DocCapture.text must be non-empty after normalization")
        return v

    def as_iceberg_row(self) -> dict[str, Any]:
        """Flatten this capture to a row compatible with the Iceberg schema."""
        return {
            "doc_id": self.doc_id,
            "capture_id": self.capture_id,
            "parent_doc_or_dup_group": self.parent_doc_or_dup_group,
            "source_type": self.source.source_type.value,
            "source_name": self.source.source_name,
            "source_locator": self.source.source_locator,
            "source_shard": self.source.source_shard,
            "source_offset_or_record_id": self.source.source_offset_or_record_id,
            "discovery_channel": self.discovery_channel,
            "job_id": self.job_id,
            "batch_id": self.batch_id,
            "ingest_version": self.ingest_version,
            "url": self.url,
            "canonical_url": self.canonical_url,
            "domain": self.domain,
            "fetch_ts": self.fetch_ts,
            "observed_ts": self.observed_ts,
            "published_ts": self.published_ts,
            "last_modified": self.last_modified,
            "content_type": self.content_type,
            "http_status": self.http_status,
            "etag": self.etag,
            "title": self.title,
            "text": self.text,
            "language": self.language,
            "content_hash": self.content_hash,
            "near_dup_hash": self.near_dup_hash,
            "robots_decision": self.robots_decision.value,
            "terms_note_if_relevant": self.terms_note_if_relevant,
        }


# Alias used by downstream code that thinks in terms of "the doc".
CanonicalDoc = DocCapture


# Field list used by the Iceberg schema builder and the JSONL staging writer.
DOC_FIELDS_ORDERED: tuple[str, ...] = (
    "doc_id",
    "capture_id",
    "parent_doc_or_dup_group",
    "source_type",
    "source_name",
    "source_locator",
    "source_shard",
    "source_offset_or_record_id",
    "discovery_channel",
    "job_id",
    "batch_id",
    "ingest_version",
    "url",
    "canonical_url",
    "domain",
    "fetch_ts",
    "observed_ts",
    "published_ts",
    "last_modified",
    "content_type",
    "http_status",
    "etag",
    "title",
    "text",
    "language",
    "content_hash",
    "near_dup_hash",
    "robots_decision",
    "terms_note_if_relevant",
)
