"""Common Crawl WET adapter.

Reads Common Crawl WET (text) shards directly. WET files contain extracted
plaintext for each captured URL plus rich metadata (URL, fetch date, content
type). They are the cheapest path to "historical body" at scale and require
no HTML extraction.

Architecture:
- ``plan(req)`` translates [start, end] into a set of crawl_ids that overlap
  the window, then enumerates WET shards via the canonical
  ``wet.paths.gz`` index file for each crawl.
- ``run_partition(partition)`` streams a single WET file with ``warcio``, yields
  one ``DocCapture`` per ``WARC-Type: conversion`` record.
- Checkpoint stores ``last_offset`` or ``last_record_id`` so re-runs resume.

We use the official ``s3://commoncrawl/...`` paths over HTTPS:
``https://data.commoncrawl.org/<path>``. No AWS credentials needed.

Crawl-id ↔ time mapping:
- Crawls are named ``CC-MAIN-YYYY-WW`` (ISO year + ISO week).
- Each crawl runs over ~2 weeks. We map [start, end] to a list of crawl_ids by
  enumerating ISO weeks in the range. If a crawl_id doesn't exist (planned
  but not published), we skip it on first fetch failure.

This file is intentionally framework-friendly: the actual WET fetch happens
in ``run_partition`` so the planner stays cheap.
"""

from __future__ import annotations

import asyncio
import gzip
import io
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

import httpx

from awareness.config import get_settings
from awareness.normalize.text import detect_language, normalize_text, safe_title
from awareness.obs.logging import get_logger
from awareness.obs.metrics import get_metrics
from awareness.schemas.doc import DocCapture, RobotsDecision, SourceKind, SourceRef
from awareness.schemas.jobs import BackfillRequest
from awareness.sources.base import Adapter, AdapterContext, PartitionSpec
from awareness.util.hashing import (
    capture_id_for,
    content_hash as compute_content_hash,
    doc_id_for,
    simhash64,
)
from awareness.util.timeutil import to_utc, utcnow
from awareness.util.urls import canonical_url, domain_of

logger = get_logger("sources.cc_wet")

CC_BASE = "https://data.commoncrawl.org"


def _iso_year_weeks(start: datetime, end: datetime) -> list[tuple[int, int]]:
    """Return ISO (year, week) tuples covering ``[start, end]``."""
    cur = to_utc(start) or utcnow()
    end_utc = to_utc(end) or utcnow()
    if cur > end_utc:
        cur, end_utc = end_utc, cur
    seen: list[tuple[int, int]] = []
    last_pair: tuple[int, int] | None = None
    while cur <= end_utc:
        iso = cur.isocalendar()
        pair = (iso.year, iso.week)
        if pair != last_pair:
            seen.append(pair)
            last_pair = pair
        cur += timedelta(days=1)
    return seen


def crawl_ids_for_range(start: datetime, end: datetime) -> list[str]:
    """Convert a date range to candidate crawl_ids like ``CC-MAIN-2024-26``."""
    pairs = _iso_year_weeks(start, end)
    # Common Crawl crawls span ~2 weeks; we coalesce to even-week starts.
    seen: set[tuple[int, int]] = set()
    out: list[str] = []
    for year, week in pairs:
        anchor_week = week if week % 2 == 1 else week - 1
        if anchor_week < 1:
            anchor_week = 1
        key = (year, anchor_week)
        if key in seen:
            continue
        seen.add(key)
        out.append(f"CC-MAIN-{year}-{anchor_week:02d}")
    return out


class CommonCrawlWetAdapter(Adapter):
    source_type = SourceKind.COMMON_CRAWL_WET

    def __init__(self, max_shards_per_crawl: int = 1) -> None:
        super().__init__()
        # Default to 1 shard per crawl for sanity in smoke tests. CLI/config can
        # override via the ``BackfillRequest.notes`` payload or per-partition.
        self._max_shards_per_crawl = max(1, max_shards_per_crawl)

    # ── planner ──────────────────────────────────────────────────────────
    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        crawls = crawl_ids_for_range(request.start, request.end)
        partitions: list[PartitionSpec] = []
        for crawl_id in crawls:
            partitions.append(
                PartitionSpec(
                    source_type=self.source_type,
                    partition_key=f"{crawl_id}:wet-paths",
                    payload={
                        "kind": "shard-discovery",
                        "crawl_id": crawl_id,
                        "max_shards": self._max_shards_per_crawl,
                        "domains": request.domains,
                        "languages": request.languages,
                    },
                )
            )
        return partitions

    # ── runner ───────────────────────────────────────────────────────────
    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        kind = partition.payload.get("kind")
        if kind == "shard-discovery":
            async for cap in self._run_discovery(partition, context):
                yield cap
        elif kind == "shard-fetch":
            async for cap in self._run_shard(partition, context):
                yield cap
        else:
            logger.warning("cc_wet_unknown_partition", kind=kind)

    async def _run_discovery(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        """Fetch the crawl's wet.paths.gz, enqueue shard partitions, yield nothing.

        Because the adapter contract is to *yield captures*, but discovery
        emits sub-partitions, we store the discovered shards in the worker
        extras via ``context.extras["enqueue"]``. The worker reads them.
        """
        crawl_id = partition.payload["crawl_id"]
        max_shards = int(partition.payload.get("max_shards", 1))
        url = f"{CC_BASE}/crawl-data/{crawl_id}/wet.paths.gz"
        logger.info("cc_wet_discovery_start", crawl_id=crawl_id, url=url)

        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            try:
                resp = await client.get(url, headers={"User-Agent": context.user_agent})
            except httpx.HTTPError as exc:
                logger.warning("cc_wet_paths_fetch_failed", crawl_id=crawl_id, err=str(exc))
                return
            if resp.status_code != 200:
                logger.warning("cc_wet_paths_not_found", crawl_id=crawl_id, status=resp.status_code)
                return
            try:
                body = gzip.decompress(resp.content).decode("utf-8", "replace")
            except OSError as exc:
                logger.warning("cc_wet_paths_decode_failed", crawl_id=crawl_id, err=str(exc))
                return

        shards = [line.strip() for line in body.splitlines() if line.strip()]
        chosen = shards[:max_shards]
        get_metrics().inc("cc_wet.shards_discovered", value=len(shards), labels={"crawl_id": crawl_id})
        get_metrics().inc("cc_wet.shards_enqueued", value=len(chosen), labels={"crawl_id": crawl_id})

        enqueue = context.extras.setdefault("enqueue", [])
        for shard in chosen:
            enqueue.append(
                PartitionSpec(
                    source_type=self.source_type,
                    partition_key=f"{crawl_id}:wet:{shard.split('/')[-1]}",
                    payload={
                        "kind": "shard-fetch",
                        "crawl_id": crawl_id,
                        "shard_path": shard,
                        "domains": partition.payload.get("domains"),
                        "languages": partition.payload.get("languages"),
                    },
                )
            )
        return
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]

    async def _run_shard(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        crawl_id = partition.payload["crawl_id"]
        shard_path = partition.payload["shard_path"]
        domains_filter = set(partition.payload.get("domains") or []) or None
        languages_filter = set(partition.payload.get("languages") or []) or None

        url = f"{CC_BASE}/{shard_path}"
        logger.info("cc_wet_shard_start", crawl_id=crawl_id, shard=shard_path, url=url)

        settings = get_settings()
        # Stream the shard to a local file, then parse with warcio. WET files
        # are typically 100-500 MB so streaming-to-disk is the cheap path.
        cache_dir = settings.warc_cache_dir or settings.data_dir / "warc"
        cache_dir.mkdir(parents=True, exist_ok=True)
        local = cache_dir / shard_path.replace("/", "_")
        if not local.exists():
            try:
                async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
                    async with client.stream(
                        "GET", url, headers={"User-Agent": context.user_agent}
                    ) as resp:
                        if resp.status_code != 200:
                            logger.warning(
                                "cc_wet_shard_not_found",
                                crawl_id=crawl_id,
                                shard=shard_path,
                                status=resp.status_code,
                            )
                            return
                        tmp = local.with_suffix(local.suffix + ".tmp")
                        with open(tmp, "wb") as fh:
                            async for chunk in resp.aiter_bytes(1 << 20):
                                if context.is_stopping():
                                    fh.close()
                                    tmp.unlink(missing_ok=True)
                                    return
                                fh.write(chunk)
                        tmp.rename(local)
                logger.info("cc_wet_shard_cached", path=str(local))
            except httpx.HTTPError as exc:
                logger.warning("cc_wet_shard_download_failed", err=str(exc))
                return

        # Parse on a worker thread so we don't block the event loop.
        await asyncio.get_event_loop().run_in_executor(
            None, _ensure_warcio_available
        )

        # Yield captures parsed from the cached file.
        for cap in await asyncio.get_event_loop().run_in_executor(
            None,
            _parse_wet_to_captures,
            local,
            crawl_id,
            shard_path,
            domains_filter,
            languages_filter,
            context.user_agent,
            context.job_id,
            context.task_id,
            context.batch_id,
            context.ingest_version,
        ):
            if context.is_stopping():
                return
            yield cap


def _ensure_warcio_available() -> None:
    import warcio  # noqa: F401


def _parse_wet_to_captures(
    path,
    crawl_id: str,
    shard_path: str,
    domains_filter: set[str] | None,
    languages_filter: set[str] | None,
    user_agent: str,
    job_id: str,
    task_id: str,
    batch_id: str,
    ingest_version: str,
) -> list[DocCapture]:
    """Synchronous WET parser used inside a worker thread."""
    from warcio.archiveiterator import ArchiveIterator  # noqa: PLC0415

    settings = get_settings()
    out: list[DocCapture] = []
    seen_in_shard = 0

    with open(path, "rb") as fh:
        for record in ArchiveIterator(fh):
            seen_in_shard += 1
            if record.rec_type != "conversion":
                continue
            url = record.rec_headers.get_header("WARC-Target-URI")
            if not url:
                continue
            cu = canonical_url(url)
            dom = domain_of(cu) if cu else None
            if domains_filter and dom not in domains_filter:
                continue
            try:
                raw = record.content_stream().read()
            except (OSError, ValueError):
                continue
            try:
                text_raw = raw.decode("utf-8", "replace")
            except (UnicodeDecodeError, AttributeError):
                continue
            norm = normalize_text(
                text_raw,
                min_chars=settings.text_min_chars,
                max_chars=settings.text_max_chars,
            )
            if norm.discarded_reason:
                continue
            lang = detect_language(norm.text) or None
            if languages_filter and lang not in languages_filter:
                continue

            ch = compute_content_hash(norm.text)
            sim = simhash64(norm.text)
            fetched_at = record.rec_headers.get_header("WARC-Date") or ""
            fetch_ts = to_utc(fetched_at) or utcnow()
            observed_ts = utcnow()
            record_id = record.rec_headers.get_header("WARC-Record-ID") or ""

            did = doc_id_for(cu, ch)
            cap = DocCapture(
                doc_id=did,
                capture_id=capture_id_for(did, observed_ts.isoformat(), shard_path),
                source=SourceRef(
                    source_type=SourceKind.COMMON_CRAWL_WET,
                    source_name=crawl_id,
                    source_locator=f"{CC_BASE}/{shard_path}",
                    source_shard=shard_path,
                    source_offset_or_record_id=record_id,
                ),
                discovery_channel=f"cc-wet:{crawl_id}",
                job_id=job_id,
                batch_id=batch_id,
                ingest_version=ingest_version,
                url=url,
                canonical_url=cu,
                domain=dom,
                fetch_ts=fetch_ts,
                observed_ts=observed_ts,
                title=safe_title(None, norm.text),
                text=norm.text,
                language=lang,
                content_hash=ch,
                near_dup_hash=sim,
                robots_decision=RobotsDecision.NOT_APPLICABLE,  # bulk corpus
                content_type="text/plain",
                http_status=200,
            )
            out.append(cap)
    logger.info(
        "cc_wet_shard_parsed",
        crawl_id=crawl_id,
        shard=shard_path,
        records_seen=seen_in_shard,
        captures_emitted=len(out),
    )
    return out
