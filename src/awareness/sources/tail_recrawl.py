"""Tail recrawl adapter.

Receives one partition per discovered URL (emitted by the feeds adapter or
by direct planner inputs), fetches the page, runs HTML→text, emits one
``DocCapture``.

Politeness:
- Robots.txt is consulted via the shared cache.
- Per-domain concurrency and delay are honored.
- If robots disallows, we emit a DocCapture only if explicitly requested.
"""

from __future__ import annotations

from typing import AsyncIterator

import httpx

from awareness.config import get_settings
from awareness.normalize.html import html_to_text
from awareness.normalize.text import detect_language
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
from awareness.util.ratelimit import PerDomainLimiter
from awareness.util.robots import RobotsCache
from awareness.util.timeutil import parse_http_date, utcnow
from awareness.util.urls import canonical_url, domain_of, is_http_url

logger = get_logger("sources.tail_recrawl")


class TailRecrawlAdapter(Adapter):
    source_type = SourceKind.TAIL_RECRAWL

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        # Reactive only; the planner never emits these directly. The feeds
        # adapter and the tail engine enqueue them as sub-partitions.
        return []

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        url = partition.payload["url"]
        discovery_channel = partition.payload.get("discovery_channel", "tail")
        source_kind = partition.payload.get("source_kind", "rss")

        if not is_http_url(url):
            return
        dom = domain_of(url)
        if not dom:
            return

        settings = get_settings()
        limiter: PerDomainLimiter = context.extras.get("limiter") or _global_limiter(settings)
        robots: RobotsCache = context.extras.get("robots") or _global_robots(settings)

        # Robots check.
        try:
            allowed = await robots.is_allowed(url, context.user_agent)
        except Exception:
            allowed = True
        robots_decision = RobotsDecision.ALLOWED if allowed else RobotsDecision.DISALLOWED
        if not allowed:
            get_metrics().inc("tail.robots_disallowed", labels={"domain": dom})
            return

        crawl_delay = robots.crawl_delay(url)
        async with limiter.domain(dom, override_delay=crawl_delay):
            try:
                async with httpx.AsyncClient(
                    timeout=settings.request_timeout_sec,
                    follow_redirects=True,
                    headers={"User-Agent": context.user_agent},
                ) as client:
                    r = await client.get(url)
            except httpx.HTTPError as exc:
                logger.warning("tail_fetch_failed", url=url, err=str(exc))
                get_metrics().inc("tail.fetch_errors", labels={"domain": dom})
                return

        get_metrics().inc("tail.fetches", labels={"domain": dom})
        if r.status_code >= 400 or not r.content:
            get_metrics().inc("tail.fetch_non_200", labels={"domain": dom, "status": str(r.status_code)})
            return

        ctype = r.headers.get("Content-Type", "")
        if "html" not in ctype.lower() and "xml" not in ctype.lower() and "text" not in ctype.lower():
            return

        html = r.text
        ext = html_to_text(html, url=url)
        if ext is None:
            get_metrics().inc("tail.text_too_short", labels={"domain": dom})
            return
        text = ext.text.text
        ch = compute_content_hash(text)
        sim = simhash64(text)

        observed_ts = utcnow()
        cu = canonical_url(ext.canonical_url_hint or url) or canonical_url(url)
        did = doc_id_for(cu, ch)

        yield DocCapture(
            doc_id=did,
            capture_id=capture_id_for(did, observed_ts.isoformat(), url),
            source=SourceRef(
                source_type=SourceKind.TAIL_RECRAWL,
                source_name="tail",
                source_locator=url,
                source_shard=discovery_channel,
                source_offset_or_record_id=None,
            ),
            discovery_channel=discovery_channel,
            job_id=context.job_id,
            batch_id=context.batch_id,
            ingest_version=context.ingest_version,
            url=url,
            canonical_url=cu,
            domain=dom,
            fetch_ts=observed_ts,
            observed_ts=observed_ts,
            published_ts=ext.published_ts,
            last_modified=parse_http_date(r.headers.get("Last-Modified")),
            content_type=ctype,
            http_status=int(r.status_code),
            etag=r.headers.get("ETag"),
            title=ext.title,
            text=text,
            language=ext.language_hint or detect_language(text),
            content_hash=ch,
            near_dup_hash=sim,
            robots_decision=robots_decision,
        )


_LIMITER: PerDomainLimiter | None = None
_ROBOTS: RobotsCache | None = None


def _global_limiter(settings) -> PerDomainLimiter:
    global _LIMITER
    if _LIMITER is None:
        _LIMITER = PerDomainLimiter(
            concurrency=settings.per_domain_concurrency,
            min_delay_sec=settings.per_domain_delay_sec,
        )
    return _LIMITER


def _global_robots(settings) -> RobotsCache:
    global _ROBOTS
    if _ROBOTS is None:
        _ROBOTS = RobotsCache(ttl=settings.robots_cache_ttl_sec)
    return _ROBOTS
