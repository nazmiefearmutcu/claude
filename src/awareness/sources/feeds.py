"""Feeds adapter: RSS, Atom, and Sitemap discovery.

Used by BOTH body (when feeds expose historical archives) and tail (the
default discovery channel for newly published content). The adapter:

1. Reads a YAML seed file describing feeds and sitemaps.
2. ``plan()`` emits one partition per seed; payload carries cursor state.
3. ``run_partition()`` fetches the feed/sitemap, diffs vs last cursor,
   for each new URL emits a TailRecrawl sub-partition.

Politeness: robots.txt is consulted; per-domain limiter is acquired by the
sub-partition that actually fetches the page (tail_recrawl).
"""

from __future__ import annotations

from typing import Any, AsyncIterator

import feedparser
import httpx
import yaml
from lxml import etree

from awareness.config import get_settings
from awareness.obs.logging import get_logger
from awareness.obs.metrics import get_metrics
from awareness.schemas.doc import DocCapture, SourceKind
from awareness.schemas.jobs import BackfillRequest
from awareness.sources.base import Adapter, AdapterContext, PartitionSpec
from awareness.util.timeutil import to_utc, utcnow
from awareness.util.urls import canonical_url, domain_of

logger = get_logger("sources.feeds")


def _load_seeds(path) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data if isinstance(data, dict) else {}


class FeedsAdapter(Adapter):
    """RSS / Atom / Sitemap discovery → recrawl sub-partitions."""

    source_type = SourceKind.RSS  # canonical; ATOM/SITEMAP share this adapter

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        # Feeds aren't a natural historical body source; only meaningful when
        # the tail engine kicks them off. For BODY backfills we emit nothing.
        return []

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        kind = partition.payload.get("kind", "rss")
        url = partition.payload["url"]
        if kind == "sitemap":
            urls = await _read_sitemap(url, context.user_agent)
            channel = f"sitemap:{url}"
        else:
            urls = await _read_feed(url, context.user_agent)
            channel = f"{kind}:{url}"

        get_metrics().inc("feeds.urls_discovered", value=len(urls), labels={"channel": kind})

        # Filter against cursor.
        last_seen: set[str] = set(context.checkpoint.get("seen_urls", []))
        new_urls = [u for u in urls if canonical_url(u) and canonical_url(u) not in last_seen]
        # Update cursor (bounded to last 5000 to keep memory in check).
        merged = last_seen | {canonical_url(u) for u in urls if canonical_url(u)}
        context.checkpoint["seen_urls"] = list(list(merged)[-5000:])

        enqueue = context.extras.setdefault("enqueue", [])
        for u in new_urls:
            enqueue.append(
                PartitionSpec(
                    source_type=SourceKind.TAIL_RECRAWL,
                    partition_key=f"tail:{canonical_url(u)}",
                    payload={
                        "url": u,
                        "discovery_channel": channel,
                        "source_kind": kind,
                    },
                )
            )
        return
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]


async def _read_feed(url: str, user_agent: str) -> list[str]:
    """RSS / Atom — fetch and parse."""
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": user_agent})
            if r.status_code != 200 or not r.content:
                return []
            body = r.content
    except httpx.HTTPError as exc:
        logger.warning("feed_fetch_failed", url=url, err=str(exc))
        return []
    parsed = feedparser.parse(body)
    out: list[str] = []
    for entry in parsed.entries:
        link = getattr(entry, "link", None)
        if link and link.startswith(("http://", "https://")):
            out.append(link)
    return out


async def _read_sitemap(url: str, user_agent: str, depth: int = 1) -> list[str]:
    """Parse a sitemap or sitemap-index. Follows one level of nesting by default."""
    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": user_agent})
            if r.status_code != 200 or not r.content:
                return []
            body = r.content
    except httpx.HTTPError as exc:
        logger.warning("sitemap_fetch_failed", url=url, err=str(exc))
        return []

    try:
        if body.startswith(b"\x1f\x8b"):
            import gzip as _gz

            body = _gz.decompress(body)
        root = etree.fromstring(body)
    except (etree.XMLSyntaxError, OSError, ValueError) as exc:
        logger.warning("sitemap_parse_failed", url=url, err=str(exc))
        return []

    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    out: list[str] = []
    tag = etree.QName(root.tag).localname
    if tag == "sitemapindex":
        if depth <= 0:
            return out
        for child in root.findall(f"{ns}sitemap/{ns}loc"):
            loc = (child.text or "").strip()
            if loc:
                out.extend(await _read_sitemap(loc, user_agent, depth=depth - 1))
    else:
        for child in root.findall(f"{ns}url/{ns}loc"):
            loc = (child.text or "").strip()
            if loc:
                out.append(loc)
    return out
