"""Common Crawl CDX index adapter.

Uses the public CDX server (``https://index.commoncrawl.org/<crawl_id>-index``)
to selectively discover URLs matching domain/URL prefix filters in a window,
then enqueues WARC-repair sub-partitions for byte-range fetches.

Acts as a *planner-style* adapter: it does the URL discovery only. The actual
text extraction is performed by the WARC-repair adapter, which receives one
partition per matched record.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import AsyncIterator

import httpx

from awareness.obs.logging import get_logger
from awareness.obs.metrics import get_metrics
from awareness.schemas.doc import DocCapture, SourceKind
from awareness.schemas.jobs import BackfillRequest
from awareness.sources.base import Adapter, AdapterContext, PartitionSpec
from awareness.sources.commoncrawl_wet import crawl_ids_for_range

logger = get_logger("sources.cc_index")
CDX_BASE = "https://index.commoncrawl.org"


class CommonCrawlIndexAdapter(Adapter):
    source_type = SourceKind.COMMON_CRAWL_INDEX

    def __init__(self, max_results_per_crawl: int = 200) -> None:
        super().__init__()
        self._max_results = max_results_per_crawl

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        if not request.domains:
            return []  # only meaningful for domain-narrowed backfills
        crawls = crawl_ids_for_range(request.start, request.end)
        out: list[PartitionSpec] = []
        for crawl_id in crawls:
            for dom in request.domains:
                out.append(
                    PartitionSpec(
                        source_type=self.source_type,
                        partition_key=f"{crawl_id}:cdx:{dom}",
                        payload={
                            "crawl_id": crawl_id,
                            "url_filter": f"*.{dom}/*",
                            "max_results": self._max_results,
                        },
                    )
                )
        return out

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        crawl_id = partition.payload["crawl_id"]
        url_filter = partition.payload["url_filter"]
        max_results = int(partition.payload.get("max_results", 200))

        cdx_url = f"{CDX_BASE}/{crawl_id}-index"
        params = {"url": url_filter, "output": "json", "limit": str(max_results)}
        logger.info("cc_index_query", crawl_id=crawl_id, filter=url_filter)
        records: list[dict] = []
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            try:
                async with client.stream(
                    "GET", cdx_url, params=params, headers={"User-Agent": context.user_agent}
                ) as resp:
                    if resp.status_code != 200:
                        logger.warning(
                            "cc_index_query_failed",
                            crawl_id=crawl_id,
                            status=resp.status_code,
                        )
                        return
                    async for line in resp.aiter_lines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            except httpx.HTTPError as exc:
                logger.warning("cc_index_query_exception", err=str(exc))
                return

        get_metrics().inc("cc_index.matches", value=len(records), labels={"crawl_id": crawl_id})

        # Enqueue WARC-repair partitions for the discovered records.
        enqueue = context.extras.setdefault("enqueue", [])
        for r in records:
            warc_path = r.get("filename")
            offset = r.get("offset")
            length = r.get("length")
            if not warc_path or offset is None or length is None:
                continue
            enqueue.append(
                PartitionSpec(
                    source_type=SourceKind.COMMON_CRAWL_WARC,
                    partition_key=f"warc:{warc_path}:{offset}",
                    payload={
                        "warc_path": warc_path,
                        "offset": int(offset),
                        "length": int(length),
                        "url": r.get("url"),
                        "timestamp": r.get("timestamp"),
                        "crawl_id": crawl_id,
                    },
                )
            )
        return
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]
