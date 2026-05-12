"""WARC targeted repair adapter — fetch one WARC record by byte range and extract."""

from __future__ import annotations

import asyncio
import io
from typing import AsyncIterator

import httpx

from awareness.normalize.html import html_to_text
from awareness.normalize.text import detect_language
from awareness.obs.logging import get_logger
from awareness.schemas.doc import DocCapture, RobotsDecision, SourceKind, SourceRef
from awareness.schemas.jobs import BackfillRequest
from awareness.sources.base import Adapter, AdapterContext, PartitionSpec
from awareness.sources.commoncrawl_wet import CC_BASE
from awareness.util.hashing import (
    capture_id_for,
    content_hash as compute_content_hash,
    doc_id_for,
    simhash64,
)
from awareness.util.timeutil import to_utc, utcnow
from awareness.util.urls import canonical_url, domain_of

logger = get_logger("sources.warc_repair")


class WarcRepairAdapter(Adapter):
    source_type = SourceKind.COMMON_CRAWL_WARC

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        # Repair is reactive — never planned directly. The CC index adapter
        # enqueues these as sub-partitions.
        return []

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        warc_path = partition.payload["warc_path"]
        offset = int(partition.payload["offset"])
        length = int(partition.payload["length"])
        url = partition.payload.get("url")
        crawl_id = partition.payload.get("crawl_id", "")

        end = offset + length - 1
        full_url = f"{CC_BASE}/{warc_path}"
        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                resp = await client.get(
                    full_url,
                    headers={"Range": f"bytes={offset}-{end}", "User-Agent": context.user_agent},
                )
                if resp.status_code not in (200, 206):
                    logger.warning("warc_range_failed", status=resp.status_code, path=warc_path)
                    return
                payload = resp.content
        except httpx.HTTPError as exc:
            logger.warning("warc_range_exception", err=str(exc))
            return

        # Parse the WARC record from the byte range.
        cap = await asyncio.get_event_loop().run_in_executor(
            None,
            _parse_warc_record,
            payload,
            warc_path,
            offset,
            url,
            crawl_id,
            context.user_agent,
            context.job_id,
            context.task_id,
            context.batch_id,
            context.ingest_version,
        )
        if cap is not None:
            yield cap


def _parse_warc_record(
    payload: bytes,
    warc_path: str,
    offset: int,
    url: str | None,
    crawl_id: str,
    user_agent: str,
    job_id: str,
    task_id: str,
    batch_id: str,
    ingest_version: str,
) -> DocCapture | None:
    from warcio.archiveiterator import ArchiveIterator  # noqa: PLC0415

    try:
        for record in ArchiveIterator(io.BytesIO(payload)):
            if record.rec_type != "response":
                continue
            target = url or record.rec_headers.get_header("WARC-Target-URI")
            if not target:
                return None
            content_type = (record.http_headers.get_header("Content-Type") or "") if record.http_headers else ""
            if "html" not in content_type.lower() and "text" not in content_type.lower():
                return None
            try:
                html = record.content_stream().read().decode("utf-8", "replace")
            except (UnicodeDecodeError, AttributeError, OSError):
                return None
            ext = html_to_text(html, url=target)
            if ext is None:
                return None
            text = ext.text.text
            cu = canonical_url(target)
            ch = compute_content_hash(text)
            sim = simhash64(text)
            fetch_ts = to_utc(record.rec_headers.get_header("WARC-Date")) or utcnow()
            observed_ts = utcnow()
            did = doc_id_for(cu, ch)
            return DocCapture(
                doc_id=did,
                capture_id=capture_id_for(did, observed_ts.isoformat(), warc_path),
                source=SourceRef(
                    source_type=SourceKind.COMMON_CRAWL_WARC,
                    source_name=crawl_id or warc_path.split("/")[-3],
                    source_locator=f"{CC_BASE}/{warc_path}",
                    source_shard=warc_path,
                    source_offset_or_record_id=str(offset),
                ),
                discovery_channel=f"cc-warc:{warc_path}",
                job_id=job_id,
                batch_id=batch_id,
                ingest_version=ingest_version,
                url=target,
                canonical_url=cu,
                domain=domain_of(cu),
                fetch_ts=fetch_ts,
                observed_ts=observed_ts,
                published_ts=ext.published_ts,
                title=ext.title,
                text=text,
                language=ext.language_hint or detect_language(text),
                content_hash=ch,
                near_dup_hash=sim,
                content_type=content_type or "text/html",
                http_status=record.http_headers.get_statuscode() if record.http_headers else None,
                robots_decision=RobotsDecision.NOT_APPLICABLE,
            )
    except Exception as exc:
        logger.debug("warc_parse_failed", err=str(exc))
    return None
