"""GDELT supplemental adapter.

GDELT publishes 15-minute master files at:
    http://data.gdeltproject.org/gdeltv2/<YYYYMMDDHHMMSS>.gkg.csv.zip
    http://data.gdeltproject.org/gdeltv2/<YYYYMMDDHHMMSS>.export.CSV.zip

We only consume the GKG (Global Knowledge Graph) CSV, which lists news article
URLs with timestamps. We then enqueue tail_recrawl partitions per URL. We do
NOT persist GDELT's analytical fields; we use it strictly as a discovery
channel for the public text web.

For the BODY backfill, this adapter walks 15-minute slots in the range; for
TAIL it's driven by the tail engine.
"""

from __future__ import annotations

import asyncio
import csv
import io
import zipfile
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import httpx

from awareness.obs.logging import get_logger
from awareness.obs.metrics import get_metrics
from awareness.schemas.doc import DocCapture, SourceKind
from awareness.schemas.jobs import BackfillRequest
from awareness.sources.base import Adapter, AdapterContext, PartitionSpec
from awareness.util.timeutil import to_utc, utcnow

logger = get_logger("sources.gdelt")
GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"


def _quarter_hours(start: datetime, end: datetime) -> list[str]:
    """Yield 15-minute slot ids in ``yyyymmddhhmmss`` form."""
    cur = to_utc(start) or utcnow()
    end = to_utc(end) or utcnow()
    # Round down to nearest 15 minutes.
    cur = cur.replace(minute=cur.minute - (cur.minute % 15), second=0, microsecond=0)
    out: list[str] = []
    while cur <= end:
        out.append(cur.strftime("%Y%m%d%H%M%S"))
        cur += timedelta(minutes=15)
    return out


class GdeltAdapter(Adapter):
    source_type = SourceKind.GDELT

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        slots = _quarter_hours(request.start, request.end)
        # Cap to avoid runaway tasks in smoke runs.
        cap = request.max_tasks or 8
        slots = slots[:cap]
        return [
            PartitionSpec(
                source_type=self.source_type,
                partition_key=f"gdelt:gkg:{slot}",
                payload={"slot": slot},
            )
            for slot in slots
        ]

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        slot = partition.payload["slot"]
        url = f"{GDELT_BASE}/{slot}.gkg.csv.zip"
        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                r = await client.get(url, headers={"User-Agent": context.user_agent})
            if r.status_code != 200:
                logger.info("gdelt_slot_missing", slot=slot, status=r.status_code)
                return
            payload = r.content
        except httpx.HTTPError as exc:
            logger.warning("gdelt_fetch_failed", err=str(exc))
            return

        urls = await asyncio.get_event_loop().run_in_executor(None, _extract_gkg_urls, payload)
        get_metrics().inc("gdelt.urls_discovered", value=len(urls), labels={"slot": slot})
        enqueue = context.extras.setdefault("enqueue", [])
        for u in urls:
            enqueue.append(
                PartitionSpec(
                    source_type=SourceKind.TAIL_RECRAWL,
                    partition_key=f"tail-gdelt:{u}",
                    payload={
                        "url": u,
                        "discovery_channel": f"gdelt:{slot}",
                        "source_kind": "gdelt",
                    },
                )
            )
        return
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]


def _extract_gkg_urls(zipped: bytes) -> list[str]:
    out: set[str] = set()
    try:
        with zipfile.ZipFile(io.BytesIO(zipped)) as z:
            for name in z.namelist():
                with z.open(name) as fh:
                    text_stream = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
                    reader = csv.reader(text_stream, delimiter="\t")
                    for row in reader:
                        if not row:
                            continue
                        # GKG v2 column layout: ``DOCUMENTIDENTIFIER`` is column index 4
                        if len(row) > 4:
                            url = row[4].strip()
                            if url.startswith(("http://", "https://")):
                                out.add(url)
    except (zipfile.BadZipFile, OSError):
        return []
    return list(out)
