"""Local fixture adapter — emits captures from an in-memory list.

Used by tests and the smoke runner when network access isn't desired. Not
registered by default in ``AdapterRegistry``; tests register an instance
explicitly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterator

from awareness.normalize.text import detect_language, normalize_text, safe_title
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


class LocalFixtureAdapter(Adapter):
    """Adapter that emits captures from a Python list. Tests-only."""

    source_type = SourceKind.LOCAL_FIXTURE

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self._rows = rows or []

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        chunks = max(1, len(self._rows) // 5 or 1)
        out: list[PartitionSpec] = []
        for i, start in enumerate(range(0, len(self._rows), chunks)):
            out.append(
                PartitionSpec(
                    source_type=self.source_type,
                    partition_key=f"fixture:{i}",
                    payload={"start": start, "end": start + chunks},
                )
            )
        return out

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        start = int(partition.payload.get("start", 0))
        end = int(partition.payload.get("end", len(self._rows)))
        for row in self._rows[start:end]:
            if context.is_stopping():
                return
            text_raw = row.get("text", "")
            norm = normalize_text(text_raw, min_chars=50)
            if norm.discarded_reason:
                continue
            url = row.get("url")
            cu = canonical_url(url) if url else None
            fetch_ts = to_utc(row.get("fetch_ts")) or utcnow()
            observed_ts = utcnow()
            ch = compute_content_hash(norm.text)
            sim = simhash64(norm.text)
            did = doc_id_for(cu, ch)
            yield DocCapture(
                doc_id=did,
                capture_id=capture_id_for(did, observed_ts.isoformat(), partition.partition_key),
                source=SourceRef(
                    source_type=SourceKind.LOCAL_FIXTURE,
                    source_name="fixture",
                    source_locator="local",
                    source_shard=partition.partition_key,
                    source_offset_or_record_id=str(row.get("id", "")),
                ),
                discovery_channel="fixture",
                job_id=context.job_id,
                batch_id=context.batch_id,
                ingest_version=context.ingest_version,
                url=url,
                canonical_url=cu,
                domain=domain_of(cu) if cu else None,
                fetch_ts=fetch_ts,
                observed_ts=observed_ts,
                title=safe_title(row.get("title"), norm.text),
                text=norm.text,
                language=row.get("language") or detect_language(norm.text),
                content_hash=ch,
                near_dup_hash=sim,
                content_type="text/plain",
                http_status=200,
                robots_decision=RobotsDecision.NOT_APPLICABLE,
            )
