"""FineWeb / FineWeb2 adapter (HuggingFace).

Streams plain-text rows from the HuggingFace datasets ``HuggingFaceFW/fineweb``
and ``HuggingFaceFW/fineweb-2``. The ``datasets`` package is optional; if it's
not installed, ``plan()`` returns an empty list with a warning and the adapter
becomes a no-op. This keeps the dependency surface lean for users who just
want the WET + tail path.

Partitioning:
- One partition per (dataset, dump, sample limit). The 'dump' aligns with a
  Common Crawl ``CC-MAIN-YYYY-WW`` value when present in FineWeb's metadata.
- Within a partition, we stream rows; each row has ``text``, ``url``,
  ``date`` (when available), and ``language``.

Resume: checkpoint stores the last consumed row index per partition.
"""

from __future__ import annotations

from typing import Any, AsyncIterator

from awareness.config import get_settings
from awareness.normalize.text import detect_language, normalize_text, safe_title
from awareness.obs.logging import get_logger
from awareness.schemas.doc import DocCapture, RobotsDecision, SourceKind, SourceRef
from awareness.schemas.jobs import BackfillRequest
from awareness.sources.base import Adapter, AdapterContext, PartitionSpec
from awareness.sources.commoncrawl_wet import crawl_ids_for_range
from awareness.util.hashing import (
    capture_id_for,
    content_hash as compute_content_hash,
    doc_id_for,
    simhash64,
)
from awareness.util.timeutil import to_utc, utcnow
from awareness.util.urls import canonical_url, domain_of

logger = get_logger("sources.fineweb")


class FineWebAdapter(Adapter):
    """Combined FineWeb + FineWeb2 adapter.

    The same adapter handles both datasets; the dataset name is on the partition.
    """

    source_type = SourceKind.FINEWEB

    def __init__(self, default_dataset: str = "HuggingFaceFW/fineweb", rows_per_partition: int = 500) -> None:
        super().__init__()
        self._default = default_dataset
        self._rows = rows_per_partition

    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        try:
            import datasets  # noqa: F401, PLC0415
        except ImportError:
            logger.info("fineweb_skipped_missing_datasets_lib")
            return []
        # Build candidate (dataset, dump) tuples.
        crawls = crawl_ids_for_range(request.start, request.end)
        # If languages requested, pivot to fineweb-2 (multilingual).
        datasets_to_use = []
        if request.languages and any(lang.lower() not in ("en", "english") for lang in request.languages):
            datasets_to_use.append(("HuggingFaceFW/fineweb-2", SourceKind.FINEWEB_2))
        else:
            datasets_to_use.append((self._default, SourceKind.FINEWEB))

        out: list[PartitionSpec] = []
        for ds_name, kind in datasets_to_use:
            for crawl_id in crawls:
                out.append(
                    PartitionSpec(
                        source_type=kind,
                        partition_key=f"{ds_name}:{crawl_id}",
                        payload={
                            "dataset": ds_name,
                            "dump": crawl_id,
                            "rows_per_partition": self._rows,
                            "languages": request.languages,
                            "domains": request.domains,
                        },
                    )
                )
        return out

    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        try:
            from datasets import load_dataset  # noqa: PLC0415
        except ImportError:
            logger.info("fineweb_run_skipped_missing_datasets_lib")
            return

        ds_name = partition.payload["dataset"]
        dump = partition.payload.get("dump")
        rows_per = int(partition.payload.get("rows_per_partition", self._rows))
        languages = set(partition.payload.get("languages") or [])
        domains_filter = set(partition.payload.get("domains") or [])
        start_offset = int(context.checkpoint.get("row_index", 0))

        settings = get_settings()

        # Stream mode is required to avoid downloading TB-scale dumps.
        try:
            ds = load_dataset(ds_name, name=dump, split="train", streaming=True)
        except Exception as exc:
            logger.warning("fineweb_load_failed", ds=ds_name, dump=dump, err=str(exc))
            return

        emitted = 0
        for i, row in enumerate(ds):
            if context.is_stopping():
                break
            if i < start_offset:
                continue
            if emitted >= rows_per:
                break
            text_raw = row.get("text") or row.get("content")
            if not text_raw:
                continue
            url = row.get("url") or row.get("source")
            row_date = row.get("date") or row.get("date_download") or row.get("published_date")
            lang = (row.get("language") or "").lower() or None
            if languages and lang and lang not in languages:
                continue
            cu = canonical_url(url) if url else None
            dom = domain_of(cu) if cu else None
            if domains_filter and dom not in domains_filter:
                continue
            norm = normalize_text(
                text_raw,
                min_chars=settings.text_min_chars,
                max_chars=settings.text_max_chars,
            )
            if norm.discarded_reason:
                continue
            ch = compute_content_hash(norm.text)
            sim = simhash64(norm.text)
            fetch_ts = to_utc(row_date) or utcnow()
            observed_ts = utcnow()
            did = doc_id_for(cu, ch)
            yield DocCapture(
                doc_id=did,
                capture_id=capture_id_for(did, observed_ts.isoformat(), str(i)),
                source=SourceRef(
                    source_type=partition.source_type,
                    source_name=ds_name,
                    source_locator=f"hf://datasets/{ds_name}",
                    source_shard=str(dump or ""),
                    source_offset_or_record_id=str(i),
                ),
                discovery_channel=f"hf:{ds_name}",
                job_id=context.job_id,
                batch_id=context.batch_id,
                ingest_version=context.ingest_version,
                url=url,
                canonical_url=cu,
                domain=dom,
                fetch_ts=fetch_ts,
                observed_ts=observed_ts,
                title=safe_title(None, norm.text),
                text=norm.text,
                language=lang or detect_language(norm.text),
                content_hash=ch,
                near_dup_hash=sim,
                content_type="text/plain",
                http_status=200,
                robots_decision=RobotsDecision.NOT_APPLICABLE,
            )
            emitted += 1
            # Update checkpoint cooperatively.
            context.checkpoint["row_index"] = i + 1
        logger.info("fineweb_partition_done", ds=ds_name, dump=dump, emitted=emitted)
