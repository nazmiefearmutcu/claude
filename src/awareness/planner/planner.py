"""Planner: turn requests into tasks and persist them.

Public entry points:
- ``submit_backfill(request)`` → job_id
- ``submit_tail()`` → job_id (one persistent job; many sub-partitions over time)
- ``status(job_id)`` → JobState + counts
- ``add_tail_partition(payload)`` → enqueue a tail recrawl partition

The planner is the only writer to the ``jobs`` and ``tasks`` tables for new
work. Workers update task status as they run.
"""

from __future__ import annotations

import uuid
from typing import Iterable

from awareness.config import get_settings
from awareness.obs.logging import get_logger
from awareness.schemas.doc import SourceKind
from awareness.schemas.jobs import (
    BackfillRequest,
    JobKind,
    JobState,
    JobStatus,
    TaskState,
    TaskStatus,
)
from awareness.sources import get_adapter_registry
from awareness.sources.base import PartitionSpec
from awareness.storage.state import StateDB
from awareness.util.timeutil import utcnow

logger = get_logger("planner")


class Planner:
    def __init__(self, state: StateDB) -> None:
        self._state = state
        self._registry = get_adapter_registry()

    # ── BODY ─────────────────────────────────────────────────────────────
    def submit_backfill(self, request: BackfillRequest) -> str:
        job_id = f"backfill-{uuid.uuid4().hex[:12]}"
        job = JobState(
            job_id=job_id,
            kind=JobKind.BACKFILL,
            status=JobStatus.PENDING,
            request=request.model_dump(mode="json"),
            notes=request.notes,
        )
        self._state.create_job(job)

        # Decide which adapters are in scope.
        wanted: set[SourceKind] = set(request.sources)
        if not wanted:
            # Sensible defaults if user gave none. Order matters for clarity.
            wanted = {
                SourceKind.COMMON_CRAWL_WET,
                SourceKind.FINEWEB,
                SourceKind.GDELT,
            }

        # Domain-narrowed backfills also activate the CC index (which enqueues
        # WARC repair as sub-partitions).
        if request.domains:
            wanted.add(SourceKind.COMMON_CRAWL_INDEX)

        total = 0
        for adapter in self._registry.all():
            if adapter.source_type not in wanted:
                continue
            partitions = adapter.plan(request)
            tasks = list(self._partitions_to_tasks(job_id, partitions))
            if not tasks:
                continue
            if request.max_tasks:
                remaining = max(0, request.max_tasks - total)
                tasks = tasks[:remaining]
                if not tasks:
                    break
            self._state.add_tasks(tasks)
            total += len(tasks)
            logger.info(
                "planner_emitted_tasks",
                job_id=job_id,
                source_type=adapter.source_type.value,
                count=len(tasks),
            )

        logger.info("planner_backfill_submitted", job_id=job_id, total_tasks=total)
        return job_id

    # ── TAIL ─────────────────────────────────────────────────────────────
    def submit_tail(self, seeds: dict) -> str:
        """Create or refresh a long-lived tail job and seed its initial partitions."""
        job_id = f"tail-{uuid.uuid4().hex[:12]}"
        job = JobState(
            job_id=job_id,
            kind=JobKind.TAIL,
            status=JobStatus.RUNNING,
            request=seeds,
            notes="tail",
            started_at=utcnow(),
        )
        self._state.create_job(job)
        self._state.set_job_status(job_id, JobStatus.RUNNING)

        # Seed initial feed/sitemap partitions.
        tasks: list[TaskState] = []
        for entry in seeds.get("feeds", []):
            url = entry.get("url")
            if not url:
                continue
            tasks.append(self._task(job_id, SourceKind.RSS, f"rss:{url}", {"kind": "rss", "url": url}))
        for entry in seeds.get("atom", []):
            url = entry.get("url")
            if not url:
                continue
            tasks.append(self._task(job_id, SourceKind.RSS, f"atom:{url}", {"kind": "atom", "url": url}))
        for entry in seeds.get("sitemaps", []):
            url = entry.get("url")
            if not url:
                continue
            tasks.append(self._task(job_id, SourceKind.RSS, f"sitemap:{url}", {"kind": "sitemap", "url": url}))

        if tasks:
            self._state.add_tasks(tasks)
        self._state.set_tail(running=True, job_id=job_id, note="tail-active")
        logger.info("planner_tail_started", job_id=job_id, seeds=len(tasks))
        return job_id

    def stop_tail(self, job_id: str, *, note: str | None = None) -> None:
        self._state.set_job_status(job_id, JobStatus.COMPLETED, note=note or "tail-stopped")
        self._state.set_tail(running=False, job_id=job_id, note=note or "tail-stopped")
        logger.info("planner_tail_stopped", job_id=job_id)

    # ── helpers ──────────────────────────────────────────────────────────
    def _task(self, job_id: str, source: SourceKind, partition_key: str, payload: dict) -> TaskState:
        return TaskState(
            task_id=f"t-{uuid.uuid4().hex[:16]}",
            job_id=job_id,
            source_type=source,
            partition_key=partition_key,
            payload=payload,
        )

    def _partitions_to_tasks(self, job_id: str, partitions: list[PartitionSpec]) -> Iterable[TaskState]:
        for p in partitions:
            yield self._task(job_id, p.source_type, p.partition_key, p.payload)

    def enqueue_subpartitions(self, job_id: str, partitions: Iterable[PartitionSpec]) -> int:
        materialized = list(partitions)
        if not materialized:
            return 0
        tasks = list(self._partitions_to_tasks(job_id, materialized))
        return self._state.add_tasks(tasks)

    # ── reporting ────────────────────────────────────────────────────────
    def status(self, job_id: str) -> dict:
        job = self._state.get_job(job_id)
        if job is None:
            return {"error": "unknown_job", "job_id": job_id}
        counts = self._state.task_status_counts(job_id)
        return {
            "job_id": job_id,
            "kind": job.kind.value,
            "status": job.status.value,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "tasks_total": job.tasks_total,
            "tasks_completed": job.tasks_completed,
            "tasks_failed": job.tasks_failed,
            "tasks_dead_lettered": job.tasks_dead_lettered,
            "docs_emitted": job.docs_emitted,
            "docs_dedup_dropped": job.docs_dedup_dropped,
            "bytes_processed": job.bytes_processed,
            "task_status_counts": counts,
            "notes": job.notes,
        }
