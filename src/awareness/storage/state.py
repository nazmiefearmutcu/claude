"""State DB: jobs, tasks, manifests, dedup index, checkpoints, DLQ.

Implementation is sync SQLAlchemy 2.x over SQLite by default. The hot path
of the pipeline is text fetching/extraction; state ops are small and infrequent
so synchronous calls behind ``asyncio.to_thread`` are simpler and more reliable
than full async SQLAlchemy. The URL is fully overridable so you can point this
at Postgres in production without code changes.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    func,
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from awareness.obs.logging import get_logger
from awareness.schemas.jobs import (
    JobKind,
    JobState,
    JobStatus,
    TaskState,
    TaskStatus,
)

logger = get_logger("storage.state")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class JobRow(Base):
    __tablename__ = "jobs"
    job_id: Mapped[str] = mapped_column(String, primary_key=True)
    kind: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[str] = mapped_column(String, index=True, default=JobStatus.PENDING.value)
    request_json: Mapped[str] = mapped_column(String, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    tasks_total: Mapped[int] = mapped_column(Integer, default=0)
    tasks_completed: Mapped[int] = mapped_column(Integer, default=0)
    tasks_failed: Mapped[int] = mapped_column(Integer, default=0)
    tasks_dead_lettered: Mapped[int] = mapped_column(Integer, default=0)
    docs_emitted: Mapped[int] = mapped_column(Integer, default=0)
    docs_dedup_dropped: Mapped[int] = mapped_column(Integer, default=0)
    bytes_processed: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str | None] = mapped_column(String, nullable=True)


class TaskRow(Base):
    __tablename__ = "tasks"
    task_id: Mapped[str] = mapped_column(String, primary_key=True)
    job_id: Mapped[str] = mapped_column(String, index=True)
    source_type: Mapped[str] = mapped_column(String, index=True)
    partition_key: Mapped[str] = mapped_column(String, index=True)
    payload_json: Mapped[str] = mapped_column(String, default="{}")
    status: Mapped[str] = mapped_column(String, index=True, default=TaskStatus.PENDING.value)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    docs_emitted: Mapped[int] = mapped_column(Integer, default=0)
    docs_dedup_dropped: Mapped[int] = mapped_column(Integer, default=0)
    bytes_processed: Mapped[int] = mapped_column(Integer, default=0)
    checkpoint_json: Mapped[str] = mapped_column(String, default="{}")
    __table_args__ = (UniqueConstraint("job_id", "partition_key", name="uq_task_part"),)


class DedupRow(Base):
    """Stores the canonical doc_id for a content_hash so re-ingests fold cleanly.

    A new capture with the same content_hash points to the existing dup-group
    via ``parent_doc_or_dup_group = first.doc_id``.
    """

    __tablename__ = "dedup_content"
    content_hash: Mapped[str] = mapped_column(String, primary_key=True)
    first_doc_id: Mapped[str] = mapped_column(String, index=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    capture_count: Mapped[int] = mapped_column(Integer, default=1)


class DedupNearRow(Base):
    """Coarse simhash-bucket index for near-dup search.

    To search for near-dupes for a 64-bit simhash ``H``, we split H into 4
    16-bit segments and store rows keyed by ``(segment_index, segment_value)``.
    Two near-dupes share at least one 16-bit segment with high probability
    (Manku/Jain pigeonhole). Query is ``WHERE seg = ? AND value = ?``.
    """

    __tablename__ = "dedup_near"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doc_id: Mapped[str] = mapped_column(String, index=True)
    near_dup_hash: Mapped[int] = mapped_column(Integer)  # signed int64 stored
    seg: Mapped[int] = mapped_column(Integer, index=True)
    seg_value: Mapped[int] = mapped_column(Integer, index=True)
    __table_args__ = (UniqueConstraint("doc_id", "seg", name="uq_dedup_near"),)


class ManifestRow(Base):
    """Tracks staging chunks ready for compaction (and what's been compacted)."""

    __tablename__ = "manifests"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    path: Mapped[str] = mapped_column(String, unique=True)
    records: Mapped[int] = mapped_column(Integer, default=0)
    bytes: Mapped[int] = mapped_column(Integer, default=0)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    compacted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class DLQRow(Base):
    __tablename__ = "dlq"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str | None] = mapped_column(String, index=True, nullable=True)
    task_id: Mapped[str | None] = mapped_column(String, index=True, nullable=True)
    payload_json: Mapped[str] = mapped_column(String, default="{}")
    error: Mapped[str] = mapped_column(String, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class TailRow(Base):
    """Single-row table tracking tail daemon state (Postgres-friendly)."""

    __tablename__ = "tail_state"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    running: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    job_id: Mapped[str | None] = mapped_column(String, nullable=True)
    notes: Mapped[str | None] = mapped_column(String, nullable=True)


class StateDB:
    """High-level state operations used by the planner/workers/tail."""

    def __init__(self, url: str) -> None:
        # Strip ``+aiosqlite`` if present; we use sync.
        if url.startswith("sqlite+aiosqlite:"):
            url = "sqlite:" + url[len("sqlite+aiosqlite:") :]
        self._url = url
        self._engine = create_engine(url, future=True)
        self._sessionmaker = sessionmaker(self._engine, expire_on_commit=False)
        self._lock = threading.RLock()
        self._initialized = False

    @property
    def url(self) -> str:
        return self._url

    def init(self) -> None:
        with self._lock:
            if self._initialized:
                return
            Base.metadata.create_all(self._engine)
            self._initialized = True

    def session(self) -> Session:
        if not self._initialized:
            self.init()
        return self._sessionmaker()

    # ── jobs ─────────────────────────────────────────────────────────────
    def create_job(self, job: JobState) -> None:
        with self.session() as s:
            row = JobRow(
                job_id=job.job_id,
                kind=job.kind.value,
                status=job.status.value,
                request_json=json.dumps(job.request),
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                tasks_total=job.tasks_total,
                notes=job.notes,
            )
            s.add(row)
            s.commit()

    def get_job(self, job_id: str) -> JobState | None:
        with self.session() as s:
            row = s.get(JobRow, job_id)
            if row is None:
                return None
            return self._job_state_from_row(row)

    def list_jobs(self, kind: JobKind | None = None, limit: int = 50) -> list[JobState]:
        with self.session() as s:
            stmt = select(JobRow).order_by(JobRow.created_at.desc()).limit(limit)
            if kind is not None:
                stmt = stmt.where(JobRow.kind == kind.value)
            return [self._job_state_from_row(r) for r in s.scalars(stmt)]

    def set_job_status(self, job_id: str, status: JobStatus, *, note: str | None = None) -> None:
        with self.session() as s:
            row = s.get(JobRow, job_id)
            if row is None:
                return
            row.status = status.value
            now = _utcnow()
            if status == JobStatus.RUNNING and row.started_at is None:
                row.started_at = now
            if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                row.completed_at = now
            if note:
                row.notes = note
            s.commit()

    def increment_job_counters(
        self,
        job_id: str,
        *,
        docs: int = 0,
        dedup_dropped: int = 0,
        bytes_: int = 0,
        completed: int = 0,
        failed: int = 0,
        dead_lettered: int = 0,
    ) -> None:
        with self.session() as s:
            stmt = (
                update(JobRow)
                .where(JobRow.job_id == job_id)
                .values(
                    docs_emitted=JobRow.docs_emitted + docs,
                    docs_dedup_dropped=JobRow.docs_dedup_dropped + dedup_dropped,
                    bytes_processed=JobRow.bytes_processed + bytes_,
                    tasks_completed=JobRow.tasks_completed + completed,
                    tasks_failed=JobRow.tasks_failed + failed,
                    tasks_dead_lettered=JobRow.tasks_dead_lettered + dead_lettered,
                )
            )
            s.execute(stmt)
            s.commit()

    @staticmethod
    def _job_state_from_row(row: JobRow) -> JobState:
        return JobState(
            job_id=row.job_id,
            kind=JobKind(row.kind),
            status=JobStatus(row.status),
            request=json.loads(row.request_json or "{}"),
            created_at=row.created_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            tasks_total=row.tasks_total,
            tasks_completed=row.tasks_completed,
            tasks_failed=row.tasks_failed,
            tasks_dead_lettered=row.tasks_dead_lettered,
            docs_emitted=row.docs_emitted,
            docs_dedup_dropped=row.docs_dedup_dropped,
            bytes_processed=row.bytes_processed,
            notes=row.notes,
        )

    # ── tasks ────────────────────────────────────────────────────────────
    def add_tasks(self, tasks: Iterable[TaskState]) -> int:
        materialized = list(tasks)
        if not materialized:
            return 0
        added = 0
        with self.session() as s:
            for t in materialized:
                row = TaskRow(
                    task_id=t.task_id,
                    job_id=t.job_id,
                    source_type=t.source_type.value,
                    partition_key=t.partition_key,
                    payload_json=json.dumps(t.payload),
                    status=t.status.value,
                    attempts=t.attempts,
                    last_error=t.last_error,
                    created_at=t.created_at,
                    started_at=t.started_at,
                    completed_at=t.completed_at,
                    docs_emitted=t.docs_emitted,
                    docs_dedup_dropped=t.docs_dedup_dropped,
                    bytes_processed=t.bytes_processed,
                    checkpoint_json=json.dumps(t.checkpoint or {}),
                )
                s.merge(row)
                added += 1
            s.execute(
                update(JobRow)
                .where(JobRow.job_id == materialized[0].job_id)
                .values(tasks_total=JobRow.tasks_total + added)
            )
            s.commit()
        return added

    def claim_pending_tasks(self, job_id: str, limit: int) -> list[TaskState]:
        """Atomically transition PENDING tasks to RUNNING for processing."""
        with self.session() as s:
            stmt = (
                select(TaskRow)
                .where(TaskRow.job_id == job_id, TaskRow.status == TaskStatus.PENDING.value)
                .order_by(TaskRow.created_at)
                .limit(limit)
            )
            rows = list(s.scalars(stmt))
            now = _utcnow()
            for r in rows:
                r.status = TaskStatus.RUNNING.value
                r.started_at = now
                r.attempts += 1
            s.commit()
            return [self._task_state_from_row(r) for r in rows]

    def complete_task(
        self,
        task_id: str,
        *,
        docs_emitted: int,
        docs_dedup_dropped: int,
        bytes_processed: int,
        checkpoint: dict[str, Any] | None,
    ) -> None:
        with self.session() as s:
            row = s.get(TaskRow, task_id)
            if row is None:
                return
            row.status = TaskStatus.COMPLETED.value
            row.completed_at = _utcnow()
            row.docs_emitted += docs_emitted
            row.docs_dedup_dropped += docs_dedup_dropped
            row.bytes_processed += bytes_processed
            if checkpoint is not None:
                row.checkpoint_json = json.dumps(checkpoint)
            s.commit()

    def fail_task(
        self,
        task_id: str,
        *,
        error: str,
        dead_letter: bool = False,
    ) -> None:
        with self.session() as s:
            row = s.get(TaskRow, task_id)
            if row is None:
                return
            row.last_error = error[:4000]
            if dead_letter:
                row.status = TaskStatus.DEAD_LETTERED.value
                row.completed_at = _utcnow()
            else:
                row.status = TaskStatus.PENDING.value  # retry
            s.commit()

    def task_status_counts(self, job_id: str) -> dict[str, int]:
        with self.session() as s:
            stmt = (
                select(TaskRow.status, func.count())
                .where(TaskRow.job_id == job_id)
                .group_by(TaskRow.status)
            )
            return {status: int(n) for status, n in s.execute(stmt).all()}

    @staticmethod
    def _task_state_from_row(row: TaskRow) -> TaskState:
        from awareness.schemas.doc import SourceKind

        return TaskState(
            task_id=row.task_id,
            job_id=row.job_id,
            source_type=SourceKind(row.source_type),
            partition_key=row.partition_key,
            payload=json.loads(row.payload_json or "{}"),
            status=TaskStatus(row.status),
            attempts=row.attempts,
            last_error=row.last_error,
            created_at=row.created_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            docs_emitted=row.docs_emitted,
            docs_dedup_dropped=row.docs_dedup_dropped,
            bytes_processed=row.bytes_processed,
            checkpoint=json.loads(row.checkpoint_json or "{}") or None,
        )

    # ── dedup ────────────────────────────────────────────────────────────
    def upsert_dedup(self, content_hash: str, doc_id: str) -> tuple[str, bool]:
        """Insert a new content_hash if absent. Returns (canonical_doc_id, was_new)."""
        with self.session() as s:
            row = s.get(DedupRow, content_hash)
            if row is None:
                s.add(DedupRow(content_hash=content_hash, first_doc_id=doc_id))
                s.commit()
                return doc_id, True
            row.capture_count += 1
            s.commit()
            return row.first_doc_id, False

    def add_near_dup_index(self, doc_id: str, simhash_unsigned: int) -> None:
        """Insert 4×16-bit segment rows for near-dup lookup. Signed int64 stored."""
        if simhash_unsigned <= 0:
            return
        signed = simhash_unsigned if simhash_unsigned < (1 << 63) else simhash_unsigned - (1 << 64)
        with self.session() as s:
            for seg in range(4):
                value = (simhash_unsigned >> (16 * seg)) & 0xFFFF
                s.merge(
                    DedupNearRow(
                        doc_id=doc_id,
                        near_dup_hash=signed,
                        seg=seg,
                        seg_value=value,
                    )
                )
            s.commit()

    def find_near_dup_candidates(self, simhash_unsigned: int) -> list[tuple[str, int]]:
        """Look up doc_ids that share at least one segment with this simhash."""
        out: dict[str, int] = {}
        with self.session() as s:
            for seg in range(4):
                value = (simhash_unsigned >> (16 * seg)) & 0xFFFF
                stmt = (
                    select(DedupNearRow.doc_id, DedupNearRow.near_dup_hash)
                    .where(DedupNearRow.seg == seg, DedupNearRow.seg_value == value)
                    .limit(256)
                )
                for did, h in s.execute(stmt).all():
                    out[did] = h
        return list(out.items())

    def dedup_stats(self) -> dict[str, int]:
        with self.session() as s:
            distinct = int(s.scalar(select(func.count(DedupRow.content_hash))) or 0)
            captures_sum = int(s.scalar(select(func.coalesce(func.sum(DedupRow.capture_count), 0))) or 0)
            return {
                "distinct_content_hashes": distinct,
                "total_captures_seen": captures_sum,
                "near_dup_index_rows": int(s.scalar(select(func.count(DedupNearRow.id))) or 0),
            }

    # ── manifests ────────────────────────────────────────────────────────
    def add_manifest(self, path: str, records: int, bytes_: int) -> None:
        with self.session() as s:
            s.merge(ManifestRow(path=path, records=records, bytes=bytes_))
            s.commit()

    def list_pending_manifests(self) -> list[dict[str, Any]]:
        with self.session() as s:
            stmt = select(ManifestRow).where(ManifestRow.compacted_at.is_(None)).order_by(ManifestRow.id)
            return [
                {"id": r.id, "path": r.path, "records": r.records, "bytes": r.bytes}
                for r in s.scalars(stmt)
            ]

    def mark_manifest_compacted(self, manifest_id: int) -> None:
        with self.session() as s:
            row = s.get(ManifestRow, manifest_id)
            if row is None:
                return
            row.compacted_at = _utcnow()
            s.commit()

    # ── DLQ ──────────────────────────────────────────────────────────────
    def add_dlq(self, job_id: str | None, task_id: str | None, payload: dict[str, Any], error: str) -> None:
        with self.session() as s:
            s.add(
                DLQRow(
                    job_id=job_id,
                    task_id=task_id,
                    payload_json=json.dumps(payload),
                    error=error[:4000],
                )
            )
            s.commit()

    # ── tail state ───────────────────────────────────────────────────────
    def set_tail(self, running: bool, job_id: str | None = None, note: str | None = None) -> None:
        with self.session() as s:
            row = s.get(TailRow, 1)
            now = _utcnow()
            if row is None:
                row = TailRow(id=1)
                s.add(row)
            row.running = 1 if running else 0
            if running:
                row.started_at = now
                row.stopped_at = None
                row.job_id = job_id
            else:
                row.stopped_at = now
            if note:
                row.notes = note
            s.commit()

    def get_tail(self) -> dict[str, Any]:
        with self.session() as s:
            row = s.get(TailRow, 1)
            if row is None:
                return {"running": False, "job_id": None, "started_at": None, "stopped_at": None}
            return {
                "running": bool(row.running),
                "job_id": row.job_id,
                "started_at": row.started_at.isoformat() if row.started_at else None,
                "stopped_at": row.stopped_at.isoformat() if row.stopped_at else None,
                "notes": row.notes,
            }
