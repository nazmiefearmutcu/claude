"""Job, task, and request schemas for the planner/orchestrator.

Job lifecycle:
    PENDING → RUNNING → (PAUSED ⇄ RUNNING) → (COMPLETED | FAILED | CANCELLED)

Tasks belong to a job and represent a single partition of work
(e.g. one WET shard, one HF dataset shard, one feed at one timestamp).
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from awareness.schemas.doc import SourceKind


class JobKind(str, Enum):
    BACKFILL = "backfill"  # historical body
    TAIL = "tail"          # live capture
    REPAIR = "repair"      # gap-fill / WARC repair


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    SKIPPED = "skipped"


class BackfillRequest(BaseModel):
    """Inputs to a BODY backfill plan."""

    model_config = ConfigDict(extra="forbid")

    start: datetime
    end: datetime  # inclusive upper bound
    sources: list[SourceKind] = Field(default_factory=list)
    # Optional partitioning hints:
    domains: list[str] | None = None  # narrow planner to these domains
    languages: list[str] | None = None
    max_tasks: int | None = None  # smoke-test cap
    notes: str | None = None


class TaskState(BaseModel):
    """One unit of work emitted by the planner."""

    model_config = ConfigDict(extra="forbid")

    task_id: str
    job_id: str
    source_type: SourceKind
    partition_key: str  # human-readable: e.g. "CC-MAIN-2024-26:wet:00042"
    payload: dict[str, Any]  # adapter-specific kwargs
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    docs_emitted: int = 0
    docs_dedup_dropped: int = 0
    bytes_processed: int = 0
    checkpoint: dict[str, Any] | None = None  # adapter-specific resume cursor


class JobState(BaseModel):
    """Planner's view of a backfill or tail job."""

    model_config = ConfigDict(extra="forbid")

    job_id: str
    kind: JobKind
    status: JobStatus = JobStatus.PENDING
    request: dict[str, Any]  # raw BackfillRequest or tail config
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    tasks_total: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_dead_lettered: int = 0
    docs_emitted: int = 0
    docs_dedup_dropped: int = 0
    bytes_processed: int = 0
    notes: str | None = None
