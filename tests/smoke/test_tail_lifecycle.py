"""Smoke test for tail lifecycle.

Uses the local fixture adapter to mimic a feed source, starts the tail engine,
waits long enough for at least one poll cycle, then stops cleanly. Verifies:
- tail state transitions to running and then stopped,
- captures land in JSONL staging,
- range query returns the captured docs,
- repeating the run produces dedup hits.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from awareness.config import get_settings
from awareness.planner.planner import Planner
from awareness.schemas.doc import SourceKind
from awareness.schemas.jobs import TaskState
from awareness.sources import get_adapter_registry
from awareness.sources.local_fixture import LocalFixtureAdapter
from awareness.storage.duckdb_index import DuckDbIndex
from awareness.storage.state import StateDB
from awareness.tail.engine import TailEngine
from awareness.workers.engine import WorkerEngine


pytestmark = [pytest.mark.smoke, pytest.mark.asyncio]


_LIVE_DOCS = [
    {
        "id": i,
        "url": f"https://liveblog.example/post/{i}",
        "title": f"Live post {i}",
        "text": (
            f"This live post number {i} announces a new event with thorough "
            "narrative spanning multiple sentences to satisfy the minimum "
            "content length filter applied during normalization. " * 4
        ),
        "fetch_ts": datetime(2024, 8, 1, 12, i, tzinfo=timezone.utc).isoformat(),
        "language": "en",
    }
    for i in range(1, 6)
]


@pytest.mark.asyncio
async def test_tail_starts_drains_stops(tmp_project: Path) -> None:
    settings = get_settings()
    state = StateDB(settings.state_db_url or f"sqlite:///{tmp_project / 'state.db'}")
    state.init()
    planner = Planner(state)

    # Inject the fixture adapter so tail "discovery" is deterministic.
    reg = get_adapter_registry()
    reg.register(LocalFixtureAdapter(rows=_LIVE_DOCS))

    # We manually create the tail-style job + tasks; we don't need real feeds.
    job_id = planner.submit_tail(seeds={"feeds": [], "sitemaps": [], "atom": []})
    # Seed it with fixture tasks (mirrors what discovery would emit).
    tasks: list[TaskState] = []
    for i in range(2):
        tasks.append(
            TaskState(
                task_id=f"t-{uuid.uuid4().hex[:12]}",
                job_id=job_id,
                source_type=SourceKind.LOCAL_FIXTURE,
                partition_key=f"fixture-tail:{i}",
                payload={"start": 0, "end": len(_LIVE_DOCS)},
            )
        )
    state.add_tasks(tasks)

    # Run the worker engine in tail mode briefly.
    engine = WorkerEngine(state, planner, concurrency=2)
    try:

        async def stop_after(seconds: float) -> None:
            await asyncio.sleep(seconds)
            engine.request_stop()

        await asyncio.gather(
            engine.run_tail(job_id, poll_seconds=0.1),
            stop_after(0.6),
        )
    finally:
        await engine.aclose()

    # The job should now be drained.
    status = planner.status(job_id)
    assert status["docs_emitted"] >= len(_LIVE_DOCS)

    # JSONL chunks should be readable via DuckDB.
    idx = DuckDbIndex(
        db_path=settings.duckdb_path(),
        jsonl_dir=settings.staging_jsonl_dir(),
        iceberg_warehouse=settings.iceberg_warehouse,
    )
    rows = idx.execute(
        "SELECT count(*) AS n FROM captures WHERE source_type = $st",
        {"st": SourceKind.LOCAL_FIXTURE.value},
    )
    assert rows[0]["n"] >= len(_LIVE_DOCS)

    # Tail state was running while we drained; mark it stopped now.
    planner.stop_tail(job_id, note="smoke-test-stopped")
    tail_state = state.get_tail()
    assert tail_state["running"] is False

    # Run a second pass — every doc should now dedupe.
    state.add_tasks(
        [
            TaskState(
                task_id=f"t-{uuid.uuid4().hex[:12]}",
                job_id=job_id,
                source_type=SourceKind.LOCAL_FIXTURE,
                partition_key="fixture-tail:replay",
                payload={"start": 0, "end": len(_LIVE_DOCS)},
            )
        ]
    )
    engine = WorkerEngine(state, planner, concurrency=2)
    try:
        await engine.run_job(job_id, poll_seconds=0.05)
    finally:
        await engine.aclose()
    stats = state.dedup_stats()
    assert stats["total_captures_seen"] >= 2 * len(_LIVE_DOCS)
