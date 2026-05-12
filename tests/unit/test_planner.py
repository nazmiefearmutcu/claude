"""Planner tests — partition emission and request routing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from awareness.planner.planner import Planner
from awareness.schemas.doc import SourceKind
from awareness.schemas.jobs import BackfillRequest, JobStatus
from awareness.sources import get_adapter_registry
from awareness.sources.commoncrawl_wet import crawl_ids_for_range
from awareness.storage.state import StateDB


def test_crawl_ids_for_range_covers_a_year() -> None:
    # Stay inside ISO year 2024 to avoid the year-boundary ISO-week shift
    # (Dec 30-31 2024 fall in ISO year 2025).
    start = datetime(2024, 1, 8, tzinfo=timezone.utc)  # ISO week 2 of 2024
    end = datetime(2024, 12, 22, tzinfo=timezone.utc)
    crawls = crawl_ids_for_range(start, end)
    assert 12 <= len(crawls) <= 30
    assert all(c.startswith("CC-MAIN-2024-") for c in crawls)


def test_planner_emits_tasks_for_default_sources(tmp_path: Path) -> None:
    db = StateDB(f"sqlite:///{tmp_path / 'state.db'}")
    db.init()
    p = Planner(db)
    req = BackfillRequest(
        start=datetime(2024, 6, 1, tzinfo=timezone.utc),
        end=datetime(2024, 6, 14, tzinfo=timezone.utc),
        max_tasks=5,
    )
    job_id = p.submit_backfill(req)
    status = p.status(job_id)
    assert status["job_id"] == job_id
    assert status["tasks_total"] >= 1
    assert status["status"] in (JobStatus.PENDING.value, JobStatus.RUNNING.value)


def test_planner_status_for_unknown_job_returns_error(tmp_path: Path) -> None:
    db = StateDB(f"sqlite:///{tmp_path / 'state.db'}")
    db.init()
    p = Planner(db)
    assert p.status("does-not-exist").get("error") == "unknown_job"


def test_adapter_registry_has_all_sources(tmp_path: Path) -> None:
    reg = get_adapter_registry()
    kinds = {a.source_type for a in reg.all()}
    # All declared kinds must be registered.
    expected = {
        SourceKind.COMMON_CRAWL_WET,
        SourceKind.COMMON_CRAWL_INDEX,
        SourceKind.COMMON_CRAWL_WARC,
        SourceKind.FINEWEB,
        SourceKind.RSS,
        SourceKind.TAIL_RECRAWL,
        SourceKind.GDELT,
    }
    assert expected.issubset(kinds)
