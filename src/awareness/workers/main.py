"""Standalone worker entry point (for ``awareness-worker`` script).

Mostly useful when running the worker pool as a separate process. The CLI
``backfill run`` command already drives the worker engine in-process.
"""

from __future__ import annotations

import asyncio
import signal

import typer

from awareness.config import get_settings
from awareness.obs.logging import configure_logging, get_logger
from awareness.planner.planner import Planner
from awareness.storage.state import StateDB
from awareness.workers.engine import WorkerEngine

app = typer.Typer(no_args_is_help=True)
logger = get_logger("workers.main")


def run() -> None:
    """Run for the highest-priority pending job until idle."""
    settings = get_settings()
    configure_logging(level=settings.log_level, json=settings.log_json, log_dir=settings.log_dir)
    state = StateDB(settings.state_db_url or "sqlite:///awareness.sqlite")
    state.init()
    planner = Planner(state)
    engine = WorkerEngine(state, planner)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _stop(*_a) -> None:
        engine.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    async def _drive() -> None:
        # Drain any non-tail job that is RUNNING or PENDING.
        jobs = state.list_jobs()
        for j in jobs:
            if j.kind.value == "tail":
                continue
            if j.status.value in ("pending", "running", "paused"):
                logger.info("worker_running_job", job_id=j.job_id, status=j.status.value)
                await engine.run_job(j.job_id)
        await engine.aclose()

    try:
        loop.run_until_complete(_drive())
    finally:
        loop.close()


if __name__ == "__main__":
    run()
