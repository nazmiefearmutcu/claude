"""``awareness`` CLI.

Subcommands:
    backfill submit     — submit a BODY job
    backfill run        — run pending tasks to completion (in-process)
    backfill status     — show job state
    tail start          — start TAIL daemon (foreground)
    tail stop           — stop the running TAIL
    tail status         — show tail state
    status              — overall system status
    health              — quick liveness check
    inspect             — query stored captures by date range
    dedup-stats         — dedup metrics
    metrics             — counters/histograms
    init                — initialize storage layout
"""

from __future__ import annotations

import asyncio
import json
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from awareness.config import get_settings
from awareness.obs.logging import configure_logging, get_logger
from awareness.obs.metrics import get_metrics
from awareness.planner.planner import Planner
from awareness.schemas.doc import SourceKind
from awareness.schemas.jobs import BackfillRequest
from awareness.storage.duckdb_index import DuckDbIndex
from awareness.storage.state import StateDB
from awareness.tail.engine import TailEngine
from awareness.util.timeutil import coerce_relative_end, to_utc
from awareness.workers.engine import WorkerEngine

app = typer.Typer(no_args_is_help=True, help="Awareness — public text internet awareness engine")
backfill_app = typer.Typer(no_args_is_help=True, help="BODY: historical backfill")
tail_app = typer.Typer(no_args_is_help=True, help="TAIL: live capture")
app.add_typer(backfill_app, name="backfill")
app.add_typer(tail_app, name="tail")

logger = get_logger("cli")
console = Console()


def _bootstrap() -> tuple[StateDB, Planner]:
    settings = get_settings()
    configure_logging(level=settings.log_level, json=settings.log_json, log_dir=settings.log_dir)
    state = StateDB(settings.state_db_url or "sqlite:///awareness.sqlite")
    state.init()
    return state, Planner(state)


@app.command()
def init() -> None:
    """Initialize storage paths, state DB, Iceberg catalog (idempotent)."""
    state, _ = _bootstrap()
    settings = get_settings()
    # Touch Iceberg if enabled.
    if settings.enable_iceberg:
        try:
            from awareness.storage.iceberg import IcebergWriter  # noqa: PLC0415

            w = IcebergWriter(catalog_db=settings.iceberg_catalog_db, warehouse=settings.iceberg_warehouse)
            w.ensure_table()
            rprint("[green]Iceberg table ready[/green]")
        except Exception as exc:
            rprint(f"[yellow]Iceberg init skipped:[/yellow] {exc}")
    rprint(f"[green]State DB:[/green] {state.url}")
    rprint(f"[green]Data dir:[/green] {settings.data_dir}")


@app.command()
def health() -> None:
    """Quick liveness check."""
    state, _ = _bootstrap()
    settings = get_settings()
    info = {
        "ok": True,
        "state_db": state.url,
        "data_dir": str(settings.data_dir),
        "iceberg_warehouse": str(settings.iceberg_warehouse),
        "tail": state.get_tail(),
    }
    print(json.dumps(info, indent=2))


@app.command()
def status() -> None:
    """Show overall system status: tail + recent jobs."""
    state, _ = _bootstrap()
    jobs = state.list_jobs(limit=10)
    table = Table("job_id", "kind", "status", "tasks", "docs", "dedup_dropped", "started")
    for j in jobs:
        table.add_row(
            j.job_id,
            j.kind.value,
            j.status.value,
            f"{j.tasks_completed}/{j.tasks_total}",
            str(j.docs_emitted),
            str(j.docs_dedup_dropped),
            j.started_at.isoformat() if j.started_at else "-",
        )
    console.print(table)
    rprint(f"[bold]Tail:[/bold] {state.get_tail()}")


@app.command(name="dedup-stats")
def dedup_stats() -> None:
    """Print dedup index statistics."""
    state, _ = _bootstrap()
    print(json.dumps(state.dedup_stats(), indent=2))


@app.command()
def metrics() -> None:
    """Dump in-process metrics snapshot."""
    print(json.dumps(get_metrics().snapshot(), indent=2))


# ── backfill ────────────────────────────────────────────────────────────
@backfill_app.command("submit")
def backfill_submit(
    start: str = typer.Option(..., "--start", help="Start date (ISO or yyyy-mm-dd)"),
    end: str = typer.Option("now", "--end", help="End date (ISO, yyyy-mm-dd, or 'now')"),
    sources: list[str] = typer.Option(  # noqa: B008
        [],
        "--source",
        "-s",
        help="Restrict to specific source kinds. Repeat. Default: CC-WET, FineWeb, GDELT.",
    ),
    domains: list[str] = typer.Option([], "--domain", help="Limit to these domains."),
    languages: list[str] = typer.Option([], "--lang", help="Limit to languages (BCP-47)."),
    max_tasks: int = typer.Option(0, "--max-tasks", help="Cap total tasks for smoke tests."),
    notes: str = typer.Option("", "--note", help="Free-form note."),
) -> None:
    state, planner = _bootstrap()
    src = [SourceKind(s) for s in sources] if sources else []
    req = BackfillRequest(
        start=to_utc(start),
        end=coerce_relative_end(end),
        sources=src,
        domains=domains or None,
        languages=languages or None,
        max_tasks=max_tasks or None,
        notes=notes or None,
    )
    job_id = planner.submit_backfill(req)
    rprint(f"[green]Submitted backfill[/green] job_id=[bold]{job_id}[/bold]")
    print(json.dumps(planner.status(job_id), indent=2, default=str))


@backfill_app.command("run")
def backfill_run(
    job_id: str = typer.Argument(..., help="Job id from `backfill submit`"),
    concurrency: int = typer.Option(0, "--concurrency", help="Override worker concurrency"),
) -> None:
    """Run pending tasks for ``job_id`` to completion (in-process)."""
    state, planner = _bootstrap()
    engine = WorkerEngine(state, planner, concurrency=concurrency or None)
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
        await engine.run_job(job_id)
        await engine.aclose()

    try:
        loop.run_until_complete(_drive())
    finally:
        loop.close()
    print(json.dumps(planner.status(job_id), indent=2, default=str))


@backfill_app.command("status")
def backfill_status(job_id: str = typer.Argument(...)) -> None:
    state, planner = _bootstrap()
    print(json.dumps(planner.status(job_id), indent=2, default=str))


# ── tail ─────────────────────────────────────────────────────────────────
@tail_app.command("start")
def tail_start(
    seeds: Optional[Path] = typer.Option(None, "--seeds", help="Path to tail_seeds.yaml"),
    duration: int = typer.Option(0, "--duration", help="Auto-stop after N seconds (0=run until SIGINT)"),
) -> None:
    """Start the tail engine in foreground. Ctrl-C stops it cleanly."""
    state, planner = _bootstrap()
    tail = TailEngine(state, planner)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shutdown = asyncio.Event()

    def _stop(*_a) -> None:
        loop.call_soon_threadsafe(shutdown.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    async def _drive() -> None:
        job_id = await tail.start(seeds_path=seeds)
        rprint(f"[green]Tail started[/green] job_id=[bold]{job_id}[/bold]")
        try:
            if duration > 0:
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=duration)
                except asyncio.TimeoutError:
                    pass
            else:
                await shutdown.wait()
        finally:
            await tail.stop()
            rprint("[yellow]Tail stopped[/yellow]")

    try:
        loop.run_until_complete(_drive())
    finally:
        loop.close()


@tail_app.command("stop")
def tail_stop() -> None:
    """Signal a running tail to stop via state DB (cooperative)."""
    state, planner = _bootstrap()
    tail = state.get_tail()
    if not tail.get("running"):
        rprint("[yellow]Tail is not running[/yellow]")
        return
    job_id = tail.get("job_id")
    if job_id:
        # We can't reach into the foreground process from here in the
        # zero-Docker setup; we mark the tail state as stopping so that the
        # next poll of the running process sees it and shuts down cleanly.
        planner.stop_tail(job_id, note="cli-requested-stop")
        rprint("[green]Tail stop requested[/green]")
    else:
        rprint("[yellow]No tail job id recorded[/yellow]")


@tail_app.command("status")
def tail_status() -> None:
    state, _ = _bootstrap()
    print(json.dumps(state.get_tail(), indent=2, default=str))


# ── inspect ──────────────────────────────────────────────────────────────
@app.command()
def inspect(
    start: str = typer.Option(..., "--start"),
    end: str = typer.Option("now", "--end"),
    limit: int = typer.Option(20, "--limit"),
    domain: str = typer.Option("", "--domain"),
    source: str = typer.Option("", "--source"),
) -> None:
    """Query stored captures by date/domain/source."""
    state, _ = _bootstrap()
    settings = get_settings()
    idx = DuckDbIndex(
        db_path=settings.duckdb_path(),
        jsonl_dir=settings.staging_jsonl_dir(),
        iceberg_warehouse=settings.iceberg_warehouse,
    )
    start_dt = to_utc(start)
    end_dt = coerce_relative_end(end)
    where = ["fetch_ts >= $start", "fetch_ts <= $end"]
    params: dict = {"start": start_dt, "end": end_dt}
    if domain:
        where.append("domain = $dom")
        params["dom"] = domain
    if source:
        where.append("source_type = $src")
        params["src"] = source
    where_sql = " AND ".join(where)
    sql = f"""
        SELECT
          doc_id, capture_id, source_type, source_name,
          fetch_ts, domain, title, length(text) AS text_len, language
        FROM captures
        WHERE {where_sql}
        ORDER BY fetch_ts DESC
        LIMIT {int(limit)}
    """
    try:
        rows = idx.execute(sql, params)
    except Exception as exc:
        rprint(f"[red]Query failed:[/red] {exc}")
        return
    if not rows:
        rprint("[yellow]No captures match.[/yellow]")
        return
    cols = list(rows[0].keys())
    table = Table(*cols)
    for r in rows:
        table.add_row(*(str(r[c]) for c in cols))
    console.print(table)


@app.command(name="counts")
def counts(
    start: str = typer.Option(..., "--start"),
    end: str = typer.Option("now", "--end"),
) -> None:
    """Aggregate counts by source and domain in [start, end]."""
    state, _ = _bootstrap()
    settings = get_settings()
    idx = DuckDbIndex(
        db_path=settings.duckdb_path(),
        jsonl_dir=settings.staging_jsonl_dir(),
        iceberg_warehouse=settings.iceberg_warehouse,
    )
    start_dt = to_utc(start)
    end_dt = coerce_relative_end(end)
    try:
        by_source = idx.execute(
            """
            SELECT source_type, COUNT(*) AS n
            FROM captures
            WHERE fetch_ts BETWEEN $start AND $end
            GROUP BY source_type
            ORDER BY n DESC
            """,
            {"start": start_dt, "end": end_dt},
        )
        by_domain = idx.execute(
            """
            SELECT domain, COUNT(*) AS n
            FROM captures
            WHERE fetch_ts BETWEEN $start AND $end AND domain IS NOT NULL
            GROUP BY domain
            ORDER BY n DESC LIMIT 25
            """,
            {"start": start_dt, "end": end_dt},
        )
        total = idx.execute(
            "SELECT COUNT(*) AS n FROM captures WHERE fetch_ts BETWEEN $start AND $end",
            {"start": start_dt, "end": end_dt},
        )
        print(json.dumps({"total": total, "by_source": by_source, "by_domain": by_domain}, indent=2, default=str))
    except Exception as exc:
        rprint(f"[red]Query failed:[/red] {exc}")


if __name__ == "__main__":
    app()
