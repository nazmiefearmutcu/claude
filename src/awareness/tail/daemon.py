"""Stand-alone tail daemon (``awareness-tail`` script).

Runs the tail engine until interrupted. Useful for systemd/launchd setups.
The CLI ``tail start`` command is a convenience wrapper for the same flow.
"""

from __future__ import annotations

import asyncio
import signal

from awareness.config import get_settings
from awareness.obs.logging import configure_logging, get_logger
from awareness.planner.planner import Planner
from awareness.storage.state import StateDB
from awareness.tail.engine import TailEngine

logger = get_logger("tail.daemon")


def run() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json=settings.log_json, log_dir=settings.log_dir)
    state = StateDB(settings.state_db_url or "sqlite:///awareness.sqlite")
    state.init()
    planner = Planner(state)
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
        await tail.start()
        await shutdown.wait()
        await tail.stop()

    try:
        loop.run_until_complete(_drive())
    finally:
        loop.close()


if __name__ == "__main__":
    run()
