"""Structured logging setup using structlog over stdlib logging."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import structlog


_CONFIGURED = False


def configure_logging(
    level: str = "INFO",
    json: bool = True,
    log_dir: Path | None = None,
) -> None:
    """Configure structlog + stdlib logging. Idempotent."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_num = getattr(logging, level.upper(), logging.INFO)

    processors_pre: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    renderer = (
        structlog.processors.JSONRenderer()
        if json
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=processors_pre + [renderer],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(level_num),
        cache_logger_on_first_use=True,
    )

    root = logging.getLogger()
    root.setLevel(level_num)
    # Reset handlers so reconfiguration is clean for tests.
    for h in list(root.handlers):
        root.removeHandler(h)

    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(level_num)
    sh.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(sh)

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "awareness.log"
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level_num)
        fh.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(fh)

    # Quiet down noisy libs by default.
    for noisy in ("httpx", "httpcore", "urllib3", "trafilatura", "asyncio"):
        logging.getLogger(noisy).setLevel(max(level_num, logging.WARNING))

    _CONFIGURED = True


def get_logger(name: str | None = None) -> Any:
    """Return a structlog bound logger."""
    if not _CONFIGURED:
        configure_logging(
            level=os.environ.get("AW_LOG_LEVEL", "INFO"),
            json=os.environ.get("AW_LOG_JSON", "true").lower() == "true",
        )
    return structlog.get_logger(name) if name else structlog.get_logger()
