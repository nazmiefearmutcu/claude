"""Pytest fixtures: temp project root + reset Settings singleton per test."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

import pytest

from awareness.config import reset_settings


@pytest.fixture()
def tmp_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Isolate every test in a fresh project root."""
    monkeypatch.setenv("AW_PROJECT_ROOT", str(tmp_path))
    # No on-disk YAML overrides during tests.
    monkeypatch.delenv("AW_CONFIG_FILE", raising=False)
    # Always JSON logging off during tests for readable failures.
    monkeypatch.setenv("AW_LOG_JSON", "false")
    monkeypatch.setenv("AW_LOG_LEVEL", "WARNING")
    reset_settings()
    yield tmp_path
    reset_settings()
