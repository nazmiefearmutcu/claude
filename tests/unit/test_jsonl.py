"""JSONL staging writer tests — atomicity, rotation, fsync."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from awareness.storage.jsonl import JsonlStagingWriter


def _row(i: int) -> dict:
    return {
        "doc_id": f"d{i:04d}",
        "fetch_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "text": "hello world",
    }


def test_jsonl_writes_and_commits(tmp_path: Path) -> None:
    w = JsonlStagingWriter(root=tmp_path, max_records_per_file=10, flush_seconds=60.0)
    w.write([_row(i) for i in range(5)])
    chunk = w.flush()
    assert chunk is not None and chunk.exists()
    assert chunk.suffix == ".jsonl"
    lines = chunk.read_text().splitlines()
    assert len(lines) == 5
    payload = json.loads(lines[0])
    assert payload["doc_id"] == "d0000"
    # No .tmp left behind.
    leftovers = list(tmp_path.rglob("*.tmp"))
    assert leftovers == []


def test_jsonl_rotates_on_record_limit(tmp_path: Path) -> None:
    w = JsonlStagingWriter(root=tmp_path, max_records_per_file=3, flush_seconds=60.0)
    w.write([_row(i) for i in range(7)])
    w.flush()
    files = sorted(tmp_path.rglob("*.jsonl"))
    # 7 records / 3 per file = at least 2 rotations.
    assert len(files) >= 2
    total = sum(1 for f in files for _ in f.read_text().splitlines())
    assert total == 7


def test_jsonl_context_manager_commits(tmp_path: Path) -> None:
    with JsonlStagingWriter(root=tmp_path) as w:
        w.write([_row(0)])
    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1
