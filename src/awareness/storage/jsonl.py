"""Atomic JSONL staging writer.

Each batch writes to ``<dir>/captures/<yyyy>/<mm>/<dd>/<batch_id>.jsonl.tmp``
and is renamed to ``.jsonl`` on commit so partial files are never readable.

Files are gzip-optional (.jsonl or .jsonl.gz) and rotate by size or count.

Design:
- One writer instance per job/worker.
- ``write()`` is thread-safe (instance lock); the underlying chunk file is owned.
- ``flush()`` finalizes the current chunk (rename to .jsonl) and rolls a new one.
- Always written before Iceberg/compaction. Iceberg can fail and we still have data.
"""

from __future__ import annotations

import gzip
import json
import os
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, IO

from awareness.obs.logging import get_logger

logger = get_logger("storage.jsonl")


def _serialize_value(v: Any) -> Any:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    return v


def _row_to_jsonl(row: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            {k: _serialize_value(v) for k, v in row.items()},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


class JsonlStagingWriter:
    """Append-only, atomic, rotating JSONL writer."""

    def __init__(
        self,
        root: Path,
        max_records_per_file: int = 5_000,
        max_bytes_per_file: int = 64 * 1024 * 1024,
        compress: bool = False,
        flush_seconds: float = 10.0,
    ) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_records = max(1, max_records_per_file)
        self._max_bytes = max(64 * 1024, max_bytes_per_file)
        self._compress = compress
        self._flush_seconds = max(1.0, flush_seconds)

        self._lock = threading.RLock()
        self._fh: IO[bytes] | None = None
        self._current_path: Path | None = None
        self._current_records = 0
        self._current_bytes = 0
        self._opened_at = 0.0
        self._committed_files: list[Path] = []

    # ------------------------------------------------------------------
    def _open_new(self) -> None:
        now = datetime.now(timezone.utc)
        # Layout: <root>/captures/YYYY/MM/DD/*.jsonl
        day_dir = self._root / "captures" / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
        day_dir.mkdir(parents=True, exist_ok=True)
        suffix = ".jsonl.gz" if self._compress else ".jsonl"
        name = f"captures-{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:8]}{suffix}.tmp"
        path = day_dir / name
        if self._compress:
            self._fh = gzip.open(path, "ab")
        else:
            # Buffered binary write; we use os.fsync in commit().
            self._fh = open(path, "ab", buffering=1024 * 64)
        self._current_path = path
        self._current_records = 0
        self._current_bytes = 0
        self._opened_at = time.time()

    def _commit_current(self) -> Path | None:
        if self._fh is None or self._current_path is None:
            return None
        try:
            self._fh.flush()
            if not self._compress:
                # GZipFile has no fileno; only fsync the plain file.
                os.fsync(self._fh.fileno())
        except OSError:
            pass
        self._fh.close()
        self._fh = None

        # Rename .tmp → final
        finalized = self._current_path.with_suffix("") if str(self._current_path).endswith(".tmp") else self._current_path
        if str(self._current_path).endswith(".tmp"):
            finalized = Path(str(self._current_path)[:-4])
        try:
            self._current_path.rename(finalized)
        except OSError as exc:
            logger.warning("jsonl_rename_failed", src=str(self._current_path), err=str(exc))
            finalized = self._current_path

        self._committed_files.append(finalized)
        self._current_path = None
        logger.info(
            "jsonl_chunk_committed",
            path=str(finalized),
            records=self._current_records,
            bytes=self._current_bytes,
        )
        return finalized

    # ------------------------------------------------------------------
    def write(self, rows: list[dict[str, Any]]) -> int:
        """Append rows to the current chunk. Rotates when limits hit."""
        if not rows:
            return 0
        written = 0
        with self._lock:
            if self._fh is None:
                self._open_new()
            assert self._fh is not None
            for row in rows:
                payload = _row_to_jsonl(row)
                if self._should_rotate(payload):
                    self._commit_current()
                    self._open_new()
                    assert self._fh is not None
                self._fh.write(payload)
                self._current_records += 1
                self._current_bytes += len(payload)
                written += 1
            if self._should_rotate_time():
                self._commit_current()
        return written

    def _should_rotate(self, next_payload: bytes) -> bool:
        if self._current_records >= self._max_records:
            return True
        if self._current_bytes + len(next_payload) > self._max_bytes:
            return True
        return self._should_rotate_time()

    def _should_rotate_time(self) -> bool:
        if self._opened_at == 0.0:
            return False
        return (time.time() - self._opened_at) >= self._flush_seconds

    # ------------------------------------------------------------------
    def flush(self) -> Path | None:
        with self._lock:
            return self._commit_current()

    def close(self) -> Path | None:
        return self.flush()

    @property
    def committed_files(self) -> list[Path]:
        with self._lock:
            return list(self._committed_files)

    # ------------------------------------------------------------------
    # Context manager.
    def __enter__(self) -> "JsonlStagingWriter":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
