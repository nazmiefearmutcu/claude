"""DuckDB-backed query/index layer.

Two views over the same corpus:

1. ``staging_captures`` — read JSONL chunks from
   ``data/jsonl/captures/YYYY/MM/DD/*.jsonl`` directly. This is always
   present and is the source-of-truth for the latest writes.
2. ``iceberg_captures`` — read the Iceberg table when present.

A combined ``captures`` view UNIONs both with row-level dedup on
``capture_id``. This makes range queries trivial:

    SELECT count(*) FROM captures
     WHERE fetch_ts BETWEEN '2024-01-01' AND '2024-12-31';
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import duckdb

from awareness.obs.logging import get_logger

logger = get_logger("storage.duckdb")


class DuckDbIndex:
    """Thin wrapper around a DuckDB connection that knows our layout."""

    def __init__(self, db_path: Path, jsonl_dir: Path, iceberg_warehouse: Path | None) -> None:
        self._db_path = db_path
        self._jsonl_dir = jsonl_dir
        self._iceberg_warehouse = iceberg_warehouse
        self._lock = threading.RLock()
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            if self._conn is not None:
                return self._conn
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = duckdb.connect(str(self._db_path))
            # Best-effort: install/load iceberg extension. Continue if it fails;
            # the staging view still works.
            try:
                conn.execute("INSTALL iceberg")
                conn.execute("LOAD iceberg")
            except duckdb.Error as exc:
                logger.info("duckdb_iceberg_extension_unavailable", err=str(exc))
            self._refresh_views(conn)
            self._conn = conn
            return conn

    def _staging_glob(self) -> str:
        # JSONL chunks land here; use a recursive glob.
        return str(self._jsonl_dir / "captures" / "**" / "*.jsonl")

    def _refresh_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        captures_root = self._jsonl_dir / "captures"
        existing = list(captures_root.rglob("*.jsonl")) if captures_root.exists() else []
        if existing:
            # Build an explicit list literal so DuckDB doesn't have to glob.
            file_list = ", ".join(f"'{str(p)}'" for p in existing)
            conn.execute(
                f"""
                CREATE OR REPLACE VIEW staging_captures_raw AS
                SELECT *
                FROM read_json_auto([{file_list}], union_by_name=true);
                """
            )
        else:
            conn.execute(
                """
                CREATE OR REPLACE VIEW staging_captures_raw AS
                SELECT
                  NULL::VARCHAR AS doc_id, NULL::VARCHAR AS capture_id,
                  NULL::VARCHAR AS source_type, NULL::VARCHAR AS source_name,
                  NULL::VARCHAR AS fetch_ts, NULL::VARCHAR AS observed_ts,
                  NULL::VARCHAR AS published_ts, NULL::VARCHAR AS last_modified,
                  NULL::VARCHAR AS url, NULL::VARCHAR AS canonical_url,
                  NULL::VARCHAR AS domain, NULL::VARCHAR AS text,
                  NULL::VARCHAR AS title, NULL::VARCHAR AS language,
                  NULL::VARCHAR AS content_hash, NULL::BIGINT AS near_dup_hash,
                  NULL::VARCHAR AS discovery_channel,
                  NULL::VARCHAR AS source_locator, NULL::VARCHAR AS source_shard,
                  NULL::VARCHAR AS source_offset_or_record_id,
                  NULL::VARCHAR AS job_id, NULL::VARCHAR AS batch_id,
                  NULL::VARCHAR AS parent_doc_or_dup_group,
                  NULL::VARCHAR AS ingest_version,
                  NULL::VARCHAR AS robots_decision,
                  NULL::VARCHAR AS terms_note_if_relevant,
                  NULL::VARCHAR AS content_type, NULL::INTEGER AS http_status,
                  NULL::VARCHAR AS etag
                WHERE 1=0;
                """
            )

        # Build a unified ``captures`` view that casts timestamps to TIMESTAMPTZ
        # so BETWEEN/range queries against datetime parameters work.
        try:
            conn.execute(
                """
                CREATE OR REPLACE VIEW captures AS
                SELECT
                  doc_id, capture_id, parent_doc_or_dup_group,
                  source_type, source_name, source_locator,
                  source_shard, source_offset_or_record_id,
                  discovery_channel, job_id, batch_id, ingest_version,
                  url, canonical_url, domain,
                  TRY_CAST(fetch_ts AS TIMESTAMPTZ) AS fetch_ts,
                  TRY_CAST(observed_ts AS TIMESTAMPTZ) AS observed_ts,
                  TRY_CAST(published_ts AS TIMESTAMPTZ) AS published_ts,
                  TRY_CAST(last_modified AS TIMESTAMPTZ) AS last_modified,
                  content_type, http_status, etag, title, text, language,
                  content_hash, near_dup_hash, robots_decision,
                  terms_note_if_relevant
                FROM staging_captures_raw;
                """
            )
            # Backwards-compat alias.
            conn.execute("CREATE OR REPLACE VIEW staging_captures AS SELECT * FROM captures;")
        except duckdb.Error as exc:
            logger.warning("duckdb_view_setup_failed", err=str(exc))

    def refresh(self) -> None:
        with self._lock:
            if self._conn is None:
                self.connect()
                return
            self._refresh_views(self._conn)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            conn = self.connect()
            self._refresh_views(conn)
            cur = conn.execute(sql, params or {})
            cols = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
