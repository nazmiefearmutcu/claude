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
        self._fts_available: bool | None = None
        self._fts_built_for_count: int = -1

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
            # FTS extension for ranked full-text search. Optional.
            try:
                conn.execute("INSTALL fts")
                conn.execute("LOAD fts")
                self._fts_available = True
            except duckdb.Error as exc:
                logger.info("duckdb_fts_extension_unavailable", err=str(exc))
                self._fts_available = False
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

    # ── full-text search ────────────────────────────────────────────────
    def _ensure_fts(self, conn: duckdb.DuckDBPyConnection) -> bool:
        """Build/refresh the FTS index on a materialized captures table.

        DuckDB's FTS extension requires a real table. We materialize the
        captures view into ``captures_idx`` and rebuild the index whenever
        the row count changes. Returns True if FTS is ready to use.
        """
        if not self._fts_available:
            return False
        # Current corpus size.
        try:
            count = int(conn.execute("SELECT COUNT(*) FROM captures").fetchone()[0])
        except duckdb.Error:
            return False
        if count == 0:
            return False
        if count == self._fts_built_for_count:
            return True
        # Rebuild materialized table + FTS index.
        try:
            conn.execute(
                """
                CREATE OR REPLACE TABLE captures_idx AS
                SELECT
                  capture_id, doc_id, parent_doc_or_dup_group,
                  source_type, source_name, discovery_channel,
                  url, canonical_url, domain,
                  fetch_ts, observed_ts, published_ts,
                  title, text, language, content_hash, near_dup_hash,
                  robots_decision
                FROM captures
                """
            )
            conn.execute(
                "PRAGMA create_fts_index('captures_idx', 'capture_id', 'title', 'text', overwrite=1, stemmer='english', stopwords='english')"
            )
            self._fts_built_for_count = count
            logger.info("duckdb_fts_index_built", rows=count)
            return True
        except duckdb.Error as exc:
            logger.warning("duckdb_fts_build_failed", err=str(exc))
            self._fts_available = False
            return False

    def search(
        self,
        query: str,
        *,
        limit: int = 30,
        offset: int = 0,
        source: str | None = None,
        domain: str | None = None,
        start: Any = None,
        end: Any = None,
    ) -> dict[str, Any]:
        """BM25-ranked search. Falls back to substring ILIKE if FTS unavailable."""
        query = (query or "").strip()
        if not query:
            return {"total": 0, "limit": limit, "offset": offset, "rows": [], "ranked": False}

        with self._lock:
            conn = self.connect()
            self._refresh_views(conn)
            fts = self._ensure_fts(conn)

            where: list[str] = []
            params: dict[str, Any] = {"q": query}
            if source:
                where.append("source_type = $src")
                params["src"] = source
            if domain:
                where.append("domain = $dom")
                params["dom"] = domain
            if start is not None:
                where.append("fetch_ts >= $start")
                params["start"] = start
            if end is not None:
                where.append("fetch_ts <= $end")
                params["end"] = end

            if fts:
                where_sql = (" AND " + " AND ".join(where)) if where else ""
                # BM25 score; null filter excludes non-matches.
                base_sql = f"""
                    FROM captures_idx
                    WHERE fts_main_captures_idx.match_bm25(capture_id, $q) IS NOT NULL
                    {where_sql}
                """
                total_row = conn.execute(f"SELECT COUNT(*) {base_sql}", params).fetchone()
                total = int(total_row[0]) if total_row else 0
                sql = f"""
                    SELECT
                      capture_id, doc_id, parent_doc_or_dup_group,
                      source_type, source_name, discovery_channel,
                      url, canonical_url, domain,
                      fetch_ts, observed_ts, published_ts,
                      title, text, language, content_hash,
                      fts_main_captures_idx.match_bm25(capture_id, $q) AS score
                    {base_sql}
                    ORDER BY score DESC
                    LIMIT {int(limit)} OFFSET {int(offset)}
                """
                rows = self._rows(conn, sql, params)
                ranked = True
            else:
                # Fallback: ILIKE on title/text, no relevance ranking.
                where.insert(0, "(title ILIKE $like OR text ILIKE $like)")
                params["like"] = f"%{query}%"
                where_sql = " AND ".join(where)
                total = int(
                    conn.execute(f"SELECT COUNT(*) FROM captures WHERE {where_sql}", params).fetchone()[0]
                )
                sql = f"""
                    SELECT
                      capture_id, doc_id, parent_doc_or_dup_group,
                      source_type, source_name, discovery_channel,
                      url, canonical_url, domain,
                      fetch_ts, observed_ts, published_ts,
                      title, text, language, content_hash,
                      NULL::DOUBLE AS score
                    FROM captures
                    WHERE {where_sql}
                    ORDER BY fetch_ts DESC
                    LIMIT {int(limit)} OFFSET {int(offset)}
                """
                rows = self._rows(conn, sql, params)
                ranked = False

            # Augment each row with a snippet + matched terms; strip the heavy
            # full text from the response payload.
            terms = _tokenize_query(query)
            results = []
            for r in rows:
                text = r.pop("text", None) or ""
                title = r.get("title") or ""
                snippet, hits = _snippet_for(text, title, terms)
                r["snippet"] = snippet
                r["snippet_hits"] = hits
                r["text_len"] = len(text)
                r["terms"] = terms
                results.append(r)
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "rows": results,
                "ranked": ranked,
                "query": query,
            }

    @staticmethod
    def _rows(conn: duckdb.DuckDBPyConnection, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        cur = conn.execute(sql, params)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None


# ── snippet helpers ────────────────────────────────────────────────────
def _tokenize_query(q: str) -> list[str]:
    import re

    return [t for t in re.findall(r"[A-Za-z0-9']+", q.lower()) if len(t) >= 2]


def _snippet_for(text: str, title: str, terms: list[str]) -> tuple[str, list[tuple[int, int]]]:
    """Find a snippet of ~200 chars centered on the first match; return
    (snippet, hits) where hits is a list of (start, end) inside the snippet
    for each matched query token, lowercased-case-insensitive.
    """
    import re

    if not text:
        text = title or ""
    if not text:
        return "", []
    if not terms:
        return text[:200].strip(), []

    pattern = re.compile(r"\b(" + "|".join(re.escape(t) for t in terms) + r")\b", re.IGNORECASE)
    m = pattern.search(text)
    if not m:
        # No exact word boundary match — use substring fallback.
        lower = text.lower()
        first_pos = min((p for p in (lower.find(t) for t in terms) if p >= 0), default=-1)
        if first_pos < 0:
            return text[:200].strip(), []
        start = max(0, first_pos - 80)
        end = min(len(text), first_pos + 140)
    else:
        start = max(0, m.start() - 80)
        end = min(len(text), m.end() + 140)

    # Expand to word boundaries for cleaner edges.
    while start > 0 and text[start - 1].isalnum():
        start -= 1
    while end < len(text) and text[end].isalnum():
        end += 1
    snippet = text[start:end].strip()
    if start > 0:
        snippet = "… " + snippet
    if end < len(text):
        snippet = snippet + " …"

    # Compute hit positions inside snippet.
    hits: list[tuple[int, int]] = []
    for mm in pattern.finditer(snippet):
        hits.append((mm.start(), mm.end()))
    return snippet, hits


def find_related_captures(
    conn: duckdb.DuckDBPyConnection, capture_id: str, *, limit: int = 12
) -> list[dict[str, Any]]:
    """Return sibling captures in the same dup_group as ``capture_id``.

    Order: most recent first; the given capture is excluded.
    """
    row = conn.execute(
        "SELECT doc_id, parent_doc_or_dup_group FROM captures WHERE capture_id = ? LIMIT 1",
        [capture_id],
    ).fetchone()
    if not row:
        return []
    doc_id, dup_group = row
    group = dup_group or doc_id
    cur = conn.execute(
        """
        SELECT
          capture_id, doc_id, source_type, source_name, domain, url,
          fetch_ts, title, length(text) AS text_len
        FROM captures
        WHERE (parent_doc_or_dup_group = ? OR doc_id = ?)
          AND capture_id <> ?
        ORDER BY fetch_ts DESC
        LIMIT ?
        """,
        [group, group, capture_id, limit],
    )
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
