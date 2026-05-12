"""Iceberg writer backed by PyIceberg's SQL catalog (SQLite).

Why this shape:
- Local-first: SqlCatalog over SQLite needs no daemon. Switching to Postgres
  is one URL change; switching to MinIO/S3 is one ``warehouse`` change.
- Each ``append()`` writes Parquet manifest+data files into the warehouse
  laid out under the table's identity. We append per-batch.
- We always write JSONL first; Iceberg is a downstream durable copy. So a
  PyIceberg blip never loses raw captures.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError, NoSuchTableError

from awareness.obs.logging import get_logger
from awareness.storage.iceberg_schema import (
    CAPTURES_TABLE_IDENTIFIER,
    ICEBERG_PARTITION_SPEC,
    ICEBERG_SCHEMA,
    pyarrow_schema,
)

logger = get_logger("storage.iceberg")


def _to_arrow(rows: Iterable[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    """Coerce row dicts to a PyArrow table matching ``schema``.

    Timestamps may arrive as ISO strings or datetimes; coerce to tz-aware UTC.
    """
    materialized = list(rows)
    if not materialized:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)

    # Build columns dict, coercing timestamps.
    cols: dict[str, list[Any]] = {f.name: [] for f in schema}
    ts_fields = {f.name for f in schema if pa.types.is_timestamp(f.type)}
    int32_fields = {f.name for f in schema if pa.types.is_int32(f.type)}
    int64_fields = {f.name for f in schema if pa.types.is_int64(f.type)}
    for row in materialized:
        for f in schema:
            v = row.get(f.name)
            if v is None:
                cols[f.name].append(None)
                continue
            if f.name in ts_fields:
                if isinstance(v, str):
                    # ISO format
                    try:
                        v = datetime.fromisoformat(v)
                    except ValueError:
                        try:
                            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
                        except ValueError:
                            v = None
                if isinstance(v, datetime):
                    if v.tzinfo is None:
                        v = v.replace(tzinfo=timezone.utc)
                    else:
                        v = v.astimezone(timezone.utc)
                cols[f.name].append(v)
                continue
            if f.name in int32_fields and isinstance(v, int):
                cols[f.name].append(int(v))
                continue
            if f.name in int64_fields and isinstance(v, int):
                # Fold unsigned 64-bit values (e.g. simhash) into signed int64
                # range expected by Arrow / Iceberg.
                if v >= (1 << 63):
                    v -= 1 << 64
                cols[f.name].append(int(v))
                continue
            cols[f.name].append(v)
    arrays = [pa.array(cols[f.name], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


class IcebergWriter:
    """Append rows to ``awareness.captures`` in an Iceberg SQL catalog."""

    def __init__(self, catalog_db: Path, warehouse: Path) -> None:
        self._catalog_db = catalog_db
        self._warehouse = warehouse
        self._lock = threading.RLock()
        self._catalog: Catalog | None = None
        self._table: Any | None = None
        self._arrow_schema = pyarrow_schema()

    def _ensure_catalog(self) -> Catalog:
        if self._catalog is None:
            self._catalog_db.parent.mkdir(parents=True, exist_ok=True)
            self._warehouse.mkdir(parents=True, exist_ok=True)
            self._catalog = SqlCatalog(
                "awareness",
                uri=f"sqlite:///{self._catalog_db}",
                warehouse=f"file://{self._warehouse}",
            )
        return self._catalog

    def ensure_table(self) -> None:
        catalog = self._ensure_catalog()
        ns, _ = CAPTURES_TABLE_IDENTIFIER
        try:
            catalog.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass

        try:
            self._table = catalog.load_table(CAPTURES_TABLE_IDENTIFIER)
        except NoSuchTableError:
            try:
                self._table = catalog.create_table(
                    identifier=CAPTURES_TABLE_IDENTIFIER,
                    schema=ICEBERG_SCHEMA,
                    partition_spec=ICEBERG_PARTITION_SPEC,
                )
            except TableAlreadyExistsError:
                self._table = catalog.load_table(CAPTURES_TABLE_IDENTIFIER)
        logger.info(
            "iceberg_table_ready",
            warehouse=str(self._warehouse),
            identifier=".".join(CAPTURES_TABLE_IDENTIFIER),
        )

    def append(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self._lock:
            if self._table is None:
                self.ensure_table()
            assert self._table is not None
            tbl = _to_arrow(rows, self._arrow_schema)
            self._table.append(tbl)
            logger.info("iceberg_appended", n=len(rows))
            return len(rows)

    def close(self) -> None:
        # PyIceberg manages its own connections; nothing to do explicitly.
        return
