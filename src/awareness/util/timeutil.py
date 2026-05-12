"""Time utilities. All public timestamps are UTC tz-aware datetimes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dateutil import parser as _dateutil_parser


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_utc(dt: datetime | str | None) -> datetime | None:
    """Convert a value to a UTC tz-aware datetime."""
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = _dateutil_parser.parse(dt)
        except (ValueError, TypeError):
            return None
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_http_date(value: str | None) -> datetime | None:
    """Parse an RFC 7231 HTTP date header. Returns UTC dt or None."""
    if not value:
        return None
    try:
        return _dateutil_parser.parse(value).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def iso(dt: datetime | None) -> str | None:
    """ISO-8601 representation in UTC, ``None`` passthrough."""
    if dt is None:
        return None
    return to_utc(dt).isoformat() if dt else None  # type: ignore[union-attr]


def floor_to_day(dt: datetime) -> datetime:
    dt = to_utc(dt)  # type: ignore[assignment]
    assert dt is not None
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def coerce_relative_end(end: Any) -> datetime:
    """Allow ``now``/``today`` literal end markers in CLI/API payloads."""
    if isinstance(end, datetime):
        return to_utc(end)  # type: ignore[return-value]
    if isinstance(end, str):
        s = end.strip().lower()
        if s in ("now", "today"):
            return utcnow()
        parsed = _dateutil_parser.parse(end)
        return to_utc(parsed)  # type: ignore[return-value]
    raise ValueError(f"Cannot coerce {end!r} to a UTC datetime")
