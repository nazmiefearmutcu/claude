"""HTML → text extraction wrapper around trafilatura.

We never persist HTML to durable storage. ``html_to_text()`` is the single
gateway used by the tail recrawl and the optional WARC-repair adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import trafilatura
from trafilatura.settings import use_config

from awareness.normalize.text import NormalizedText, normalize_text
from awareness.obs.logging import get_logger
from awareness.util.timeutil import to_utc

logger = get_logger("normalize.html")


_TRAFILATURA_CFG = use_config()
_TRAFILATURA_CFG.set("DEFAULT", "DOWNLOAD_TIMEOUT", "10")
_TRAFILATURA_CFG.set("DEFAULT", "EXTRACTION_TIMEOUT", "20")


@dataclass(slots=True)
class HtmlExtraction:
    text: NormalizedText
    title: str | None
    published_ts: datetime | None
    canonical_url_hint: str | None
    language_hint: str | None
    raw_metadata: dict[str, Any]


def _trafilatura_metadata(html: str, url: str | None) -> dict[str, Any]:
    try:
        meta = trafilatura.extract_metadata(html, default_url=url)
        if meta is None:
            return {}
        return meta.as_dict() if hasattr(meta, "as_dict") else dict(vars(meta))
    except Exception:  # extract_metadata is fragile; never let it kill the worker
        return {}


def html_to_text(
    html: str,
    *,
    url: str | None = None,
    min_chars: int = 200,
    max_chars: int = 1_500_000,
) -> HtmlExtraction | None:
    """Return an ``HtmlExtraction`` or ``None`` if the page yields no usable text."""
    if not html:
        return None
    try:
        text = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=True,
            include_formatting=False,
            favor_recall=True,
            output_format="txt",
            config=_TRAFILATURA_CFG,
        )
    except Exception as exc:  # never raise to caller
        logger.debug("trafilatura_extract_failed", err=str(exc), url=url)
        return None

    if not text:
        return None

    meta = _trafilatura_metadata(html, url)
    title = meta.get("title")
    published = to_utc(meta.get("date"))
    canonical_hint = meta.get("url")
    language_hint = meta.get("language")

    normalized = normalize_text(text, title=title, min_chars=min_chars, max_chars=max_chars)
    if normalized.discarded_reason:
        return None
    return HtmlExtraction(
        text=normalized,
        title=normalized.title,
        published_ts=published,
        canonical_url_hint=canonical_hint,
        language_hint=language_hint,
        raw_metadata=meta,
    )
