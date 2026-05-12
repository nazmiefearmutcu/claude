"""Plain-text normalization shared by all adapters.

All adapters end up calling ``normalize_text(...)`` so the canonical doc has
a single, consistent shape regardless of source.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from awareness.obs.logging import get_logger

logger = get_logger("normalize.text")


_CONTROL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")
_MULTISPACE = re.compile(r"[ \t]{2,}")
_MULTINEWLINE = re.compile(r"\n{3,}")
_TRAILING_WS = re.compile(r"[ \t]+\n")


@dataclass(slots=True)
class NormalizedText:
    text: str
    n_chars: int
    n_words: int
    n_lines: int
    discarded_reason: str | None = None  # set if the text fails minimum quality
    title: str | None = None


def _basic_clean(text: str) -> str:
    if not text:
        return ""
    s = unicodedata.normalize("NFKC", text)
    s = _CONTROL_RE.sub("", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _TRAILING_WS.sub("\n", s)
    s = _MULTISPACE.sub(" ", s)
    s = _MULTINEWLINE.sub("\n\n", s)
    return s.strip()


def safe_title(raw_title: str | None, fallback_text: str) -> str | None:
    """Title that is safe to persist; falls back to first text line if missing."""
    if raw_title and raw_title.strip():
        return _basic_clean(raw_title)[:512] or None
    head = fallback_text.lstrip()
    if not head:
        return None
    first_line = head.split("\n", 1)[0].strip()
    return first_line[:512] if first_line else None


def normalize_text(
    text: str,
    *,
    title: str | None = None,
    min_chars: int = 200,
    max_chars: int = 1_500_000,
) -> NormalizedText:
    """Return a quality-checked, cleaned text payload."""
    cleaned = _basic_clean(text or "")
    if not cleaned:
        return NormalizedText(text="", n_chars=0, n_words=0, n_lines=0, discarded_reason="empty")
    if len(cleaned) < min_chars:
        return NormalizedText(
            text=cleaned,
            n_chars=len(cleaned),
            n_words=len(cleaned.split()),
            n_lines=cleaned.count("\n") + 1,
            discarded_reason=f"too_short<{min_chars}",
            title=safe_title(title, cleaned),
        )
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars]
    return NormalizedText(
        text=cleaned,
        n_chars=len(cleaned),
        n_words=len(cleaned.split()),
        n_lines=cleaned.count("\n") + 1,
        title=safe_title(title, cleaned),
    )


def detect_language(text: str) -> str | None:
    """Best-effort language code (BCP-47 / ISO-639-1). Returns None on failure."""
    if not text or len(text) < 80:
        return None
    try:
        from langdetect import detect, DetectorFactory  # noqa: PLC0415

        DetectorFactory.seed = 0  # deterministic
        return detect(text[:5000])
    except (ImportError, Exception):  # langdetect raises LangDetectException
        return None
