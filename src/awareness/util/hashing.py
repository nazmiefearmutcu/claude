"""Content hashing + simhash for near-duplicate detection.

We use:
- xxhash64 for fast exact-content hash (hex digest, 16 chars).
- A token-level simhash with mmh3 128-bit -> folded to 64-bit. Tokens are
  3-shingles over normalized lowercase text. This is robust enough for the
  early dedup needs without minhash machinery.
"""

from __future__ import annotations

import re
import unicodedata

import mmh3
import xxhash

_WS_RE = re.compile(r"\s+", re.UNICODE)
_NON_ALNUM = re.compile(r"[^0-9a-z\s]+", re.UNICODE)


def normalize_for_hash(text: str) -> str:
    """Lowercase + strip punctuation + collapse whitespace + NFKC."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKC", text).lower()
    s = _NON_ALNUM.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def content_hash(text: str) -> str:
    """Stable 64-bit xxhash of the normalized text, hex-encoded."""
    return xxhash.xxh3_64_hexdigest(normalize_for_hash(text))


def _shingles(tokens: list[str], k: int = 3) -> list[str]:
    if len(tokens) < k:
        return [" ".join(tokens)] if tokens else []
    return [" ".join(tokens[i : i + k]) for i in range(len(tokens) - k + 1)]


def simhash64(text: str, k: int = 3) -> int:
    """Compute a 64-bit simhash. Returns an unsigned int."""
    normalized = normalize_for_hash(text)
    if not normalized:
        return 0
    tokens = normalized.split(" ")
    grams = _shingles(tokens, k=k)
    if not grams:
        return 0

    bit_sums = [0] * 64
    for g in grams:
        h64 = mmh3.hash64(g.encode("utf-8"), signed=False)[0]
        for bit in range(64):
            if h64 & (1 << bit):
                bit_sums[bit] += 1
            else:
                bit_sums[bit] -= 1
    out = 0
    for bit in range(64):
        if bit_sums[bit] >= 0:
            out |= 1 << bit
    return out & 0xFFFFFFFFFFFFFFFF


def hamming64(a: int, b: int) -> int:
    """Hamming distance between two 64-bit ints."""
    return ((a ^ b) & 0xFFFFFFFFFFFFFFFF).bit_count()


def near_duplicate(a: int, b: int, threshold: int = 3) -> bool:
    """True if simhash Hamming distance is at most ``threshold`` bits."""
    return hamming64(a, b) <= threshold


# A stable doc_id derived from (canonical_url || content_hash).
def doc_id_for(canonical_url: str | None, content_hash_hex: str) -> str:
    """Deterministic doc_id. xxhash3_128 of url+content for stable identity."""
    key = (canonical_url or "") + "::" + content_hash_hex
    return xxhash.xxh3_128_hexdigest(key)


def capture_id_for(doc_id: str, observed_ts_iso: str, source_locator: str | None) -> str:
    """Per-capture unique id."""
    key = f"{doc_id}|{observed_ts_iso}|{source_locator or ''}"
    return xxhash.xxh3_128_hexdigest(key)
