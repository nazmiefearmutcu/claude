"""URL canonicalization and identity helpers."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import tldextract


# Tracking params that should be stripped during canonicalization.
_TRACKING_PARAMS: frozenset[str] = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "utm_id",
        "utm_name",
        "gclid",
        "gclsrc",
        "fbclid",
        "mc_cid",
        "mc_eid",
        "msclkid",
        "yclid",
        "_hsenc",
        "_hsmi",
        "ref",
        "ref_src",
        "ref_url",
        "referrer",
        "share",
        "trk",
        "spm",
    }
)


_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


def canonical_url(url: str | None) -> str | None:
    """Canonicalize a URL for dedup/identity purposes.

    Operations:
      - scheme/host lowercased
      - default ports dropped
      - trailing slash on path normalized only for paths == "/"
      - tracking query parameters stripped
      - remaining query keys sorted
      - fragment dropped
    """
    if not url:
        return None
    try:
        parts = urlsplit(url.strip())
    except (ValueError, AttributeError):
        return None
    if not parts.scheme or not parts.netloc:
        return None

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    # Drop default ports.
    if ":" in netloc:
        host, _, port = netloc.rpartition(":")
        if (scheme, port) in (("http", "80"), ("https", "443")):
            netloc = host

    path = parts.path or "/"
    if path == "":
        path = "/"

    # Filter and sort query params for stable identity.
    pairs = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() not in _TRACKING_PARAMS]
    pairs.sort()
    query = urlencode(pairs, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def domain_of(url: str | None) -> str | None:
    """Return the registered domain (eTLD+1) of a URL."""
    if not url:
        return None
    try:
        ext = _TLD_EXTRACT(url)
    except (ValueError, AttributeError):
        return None
    # ``top_domain_under_public_suffix`` is the modern alias for the deprecated
    # ``registered_domain`` attribute; fall back for older tldextract versions.
    primary = getattr(ext, "top_domain_under_public_suffix", None) or ext.registered_domain
    if primary:
        return primary.lower()
    if ext.domain:
        return ext.domain.lower()
    return None


def is_http_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        scheme = urlsplit(url).scheme.lower()
    except (ValueError, AttributeError):
        return False
    return scheme in ("http", "https")
