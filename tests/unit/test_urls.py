"""URL canonicalization tests."""

from awareness.util.urls import canonical_url, domain_of, is_http_url


def test_canonical_url_lowercases_scheme_host_and_strips_default_port() -> None:
    assert canonical_url("HTTPS://Example.COM:443/foo") == "https://example.com/foo"
    assert canonical_url("http://Example.COM:80/foo") == "http://example.com/foo"


def test_canonical_url_drops_tracking_params() -> None:
    raw = "https://news.example/article?id=42&utm_source=tw&utm_medium=organic&fbclid=xyz"
    out = canonical_url(raw)
    assert out is not None
    assert "utm_source" not in out
    assert "utm_medium" not in out
    assert "fbclid" not in out
    assert "id=42" in out


def test_canonical_url_drops_fragment_and_sorts_query() -> None:
    a = canonical_url("https://x.test/p?b=2&a=1#section")
    b = canonical_url("https://x.test/p?a=1&b=2")
    assert a == b


def test_canonical_url_handles_garbage() -> None:
    assert canonical_url("") is None
    assert canonical_url(None) is None
    assert canonical_url("not a url") is None


def test_domain_of_returns_etld_plus_one() -> None:
    assert domain_of("https://news.bbc.co.uk/x") == "bbc.co.uk"
    assert domain_of("https://example.com/y") == "example.com"
    assert domain_of("ftp://anything") is None or domain_of("ftp://anything") == "anything"


def test_is_http_url() -> None:
    assert is_http_url("https://x.test")
    assert is_http_url("http://x.test")
    assert not is_http_url("ftp://x.test")
    assert not is_http_url("")
