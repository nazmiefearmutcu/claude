"""Unit tests for hashing + simhash + doc identity."""

from __future__ import annotations

from awareness.util.hashing import (
    capture_id_for,
    content_hash,
    doc_id_for,
    hamming64,
    near_duplicate,
    normalize_for_hash,
    simhash64,
)


def test_normalize_for_hash_is_stable() -> None:
    a = "Hello,   World!\n  This\tis a TEST."
    b = "hello world! this is a test"
    assert normalize_for_hash(a) == normalize_for_hash(b)


def test_content_hash_invariants() -> None:
    same1 = content_hash("Hello World")
    same2 = content_hash("hello  world")
    assert same1 == same2
    diff = content_hash("Goodbye World")
    assert diff != same1


def test_doc_id_combines_url_and_content() -> None:
    h = content_hash("the quick brown fox")
    a = doc_id_for("https://a.test/x", h)
    b = doc_id_for("https://b.test/x", h)  # different url, same content
    c = doc_id_for("https://a.test/x", h)
    assert a == c
    assert a != b


def test_capture_id_changes_with_observed() -> None:
    did = "deadbeef"
    a = capture_id_for(did, "2024-01-01T00:00:00+00:00", "shard1")
    b = capture_id_for(did, "2024-01-01T00:00:01+00:00", "shard1")
    assert a != b


def test_simhash_near_duplicate() -> None:
    base = " ".join(["lorem ipsum dolor sit amet consectetur adipiscing elit"] * 30)
    h1 = simhash64(base)
    near = base + " plus one extra sentence at the end"  # near-identical
    h2 = simhash64(near)
    different = "the rain in spain falls mainly on the plain " * 20
    h3 = simhash64(different)
    assert h1 != 0
    # Near-duplicate distance should be small.
    assert near_duplicate(h1, h2, threshold=12)
    # Different text should be far.
    assert not near_duplicate(h1, h3, threshold=10)


def test_hamming_basic() -> None:
    assert hamming64(0, 0) == 0
    assert hamming64(0xFF, 0x00) == 8
    assert hamming64(0xFFFFFFFFFFFFFFFF, 0) == 64
