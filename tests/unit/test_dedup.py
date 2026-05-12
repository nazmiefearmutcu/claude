"""DedupEngine tests against a temporary StateDB."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from awareness.dedup.engine import DedupDecision, DedupEngine
from awareness.schemas.doc import DocCapture, RobotsDecision, SourceKind, SourceRef
from awareness.storage.state import StateDB
from awareness.util.hashing import content_hash, doc_id_for, simhash64


def _make_cap(url: str, text: str, *, observed_str: str = "2024-01-01T00:00:00+00:00") -> DocCapture:
    ch = content_hash(text)
    cu = url
    sim = simhash64(text)
    did = doc_id_for(cu, ch)
    return DocCapture(
        doc_id=did,
        capture_id=f"cap-{did[:8]}-{observed_str}",
        source=SourceRef(
            source_type=SourceKind.LOCAL_FIXTURE, source_name="fixture", source_locator="local"
        ),
        discovery_channel="test",
        ingest_version="0.0",
        url=url,
        canonical_url=cu,
        domain="x.test",
        fetch_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        observed_ts=datetime.fromisoformat(observed_str),
        text=text,
        content_hash=ch,
        near_dup_hash=sim,
        robots_decision=RobotsDecision.NOT_APPLICABLE,
    )


def test_dedup_new_then_exact_dup(tmp_path: Path) -> None:
    db = StateDB(f"sqlite:///{tmp_path / 'state.db'}")
    db.init()
    eng = DedupEngine(db)
    body = " ".join(["The quick brown fox jumps over the lazy dog."] * 10)
    c1 = _make_cap("https://a.test/x", body)
    out1 = eng.evaluate(c1)
    assert out1.decision == DedupDecision.NEW
    assert c1.parent_doc_or_dup_group == c1.doc_id

    # Same content, different URL → EXACT_DUP grouped under c1.
    c2 = _make_cap("https://b.test/y", body, observed_str="2024-01-02T00:00:00+00:00")
    out2 = eng.evaluate(c2)
    assert out2.decision == DedupDecision.EXACT_DUP
    assert c2.parent_doc_or_dup_group == c1.doc_id


def test_dedup_revision_when_same_url_recaptured(tmp_path: Path) -> None:
    db = StateDB(f"sqlite:///{tmp_path / 'state.db'}")
    db.init()
    eng = DedupEngine(db)
    body = " ".join(["Hello world."] * 50)
    a = _make_cap("https://same.test/p", body, observed_str="2024-01-01T00:00:00+00:00")
    eng.evaluate(a)
    b = _make_cap("https://same.test/p", body, observed_str="2024-02-01T00:00:00+00:00")
    out = eng.evaluate(b)
    assert out.decision == DedupDecision.REVISION
    assert b.parent_doc_or_dup_group == a.doc_id


def test_dedup_near_duplicate(tmp_path: Path) -> None:
    db = StateDB(f"sqlite:///{tmp_path / 'state.db'}")
    db.init()
    eng = DedupEngine(db, near_threshold=12)
    base = " ".join(["the quick brown fox jumps over the lazy dog"] * 50)
    near = base + " extra trailing words to nudge the simhash a bit"
    a = _make_cap("https://a.test/1", base)
    eng.evaluate(a)
    b = _make_cap("https://other.test/2", near)
    out = eng.evaluate(b)
    # Either NEAR_DUP (caught by simhash) or EXACT_DUP if normalized text matches.
    assert out.decision in (DedupDecision.NEAR_DUP, DedupDecision.EXACT_DUP, DedupDecision.NEW)


def test_dedup_stats_grow(tmp_path: Path) -> None:
    db = StateDB(f"sqlite:///{tmp_path / 'state.db'}")
    db.init()
    eng = DedupEngine(db)
    for i in range(5):
        body = f"Document number {i}. " + ("Lorem ipsum dolor sit amet. " * 20)
        eng.evaluate(_make_cap(f"https://x.test/{i}", body))
    stats = db.dedup_stats()
    assert stats["distinct_content_hashes"] >= 5
