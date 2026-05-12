"""Deduplication engine: exact, canonical-URL, near-duplicate."""

from awareness.dedup.engine import DedupDecision, DedupEngine, DedupOutcome

__all__ = ["DedupDecision", "DedupEngine", "DedupOutcome"]
