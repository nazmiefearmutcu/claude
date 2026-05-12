"""Deduplication engine — exact + canonical-URL + simhash near-duplicate.

Design principles (per spec):
- We always persist captures for provenance. Dedup never drops a row from the
  durable corpus by itself.
- ``parent_doc_or_dup_group`` is set so downstream queries can fold captures
  into canonical docs (``WHERE doc_id = parent_doc_or_dup_group``).
- Decision space:
    * NEW            — first time we see this content_hash
    * REVISION       — same canonical URL re-fetched, same content
    * EXACT_DUP      — same content seen from a different canonical URL
    * NEAR_DUP       — near-duplicate of an existing doc by simhash threshold

The engine writes dedup index rows as a side effect and mutates
``cap.parent_doc_or_dup_group`` in place.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from awareness.obs.logging import get_logger
from awareness.schemas.doc import DocCapture
from awareness.storage.state import StateDB
from awareness.util.hashing import hamming64

logger = get_logger("dedup")


class DedupDecision(str, Enum):
    NEW = "new"
    REVISION = "revision"
    EXACT_DUP = "exact_dup"
    NEAR_DUP = "near_dup"


@dataclass(slots=True)
class DedupOutcome:
    decision: DedupDecision
    dup_group: str
    reason: str

    @property
    def is_unique(self) -> bool:
        return self.decision == DedupDecision.NEW


class DedupEngine:
    def __init__(self, state: StateDB, near_threshold: int = 3) -> None:
        self._state = state
        self._near_threshold = max(0, near_threshold)

    def evaluate(self, cap: DocCapture) -> DedupOutcome:
        """Decide dedup state for ``cap`` and update its ``parent_doc_or_dup_group``."""
        # Step 1: register/observe the content_hash.
        canonical_doc_id, was_new = self._state.upsert_dedup(cap.content_hash, cap.doc_id)

        if not was_new:
            cap.parent_doc_or_dup_group = canonical_doc_id
            if canonical_doc_id == cap.doc_id:
                # Same URL+content already seen; this is a fresh capture (different fetch_ts).
                return DedupOutcome(
                    decision=DedupDecision.REVISION,
                    dup_group=canonical_doc_id,
                    reason="same_url_content_recaptured",
                )
            return DedupOutcome(
                decision=DedupDecision.EXACT_DUP,
                dup_group=canonical_doc_id,
                reason="content_hash_match",
            )

        # Step 2: near-duplicate scan via simhash segment buckets.
        if cap.near_dup_hash is not None and cap.near_dup_hash > 0:
            best_doc: str | None = None
            best_dist: int = 65
            for other_doc_id, other_signed in self._state.find_near_dup_candidates(cap.near_dup_hash):
                if other_doc_id == cap.doc_id:
                    continue
                other_unsigned = other_signed if other_signed >= 0 else (other_signed + (1 << 64))
                dist = hamming64(cap.near_dup_hash, other_unsigned)
                if dist < best_dist:
                    best_dist = dist
                    best_doc = other_doc_id
            if best_doc is not None and best_dist <= self._near_threshold:
                cap.parent_doc_or_dup_group = best_doc
                self._state.add_near_dup_index(cap.doc_id, cap.near_dup_hash)
                return DedupOutcome(
                    decision=DedupDecision.NEAR_DUP,
                    dup_group=best_doc,
                    reason=f"simhash_hamming={best_dist}",
                )

        # Step 3: brand new canonical doc.
        if cap.near_dup_hash is not None and cap.near_dup_hash > 0:
            self._state.add_near_dup_index(cap.doc_id, cap.near_dup_hash)
        cap.parent_doc_or_dup_group = cap.doc_id
        return DedupOutcome(decision=DedupDecision.NEW, dup_group=cap.doc_id, reason="new_content")
