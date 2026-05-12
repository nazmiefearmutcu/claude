"""Per-domain async rate limiter and concurrency cap.

Async semaphores per (registered) domain, with a minimum inter-fetch delay
derived from either robots.txt crawl-delay or a global default.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class _DomainSlot:
    sem: asyncio.Semaphore
    last_release: float


class PerDomainLimiter:
    """Provide per-domain concurrency and inter-request spacing."""

    def __init__(self, concurrency: int = 2, min_delay_sec: float = 1.0) -> None:
        self._concurrency = max(1, concurrency)
        self._min_delay = max(0.0, min_delay_sec)
        self._slots: dict[str, _DomainSlot] = {}
        self._lock = asyncio.Lock()

    def _slot(self, domain: str) -> _DomainSlot:
        slot = self._slots.get(domain)
        if slot is None:
            slot = _DomainSlot(sem=asyncio.Semaphore(self._concurrency), last_release=0.0)
            self._slots[domain] = slot
        return slot

    async def acquire(self, domain: str, override_delay: float | None = None) -> None:
        async with self._lock:
            slot = self._slot(domain)
        await slot.sem.acquire()
        # Inter-fetch spacing: spin-sleep without holding lock.
        delay = self._min_delay if override_delay is None else max(0.0, override_delay)
        if delay > 0:
            now = time.time()
            wait = (slot.last_release + delay) - now
            if wait > 0:
                await asyncio.sleep(wait)

    def release(self, domain: str) -> None:
        slot = self._slots.get(domain)
        if not slot:
            return
        slot.last_release = time.time()
        slot.sem.release()

    class _DomainCtx:
        def __init__(self, parent: "PerDomainLimiter", domain: str, override_delay: float | None) -> None:
            self.parent = parent
            self.domain = domain
            self.override_delay = override_delay

        async def __aenter__(self) -> None:
            await self.parent.acquire(self.domain, self.override_delay)

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.parent.release(self.domain)

    def domain(self, domain: str, override_delay: float | None = None) -> "PerDomainLimiter._DomainCtx":
        """Async context manager for an acquire/release pair."""
        return PerDomainLimiter._DomainCtx(self, domain, override_delay)
