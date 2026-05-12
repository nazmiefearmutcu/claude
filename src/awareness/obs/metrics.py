"""Light-weight, thread-safe in-process metrics registry.

A real deployment swaps this for Prometheus. The interface is the same:
``inc()``, ``add()``, ``observe()``, ``snapshot()``.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Histogram:
    count: int = 0
    sum: float = 0.0
    min: float = float("inf")
    max: float = 0.0
    samples: list[float] = field(default_factory=list)
    max_samples: int = 256

    def observe(self, v: float) -> None:
        self.count += 1
        self.sum += v
        self.min = min(self.min, v)
        self.max = max(self.max, v)
        if len(self.samples) < self.max_samples:
            self.samples.append(v)

    def as_dict(self) -> dict[str, Any]:
        avg = self.sum / self.count if self.count else 0.0
        return {
            "count": self.count,
            "sum": round(self.sum, 4),
            "min": round(self.min if self.count else 0.0, 4),
            "max": round(self.max, 4),
            "avg": round(avg, 4),
        }


class MetricsRegistry:
    """Thread-safe counters and histograms keyed by name and label tuple."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._hist: dict[tuple[str, tuple[tuple[str, str], ...]], _Histogram] = defaultdict(_Histogram)
        self._started_at = time.time()

    @staticmethod
    def _labels_key(labels: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
        if not labels:
            return ()
        return tuple(sorted(labels.items()))

    def inc(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
        with self._lock:
            self._counters[(name, self._labels_key(labels))] += value

    def add(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        self.inc(name, value, labels)

    def set(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        with self._lock:
            self._gauges[(name, self._labels_key(labels))] = value

    def observe(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        with self._lock:
            self._hist[(name, self._labels_key(labels))].observe(value)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = [
                {"name": n, "labels": dict(lbl), "value": round(v, 4)}
                for (n, lbl), v in sorted(self._counters.items())
            ]
            gauges = [
                {"name": n, "labels": dict(lbl), "value": v}
                for (n, lbl), v in sorted(self._gauges.items())
            ]
            histograms = [
                {"name": n, "labels": dict(lbl), **h.as_dict()}
                for (n, lbl), h in sorted(self._hist.items())
            ]
            return {
                "uptime_seconds": round(time.time() - self._started_at, 2),
                "counters": counters,
                "gauges": gauges,
                "histograms": histograms,
            }


_REGISTRY: MetricsRegistry | None = None
_REG_LOCK = threading.Lock()


def get_metrics() -> MetricsRegistry:
    global _REGISTRY
    if _REGISTRY is None:
        with _REG_LOCK:
            if _REGISTRY is None:
                _REGISTRY = MetricsRegistry()
    return _REGISTRY
