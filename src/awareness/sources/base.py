"""Adapter base interface and registry.

An ``Adapter`` knows two things:
1. How to ``plan`` a date range / request into one or more partitions (tasks).
2. How to ``run_partition`` a single partition and yield ``DocCapture`` objects.

The planner calls ``plan()`` (sync) to produce tasks; the workers call
``run_partition()`` (async) for each task. Adapters MUST be idempotent and
support resume via the ``checkpoint`` argument.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable

from awareness.obs.logging import get_logger
from awareness.schemas.doc import DocCapture, SourceKind
from awareness.schemas.jobs import BackfillRequest


logger = get_logger("sources.base")


@dataclass(slots=True)
class PartitionSpec:
    """One unit of work the planner emits for an adapter."""

    source_type: SourceKind
    partition_key: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AdapterContext:
    """Per-run context passed by the worker to the adapter.

    The worker injects services the adapter needs: a checkpoint reader, a
    stop signal for graceful shutdown, the http client, the robots cache,
    and a callback to emit captures.
    """

    user_agent: str
    job_id: str
    task_id: str
    batch_id: str
    ingest_version: str
    checkpoint: dict[str, Any]
    is_stopping: Callable[[], bool]
    # Anything else adapter-specific:
    extras: dict[str, Any] = field(default_factory=dict)


class Adapter(ABC):
    """Abstract base for all source adapters."""

    source_type: SourceKind

    def __init__(self) -> None:
        if not hasattr(self, "source_type"):
            raise TypeError(f"{type(self).__name__} must declare source_type")

    @abstractmethod
    def plan(self, request: BackfillRequest) -> list[PartitionSpec]:
        """Convert a backfill request into source-native partitions."""

    @abstractmethod
    async def run_partition(
        self,
        partition: PartitionSpec,
        context: AdapterContext,
    ) -> AsyncIterator[DocCapture]:
        """Yield captures for a partition. Must honor ``context.is_stopping``."""
        # The signature is async iterator. Subclasses use `async def` + `yield`.
        raise NotImplementedError
        if False:  # pragma: no cover -- ensure async generator typing
            yield  # type: ignore[unreachable]


class AdapterRegistry:
    def __init__(self) -> None:
        self._by_kind: dict[SourceKind, Adapter] = {}

    def register(self, adapter: Adapter) -> None:
        self._by_kind[adapter.source_type] = adapter
        logger.info("adapter_registered", source_type=adapter.source_type.value)

    def get(self, kind: SourceKind) -> Adapter | None:
        return self._by_kind.get(kind)

    def all(self) -> list[Adapter]:
        return list(self._by_kind.values())


_REGISTRY: AdapterRegistry | None = None


def get_adapter_registry() -> AdapterRegistry:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = AdapterRegistry()
        _register_defaults(_REGISTRY)
    return _REGISTRY


def _register_defaults(reg: AdapterRegistry) -> None:
    # Imported here to avoid circular imports at module load.
    from awareness.sources.commoncrawl_wet import CommonCrawlWetAdapter
    from awareness.sources.fineweb import FineWebAdapter
    from awareness.sources.feeds import FeedsAdapter
    from awareness.sources.tail_recrawl import TailRecrawlAdapter
    from awareness.sources.gdelt import GdeltAdapter
    from awareness.sources.warc_repair import WarcRepairAdapter
    from awareness.sources.cc_index import CommonCrawlIndexAdapter

    for cls in (
        CommonCrawlWetAdapter,
        CommonCrawlIndexAdapter,
        FineWebAdapter,
        FeedsAdapter,
        TailRecrawlAdapter,
        GdeltAdapter,
        WarcRepairAdapter,
    ):
        try:
            reg.register(cls())
        except Exception as exc:
            logger.warning("adapter_register_failed", cls=cls.__name__, err=str(exc))
