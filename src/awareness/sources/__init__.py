"""Source adapters: one per data tier."""

from awareness.sources.base import (
    Adapter,
    AdapterContext,
    AdapterRegistry,
    PartitionSpec,
    get_adapter_registry,
)

__all__ = [
    "Adapter",
    "AdapterContext",
    "AdapterRegistry",
    "PartitionSpec",
    "get_adapter_registry",
]
