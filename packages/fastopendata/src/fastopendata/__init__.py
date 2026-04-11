"""FastOpenData — open data ingestion and processing for PyCypher.

This package provides utilities for loading, transforming, and ingesting
public open datasets into pycypher-compatible graph structures.

Subpackages
-----------
* :mod:`fastopendata.streaming` — real-time streaming engine with windowing,
  joins, and incremental views
* :mod:`fastopendata.schema_evolution` — versioned schema registry with
  compatibility checking and data lineage
* :mod:`fastopendata.analytics` — query performance metrics, trend analysis,
  and regression detection
* :mod:`fastopendata.multiverse` — speculative parallel execution with
  collapse strategies
* :mod:`fastopendata.swarm` — swarm intelligence optimizers (ant colony, bee
  colony, particle swarm)
* :mod:`fastopendata.loadbalancer` — adaptive load balancing and routing

Heavy submodules (streaming, schema_evolution, analytics, pipeline) are
loaded lazily on first access to reduce startup time for config-only usage.
"""

from __future__ import annotations

__version__ = "0.0.1"

# Config is lightweight — always load eagerly.
from fastopendata.config import Config, config

# Lazy-loaded symbols: mapped to (module_path, attribute_name).
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    # Pipeline
    "GraphPipeline": ("fastopendata.pipeline", "GraphPipeline"),
    "load_available_datasets": (
        "fastopendata.pipeline",
        "load_available_datasets",
    ),
    # Streaming
    "IncrementalView": ("fastopendata.streaming", "IncrementalView"),
    "StreamBuffer": ("fastopendata.streaming", "StreamBuffer"),
    "StreamEngine": ("fastopendata.streaming", "StreamEngine"),
    "StreamRecord": ("fastopendata.streaming", "StreamRecord"),
    "StreamTableJoin": ("fastopendata.streaming", "StreamTableJoin"),
    "TumblingWindow": ("fastopendata.streaming", "TumblingWindow"),
    "WatermarkTracker": ("fastopendata.streaming", "WatermarkTracker"),
    # Schema evolution
    "CompatibilityLevel": (
        "fastopendata.schema_evolution",
        "CompatibilityLevel",
    ),
    "FieldSchema": ("fastopendata.schema_evolution", "FieldSchema"),
    "FieldType": ("fastopendata.schema_evolution", "FieldType"),
    "LineageGraph": ("fastopendata.schema_evolution", "LineageGraph"),
    "SchemaRegistry": ("fastopendata.schema_evolution", "SchemaRegistry"),
    "TableSchema": ("fastopendata.schema_evolution", "TableSchema"),
    # Analytics
    "AnalyticsEngine": ("fastopendata.analytics", "AnalyticsEngine"),
    "MetricsCollector": ("fastopendata.analytics", "MetricsCollector"),
    "RegressionDetector": ("fastopendata.analytics", "RegressionDetector"),
}

__all__: list[str] = [
    # Config (eager)
    "Config",
    "config",
    # Pipeline (lazy)
    "GraphPipeline",
    "load_available_datasets",
    # Streaming (lazy)
    "IncrementalView",
    "StreamBuffer",
    "StreamEngine",
    "StreamRecord",
    "StreamTableJoin",
    "TumblingWindow",
    "WatermarkTracker",
    # Schema evolution (lazy)
    "CompatibilityLevel",
    "FieldSchema",
    "FieldType",
    "LineageGraph",
    "SchemaRegistry",
    "TableSchema",
    # Analytics (lazy)
    "AnalyticsEngine",
    "MetricsCollector",
    "RegressionDetector",
]


def __getattr__(name: str) -> object:
    """Lazily import heavy submodules on first attribute access."""
    if name in _LAZY_IMPORTS:
        module_path, attr_name = _LAZY_IMPORTS[name]
        import importlib

        module = importlib.import_module(module_path)
        value = getattr(module, attr_name)
        # Cache on the module to avoid repeated imports.
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
