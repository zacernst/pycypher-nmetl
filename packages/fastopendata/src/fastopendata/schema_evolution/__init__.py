"""Schema evolution and data lineage for FastOpenData.

Provides automatic schema drift detection, compatibility analysis,
version management with rollback, and lightweight data lineage tracking
for the fastopendata processing pipeline.

Core components
---------------
* :class:`FieldSchema` — typed field descriptor with nullability/metadata
* :class:`TableSchema` — ordered collection of fields with version tracking
* :class:`SchemaRegistry` — versioned schema store with compatibility checks
* :class:`CompatibilityChecker` — backward/forward/full compatibility analysis
* :class:`SchemaMerger` — intelligent schema merging with conflict resolution
* :class:`LineageGraph` — DAG-based data lineage with impact analysis
"""

from __future__ import annotations

from fastopendata.schema_evolution.lineage import (
    LineageEdge,
    LineageGraph,
    LineageNode,
)
from fastopendata.schema_evolution.registry import SchemaRegistry
from fastopendata.schema_evolution.schema import (
    CompatibilityChecker,
    CompatibilityLevel,
    CompatibilityResult,
    FieldSchema,
    FieldType,
    SchemaDiff,
    SchemaMerger,
    TableSchema,
)

__all__: list[str] = [
    "CompatibilityChecker",
    "CompatibilityLevel",
    "CompatibilityResult",
    "FieldSchema",
    "FieldType",
    "LineageEdge",
    "LineageGraph",
    "LineageNode",
    "SchemaDiff",
    "SchemaMerger",
    "SchemaRegistry",
    "TableSchema",
]
