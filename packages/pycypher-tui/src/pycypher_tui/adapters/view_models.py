"""Immutable view model dataclasses for TUI screens.

These decouple TUI screens from pycypher internal data structures.
Screens consume view models exclusively; the DataModelAdapter produces them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationIssue:
    """A single validation finding."""

    level: str  # "error" | "warning" | "info"
    message: str
    field: str | None = None  # which field triggered the issue
    fix_hint: str | None = None  # suggested fix


@dataclass(frozen=True)
class PropertyViewModel:
    """A property (column) of an entity or relationship type."""

    name: str
    dtype: str  # inferred or declared type
    nullable: bool = True
    source_id: str | None = None  # which source declared it


@dataclass(frozen=True)
class EntitySourceViewModel:
    """View of a single entity data source."""

    source_id: str
    uri: str
    entity_type: str
    id_col: str | None
    query: str | None
    schema_hints: tuple[tuple[str, str], ...] = ()  # (col, type) pairs
    row_count: int | None = None


@dataclass(frozen=True)
class EntityViewModel:
    """Summary view of an entity type for list display."""

    entity_type: str
    source_count: int
    property_names: tuple[str, ...]
    id_column: str | None
    has_index: bool = False
    row_count: int | None = None  # None if index unavailable


@dataclass(frozen=True)
class EntityDetailViewModel:
    """Detailed view of an entity type for the detail panel."""

    entity_type: str
    sources: tuple[EntitySourceViewModel, ...]
    properties: tuple[PropertyViewModel, ...]
    validation_issues: tuple[ValidationIssue, ...]
    row_count: int | None = None


@dataclass(frozen=True)
class ColumnMappingViewModel:
    """A column mapping for a relationship source."""

    source_col: str
    target_col: str
    source_entity: str | None = None  # resolved entity type
    target_entity: str | None = None  # resolved entity type


@dataclass(frozen=True)
class RelationshipSourceViewModel:
    """View of a single relationship data source."""

    source_id: str
    uri: str
    relationship_type: str
    source_col: str
    target_col: str
    id_col: str | None = None
    query: str | None = None
    schema_hints: tuple[tuple[str, str], ...] = ()
    row_count: int | None = None


@dataclass(frozen=True)
class RelationshipViewModel:
    """Summary view of a relationship type for list display."""

    relationship_type: str
    source_entity: str | None  # resolved or None
    target_entity: str | None
    source_count: int
    column_mappings: tuple[ColumnMappingViewModel, ...]
    validation_status: str  # "valid" | "warning" | "error"


@dataclass(frozen=True)
class RelationshipDetailViewModel:
    """Detailed view of a relationship type for the detail panel."""

    relationship_type: str
    sources: tuple[RelationshipSourceViewModel, ...]
    column_mappings: tuple[ColumnMappingViewModel, ...]
    validation_issues: tuple[ValidationIssue, ...]


@dataclass(frozen=True)
class SourceMappingViewModel:
    """View of a data source's mapping to the model."""

    source_id: str
    uri: str
    maps_to: str  # entity_type or relationship_type
    mapping_type: str  # "entity" | "relationship"
    status: str  # "connected" | "orphaned" | "error"


@dataclass(frozen=True)
class ModelStatsViewModel:
    """Aggregate model statistics."""

    entity_type_count: int
    relationship_type_count: int
    total_source_count: int
    total_entity_rows: int | None = None
    total_relationship_rows: int | None = None
    query_count: int = 0
    output_count: int = 0
