"""DataModelAdapter — facade joining PipelineConfig + GraphIndexManager for TUI.

Screens never access pycypher internals directly. This adapter produces
immutable view models from the current configuration and optional runtime
indexes. The cache is refreshed when ``refresh()`` is called (typically in
response to a ConfigChanged message on the Textual message bus).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pycypher_tui.adapters.view_models import (
    ColumnMappingViewModel,
    EntityDetailViewModel,
    EntitySourceViewModel,
    EntityViewModel,
    ModelStatsViewModel,
    PropertyViewModel,
    RelationshipDetailViewModel,
    RelationshipSourceViewModel,
    RelationshipViewModel,
    SourceMappingViewModel,
    ValidationIssue,
)

if TYPE_CHECKING:
    from pycypher.graph_index import GraphIndexManager

    from pycypher_tui.config.pipeline import ConfigManager

logger = logging.getLogger(__name__)


class DataModelAdapter:
    """Facade joining PipelineConfig + GraphIndexManager for TUI consumption.

    The adapter caches computed view models and invalidates them on
    ``refresh()``. When ``index_manager`` is ``None`` (config-only mode),
    statistics fields degrade gracefully to ``None``.
    """

    def __init__(
        self,
        config_manager: ConfigManager,
        index_manager: GraphIndexManager | None = None,
    ) -> None:
        self._config_manager = config_manager
        self._index_manager = index_manager

        # Cached view models — invalidated by refresh()
        self._entity_cache: list[EntityViewModel] | None = None
        self._relationship_cache: list[RelationshipViewModel] | None = None
        self._source_mapping_cache: list[SourceMappingViewModel] | None = None
        self._stats_cache: ModelStatsViewModel | None = None

    # -- Cache management ---------------------------------------------------

    def refresh(self) -> None:
        """Invalidate all cached view models.

        Call this in response to ConfigChanged messages.
        """
        self._entity_cache = None
        self._relationship_cache = None
        self._source_mapping_cache = None
        self._stats_cache = None

    # -- Entity access ------------------------------------------------------

    def entity_types(self) -> list[EntityViewModel]:
        """Return summary view models for all entity types."""
        if self._entity_cache is not None:
            return self._entity_cache

        config = self._config_manager.get_config()
        # Group entity sources by type
        by_type: dict[str, list] = {}
        for source in config.sources.entities:
            by_type.setdefault(source.entity_type, []).append(source)

        result: list[EntityViewModel] = []
        for entity_type in sorted(by_type):
            sources = by_type[entity_type]
            # Collect property names from schema_hints across sources
            property_names: set[str] = set()
            id_col: str | None = None
            for src in sources:
                if src.schema_hints:
                    property_names.update(src.schema_hints.keys())
                if src.id_col and id_col is None:
                    id_col = src.id_col

            result.append(
                EntityViewModel(
                    entity_type=entity_type,
                    source_count=len(sources),
                    property_names=tuple(sorted(property_names)),
                    id_column=id_col,
                    has_index=self._has_entity_index(entity_type),
                    row_count=self._get_entity_row_count(entity_type),
                )
            )

        self._entity_cache = result
        return result

    def entity_detail(self, entity_type: str) -> EntityDetailViewModel:
        """Return detailed view model for a specific entity type."""
        config = self._config_manager.get_config()
        sources = [s for s in config.sources.entities if s.entity_type == entity_type]

        source_vms: list[EntitySourceViewModel] = []
        all_properties: dict[str, PropertyViewModel] = {}

        for src in sources:
            hints = tuple(src.schema_hints.items()) if src.schema_hints else ()
            source_vms.append(
                EntitySourceViewModel(
                    source_id=src.id,
                    uri=src.uri,
                    entity_type=src.entity_type,
                    id_col=src.id_col,
                    query=src.query,
                    schema_hints=hints,
                )
            )
            # Build property list from schema hints
            if src.schema_hints:
                for col_name, col_type in src.schema_hints.items():
                    if col_name not in all_properties:
                        all_properties[col_name] = PropertyViewModel(
                            name=col_name,
                            dtype=col_type,
                            source_id=src.id,
                        )

        # Validate
        issues = self._validate_entity(entity_type, sources)

        return EntityDetailViewModel(
            entity_type=entity_type,
            sources=tuple(source_vms),
            properties=tuple(all_properties.values()),
            validation_issues=tuple(issues),
            row_count=self._get_entity_row_count(entity_type),
        )

    # -- Relationship access ------------------------------------------------

    def relationship_types(self) -> list[RelationshipViewModel]:
        """Return summary view models for all relationship types."""
        if self._relationship_cache is not None:
            return self._relationship_cache

        config = self._config_manager.get_config()
        # Group relationship sources by type
        by_type: dict[str, list] = {}
        for source in config.sources.relationships:
            by_type.setdefault(source.relationship_type, []).append(source)

        # Build entity ID column lookup for resolving endpoints
        entity_by_id_col = self._build_entity_id_col_lookup()

        result: list[RelationshipViewModel] = []
        for rel_type in sorted(by_type):
            sources = by_type[rel_type]
            mappings: list[ColumnMappingViewModel] = []
            for src in sources:
                source_entity = entity_by_id_col.get(src.source_col)
                target_entity = entity_by_id_col.get(src.target_col)
                mappings.append(
                    ColumnMappingViewModel(
                        source_col=src.source_col,
                        target_col=src.target_col,
                        source_entity=source_entity,
                        target_entity=target_entity,
                    )
                )

            # Determine validation status
            status = self._compute_relationship_status(rel_type, sources, entity_by_id_col)

            result.append(
                RelationshipViewModel(
                    relationship_type=rel_type,
                    source_entity=mappings[0].source_entity if mappings else None,
                    target_entity=mappings[0].target_entity if mappings else None,
                    source_count=len(sources),
                    column_mappings=tuple(mappings),
                    validation_status=status,
                )
            )

        self._relationship_cache = result
        return result

    def relationship_detail(self, rel_type: str) -> RelationshipDetailViewModel:
        """Return detailed view model for a specific relationship type."""
        config = self._config_manager.get_config()
        sources = [
            r for r in config.sources.relationships
            if r.relationship_type == rel_type
        ]

        entity_by_id_col = self._build_entity_id_col_lookup()

        source_vms: list[RelationshipSourceViewModel] = []
        mappings: list[ColumnMappingViewModel] = []

        for src in sources:
            hints = tuple(src.schema_hints.items()) if src.schema_hints else ()
            source_vms.append(
                RelationshipSourceViewModel(
                    source_id=src.id,
                    uri=src.uri,
                    relationship_type=src.relationship_type,
                    source_col=src.source_col,
                    target_col=src.target_col,
                    id_col=src.id_col,
                    query=src.query,
                    schema_hints=hints,
                )
            )
            source_entity = entity_by_id_col.get(src.source_col)
            target_entity = entity_by_id_col.get(src.target_col)
            mappings.append(
                ColumnMappingViewModel(
                    source_col=src.source_col,
                    target_col=src.target_col,
                    source_entity=source_entity,
                    target_entity=target_entity,
                )
            )

        issues = self._validate_relationship(rel_type, sources, entity_by_id_col)

        return RelationshipDetailViewModel(
            relationship_type=rel_type,
            sources=tuple(source_vms),
            column_mappings=tuple(mappings),
            validation_issues=tuple(issues),
        )

    # -- Source mapping access -----------------------------------------------

    def source_mappings(self) -> list[SourceMappingViewModel]:
        """Return view models for all source-to-model mappings."""
        if self._source_mapping_cache is not None:
            return self._source_mapping_cache

        config = self._config_manager.get_config()
        result: list[SourceMappingViewModel] = []

        for src in config.sources.entities:
            result.append(
                SourceMappingViewModel(
                    source_id=src.id,
                    uri=src.uri,
                    maps_to=src.entity_type,
                    mapping_type="entity",
                    status="connected",
                )
            )

        for src in config.sources.relationships:
            result.append(
                SourceMappingViewModel(
                    source_id=src.id,
                    uri=src.uri,
                    maps_to=src.relationship_type,
                    mapping_type="relationship",
                    status="connected",
                )
            )

        self._source_mapping_cache = result
        return result

    # -- Statistics ----------------------------------------------------------

    def model_statistics(self) -> ModelStatsViewModel:
        """Return aggregate model statistics."""
        if self._stats_cache is not None:
            return self._stats_cache

        config = self._config_manager.get_config()

        entity_types = {s.entity_type for s in config.sources.entities}
        rel_types = {s.relationship_type for s in config.sources.relationships}
        total_sources = len(config.sources.entities) + len(config.sources.relationships)

        self._stats_cache = ModelStatsViewModel(
            entity_type_count=len(entity_types),
            relationship_type_count=len(rel_types),
            total_source_count=total_sources,
            total_entity_rows=self._get_total_entity_rows(),
            total_relationship_rows=self._get_total_relationship_rows(),
            query_count=len(config.queries),
            output_count=len(config.output),
        )
        return self._stats_cache

    # -- Private helpers -----------------------------------------------------

    def _build_entity_id_col_lookup(self) -> dict[str, str]:
        """Build a mapping from id_col -> entity_type for resolving endpoints."""
        config = self._config_manager.get_config()
        lookup: dict[str, str] = {}
        for entity in config.sources.entities:
            if entity.id_col:
                lookup[entity.id_col] = entity.entity_type
        return lookup

    def _has_entity_index(self, entity_type: str) -> bool:
        """Check if an entity type has a runtime index."""
        if self._index_manager is None:
            return False
        try:
            label_indexes = getattr(self._index_manager, "_label", {})
            return entity_type in label_indexes
        except Exception:
            return False

    def _get_entity_row_count(self, entity_type: str) -> int | None:
        """Get row count for an entity type from runtime indexes."""
        if self._index_manager is None:
            return None
        try:
            label_indexes = getattr(self._index_manager, "_label", {})
            idx = label_indexes.get(entity_type)
            if idx is not None:
                return len(idx)
        except Exception:
            pass
        return None

    def _get_total_entity_rows(self) -> int | None:
        """Get total entity row count across all types."""
        if self._index_manager is None:
            return None
        total = 0
        config = self._config_manager.get_config()
        entity_types = {s.entity_type for s in config.sources.entities}
        for etype in entity_types:
            count = self._get_entity_row_count(etype)
            if count is not None:
                total += count
            else:
                return None  # incomplete data
        return total

    def _get_total_relationship_rows(self) -> int | None:
        """Get total relationship row count across all types."""
        if self._index_manager is None:
            return None
        # Relationship row counts require adjacency indexes
        try:
            adj_indexes = getattr(self._index_manager, "_adjacency", {})
            if not adj_indexes:
                return None
            total = 0
            for idx in adj_indexes.values():
                total += len(idx)
            return total
        except Exception:
            return None

    def _validate_entity(self, entity_type: str, sources: list) -> list[ValidationIssue]:
        """Run validation checks on an entity type."""
        issues: list[ValidationIssue] = []

        if not sources:
            issues.append(
                ValidationIssue(
                    level="error",
                    message=f"No data sources defined for entity type '{entity_type}'",
                )
            )
            return issues

        # Check for missing URI
        for src in sources:
            if not src.uri:
                issues.append(
                    ValidationIssue(
                        level="error",
                        message=f"Source '{src.id}' has no URI",
                        field="uri",
                        fix_hint="Add a URI pointing to the data file or database",
                    )
                )

        # Check for duplicate source IDs
        seen_ids: set[str] = set()
        for src in sources:
            if src.id in seen_ids:
                issues.append(
                    ValidationIssue(
                        level="error",
                        message=f"Duplicate source ID '{src.id}'",
                        field="source_id",
                        fix_hint="Use unique IDs for each data source",
                    )
                )
            seen_ids.add(src.id)

        return issues

    def _validate_relationship(
        self,
        rel_type: str,
        sources: list,
        entity_by_id_col: dict[str, str],
    ) -> list[ValidationIssue]:
        """Run validation checks on a relationship type."""
        issues: list[ValidationIssue] = []

        if not sources:
            issues.append(
                ValidationIssue(
                    level="error",
                    message=f"No data sources defined for relationship type '{rel_type}'",
                )
            )
            return issues

        config = self._config_manager.get_config()
        has_entities = len(config.sources.entities) > 0

        for src in sources:
            if not src.uri:
                issues.append(
                    ValidationIssue(
                        level="error",
                        message=f"Source '{src.id}' has no URI",
                        field="uri",
                        fix_hint="Add a URI pointing to the data file or database",
                    )
                )

            # Check if source/target columns can resolve to entity types
            if has_entities:
                if src.source_col not in entity_by_id_col:
                    issues.append(
                        ValidationIssue(
                            level="warning",
                            message=(
                                f"Source column '{src.source_col}' does not match "
                                f"any entity ID column"
                            ),
                            field="source_col",
                            fix_hint="Ensure an entity source has a matching id_col",
                        )
                    )
                if src.target_col not in entity_by_id_col:
                    issues.append(
                        ValidationIssue(
                            level="warning",
                            message=(
                                f"Target column '{src.target_col}' does not match "
                                f"any entity ID column"
                            ),
                            field="target_col",
                            fix_hint="Ensure an entity source has a matching id_col",
                        )
                    )
            else:
                issues.append(
                    ValidationIssue(
                        level="warning",
                        message="No entity sources defined to validate column mappings",
                    )
                )

        return issues

    def _compute_relationship_status(
        self,
        rel_type: str,
        sources: list,
        entity_by_id_col: dict[str, str],
    ) -> str:
        """Compute validation status string for a relationship type."""
        issues = self._validate_relationship(rel_type, sources, entity_by_id_col)
        if any(i.level == "error" for i in issues):
            return "error"
        if any(i.level == "warning" for i in issues):
            return "warning"
        return "valid"
