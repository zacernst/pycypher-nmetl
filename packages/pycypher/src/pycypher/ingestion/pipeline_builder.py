"""TUI-friendly high-level pipeline configuration API.

``PipelineBuilder`` wraps :class:`~pycypher.ingestion.config.PipelineConfig`
with stateful operations, undo/redo, snapshot/diff, and YAML round-tripping.

Usage example::

    builder = PipelineBuilder()
    builder.add_entity_source("people", "data/people.csv", "Person", id_col="id")
    builder.add_query("q1", inline="MATCH (p:Person) RETURN p.name")
    builder.save("pipeline.yaml")

    # Undo the last operation
    builder.undo()
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    QueryConfig,
    RelationshipSourceConfig,
    load_pipeline_config,
)

# ---------------------------------------------------------------------------
# Data classes for operation history and snapshots
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineOperation:
    """Record of a single builder operation for history display."""

    operation_type: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineSnapshot:
    """Immutable snapshot of pipeline configuration state."""

    config: PipelineConfig


@dataclass
class SchemaInfo:
    """Schema information for a data source."""

    columns: list[dict[str, str]]  # [{"name": ..., "type": ...}, ...]
    row_count: int | None = None


# ---------------------------------------------------------------------------
# Internal undo/redo entry — stores config snapshots
# ---------------------------------------------------------------------------


@dataclass
class _UndoEntry:
    """An undo stack entry: the config state *before* the operation."""

    config_before: PipelineConfig
    operation: PipelineOperation


# ---------------------------------------------------------------------------
# PipelineBuilder
# ---------------------------------------------------------------------------


class PipelineBuilder:
    """High-level, TUI-friendly pipeline configuration builder.

    Wraps :class:`PipelineConfig` with stateful CRUD operations, undo/redo,
    snapshot/diff, validation, dirty tracking, and YAML serialization.
    """

    def __init__(self, config: PipelineConfig | None = None) -> None:
        self._config: PipelineConfig = config or PipelineConfig()
        self._undo_stack: list[_UndoEntry] = []
        self._redo_stack: list[_UndoEntry] = []
        self._dirty: bool = False
        self._save_point: PipelineConfig = copy.deepcopy(self._config)

    # -- Construction -------------------------------------------------------

    @classmethod
    def from_config(cls, config: PipelineConfig) -> PipelineBuilder:
        """Create a builder from an existing :class:`PipelineConfig`.

        Makes a deep copy so the original config is not mutated.
        """
        return cls(config=copy.deepcopy(config))

    @classmethod
    def from_yaml_file(cls, path: str | Path) -> PipelineBuilder:
        """Load a builder from a YAML pipeline configuration file."""
        config = load_pipeline_config(path)
        return cls(config=config)

    # -- State accessors ----------------------------------------------------

    def get_config(self) -> PipelineConfig:
        """Return the current :class:`PipelineConfig`."""
        return self._config

    def is_dirty(self) -> bool:
        """Return ``True`` if config has been modified since last save/clean."""
        return self._dirty

    def mark_clean(self) -> None:
        """Mark the config as clean (e.g., after saving)."""
        self._dirty = False
        self._save_point = copy.deepcopy(self._config)

    # -- Internal helpers ---------------------------------------------------

    def _record_operation(self, op: PipelineOperation) -> None:
        """Push an undo entry and clear the redo stack."""
        self._undo_stack.append(
            _UndoEntry(
                config_before=copy.deepcopy(self._config),
                operation=op,
            )
        )
        self._redo_stack.clear()
        self._dirty = True

    def _apply_config(self, config: PipelineConfig) -> None:
        """Replace the current config (used by undo/redo/restore)."""
        self._config = copy.deepcopy(config)

    def _source_id_exists(self, source_id: str) -> bool:
        """Check if a source id already exists in entities or relationships."""
        for s in self._config.sources.entities:
            if s.id == source_id:
                return True
        for s in self._config.sources.relationships:
            if s.id == source_id:
                return True
        return False

    # -- Entity source operations -------------------------------------------

    def add_entity_source(
        self,
        source_id: str,
        uri: str,
        entity_type: str,
        *,
        id_col: str | None = None,
        query: str | None = None,
        schema_hints: dict[str, str] | None = None,
    ) -> None:
        """Add an entity data source to the pipeline configuration.

        Raises:
            ValueError: If *source_id* already exists.
        """
        if self._source_id_exists(source_id):
            msg = f"Source id {source_id!r} already exists."
            raise ValueError(msg)

        # Record *before* we mutate
        self._record_operation(
            PipelineOperation(
                operation_type="add_entity_source",
                details={"source_id": source_id, "entity_type": entity_type},
            )
        )
        self._config.sources.entities.append(
            EntitySourceConfig(
                id=source_id,
                uri=uri,
                entity_type=entity_type,
                id_col=id_col,
                query=query,
                schema_hints=schema_hints,
            )
        )

    def remove_entity_source(self, source_id: str) -> None:
        """Remove an entity source by its id.

        Raises:
            KeyError: If no entity source with *source_id* exists.
        """
        for i, s in enumerate(self._config.sources.entities):
            if s.id == source_id:
                self._record_operation(
                    PipelineOperation(
                        operation_type="remove_entity_source",
                        details={"source_id": source_id},
                    )
                )
                del self._config.sources.entities[i]
                return
        msg = f"Entity source {source_id!r} not found."
        raise KeyError(msg)

    def update_entity_source(self, source_id: str, **kwargs: Any) -> None:
        """Update fields of an existing entity source.

        Raises:
            KeyError: If no entity source with *source_id* exists.
        """
        for i, s in enumerate(self._config.sources.entities):
            if s.id == source_id:
                self._record_operation(
                    PipelineOperation(
                        operation_type="update_entity_source",
                        details={"source_id": source_id, **kwargs},
                    )
                )
                data = s.model_dump()
                data.update(kwargs)
                self._config.sources.entities[i] = (
                    EntitySourceConfig.model_validate(data)
                )
                return
        msg = f"Entity source {source_id!r} not found."
        raise KeyError(msg)

    def list_entity_sources(self) -> list[dict[str, Any]]:
        """Return summary dicts for all entity sources."""
        return [
            {
                "id": s.id,
                "uri": s.uri,
                "entity_type": s.entity_type,
                "id_col": s.id_col,
            }
            for s in self._config.sources.entities
        ]

    # -- Relationship source operations -------------------------------------

    def add_relationship_source(
        self,
        source_id: str,
        uri: str,
        relationship_type: str,
        source_col: str,
        target_col: str,
        *,
        id_col: str | None = None,
        query: str | None = None,
        schema_hints: dict[str, str] | None = None,
    ) -> None:
        """Add a relationship data source to the pipeline configuration.

        Raises:
            ValueError: If *source_id* already exists.
        """
        if self._source_id_exists(source_id):
            msg = f"Source id {source_id!r} already exists."
            raise ValueError(msg)

        self._record_operation(
            PipelineOperation(
                operation_type="add_relationship_source",
                details={
                    "source_id": source_id,
                    "relationship_type": relationship_type,
                },
            )
        )
        self._config.sources.relationships.append(
            RelationshipSourceConfig(
                id=source_id,
                uri=uri,
                relationship_type=relationship_type,
                source_col=source_col,
                target_col=target_col,
                id_col=id_col,
                query=query,
                schema_hints=schema_hints,
            )
        )

    def remove_relationship_source(self, source_id: str) -> None:
        """Remove a relationship source by its id.

        Raises:
            KeyError: If no relationship source with *source_id* exists.
        """
        for i, s in enumerate(self._config.sources.relationships):
            if s.id == source_id:
                self._record_operation(
                    PipelineOperation(
                        operation_type="remove_relationship_source",
                        details={"source_id": source_id},
                    )
                )
                del self._config.sources.relationships[i]
                return
        msg = f"Relationship source {source_id!r} not found."
        raise KeyError(msg)

    def update_relationship_source(self, source_id: str, **kwargs: Any) -> None:
        """Update fields of an existing relationship source.

        Raises:
            KeyError: If no relationship source with *source_id* exists.
        """
        for i, s in enumerate(self._config.sources.relationships):
            if s.id == source_id:
                self._record_operation(
                    PipelineOperation(
                        operation_type="update_relationship_source",
                        details={"source_id": source_id, **kwargs},
                    )
                )
                data = s.model_dump()
                data.update(kwargs)
                self._config.sources.relationships[i] = (
                    RelationshipSourceConfig.model_validate(data)
                )
                return
        msg = f"Relationship source {source_id!r} not found."
        raise KeyError(msg)

    def list_relationship_sources(self) -> list[dict[str, Any]]:
        """Return summary dicts for all relationship sources."""
        return [
            {
                "id": s.id,
                "uri": s.uri,
                "relationship_type": s.relationship_type,
                "source_col": s.source_col,
                "target_col": s.target_col,
            }
            for s in self._config.sources.relationships
        ]

    # -- Query operations ---------------------------------------------------

    def add_query(
        self,
        query_id: str,
        *,
        inline: str | None = None,
        source: str | None = None,
        description: str | None = None,
    ) -> None:
        """Add a Cypher query to the pipeline configuration.

        Raises:
            ValueError: If *query_id* already exists.
        """
        for q in self._config.queries:
            if q.id == query_id:
                msg = f"Query id {query_id!r} already exists."
                raise ValueError(msg)

        self._record_operation(
            PipelineOperation(
                operation_type="add_query",
                details={"query_id": query_id},
            )
        )
        self._config.queries.append(
            QueryConfig(
                id=query_id,
                inline=inline,
                source=source,
                description=description,
            )
        )

    def remove_query(self, query_id: str) -> None:
        """Remove a query by its id.

        Raises:
            KeyError: If no query with *query_id* exists.
        """
        for i, q in enumerate(self._config.queries):
            if q.id == query_id:
                self._record_operation(
                    PipelineOperation(
                        operation_type="remove_query",
                        details={"query_id": query_id},
                    )
                )
                del self._config.queries[i]
                return
        msg = f"Query {query_id!r} not found."
        raise KeyError(msg)

    # -- Output operations --------------------------------------------------

    def add_output(
        self,
        query_id: str,
        uri: str,
        *,
        format: str | None = None,
    ) -> None:
        """Add an output sink for a query result."""
        self._record_operation(
            PipelineOperation(
                operation_type="add_output",
                details={"query_id": query_id, "uri": uri},
            )
        )
        self._config.output.append(
            OutputConfig(query_id=query_id, uri=uri, format=format)
        )

    def remove_output(self, query_id: str, uri: str) -> None:
        """Remove an output by query_id and uri.

        Raises:
            KeyError: If no matching output exists.
        """
        for i, o in enumerate(self._config.output):
            if o.query_id == query_id and o.uri == uri:
                self._record_operation(
                    PipelineOperation(
                        operation_type="remove_output",
                        details={"query_id": query_id, "uri": uri},
                    )
                )
                del self._config.output[i]
                return
        msg = f"Output for query {query_id!r} at {uri!r} not found."
        raise KeyError(msg)

    # -- Undo / redo --------------------------------------------------------

    def can_undo(self) -> bool:
        """Return ``True`` if there are operations to undo."""
        return len(self._undo_stack) > 0

    def can_redo(self) -> bool:
        """Return ``True`` if there are operations to redo."""
        return len(self._redo_stack) > 0

    def undo(self) -> None:
        """Revert the most recent operation.

        Raises:
            IndexError: If the undo stack is empty.
        """
        if not self._undo_stack:
            msg = "Nothing to undo."
            raise IndexError(msg)
        entry = self._undo_stack.pop()
        # Push current state onto redo stack
        self._redo_stack.append(
            _UndoEntry(
                config_before=copy.deepcopy(self._config),
                operation=entry.operation,
            )
        )
        self._apply_config(entry.config_before)
        self._dirty = True

    def redo(self) -> None:
        """Re-apply the most recently undone operation.

        Raises:
            IndexError: If the redo stack is empty.
        """
        if not self._redo_stack:
            msg = "Nothing to redo."
            raise IndexError(msg)
        entry = self._redo_stack.pop()
        # Push current state onto undo stack (without clearing redo)
        self._undo_stack.append(
            _UndoEntry(
                config_before=copy.deepcopy(self._config),
                operation=entry.operation,
            )
        )
        self._apply_config(entry.config_before)
        self._dirty = True

    # -- Snapshot / diff / restore ------------------------------------------

    def snapshot(self) -> PipelineSnapshot:
        """Capture a snapshot of the current configuration state."""
        return PipelineSnapshot(config=copy.deepcopy(self._config))

    def diff(self, previous: PipelineSnapshot) -> dict[str, list[str]]:
        """Compute a diff between current state and a previous snapshot.

        Returns:
            Dict with keys: added_entities, removed_entities,
            added_relationships, removed_relationships,
            added_queries, removed_queries.
        """
        prev_entity_ids = {
            s.id for s in previous.config.sources.entities
        }
        curr_entity_ids = {
            s.id for s in self._config.sources.entities
        }
        prev_rel_ids = {
            s.id for s in previous.config.sources.relationships
        }
        curr_rel_ids = {
            s.id for s in self._config.sources.relationships
        }
        prev_query_ids = {q.id for q in previous.config.queries}
        curr_query_ids = {q.id for q in self._config.queries}

        return {
            "added_entities": sorted(curr_entity_ids - prev_entity_ids),
            "removed_entities": sorted(prev_entity_ids - curr_entity_ids),
            "added_relationships": sorted(curr_rel_ids - prev_rel_ids),
            "removed_relationships": sorted(prev_rel_ids - curr_rel_ids),
            "added_queries": sorted(curr_query_ids - prev_query_ids),
            "removed_queries": sorted(prev_query_ids - curr_query_ids),
        }

    def restore(self, snapshot: PipelineSnapshot) -> None:
        """Restore builder state from a previous snapshot."""
        self._record_operation(
            PipelineOperation(
                operation_type="restore",
                details={},
            )
        )
        self._apply_config(snapshot.config)

    # -- Validation ---------------------------------------------------------

    def validate(self) -> list[str]:
        """Validate the current configuration for semantic correctness.

        Returns:
            List of error message strings (empty if valid).
        """
        errors: list[str] = []
        query_ids = {q.id for q in self._config.queries}
        for o in self._config.output:
            if o.query_id not in query_ids:
                errors.append(
                    f"Output references non-existent query {o.query_id!r}."
                )
        # Check for duplicate source ids
        all_source_ids: list[str] = []
        for s in self._config.sources.entities:
            all_source_ids.append(s.id)
        for s in self._config.sources.relationships:
            all_source_ids.append(s.id)
        seen: set[str] = set()
        for sid in all_source_ids:
            if sid in seen:
                errors.append(f"Duplicate source id {sid!r}.")
            seen.add(sid)
        return errors

    # -- History ------------------------------------------------------------

    def history(self) -> list[PipelineOperation]:
        """Return the list of operations performed (oldest first)."""
        return [entry.operation for entry in self._undo_stack]

    # -- YAML serialization -------------------------------------------------

    def to_yaml(self) -> str:
        """Serialize the current config to a YAML string."""
        data = self._config.model_dump(exclude_none=True, exclude_defaults=False)
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    def save(self, path: str | Path) -> None:
        """Write the current config to a YAML file.

        Also marks the builder as clean.
        """
        path = Path(path)
        path.write_text(self.to_yaml(), encoding="utf-8")
        self.mark_clean()
