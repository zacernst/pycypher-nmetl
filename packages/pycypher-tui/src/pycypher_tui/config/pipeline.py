"""TUI configuration manager with atomic save, backup, and validation.

Wraps :class:`~pycypher.ingestion.pipeline_builder.PipelineBuilder` with
TUI-specific features: atomic file writes, automatic backups, structured
validation via :mod:`pycypher.ingestion.validation`, and a simplified
API surface for screen widgets.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from pycypher.ingestion.config import PipelineConfig
from pycypher.ingestion.pipeline_builder import (
    PipelineBuilder,
    PipelineOperation,
    PipelineSnapshot,
)
from pycypher.ingestion.validation import ValidationResult, validate_config

__all__ = ["ConfigManager"]


class ConfigManager:
    """High-level TUI configuration manager.

    Provides a simplified interface for TUI screens, delegating to
    :class:`PipelineBuilder` for CRUD/undo/redo and to
    :mod:`pycypher.ingestion.validation` for structured validation.

    Features beyond ``PipelineBuilder``:

    - **Atomic saves**: writes to a temp file then renames, preventing
      partial writes on crash.
    - **Automatic backups**: saves a ``.bak`` copy before overwriting.
    - **Structured validation**: returns :class:`ValidationResult` objects
      with categorised errors and fix suggestions.
    """

    def __init__(self, builder: PipelineBuilder | None = None) -> None:
        self._builder = builder or PipelineBuilder()
        self._file_path: Path | None = None

    # -- Construction -------------------------------------------------------

    @classmethod
    def from_file(cls, path: str | Path) -> ConfigManager:
        """Load a configuration from a YAML file."""
        path = Path(path)
        builder = PipelineBuilder.from_yaml_file(path)
        mgr = cls(builder=builder)
        mgr._file_path = path
        return mgr

    @classmethod
    def from_config(cls, config: PipelineConfig) -> ConfigManager:
        """Create from an existing PipelineConfig."""
        builder = PipelineBuilder.from_config(config)
        return cls(builder=builder)

    # -- State accessors ----------------------------------------------------

    def get_config(self) -> PipelineConfig:
        """Return the current PipelineConfig."""
        return self._builder.get_config()

    def is_dirty(self) -> bool:
        """True if config has unsaved changes."""
        return self._builder.is_dirty()

    def is_empty(self) -> bool:
        """True if config has no sources, queries, or outputs."""
        cfg = self._builder.get_config()
        return (
            len(cfg.sources.entities) == 0
            and len(cfg.sources.relationships) == 0
            and len(cfg.queries) == 0
            and len(cfg.output) == 0
        )

    # -- CRUD operations (delegate to builder) ------------------------------

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
        """Add an entity data source."""
        self._builder.add_entity_source(
            source_id, uri, entity_type,
            id_col=id_col, query=query, schema_hints=schema_hints,
        )

    def update_entity_source(self, source_id: str, **kwargs) -> None:
        """Update fields of an existing entity source."""
        self._builder.update_entity_source(source_id, **kwargs)

    def remove_entity_source(self, source_id: str) -> None:
        """Remove an entity source by id."""
        self._builder.remove_entity_source(source_id)

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
        """Add a relationship data source."""
        self._builder.add_relationship_source(
            source_id, uri, relationship_type, source_col, target_col,
            id_col=id_col, query=query, schema_hints=schema_hints,
        )

    def update_relationship_source(self, source_id: str, **kwargs) -> None:
        """Update fields of an existing relationship source."""
        self._builder.update_relationship_source(source_id, **kwargs)

    def remove_relationship_source(self, source_id: str) -> None:
        """Remove a relationship source by id."""
        self._builder.remove_relationship_source(source_id)

    def add_query(
        self,
        query_id: str,
        *,
        inline: str | None = None,
        source: str | None = None,
        description: str | None = None,
    ) -> None:
        """Add a Cypher query."""
        self._builder.add_query(
            query_id, inline=inline, source=source, description=description,
        )

    def remove_query(self, query_id: str) -> None:
        """Remove a query by id."""
        self._builder.remove_query(query_id)

    def add_output(
        self,
        query_id: str,
        uri: str,
        *,
        format: str | None = None,
    ) -> None:
        """Add an output sink."""
        self._builder.add_output(query_id, uri, format=format)

    def remove_output(self, query_id: str, uri: str) -> None:
        """Remove an output sink."""
        self._builder.remove_output(query_id, uri)

    # -- Undo / redo --------------------------------------------------------

    def can_undo(self) -> bool:
        return self._builder.can_undo()

    def can_redo(self) -> bool:
        return self._builder.can_redo()

    def undo(self) -> None:
        self._builder.undo()

    def redo(self) -> None:
        self._builder.redo()

    # -- History / snapshot / diff ------------------------------------------

    def history(self) -> list[PipelineOperation]:
        return self._builder.history()

    def snapshot(self) -> PipelineSnapshot:
        return self._builder.snapshot()

    def diff(self, previous: PipelineSnapshot) -> dict[str, list[str]]:
        return self._builder.diff(previous)

    # -- Validation ---------------------------------------------------------

    def validate(self) -> ValidationResult:
        """Validate the current config with structured error objects.

        Returns a :class:`ValidationResult` with categorised errors and
        fix suggestions suitable for TUI display.
        """
        return validate_config(self._builder.get_config())

    # -- Persistence --------------------------------------------------------

    def save(self, path: str | Path | None = None) -> None:
        """Save the configuration with atomic write and backup.

        1. If the target file exists, copy it to ``<name>.bak``.
        2. Write to a temp file in the same directory.
        3. Rename the temp file over the target (atomic on POSIX).
        4. Mark the builder as clean.

        Args:
            path: Target file path.  Uses the path from ``from_file()``
                if not provided.

        Raises:
            ValueError: If no path is provided and none was set via ``from_file``.
        """
        save_path = Path(path) if path is not None else self._file_path
        if save_path is None:
            msg = "No file path provided. Pass a path or use from_file()."
            raise ValueError(msg)

        save_path = save_path.resolve()

        # Step 1: Backup existing file
        if save_path.exists():
            backup_path = save_path.with_suffix(save_path.suffix + ".bak")
            backup_path.write_text(
                save_path.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

        # Step 2: Write to temp file in same directory (for atomic rename)
        yaml_content = self._builder.to_yaml()
        fd, tmp_path = tempfile.mkstemp(
            dir=save_path.parent,
            suffix=".tmp",
            prefix=".pipeline_",
        )
        try:
            os.write(fd, yaml_content.encode("utf-8"))
            os.close(fd)
            # Step 3: Atomic rename
            os.replace(tmp_path, save_path)
        except BaseException:
            os.close(fd) if not os.get_inheritable(fd) else None
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

        # Step 4: Mark clean
        self._builder.mark_clean()
        self._file_path = save_path
