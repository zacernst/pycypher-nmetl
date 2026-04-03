"""Versioned schema registry with compatibility enforcement and rollback.

:class:`SchemaRegistry` stores the full history of schema versions for
each table, enforces a configurable compatibility level on evolution,
and supports rollback to any prior version.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from fastopendata.schema_evolution.schema import (
    CompatibilityChecker,
    CompatibilityLevel,
    CompatibilityResult,
    SchemaDiff,
    TableSchema,
)


@dataclass
class SchemaHistory:
    """Full version history for a single table's schema."""

    table_name: str
    versions: list[TableSchema] = field(default_factory=list)

    @property
    def latest(self) -> TableSchema | None:
        return self.versions[-1] if self.versions else None

    @property
    def version_count(self) -> int:
        return len(self.versions)

    def get_version(self, version: int) -> TableSchema | None:
        for schema in self.versions:
            if schema.version == version:
                return schema
        return None


class SchemaRegistry:
    """Centralized schema store with compatibility enforcement.

    Parameters
    ----------
    compatibility_level : CompatibilityLevel
        Default compatibility policy applied to all schema evolutions.

    """

    def __init__(
        self,
        compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD,
    ) -> None:
        self._histories: dict[str, SchemaHistory] = {}
        self._compatibility_level = compatibility_level
        self._checker = CompatibilityChecker()

    @property
    def compatibility_level(self) -> CompatibilityLevel:
        return self._compatibility_level

    @property
    def table_names(self) -> list[str]:
        return list(self._histories.keys())

    def register(self, schema: TableSchema) -> CompatibilityResult:
        """Register a new schema version.

        If the table already has a schema, the new version is checked
        against the latest for compatibility.  Registration fails if
        the check does not pass.
        """
        name = schema.name
        if name not in self._histories:
            self._histories[name] = SchemaHistory(table_name=name)
            self._histories[name].versions.append(schema)
            return CompatibilityResult(
                compatible=True, level=self._compatibility_level
            )

        history = self._histories[name]
        latest = history.latest
        if latest is None:
            history.versions.append(schema)
            return CompatibilityResult(
                compatible=True, level=self._compatibility_level
            )

        result = self._checker.check(latest, schema, self._compatibility_level)
        if result.compatible:
            history.versions.append(schema)
        return result

    def get_latest(self, table_name: str) -> TableSchema | None:
        history = self._histories.get(table_name)
        return history.latest if history else None

    def get_version(self, table_name: str, version: int) -> TableSchema | None:
        history = self._histories.get(table_name)
        return history.get_version(version) if history else None

    def get_history(self, table_name: str) -> SchemaHistory | None:
        return self._histories.get(table_name)

    def diff(
        self,
        table_name: str,
        old_version: int,
        new_version: int,
    ) -> SchemaDiff | None:
        """Compute the diff between two versions of a table's schema."""
        history = self._histories.get(table_name)
        if history is None:
            return None
        old = history.get_version(old_version)
        new = history.get_version(new_version)
        if old is None or new is None:
            return None
        return SchemaDiff.compute(old, new)

    def rollback(
        self, table_name: str, target_version: int
    ) -> TableSchema | None:
        """Rollback to a prior version, removing all later versions.

        Returns the rolled-back schema, or *None* if the version doesn't exist.
        """
        history = self._histories.get(table_name)
        if history is None:
            return None
        target = history.get_version(target_version)
        if target is None:
            return None
        # Trim versions after target
        history.versions = [
            v for v in history.versions if v.version <= target_version
        ]
        return target

    def check_compatibility(
        self,
        table_name: str,
        proposed: TableSchema,
    ) -> CompatibilityResult:
        """Dry-run compatibility check without registering."""
        latest = self.get_latest(table_name)
        if latest is None:
            return CompatibilityResult(
                compatible=True, level=self._compatibility_level
            )
        return self._checker.check(latest, proposed, self._compatibility_level)

    def set_compatibility_level(self, level: CompatibilityLevel) -> None:
        """Change the global compatibility policy."""
        self._compatibility_level = level
