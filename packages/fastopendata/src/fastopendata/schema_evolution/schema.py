"""Schema definitions, diff computation, compatibility checking, and merging.

This module implements the core schema evolution logic:

* :class:`FieldSchema` and :class:`TableSchema` describe dataset structure.
* :class:`SchemaDiff` computes structural deltas between schema versions.
* :class:`CompatibilityChecker` validates whether a schema change is safe
  at a given compatibility level (backward, forward, full, none).
* :class:`SchemaMerger` resolves two schemas into a unified superset with
  configurable conflict resolution strategies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


class FieldType(Enum):
    """Supported field data types."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"
    NULL = "null"


# Type widening rules: a field can be widened from source → target safely.
_WIDENING_RULES: dict[FieldType, set[FieldType]] = {
    FieldType.INTEGER: {FieldType.FLOAT, FieldType.STRING},
    FieldType.FLOAT: {FieldType.STRING},
    FieldType.BOOLEAN: {FieldType.STRING, FieldType.INTEGER},
    FieldType.DATE: {FieldType.TIMESTAMP, FieldType.STRING},
    FieldType.TIMESTAMP: {FieldType.STRING},
}


@dataclass(frozen=True, slots=True)
class FieldSchema:
    """Descriptor for a single typed field in a table schema."""

    name: str
    field_type: FieldType
    nullable: bool = True
    default: Any = None
    metadata: dict[str, str] = field(default_factory=dict)

    def is_compatible_with(self, other: FieldSchema) -> bool:
        """Return True if *other* is a compatible evolution of this field."""
        if self.field_type == other.field_type:
            # Making a non-nullable field nullable is safe (backward compat)
            return True
        # Check widening rules
        return other.field_type in _WIDENING_RULES.get(self.field_type, set())


@dataclass(frozen=True, slots=True)
class TableSchema:
    """Ordered collection of fields with a version label."""

    name: str
    fields: tuple[FieldSchema, ...]
    version: int = 1
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def get_field(self, name: str) -> FieldSchema | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def field_map(self) -> dict[str, FieldSchema]:
        return {f.name: f for f in self.fields}

    def add_field(self, field_schema: FieldSchema) -> TableSchema:
        """Return a new schema with the field appended and version bumped."""
        return TableSchema(
            name=self.name,
            fields=(*self.fields, field_schema),
            version=self.version + 1,
            metadata=self.metadata,
        )

    def remove_field(self, field_name: str) -> TableSchema:
        """Return a new schema without the named field, version bumped."""
        return TableSchema(
            name=self.name,
            fields=tuple(f for f in self.fields if f.name != field_name),
            version=self.version + 1,
            metadata=self.metadata,
        )


# ---------------------------------------------------------------------------
# Schema diffing
# ---------------------------------------------------------------------------


class DiffType(Enum):
    FIELD_ADDED = auto()
    FIELD_REMOVED = auto()
    TYPE_CHANGED = auto()
    NULLABILITY_CHANGED = auto()
    DEFAULT_CHANGED = auto()


@dataclass(frozen=True, slots=True)
class DiffEntry:
    """A single atomic change between two schema versions."""

    diff_type: DiffType
    field_name: str
    old_value: Any = None
    new_value: Any = None


@dataclass(frozen=True, slots=True)
class SchemaDiff:
    """Complete diff between two :class:`TableSchema` instances."""

    old_schema: TableSchema
    new_schema: TableSchema
    entries: tuple[DiffEntry, ...]

    @property
    def has_changes(self) -> bool:
        return len(self.entries) > 0

    @property
    def added_fields(self) -> list[str]:
        return [
            e.field_name for e in self.entries if e.diff_type == DiffType.FIELD_ADDED
        ]

    @property
    def removed_fields(self) -> list[str]:
        return [
            e.field_name for e in self.entries if e.diff_type == DiffType.FIELD_REMOVED
        ]

    @property
    def type_changes(self) -> list[DiffEntry]:
        return [e for e in self.entries if e.diff_type == DiffType.TYPE_CHANGED]

    @classmethod
    def compute(cls, old: TableSchema, new: TableSchema) -> SchemaDiff:
        """Compute the diff between *old* and *new* schemas."""
        entries: list[DiffEntry] = []
        old_map = old.field_map()
        new_map = new.field_map()

        # Detect removed and changed fields
        for name, old_field in old_map.items():
            if name not in new_map:
                entries.append(
                    DiffEntry(DiffType.FIELD_REMOVED, name, old_value=old_field),
                )
                continue
            new_field = new_map[name]
            if old_field.field_type != new_field.field_type:
                entries.append(
                    DiffEntry(
                        DiffType.TYPE_CHANGED,
                        name,
                        old_value=old_field.field_type,
                        new_value=new_field.field_type,
                    ),
                )
            if old_field.nullable != new_field.nullable:
                entries.append(
                    DiffEntry(
                        DiffType.NULLABILITY_CHANGED,
                        name,
                        old_value=old_field.nullable,
                        new_value=new_field.nullable,
                    ),
                )
            if old_field.default != new_field.default:
                entries.append(
                    DiffEntry(
                        DiffType.DEFAULT_CHANGED,
                        name,
                        old_value=old_field.default,
                        new_value=new_field.default,
                    ),
                )

        # Detect added fields
        for name, new_field in new_map.items():
            if name not in old_map:
                entries.append(
                    DiffEntry(DiffType.FIELD_ADDED, name, new_value=new_field),
                )

        return cls(old_schema=old, new_schema=new, entries=tuple(entries))


# ---------------------------------------------------------------------------
# Compatibility checking
# ---------------------------------------------------------------------------


class CompatibilityLevel(Enum):
    """How strict the compatibility check should be."""

    NONE = "none"  # Any change allowed
    BACKWARD = "backward"  # New schema can read old data
    FORWARD = "forward"  # Old schema can read new data
    FULL = "full"  # Both backward and forward


@dataclass(frozen=True, slots=True)
class CompatibilityResult:
    """Outcome of a compatibility check."""

    compatible: bool
    level: CompatibilityLevel
    violations: tuple[str, ...] = ()


class CompatibilityChecker:
    """Validates schema changes against a compatibility policy."""

    def check(
        self,
        old: TableSchema,
        new: TableSchema,
        level: CompatibilityLevel,
    ) -> CompatibilityResult:
        if level == CompatibilityLevel.NONE:
            return CompatibilityResult(compatible=True, level=level)

        diff = SchemaDiff.compute(old, new)
        violations: list[str] = []

        if level in (CompatibilityLevel.BACKWARD, CompatibilityLevel.FULL):
            violations.extend(self._check_backward(diff))

        if level in (CompatibilityLevel.FORWARD, CompatibilityLevel.FULL):
            violations.extend(self._check_forward(diff))

        return CompatibilityResult(
            compatible=len(violations) == 0,
            level=level,
            violations=tuple(violations),
        )

    def _check_backward(self, diff: SchemaDiff) -> list[str]:
        """Backward: new schema must be able to read data written by old schema."""
        violations: list[str] = []
        for entry in diff.entries:
            violation = self._check_backward_entry(entry, diff)
            if violation:
                violations.append(violation)
        return violations

    @staticmethod
    def _check_backward_entry(
        entry: DiffEntry,
        diff: SchemaDiff,
    ) -> str | None:
        """Return a violation message if *entry* breaks backward compat."""
        if entry.diff_type == DiffType.FIELD_REMOVED:
            old_field = diff.old_schema.get_field(entry.field_name)
            if old_field and not old_field.nullable:
                return (
                    f"Removing non-nullable field '{entry.field_name}' "
                    f"breaks backward compatibility"
                )

        elif entry.diff_type == DiffType.TYPE_CHANGED:
            old_field = diff.old_schema.get_field(entry.field_name)
            if old_field and not old_field.is_compatible_with(
                diff.new_schema.get_field(entry.field_name),  # type: ignore[arg-type]
            ):
                return (
                    f"Type change '{entry.old_value}' → '{entry.new_value}' "
                    f"on field '{entry.field_name}' is not a safe widening"
                )

        elif entry.diff_type == DiffType.NULLABILITY_CHANGED:
            if entry.old_value is True and entry.new_value is False:
                return (
                    f"Making field '{entry.field_name}' non-nullable "
                    f"breaks backward compatibility"
                )

        elif entry.diff_type == DiffType.FIELD_ADDED:
            new_field: FieldSchema = entry.new_value
            if not new_field.nullable and new_field.default is None:
                return (
                    f"Adding non-nullable field "
                    f"'{entry.field_name}' without default "
                    f"breaks backward compatibility"
                )

        return None

    def _check_forward(self, diff: SchemaDiff) -> list[str]:
        """Forward: old schema must be able to read data written by new schema."""
        violations: list[str] = []

        for entry in diff.entries:
            if entry.diff_type == DiffType.FIELD_ADDED:
                new_field: FieldSchema = entry.new_value
                if not new_field.nullable:
                    violations.append(
                        f"Adding non-nullable field '{entry.field_name}' "
                        f"breaks forward compatibility "
                        f"(old readers won't know about it)",
                    )

            elif entry.diff_type == DiffType.TYPE_CHANGED:
                # For forward compat, we need the *reverse* widening to be valid
                new_field_schema = diff.new_schema.get_field(entry.field_name)
                old_field_schema = diff.old_schema.get_field(entry.field_name)
                if (
                    new_field_schema
                    and old_field_schema
                    and not new_field_schema.is_compatible_with(old_field_schema)
                ):
                    violations.append(
                        f"Type change '{entry.old_value}' → '{entry.new_value}' "
                        f"on field '{entry.field_name}' breaks forward compatibility",
                    )

        return violations


# ---------------------------------------------------------------------------
# Schema merging
# ---------------------------------------------------------------------------


class ConflictStrategy(Enum):
    """How to resolve type conflicts when merging schemas."""

    WIDEN = "widen"  # Use the wider type
    PREFER_LEFT = "left"  # Keep the left schema's type
    PREFER_RIGHT = "right"  # Keep the right schema's type
    FAIL = "fail"  # Raise on conflict


class SchemaMerger:
    """Merge two schemas into a unified superset.

    Parameters
    ----------
    conflict_strategy : ConflictStrategy
        How to handle type mismatches on shared fields.

    """

    def __init__(
        self,
        conflict_strategy: ConflictStrategy = ConflictStrategy.WIDEN,
    ) -> None:
        self._strategy = conflict_strategy

    def merge(self, left: TableSchema, right: TableSchema) -> TableSchema:
        """Return a new schema that is the superset of *left* and *right*."""
        left_map = left.field_map()
        right_map = right.field_map()
        all_names = list(dict.fromkeys([*left.field_names, *right.field_names]))

        merged_fields: list[FieldSchema] = []
        for name in all_names:
            l_field = left_map.get(name)
            r_field = right_map.get(name)

            if l_field and not r_field:
                # Only in left — make nullable in merged (right doesn't have it)
                merged_fields.append(
                    FieldSchema(
                        name=name,
                        field_type=l_field.field_type,
                        nullable=True,
                        default=l_field.default,
                        metadata=l_field.metadata,
                    ),
                )
            elif r_field and not l_field:
                merged_fields.append(
                    FieldSchema(
                        name=name,
                        field_type=r_field.field_type,
                        nullable=True,
                        default=r_field.default,
                        metadata=r_field.metadata,
                    ),
                )
            elif l_field and r_field:
                merged_fields.append(self._resolve(l_field, r_field))

        return TableSchema(
            name=left.name,
            fields=tuple(merged_fields),
            version=max(left.version, right.version) + 1,
        )

    def _resolve(self, left: FieldSchema, right: FieldSchema) -> FieldSchema:
        if left.field_type == right.field_type:
            return FieldSchema(
                name=left.name,
                field_type=left.field_type,
                nullable=left.nullable or right.nullable,
                default=left.default if left.default is not None else right.default,
                metadata={**left.metadata, **right.metadata},
            )

        if self._strategy == ConflictStrategy.FAIL:
            msg = (
                f"Type conflict on field '{left.name}': "
                f"{left.field_type.value} vs {right.field_type.value}"
            )
            raise ValueError(msg)

        if self._strategy == ConflictStrategy.PREFER_LEFT:
            chosen_type = left.field_type
        elif self._strategy == ConflictStrategy.PREFER_RIGHT:
            chosen_type = right.field_type
        elif right.field_type in _WIDENING_RULES.get(left.field_type, set()):
            # WIDEN: left can widen to right (e.g. INT → FLOAT)
            chosen_type = right.field_type
        elif left.field_type in _WIDENING_RULES.get(right.field_type, set()):
            # WIDEN: right can widen to left (e.g. right=INT, left=FLOAT)
            chosen_type = left.field_type
        else:
            # No widening path — fall back to STRING
            chosen_type = FieldType.STRING

        return FieldSchema(
            name=left.name,
            field_type=chosen_type,
            nullable=True,
            metadata={**left.metadata, **right.metadata},
        )
