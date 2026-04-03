"""Structured configuration validation for TUI and programmatic use.

Provides validation of pipeline configurations with structured error
objects suitable for real-time TUI display, including:

- **Syntactic validation**: YAML structure, required fields, type correctness
- **Semantic validation**: Cross-references between config sections
- **Incremental validation**: Per-field validation for real-time feedback
- **Legacy detection**: Identifies old config formats and suggests migration

The primary entry points are:

- :func:`validate_config_dict` — validate a raw dict (from YAML parse)
- :func:`validate_config` — validate an already-parsed :class:`PipelineConfig`
- :func:`validate_field` — validate a single field value incrementally

All return :class:`ValidationResult` objects with structured errors.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse

from pycypher.ingestion.config import (
    CURRENT_CONFIG_VERSION,
    SUPPORTED_CONFIG_VERSIONS,
    PipelineConfig,
    _check_output_uri,
    _check_source_uri,
)
from pycypher.ingestion.data_sources import _SQL_SCHEMES, _SUPPORTED_EXTENSIONS

__all__ = [
    "ErrorCategory",
    "ErrorSeverity",
    "ValidationError",
    "ValidationResult",
    "detect_legacy_format",
    "suggest_migration",
    "validate_config",
    "validate_config_dict",
    "validate_field",
]

_VERSION_RE = re.compile(r"^\d+\.\d+$")
_EXT_LIST = ", ".join(sorted(_SUPPORTED_EXTENSIONS))


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


class ErrorCategory(StrEnum):
    """Category of a validation error."""

    SYNTACTIC = "syntactic"
    """Structural issue: missing field, wrong type, bad format."""

    SEMANTIC = "semantic"
    """Cross-reference issue: dangling references, inconsistencies."""


class ErrorSeverity(StrEnum):
    """Severity level for validation issues."""

    ERROR = "error"
    """Blocks pipeline execution."""

    WARNING = "warning"
    """Potential issue but config can still be used."""


# ---------------------------------------------------------------------------
# Structured error and result objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationError:
    """A single validation issue with structured metadata.

    Attributes:
        field: Dotted path to the problematic field (e.g. ``"sources.entities.0.uri"``).
        message: Human-readable description of the problem.
        category: Whether this is a syntactic or semantic issue.
        severity: Error (blocking) or warning (informational).
        suggestion: Optional fix suggestion for display in TUI.

    """

    field: str
    message: str
    category: ErrorCategory
    severity: ErrorSeverity = ErrorSeverity.ERROR
    suggestion: str | None = None


@dataclass
class ValidationResult:
    """Collects validation errors and warnings from a config check.

    Used by TUI widgets to display real-time validation feedback with
    structured error information.
    """

    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True when no blocking errors exist (warnings are OK)."""
        return len(self.errors) == 0

    def add_error(
        self,
        *,
        field: str,
        message: str,
        category: ErrorCategory,
        suggestion: str | None = None,
    ) -> None:
        """Append a blocking error."""
        self.errors.append(
            ValidationError(
                field=field,
                message=message,
                category=category,
                severity=ErrorSeverity.ERROR,
                suggestion=suggestion,
            )
        )

    def add_warning(
        self,
        *,
        field: str,
        message: str,
        suggestion: str | None = None,
    ) -> None:
        """Append a non-blocking warning."""
        self.warnings.append(
            ValidationError(
                field=field,
                message=message,
                category=ErrorCategory.SEMANTIC,
                severity=ErrorSeverity.WARNING,
                suggestion=suggestion,
            )
        )

    def errors_by_category(self, category: ErrorCategory) -> list[ValidationError]:
        """Return errors filtered to a specific category."""
        return [e for e in self.errors if e.category == category]


# ---------------------------------------------------------------------------
# Full config validation (from dict)
# ---------------------------------------------------------------------------


def validate_config_dict(config_dict: dict[str, Any]) -> ValidationResult:
    """Validate a raw configuration dict (e.g. from ``yaml.safe_load``).

    Performs both syntactic validation (via Pydantic model parsing) and
    semantic validation (cross-reference checks).  Collects all errors
    rather than failing on the first one.

    Args:
        config_dict: The raw configuration dictionary.

    Returns:
        A :class:`ValidationResult` with all discovered issues.

    """
    result = ValidationResult()

    # Phase 1: Syntactic validation via Pydantic
    cfg = _validate_syntactic(config_dict, result)

    # Phase 2: Semantic validation (only if syntactic passed)
    if cfg is not None:
        _validate_semantic(cfg, result)

    return result


def _validate_syntactic(
    config_dict: dict[str, Any],
    result: ValidationResult,
) -> PipelineConfig | None:
    """Try to parse the dict into a PipelineConfig, capturing errors."""
    try:
        from pydantic import ValidationError as PydanticValidationError

        return PipelineConfig.model_validate(config_dict)
    except PydanticValidationError as exc:
        for err in exc.errors():
            loc_parts = [str(p) for p in err.get("loc", ())]
            field_path = ".".join(loc_parts) if loc_parts else "(root)"
            msg = err.get("msg", "validation error")

            suggestion = _suggest_fix_for_pydantic_error(field_path, msg)
            result.add_error(
                field=field_path,
                message=msg,
                category=ErrorCategory.SYNTACTIC,
                suggestion=suggestion,
            )
        return None


def _validate_semantic(cfg: PipelineConfig, result: ValidationResult) -> None:
    """Check cross-references and consistency on a valid PipelineConfig."""
    query_ids = {q.id for q in cfg.queries}

    # Check output references
    output_query_ids: list[str] = []
    for i, out in enumerate(cfg.output):
        if out.query_id not in query_ids:
            result.add_error(
                field=f"output.{i}.query_id",
                message=(
                    f"Output references query '{out.query_id}' which is not "
                    f"defined. Available queries: {', '.join(sorted(query_ids)) or '(none)'}."
                ),
                category=ErrorCategory.SEMANTIC,
                suggestion=f"Change query_id to one of: {', '.join(sorted(query_ids))}",
            )
        output_query_ids.append(out.query_id)

    # Warn about queries without output sinks
    output_set = set(output_query_ids)
    for q in cfg.queries:
        if q.id not in output_set:
            result.add_warning(
                field=f"queries.{q.id}",
                message=(
                    f"Query '{q.id}' has no output sink configured. "
                    "Its results will not be written anywhere."
                ),
                suggestion=f"Add an output entry with query_id: '{q.id}'",
            )

    # Warn about duplicate output targets for the same query
    from collections import Counter

    counts = Counter(output_query_ids)
    for qid, count in counts.items():
        if count > 1:
            result.add_warning(
                field="output",
                message=(
                    f"Query '{qid}' has {count} output sinks. "
                    "This is valid but may indicate a configuration error."
                ),
            )


# ---------------------------------------------------------------------------
# Full config validation (from PipelineConfig)
# ---------------------------------------------------------------------------


def validate_config(cfg: PipelineConfig) -> ValidationResult:
    """Validate an already-parsed PipelineConfig for semantic issues.

    Syntactic validation was already performed by Pydantic during
    construction, so this only checks cross-references and consistency.

    Args:
        cfg: A validated PipelineConfig instance.

    Returns:
        A :class:`ValidationResult` with any semantic issues.

    """
    result = ValidationResult()
    _validate_semantic(cfg, result)
    return result


# ---------------------------------------------------------------------------
# Incremental field-level validation
# ---------------------------------------------------------------------------

# Maps field path patterns to validation functions.
# Each function takes (value) and returns (is_valid, error_message).
_FIELD_VALIDATORS: dict[str, Any] = {}


def _register_field_validator(pattern: str):
    """Decorator to register a field-level validator."""

    def decorator(fn):
        _FIELD_VALIDATORS[pattern] = fn
        return fn

    return decorator


@_register_field_validator("version")
def _validate_version(value: Any) -> tuple[bool, str]:
    if not isinstance(value, str):
        return False, "Version must be a string."
    if not _VERSION_RE.match(value):
        return False, f"Invalid version format: {value!r}. Expected '<major>.<minor>'."
    return True, ""


@_register_field_validator("sources.entities.uri")
def _validate_entity_uri(value: Any) -> tuple[bool, str]:
    if not isinstance(value, str):
        return False, "URI must be a string."
    try:
        _check_source_uri(value, query=None)
    except ValueError as exc:
        return False, str(exc.args[0] if exc.args else exc)
    return True, ""


@_register_field_validator("sources.relationships.uri")
def _validate_relationship_uri(value: Any) -> tuple[bool, str]:
    return _validate_entity_uri(value)


@_register_field_validator("output.uri")
def _validate_output_uri(value: Any) -> tuple[bool, str]:
    if not isinstance(value, str):
        return False, "URI must be a string."
    try:
        _check_output_uri(value)
    except ValueError as exc:
        return False, str(exc.args[0] if exc.args else exc)
    return True, ""


def validate_field(field_path: str, value: Any) -> ValidationResult:
    """Validate a single field value for real-time TUI feedback.

    Looks up a validator by matching the field path against registered
    patterns.  Unknown fields pass validation (no rule to check).

    Args:
        field_path: Dotted path like ``"sources.entities.uri"`` or ``"version"``.
        value: The field value to validate.

    Returns:
        A :class:`ValidationResult` with at most one error.

    """
    result = ValidationResult()

    # Try exact match first, then strip numeric indices
    validator = _FIELD_VALIDATORS.get(field_path)
    if validator is None:
        # Strip numeric path components: "sources.entities.0.uri" → "sources.entities.uri"
        normalized = ".".join(
            p for p in field_path.split(".") if not p.isdigit()
        )
        validator = _FIELD_VALIDATORS.get(normalized)

    if validator is not None:
        is_valid, message = validator(value)
        if not is_valid:
            result.add_error(
                field=field_path,
                message=message,
                category=ErrorCategory.SYNTACTIC,
                suggestion=_suggest_fix_for_field(field_path),
            )

    return result


# ---------------------------------------------------------------------------
# Fix suggestions
# ---------------------------------------------------------------------------


def _suggest_fix_for_pydantic_error(field: str, message: str) -> str | None:
    """Generate a fix suggestion from a Pydantic error."""
    lower_msg = message.lower()
    if "required" in lower_msg:
        return f"Add the required field '{field.split('.')[-1]}' to your configuration."
    if "uri" in lower_msg and "empty" in lower_msg:
        return f"Provide a valid file path or URI (supported extensions: {_EXT_LIST})."
    if "extension" in lower_msg:
        return f"Use a supported file extension: {_EXT_LIST}."
    if "version" in field and "format" in lower_msg:
        return f"Use version format '<major>.<minor>' (e.g. '{CURRENT_CONFIG_VERSION}')."
    if "duplicate" in lower_msg:
        return "Ensure all source IDs are unique across entities and relationships."
    return None


def _suggest_fix_for_field(field_path: str) -> str | None:
    """Generate a context-specific fix suggestion for a field."""
    if "uri" in field_path:
        return f"Provide a valid URI with a supported extension ({_EXT_LIST})."
    if "version" in field_path:
        return f"Use '{CURRENT_CONFIG_VERSION}' or another supported version."
    return None


# ---------------------------------------------------------------------------
# Legacy format detection and migration
# ---------------------------------------------------------------------------

# Keys that indicate the old format (pre-PipelineConfig)
_LEGACY_KEYS = frozenset({
    "fact_collection",
    "fact_collection_class",
    "fact_collection_kwargs",
    "data_sources",
    "run_monitor",
})


def detect_legacy_format(config_dict: dict[str, Any]) -> bool:
    """Check whether a config dict uses the legacy (pre-v1.0) format.

    Legacy configs are identified by the presence of keys like
    ``data_sources``, ``fact_collection``, or ``mappings`` entries.

    Args:
        config_dict: A raw dict parsed from YAML.

    Returns:
        ``True`` if the dict appears to use the legacy format.

    """
    if _LEGACY_KEYS & set(config_dict.keys()):
        return True

    # Check for mapping-style data_sources entries
    for ds in config_dict.get("data_sources", []):
        if isinstance(ds, dict) and "mappings" in ds:
            return True

    return False


def suggest_migration(config_dict: dict[str, Any]) -> str | None:
    """Generate migration guidance for a legacy config format.

    Args:
        config_dict: A raw dict parsed from YAML.

    Returns:
        A multi-line string with migration instructions, or ``None`` if
        the config is already in modern format.

    """
    if not detect_legacy_format(config_dict):
        return None

    lines = [
        "This configuration uses a legacy format that is no longer supported.",
        "Please migrate to the v1.0 schema:",
        "",
        "1. Add 'version: \"1.0\"' at the top of the file.",
        "2. Replace 'data_sources' with 'sources.entities' and 'sources.relationships'.",
        "3. Replace 'mappings' entries:",
        "   - Entity sources: set 'entity_type', 'id_col', and optionally 'schema_hints'.",
        "   - Relationship sources: set 'relationship_type', 'source_col', and 'target_col'.",
        "4. Remove 'fact_collection', 'fact_collection_class', 'run_monitor',",
        "   and 'logging_level' (these are now controlled by runtime config).",
        "5. Add 'queries' section with Cypher queries (inline or file references).",
        "6. Add 'output' section to define output sinks.",
        "",
        f"See the canonical schema in 'pycypher.ingestion.config.PipelineConfig'.",
    ]
    return "\n".join(lines)
