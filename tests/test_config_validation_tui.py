"""Tests for the configuration validation module (TUI support).

Covers:
- Syntactic validation (YAML structure, required fields, type correctness)
- Semantic validation (cross-references, data consistency)
- Incremental field-level validation for TUI real-time feedback
- Structured ValidationResult objects with error categories and fix suggestions
- Legacy config detection and migration guidance
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)
from pycypher.ingestion.validation import (
    ErrorCategory,
    ErrorSeverity,
    ValidationError as ConfigValidationError,
    ValidationResult,
    validate_config,
    validate_config_dict,
    validate_field,
    detect_legacy_format,
    suggest_migration,
)

FIXTURES = Path(__file__).parent / "fixtures" / "configs"


# ===========================================================================
# ValidationResult structure
# ===========================================================================


class TestValidationResult:
    """Tests for the ValidationResult data structure."""

    def test_empty_result_is_valid(self):
        result = ValidationResult()
        assert result.is_valid
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_result_with_error_is_invalid(self):
        result = ValidationResult()
        result.add_error(
            field="sources.entities.0.uri",
            message="URI must not be empty",
            category=ErrorCategory.SYNTACTIC,
        )
        assert not result.is_valid
        assert len(result.errors) == 1

    def test_result_with_warning_is_still_valid(self):
        result = ValidationResult()
        result.add_warning(
            field="version",
            message="Config version 1.1 is not explicitly supported",
        )
        assert result.is_valid
        assert len(result.warnings) == 1

    def test_error_has_structured_fields(self):
        result = ValidationResult()
        result.add_error(
            field="sources.entities.0.uri",
            message="Source URI '' has an unrecognised file extension.",
            category=ErrorCategory.SYNTACTIC,
            suggestion="Provide a file path with extension: .csv, .json, .parquet",
        )
        err = result.errors[0]
        assert err.field == "sources.entities.0.uri"
        assert err.message == "Source URI '' has an unrecognised file extension."
        assert err.category == ErrorCategory.SYNTACTIC
        assert err.severity == ErrorSeverity.ERROR
        assert "extension" in err.suggestion

    def test_multiple_errors_collected(self):
        result = ValidationResult()
        result.add_error(
            field="sources.entities.0.uri",
            message="URI must not be empty",
            category=ErrorCategory.SYNTACTIC,
        )
        result.add_error(
            field="queries.0.id",
            message="Query id is required",
            category=ErrorCategory.SYNTACTIC,
        )
        assert len(result.errors) == 2
        assert not result.is_valid

    def test_errors_by_category(self):
        result = ValidationResult()
        result.add_error(
            field="sources.entities.0.uri",
            message="URI empty",
            category=ErrorCategory.SYNTACTIC,
        )
        result.add_error(
            field="output.0.query_id",
            message="References non-existent query 'foo'",
            category=ErrorCategory.SEMANTIC,
        )
        syntactic = result.errors_by_category(ErrorCategory.SYNTACTIC)
        semantic = result.errors_by_category(ErrorCategory.SEMANTIC)
        assert len(syntactic) == 1
        assert len(semantic) == 1


# ===========================================================================
# Syntactic validation (YAML structure)
# ===========================================================================


class TestSyntacticValidation:
    """Tests for structural/syntactic validation of config dicts."""

    def test_valid_minimal_config(self):
        config_dict = {
            "version": "1.0",
            "sources": {
                "entities": [
                    {
                        "id": "persons",
                        "uri": "file:///data/persons.parquet",
                        "entity_type": "Person",
                    }
                ]
            },
        }
        result = validate_config_dict(config_dict)
        assert result.is_valid

    def test_missing_version_uses_default(self):
        config_dict = {
            "sources": {
                "entities": [
                    {
                        "id": "persons",
                        "uri": "file:///data/persons.parquet",
                        "entity_type": "Person",
                    }
                ]
            },
        }
        result = validate_config_dict(config_dict)
        assert result.is_valid

    def test_invalid_version_format(self):
        config_dict = {"version": "abc"}
        result = validate_config_dict(config_dict)
        assert not result.is_valid
        assert any("version" in e.field for e in result.errors)

    def test_empty_source_uri(self):
        config_dict = {
            "version": "1.0",
            "sources": {
                "entities": [
                    {"id": "x", "uri": "", "entity_type": "X"}
                ]
            },
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid
        assert any("uri" in e.message.lower() for e in result.errors)

    def test_duplicate_source_ids(self):
        config_dict = {
            "version": "1.0",
            "sources": {
                "entities": [
                    {"id": "dup", "uri": "file:///a.csv", "entity_type": "A"},
                    {"id": "dup", "uri": "file:///b.csv", "entity_type": "B"},
                ]
            },
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid
        assert any("duplicate" in e.message.lower() for e in result.errors)

    def test_query_both_source_and_inline(self):
        config_dict = {
            "version": "1.0",
            "queries": [
                {
                    "id": "q1",
                    "source": "queries/q1.cypher",
                    "inline": "MATCH (n) RETURN n",
                }
            ],
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid

    def test_query_neither_source_nor_inline(self):
        config_dict = {
            "version": "1.0",
            "queries": [{"id": "q1"}],
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid

    def test_function_both_module_and_callable(self):
        config_dict = {
            "version": "1.0",
            "functions": [
                {
                    "module": "mymod",
                    "names": ["fn"],
                    "callable": "mymod.fn",
                }
            ],
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid

    def test_relationship_missing_source_col(self):
        config_dict = {
            "version": "1.0",
            "sources": {
                "relationships": [
                    {
                        "id": "rel",
                        "uri": "file:///r.csv",
                        "relationship_type": "REL",
                        "target_col": "tgt",
                    }
                ]
            },
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid


# ===========================================================================
# Semantic validation (cross-references, consistency)
# ===========================================================================


class TestSemanticValidation:
    """Tests for semantic/cross-reference validation."""

    def test_output_references_nonexistent_query(self):
        config_dict = {
            "version": "1.0",
            "queries": [
                {"id": "q1", "inline": "MATCH (n) RETURN n"}
            ],
            "output": [
                {"query_id": "nonexistent", "uri": "file:///out.csv"}
            ],
        }
        result = validate_config_dict(config_dict)
        assert not result.is_valid
        assert any(
            e.category == ErrorCategory.SEMANTIC
            for e in result.errors
        )
        assert any("nonexistent" in e.message for e in result.errors)

    def test_output_references_valid_query(self):
        config_dict = {
            "version": "1.0",
            "queries": [
                {"id": "q1", "inline": "MATCH (n) RETURN n"}
            ],
            "output": [
                {"query_id": "q1", "uri": "file:///out.csv"}
            ],
        }
        result = validate_config_dict(config_dict)
        assert result.is_valid

    def test_warns_on_queries_without_output(self):
        config_dict = {
            "version": "1.0",
            "queries": [
                {"id": "q1", "inline": "MATCH (n) RETURN n"},
                {"id": "q2", "inline": "MATCH (m) RETURN m"},
            ],
            "output": [
                {"query_id": "q1", "uri": "file:///out.csv"}
            ],
        }
        result = validate_config_dict(config_dict)
        assert result.is_valid  # warnings don't invalidate
        assert len(result.warnings) >= 1
        assert any("q2" in w.message for w in result.warnings)

    def test_duplicate_output_query_ids_warns(self):
        config_dict = {
            "version": "1.0",
            "queries": [
                {"id": "q1", "inline": "MATCH (n) RETURN n"}
            ],
            "output": [
                {"query_id": "q1", "uri": "file:///a.csv"},
                {"query_id": "q1", "uri": "file:///b.csv"},
            ],
        }
        result = validate_config_dict(config_dict)
        # Multiple outputs for same query is valid but worth a warning
        assert len(result.warnings) >= 1


# ===========================================================================
# Incremental field-level validation
# ===========================================================================


class TestIncrementalValidation:
    """Tests for field-level validation suitable for TUI real-time feedback."""

    def test_validate_entity_uri_valid(self):
        result = validate_field("sources.entities.uri", "file:///data/test.csv")
        assert result.is_valid

    def test_validate_entity_uri_empty(self):
        result = validate_field("sources.entities.uri", "")
        assert not result.is_valid
        assert any("empty" in e.message.lower() for e in result.errors)

    def test_validate_entity_uri_bad_extension(self):
        result = validate_field("sources.entities.uri", "file:///data/test.xlsx")
        assert not result.is_valid
        assert any("extension" in e.message.lower() for e in result.errors)

    def test_validate_version_valid(self):
        result = validate_field("version", "1.0")
        assert result.is_valid

    def test_validate_version_invalid(self):
        result = validate_field("version", "xyz")
        assert not result.is_valid

    def test_validate_output_uri_valid(self):
        result = validate_field("output.uri", "file:///out/data.parquet")
        assert result.is_valid

    def test_validate_output_uri_bad(self):
        result = validate_field("output.uri", "file:///out/data.xlsx")
        assert not result.is_valid

    def test_validate_unknown_field_passes(self):
        """Unknown field paths should pass (no validation rule)."""
        result = validate_field("some.unknown.field", "anything")
        assert result.is_valid


# ===========================================================================
# Validate from PipelineConfig object
# ===========================================================================


class TestValidateFromConfig:
    """Tests for validating an already-parsed PipelineConfig."""

    def test_valid_config(self):
        cfg = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="p",
                        uri="file:///data/p.csv",
                        entity_type="Person",
                    )
                ]
            ),
            queries=[
                QueryConfig(id="q1", inline="MATCH (n) RETURN n")
            ],
            output=[
                OutputConfig(query_id="q1", uri="file:///out.csv")
            ],
        )
        result = validate_config(cfg)
        assert result.is_valid

    def test_semantic_error_output_ref(self):
        cfg = PipelineConfig(
            version="1.0",
            queries=[
                QueryConfig(id="q1", inline="MATCH (n) RETURN n")
            ],
            output=[
                OutputConfig(query_id="missing", uri="file:///out.csv")
            ],
        )
        result = validate_config(cfg)
        assert not result.is_valid
        assert any(e.category == ErrorCategory.SEMANTIC for e in result.errors)


# ===========================================================================
# Legacy format detection and migration
# ===========================================================================


class TestLegacyDetection:
    """Tests for detecting legacy configuration format and suggesting migration."""

    def test_detect_legacy_format_with_data_sources(self):
        legacy = {
            "fact_collection": "SimpleFactCollection",
            "data_sources": [{"name": "table", "uri": "file:///x.csv"}],
        }
        assert detect_legacy_format(legacy) is True

    def test_detect_legacy_format_with_mappings(self):
        legacy = {
            "data_sources": [
                {
                    "name": "t",
                    "uri": "file:///x.csv",
                    "mappings": [{"attribute_key": "k"}],
                }
            ]
        }
        assert detect_legacy_format(legacy) is True

    def test_modern_format_not_detected_as_legacy(self):
        modern = {
            "version": "1.0",
            "sources": {"entities": []},
        }
        assert detect_legacy_format(modern) is False

    def test_suggest_migration_returns_guidance(self):
        legacy = {
            "fact_collection": "SimpleFactCollection",
            "data_sources": [{"name": "t", "uri": "file:///x.csv"}],
        }
        guidance = suggest_migration(legacy)
        assert isinstance(guidance, str)
        assert "version" in guidance.lower()
        assert "sources" in guidance.lower()

    def test_suggest_migration_for_modern_returns_none(self):
        modern = {
            "version": "1.0",
            "sources": {"entities": []},
        }
        guidance = suggest_migration(modern)
        assert guidance is None
