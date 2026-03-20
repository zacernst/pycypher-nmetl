"""TDD tests for the input_validator module.

Sprint 6, Phase 4.1: Input Validation — validates multi-query inputs
before processing through the composition pipeline.

RED phase: interface contracts, format validation, parsing validation,
uniqueness checks, and error diagnostics.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Interface contract tests
# ---------------------------------------------------------------------------


class TestInputValidatorInterfaceContract:
    """InputValidator must expose a well-defined public API."""

    def test_validator_has_validate_method(self) -> None:
        """InputValidator exposes a validate() method."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        assert callable(validator.validate)

    def test_validate_returns_validation_result(self) -> None:
        """validate() returns an InputValidationResult."""
        from pycypher.input_validator import (
            InputValidationResult,
            InputValidator,
        )

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert isinstance(result, InputValidationResult)

    def test_result_has_is_valid(self) -> None:
        """InputValidationResult has an is_valid boolean property."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert isinstance(result.is_valid, bool)

    def test_result_has_errors_list(self) -> None:
        """InputValidationResult has an errors list."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert isinstance(result.errors, list)


# ---------------------------------------------------------------------------
# Valid input acceptance tests
# ---------------------------------------------------------------------------


class TestValidInputAcceptance:
    """Validator must accept well-formed inputs."""

    def test_accept_single_query(self) -> None:
        """Single valid query passes validation."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert result.is_valid

    def test_accept_multiple_queries(self) -> None:
        """Multiple valid queries pass validation."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert result.is_valid

    def test_accept_empty_list(self) -> None:
        """Empty query list is valid (no-op)."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate([])
        assert result.is_valid


# ---------------------------------------------------------------------------
# Query ID uniqueness tests
# ---------------------------------------------------------------------------


class TestQueryIdUniqueness:
    """Validator must detect duplicate query IDs."""

    def test_reject_duplicate_query_ids(self) -> None:
        """Duplicate query IDs are rejected."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert not result.is_valid
        assert any("duplicate" in str(e).lower() for e in result.errors)


# ---------------------------------------------------------------------------
# Empty/whitespace query detection tests
# ---------------------------------------------------------------------------


class TestEmptyQueryDetection:
    """Validator must detect empty or whitespace-only queries."""

    def test_reject_empty_cypher_string(self) -> None:
        """Empty Cypher string is rejected."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate([("q1", "")])
        assert not result.is_valid

    def test_reject_whitespace_only_cypher(self) -> None:
        """Whitespace-only Cypher string is rejected."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate([("q1", "   \n\t  ")])
        assert not result.is_valid

    def test_reject_empty_query_id(self) -> None:
        """Empty query ID is rejected."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert not result.is_valid


# ---------------------------------------------------------------------------
# Parse validation tests
# ---------------------------------------------------------------------------


class TestParseValidation:
    """Validator must check that individual queries are parseable."""

    def test_reject_unparseable_query(self) -> None:
        """Unparseable Cypher string is rejected."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "THIS IS NOT VALID CYPHER AT ALL"),
            ]
        )
        assert not result.is_valid
        assert any("parse" in str(e).lower() for e in result.errors)


# ---------------------------------------------------------------------------
# Error diagnostics tests
# ---------------------------------------------------------------------------


class TestInputErrorDiagnostics:
    """Validation errors must provide actionable diagnostics."""

    def test_error_identifies_query_id(self) -> None:
        """Validation error identifies the problematic query ID."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate([("bad_query", "")])
        assert not result.is_valid
        assert any("bad_query" in str(e) for e in result.errors)

    def test_multiple_errors_collected(self) -> None:
        """Validator collects all errors, not just the first."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", ""),
                ("q1", "   "),
            ]
        )
        assert not result.is_valid
        # Should report both empty content AND duplicate ID
        assert len(result.errors) >= 2

    def test_str_representation(self) -> None:
        """InputValidationResult has a useful string representation."""
        from pycypher.input_validator import InputValidator

        validator = InputValidator()
        result = validator.validate(
            [
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ]
        )
        assert isinstance(str(result), str)
        assert len(str(result)) > 0
