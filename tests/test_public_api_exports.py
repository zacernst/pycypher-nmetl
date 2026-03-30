"""Tests that the public pycypher package exports contain expected symbols.

These tests act as a guard against accidentally removing items from the
public API surface.  They import from the top-level ``pycypher`` package
(not from submodules) to verify that users can access these names directly.

TDD: tests written before the implementation (adding SemanticValidator and
validate_query to __init__.py).
"""

from __future__ import annotations

import pycypher


class TestAllExportsImportable:
    """Every name in __all__ must be importable from the top-level package."""

    def test_all_exports_are_accessible(self) -> None:
        """Verify every __all__ entry resolves via getattr."""
        import warnings

        for name in pycypher.__all__:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                obj = getattr(pycypher, name)
                assert obj is not None, f"{name} resolved to None"

    def test_all_count(self) -> None:
        """Guard against silent removal of public symbols."""
        assert len(pycypher.__all__) >= 27


class TestCoreExports:
    """Core symbols must be importable from the top-level pycypher package."""

    def test_star_importable(self) -> None:
        from pycypher import Star

        assert Star is not None

    def test_context_importable(self) -> None:
        from pycypher import Context

        assert Context is not None

    def test_entity_table_importable(self) -> None:
        from pycypher import EntityTable

        assert EntityTable is not None

    def test_context_builder_importable(self) -> None:
        from pycypher import ContextBuilder

        assert ContextBuilder is not None

    def test_relationship_table_importable(self) -> None:
        from pycypher import RelationshipTable

        assert RelationshipTable is not None

    def test_entity_mapping_importable(self) -> None:
        from pycypher import EntityMapping

        assert EntityMapping is not None

    def test_relationship_mapping_importable(self) -> None:
        from pycypher import RelationshipMapping

        assert RelationshipMapping is not None

    def test_id_column_importable(self) -> None:
        from pycypher import ID_COLUMN

        assert isinstance(ID_COLUMN, str)

    def test_result_cache_importable(self) -> None:
        from pycypher import ResultCache

        assert ResultCache is not None

    def test_get_cache_stats_importable(self) -> None:
        from pycypher import get_cache_stats

        assert callable(get_cache_stats)


class TestExceptionExports:
    """All public exception classes must be importable from pycypher."""

    def test_ast_conversion_error(self) -> None:
        from pycypher import ASTConversionError

        assert issubclass(ASTConversionError, Exception)

    def test_grammar_transformer_sync_error(self) -> None:
        from pycypher import GrammarTransformerSyncError

        assert issubclass(GrammarTransformerSyncError, Exception)

    def test_incompatible_operator_error(self) -> None:
        from pycypher import IncompatibleOperatorError

        assert issubclass(IncompatibleOperatorError, TypeError)

    def test_invalid_cast_error(self) -> None:
        from pycypher import InvalidCastError

        assert issubclass(InvalidCastError, ValueError)

    def test_temporal_arithmetic_error(self) -> None:
        from pycypher import TemporalArithmeticError

        assert issubclass(TemporalArithmeticError, Exception)

    def test_wrong_cypher_type_error(self) -> None:
        from pycypher import WrongCypherTypeError

        assert issubclass(WrongCypherTypeError, TypeError)

    def test_function_argument_error(self) -> None:
        from pycypher import FunctionArgumentError

        assert issubclass(FunctionArgumentError, Exception)

    def test_graph_type_not_found_error(self) -> None:
        from pycypher import GraphTypeNotFoundError

        assert issubclass(GraphTypeNotFoundError, ValueError)

    def test_missing_parameter_error(self) -> None:
        from pycypher import MissingParameterError

        assert issubclass(MissingParameterError, Exception)

    def test_query_memory_budget_error(self) -> None:
        from pycypher import QueryMemoryBudgetError

        assert issubclass(QueryMemoryBudgetError, Exception)

    def test_query_timeout_error(self) -> None:
        from pycypher import QueryTimeoutError

        assert issubclass(QueryTimeoutError, TimeoutError)

    def test_unsupported_function_error(self) -> None:
        from pycypher import UnsupportedFunctionError

        assert issubclass(UnsupportedFunctionError, Exception)

    def test_variable_not_found_error(self) -> None:
        from pycypher import VariableNotFoundError

        assert issubclass(VariableNotFoundError, Exception)

    def test_variable_type_mismatch_error(self) -> None:
        from pycypher import VariableTypeMismatchError

        assert issubclass(VariableTypeMismatchError, Exception)

    def test_exception_hierarchy_grammar_sync_is_ast_conversion(
        self,
    ) -> None:
        """GrammarTransformerSyncError is a subclass of ASTConversionError."""
        from pycypher import ASTConversionError, GrammarTransformerSyncError

        assert issubclass(GrammarTransformerSyncError, ASTConversionError)


class TestDeprecationHandling:
    """Deprecated aliases must work but emit warnings."""

    def test_arrow_ingestion_emits_deprecation_warning(self) -> None:
        """ArrowIngestion triggers DeprecationWarning."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = pycypher.ArrowIngestion
            deprecations = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecations) >= 1
            assert "DuckDBReader" in str(deprecations[0].message)

    def test_typo_suggestion(self) -> None:
        """Accessing a close-but-wrong name raises AttributeError with hint."""
        import pytest

        with pytest.raises(AttributeError, match="Did you mean"):
            _ = pycypher.Starr


class TestValidatorExports:
    """SemanticValidator and validate_query must be importable from pycypher."""

    def test_semantic_validator_importable(self) -> None:
        """SemanticValidator class is accessible from pycypher top-level."""
        from pycypher import SemanticValidator

        assert SemanticValidator is not None

    def test_validate_query_importable(self) -> None:
        """validate_query() helper is accessible from pycypher top-level."""
        from pycypher import validate_query

        assert callable(validate_query)

    def test_semantic_validator_in_all(self) -> None:
        """SemanticValidator appears in pycypher.__all__."""
        assert "SemanticValidator" in pycypher.__all__

    def test_validate_query_in_all(self) -> None:
        """validate_query appears in pycypher.__all__."""
        assert "validate_query" in pycypher.__all__

    def test_semantic_validator_catches_undefined_var(self) -> None:
        """SemanticValidator detects undefined variable use via validate_query."""
        from pycypher import validate_query

        errors = validate_query("MATCH (n:Person) RETURN m")
        error_messages = [e.message for e in errors]
        assert any("m" in msg for msg in error_messages)

    def test_validate_query_returns_empty_for_valid_query(self) -> None:
        """validate_query returns no errors for a well-formed query."""
        from pycypher import validate_query

        errors = validate_query("MATCH (n:Person) RETURN n.name")
        # Filter to ERROR-severity only (warnings are acceptable)
        from pycypher.semantic_validator import ErrorSeverity

        hard_errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert not hard_errors
