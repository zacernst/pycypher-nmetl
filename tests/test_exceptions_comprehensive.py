"""Comprehensive tests for all PyCypher exception classes.

Covers branch paths, edge cases, and message formatting that
existing tests miss — targeting 70%+ coverage for exceptions.py.
"""

from __future__ import annotations

import pytest
from pycypher.exceptions import (
    ASTConversionError,
    FunctionArgumentError,
    GrammarTransformerSyncError,
    GraphTypeNotFoundError,
    IncompatibleOperatorError,
    InvalidCastError,
    MissingParameterError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    TemporalArithmeticError,
    UnsupportedFunctionError,
    VariableNotFoundError,
    VariableTypeMismatchError,
    WrongCypherTypeError,
)

# ---------------------------------------------------------------------------
# GraphTypeNotFoundError
# ---------------------------------------------------------------------------


class TestGraphTypeNotFoundError:
    def test_default_message(self) -> None:
        exc = GraphTypeNotFoundError("Person")
        assert "Person" in str(exc)
        assert "not registered" in str(exc)

    def test_custom_message(self) -> None:
        exc = GraphTypeNotFoundError("Person", "custom detail")
        assert str(exc).startswith("custom detail")

    def test_type_name_attribute(self) -> None:
        exc = GraphTypeNotFoundError("KNOWS")
        assert exc.type_name == "KNOWS"

    def test_is_value_error(self) -> None:
        assert issubclass(GraphTypeNotFoundError, ValueError)

    def test_repr(self) -> None:
        exc = GraphTypeNotFoundError("Ghost")
        assert repr(exc) == "GraphTypeNotFoundError(type_name='Ghost')"


# ---------------------------------------------------------------------------
# WrongCypherTypeError
# ---------------------------------------------------------------------------


class TestWrongCypherTypeErrorComprehensive:
    def test_repr(self) -> None:
        exc = WrongCypherTypeError("bad type")
        assert repr(exc) == "WrongCypherTypeError(message='bad type')"

    def test_inherits_type_error(self) -> None:
        with pytest.raises(TypeError):
            raise WrongCypherTypeError("x")


# ---------------------------------------------------------------------------
# InvalidCastError
# ---------------------------------------------------------------------------


class TestInvalidCastErrorComprehensive:
    def test_repr(self) -> None:
        exc = InvalidCastError("cast failed")
        assert repr(exc) == "InvalidCastError(message='cast failed')"

    def test_inherits_value_error(self) -> None:
        with pytest.raises(ValueError, match="cast failed"):
            raise InvalidCastError("cast failed")


# ---------------------------------------------------------------------------
# ASTConversionError
# ---------------------------------------------------------------------------


class TestASTConversionErrorComprehensive:
    def test_no_fragment(self) -> None:
        exc = ASTConversionError("parse failed")
        assert str(exc).startswith("parse failed")
        assert exc.query_fragment == ""
        assert exc.node_type == ""

    def test_with_short_fragment(self) -> None:
        exc = ASTConversionError("failed", query_fragment="MATCH (n)")
        assert "MATCH (n)" in str(exc)

    def test_long_fragment_truncated(self) -> None:
        long_query = "A" * 100
        exc = ASTConversionError("failed", query_fragment=long_query)
        msg = str(exc)
        assert "..." in msg
        # Truncated to 50 chars + "..."
        assert len(long_query) > 50

    def test_repr_no_context(self) -> None:
        exc = ASTConversionError("parse failed")
        r = repr(exc)
        assert r.startswith("ASTConversionError(")
        assert "parse failed" in r

    def test_repr_with_node_type_only(self) -> None:
        exc = ASTConversionError("failed", node_type="FooNode")
        r = repr(exc)
        assert "node_type='FooNode'" in r


# ---------------------------------------------------------------------------
# GrammarTransformerSyncError
# ---------------------------------------------------------------------------


class TestGrammarTransformerSyncError:
    def test_is_ast_conversion_error(self) -> None:
        assert issubclass(GrammarTransformerSyncError, ASTConversionError)
        assert issubclass(GrammarTransformerSyncError, ValueError)

    def test_with_missing_node_type(self) -> None:
        exc = GrammarTransformerSyncError(
            "out of sync",
            missing_node_type="FooBar",
        )
        assert exc.missing_node_type == "FooBar"
        assert "FooBar" in str(exc)
        assert "out of sync" in str(exc)

    def test_without_missing_node_type(self) -> None:
        exc = GrammarTransformerSyncError("generic sync issue")
        assert exc.missing_node_type == ""

    def test_with_query_fragment(self) -> None:
        exc = GrammarTransformerSyncError(
            "sync issue",
            missing_node_type="Bar",
            query_fragment="MATCH (x)",
        )
        assert exc.query_fragment == "MATCH (x)"

    def test_repr_no_context(self) -> None:
        exc = GrammarTransformerSyncError("generic")
        r = repr(exc)
        assert r.startswith("GrammarTransformerSyncError(")

    def test_repr_with_query_fragment(self) -> None:
        exc = GrammarTransformerSyncError(
            "err",
            missing_node_type="X",
            query_fragment="Q",
        )
        r = repr(exc)
        assert "missing_node_type='X'" in r
        assert "query_fragment='Q'" in r


# ---------------------------------------------------------------------------
# VariableNotFoundError
# ---------------------------------------------------------------------------


class TestVariableNotFoundErrorComprehensive:
    def test_empty_available(self) -> None:
        exc = VariableNotFoundError("x", [])
        assert "No variables" in str(exc) and "scope" in str(exc)

    def test_with_available(self) -> None:
        exc = VariableNotFoundError("x", ["a", "b"])
        assert "a, b" in str(exc)

    def test_with_hint(self) -> None:
        exc = VariableNotFoundError(
            "persn",
            ["person"],
            hint="  Did you mean 'person'?",
        )
        assert exc.hint == "  Did you mean 'person'?"
        assert "Did you mean" in str(exc)

    def test_repr(self) -> None:
        exc = VariableNotFoundError("x", ["a"])
        r = repr(exc)
        assert "variable_name='x'" in r
        assert "available_variables=['a']" in r


# ---------------------------------------------------------------------------
# VariableTypeMismatchError
# ---------------------------------------------------------------------------


class TestVariableTypeMismatchErrorComprehensive:
    def test_basic_message(self) -> None:
        exc = VariableTypeMismatchError("n", "Node", "Relationship")
        msg = str(exc)
        assert "'n'" in msg
        assert "'Relationship'" in msg
        assert "'Node'" in msg

    def test_with_suggestion(self) -> None:
        exc = VariableTypeMismatchError(
            "n",
            "Node",
            "Rel",
            suggestion="Use a different variable",
        )
        assert "Use a different variable" in str(exc)
        assert exc.suggestion == "Use a different variable"

    def test_without_suggestion(self) -> None:
        exc = VariableTypeMismatchError("n", "Node", "Rel")
        assert exc.suggestion == ""
        # No trailing space from empty suggestion
        assert str(exc).endswith("expected.")

    def test_repr(self) -> None:
        exc = VariableTypeMismatchError("x", "int", "str")
        r = repr(exc)
        assert "variable_name='x'" in r
        assert "expected_type='int'" in r
        assert "actual_type='str'" in r

    def test_is_value_error(self) -> None:
        assert issubclass(VariableTypeMismatchError, ValueError)


# ---------------------------------------------------------------------------
# IncompatibleOperatorError
# ---------------------------------------------------------------------------


class TestIncompatibleOperatorErrorComprehensive:
    def test_without_suggestion(self) -> None:
        exc = IncompatibleOperatorError("+", "str", "int")
        assert exc.suggestion == ""
        assert str(exc) == "Operator '+' incompatible between 'str' and 'int'"

    def test_with_suggestion(self) -> None:
        exc = IncompatibleOperatorError("+", "str", "int", "Use toString()")
        assert "Use toString()" in str(exc)

    def test_is_type_error(self) -> None:
        assert issubclass(IncompatibleOperatorError, TypeError)


# ---------------------------------------------------------------------------
# TemporalArithmeticError
# ---------------------------------------------------------------------------


class TestTemporalArithmeticErrorComprehensive:
    def test_default_example_plus(self) -> None:
        exc = TemporalArithmeticError("+", "date", "string")
        assert "duration" in str(exc)

    def test_default_example_minus(self) -> None:
        exc = TemporalArithmeticError("-", "date", "string")
        assert "duration" in str(exc)

    def test_default_example_other_op(self) -> None:
        exc = TemporalArithmeticError("*", "date", "int")
        assert "duration()" in str(exc)

    def test_custom_example(self) -> None:
        exc = TemporalArithmeticError(
            "+",
            "date",
            "int",
            example="custom example",
        )
        assert exc.example == "custom example"
        assert "custom example" in str(exc)

    def test_inherits_incompatible_operator(self) -> None:
        assert issubclass(TemporalArithmeticError, IncompatibleOperatorError)

    def test_repr(self) -> None:
        exc = TemporalArithmeticError("+", "date", "str")
        r = repr(exc)
        assert "TemporalArithmeticError(" in r
        assert "operator='+'" in r


# ---------------------------------------------------------------------------
# UnsupportedFunctionError
# ---------------------------------------------------------------------------


class TestUnsupportedFunctionErrorComprehensive:
    def test_without_category(self) -> None:
        exc = UnsupportedFunctionError("bad", ["good1", "good2"])
        assert exc.category == ""
        msg = str(exc)
        assert "function:" in msg  # No category prefix
        assert "bad" in msg

    def test_with_category(self) -> None:
        exc = UnsupportedFunctionError(
            "bad",
            ["good1"],
            category="aggregation",
        )
        assert "aggregation function" in str(exc)

    def test_supported_sorted(self) -> None:
        exc = UnsupportedFunctionError("x", ["z", "a", "m"])
        assert "a, m, z" in str(exc)

    def test_repr_without_category(self) -> None:
        exc = UnsupportedFunctionError("bad", ["good"])
        r = repr(exc)
        assert "function_name='bad'" in r
        assert "category" not in r


# ---------------------------------------------------------------------------
# FunctionArgumentError
# ---------------------------------------------------------------------------


class TestFunctionArgumentErrorComprehensive:
    def test_without_description(self) -> None:
        exc = FunctionArgumentError("left", 2, 1)
        assert exc.argument_description == ""
        msg = str(exc)
        assert "left" in msg
        assert "2" in msg
        assert "1" in msg
        assert "Expected:" not in msg

    def test_with_description(self) -> None:
        exc = FunctionArgumentError("left", 2, 1, "string and length")
        assert "Expected: string and length" in str(exc)

    def test_is_value_error(self) -> None:
        assert issubclass(FunctionArgumentError, ValueError)


# ---------------------------------------------------------------------------
# QueryTimeoutError
# ---------------------------------------------------------------------------


class TestQueryTimeoutErrorComprehensive:
    def test_basic(self) -> None:
        exc = QueryTimeoutError(30.0)
        assert "30.0s" in str(exc)
        assert exc.elapsed_seconds == 0.0
        assert exc.query_fragment == ""

    def test_with_elapsed(self) -> None:
        exc = QueryTimeoutError(30.0, elapsed_seconds=35.2)
        assert "35.2s" in str(exc)

    def test_with_query_fragment(self) -> None:
        exc = QueryTimeoutError(10.0, query_fragment="MATCH (n) RETURN n")
        assert "MATCH (n) RETURN n" in str(exc)

    def test_long_query_fragment_truncated(self) -> None:
        long_q = "X" * 200
        exc = QueryTimeoutError(10.0, query_fragment=long_q)
        assert "..." in str(exc)

    def test_is_timeout_error(self) -> None:
        assert issubclass(QueryTimeoutError, TimeoutError)

    def test_repr(self) -> None:
        exc = QueryTimeoutError(5.0, 6.0)
        r = repr(exc)
        assert "timeout_seconds=5.0" in r
        assert "elapsed_seconds=6.0" in r


# ---------------------------------------------------------------------------
# QueryMemoryBudgetError
# ---------------------------------------------------------------------------


class TestQueryMemoryBudgetErrorComprehensive:
    def test_default_suggestion(self) -> None:
        exc = QueryMemoryBudgetError(200_000_000, 100_000_000)
        msg = str(exc)
        assert "190MB" in msg or "191MB" in msg  # 200M / 1024^2
        assert "95MB" in msg or "LIMIT" in msg
        assert "LIMIT" in msg  # Default suggestion

    def test_custom_suggestion(self) -> None:
        exc = QueryMemoryBudgetError(
            200_000_000,
            100_000_000,
            suggestion="Use sampling",
        )
        assert "Use sampling" in str(exc)
        assert "LIMIT" not in str(exc)  # Custom replaces default

    def test_is_memory_error(self) -> None:
        assert issubclass(QueryMemoryBudgetError, MemoryError)

    def test_attributes(self) -> None:
        exc = QueryMemoryBudgetError(500, 100, "reduce")
        assert exc.estimated_bytes == 500
        assert exc.budget_bytes == 100
        assert exc.suggestion == "reduce"


# ---------------------------------------------------------------------------
# MissingParameterError
# ---------------------------------------------------------------------------


class TestMissingParameterErrorComprehensive:
    def test_default_example(self) -> None:
        exc = MissingParameterError("name")
        assert "$name" in str(exc)
        assert "parameters=" in str(exc)

    def test_custom_example(self) -> None:
        exc = MissingParameterError("id", example_usage="params={'id': 42}")
        assert exc.example_usage == "params={'id': 42}"
        assert "params={'id': 42}" in str(exc)

    def test_is_value_error(self) -> None:
        assert issubclass(MissingParameterError, ValueError)

    def test_repr(self) -> None:
        exc = MissingParameterError("x")
        assert repr(exc) == "MissingParameterError(parameter_name='x')"
