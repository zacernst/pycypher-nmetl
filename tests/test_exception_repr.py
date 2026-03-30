"""Tests for __repr__ methods on PyCypher exception classes.

Verifies that exception __repr__ output exposes structured attributes
for REPL/debugger inspection, making it easy to programmatically access
error details when catching exceptions interactively.
"""

from __future__ import annotations

from pycypher.exceptions import (
    ASTConversionError,
    FunctionArgumentError,
    GrammarTransformerSyncError,
    GraphTypeNotFoundError,
    IncompatibleOperatorError,
    MissingParameterError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    TemporalArithmeticError,
    UnsupportedFunctionError,
    VariableNotFoundError,
    VariableTypeMismatchError,
)


class TestGraphTypeNotFoundErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = GraphTypeNotFoundError("Ghost")
        assert repr(exc).startswith("GraphTypeNotFoundError(")

    def test_repr_contains_type_name(self) -> None:
        exc = GraphTypeNotFoundError("Ghost")
        assert "type_name='Ghost'" in repr(exc)


class TestVariableNotFoundErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = VariableNotFoundError("m", ["n", "p"])
        assert repr(exc).startswith("VariableNotFoundError(")

    def test_repr_contains_variable_name(self) -> None:
        exc = VariableNotFoundError("m", ["n", "p"])
        assert "variable_name='m'" in repr(exc)

    def test_repr_contains_available_variables(self) -> None:
        exc = VariableNotFoundError("m", ["n", "p"])
        assert "available_variables=['n', 'p']" in repr(exc)


class TestVariableTypeMismatchErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = VariableTypeMismatchError("x", "Node", "Relationship")
        assert repr(exc).startswith("VariableTypeMismatchError(")

    def test_repr_contains_variable_and_types(self) -> None:
        exc = VariableTypeMismatchError("x", "Node", "Relationship")
        r = repr(exc)
        assert "variable_name='x'" in r
        assert "expected_type='Node'" in r
        assert "actual_type='Relationship'" in r


class TestIncompatibleOperatorErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = IncompatibleOperatorError("+", "string", "integer")
        assert repr(exc).startswith("IncompatibleOperatorError(")

    def test_repr_contains_operator_and_types(self) -> None:
        exc = IncompatibleOperatorError("+", "string", "integer")
        r = repr(exc)
        assert "operator='+'" in r
        assert "left_type='string'" in r
        assert "right_type='integer'" in r


class TestTemporalArithmeticErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = TemporalArithmeticError("+", "date", "string")
        assert repr(exc).startswith("TemporalArithmeticError(")

    def test_repr_contains_operator_and_types(self) -> None:
        exc = TemporalArithmeticError("+", "date", "string")
        r = repr(exc)
        assert "operator='+'" in r
        assert "left_type='date'" in r
        assert "right_type='string'" in r


class TestUnsupportedFunctionErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = UnsupportedFunctionError("badFunc", ["count", "sum"])
        assert repr(exc).startswith("UnsupportedFunctionError(")

    def test_repr_contains_function_name(self) -> None:
        exc = UnsupportedFunctionError("badFunc", ["count", "sum"])
        assert "function_name='badFunc'" in repr(exc)

    def test_repr_contains_supported_functions(self) -> None:
        exc = UnsupportedFunctionError("badFunc", ["count", "sum"])
        assert "supported_functions=['count', 'sum']" in repr(exc)

    def test_repr_contains_category_when_set(self) -> None:
        exc = UnsupportedFunctionError("badFunc", ["count"], "aggregation")
        assert "category='aggregation'" in repr(exc)


class TestFunctionArgumentErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = FunctionArgumentError("left", 2, 1)
        assert repr(exc).startswith("FunctionArgumentError(")

    def test_repr_contains_function_and_counts(self) -> None:
        exc = FunctionArgumentError("left", 2, 1)
        r = repr(exc)
        assert "function_name='left'" in r
        assert "expected_args=2" in r
        assert "actual_args=1" in r


class TestQueryTimeoutErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = QueryTimeoutError(30.0, 35.2)
        assert repr(exc).startswith("QueryTimeoutError(")

    def test_repr_contains_timeout_details(self) -> None:
        exc = QueryTimeoutError(30.0, 35.2)
        r = repr(exc)
        assert "timeout_seconds=30.0" in r
        assert "elapsed_seconds=35.2" in r


class TestQueryMemoryBudgetErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = QueryMemoryBudgetError(200_000_000, 100_000_000)
        assert repr(exc).startswith("QueryMemoryBudgetError(")

    def test_repr_contains_memory_details(self) -> None:
        exc = QueryMemoryBudgetError(200_000_000, 100_000_000)
        r = repr(exc)
        assert "estimated_bytes=200000000" in r
        assert "budget_bytes=100000000" in r


class TestMissingParameterErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = MissingParameterError("user_id")
        assert repr(exc).startswith("MissingParameterError(")

    def test_repr_contains_parameter_name(self) -> None:
        exc = MissingParameterError("user_id")
        assert "parameter_name='user_id'" in repr(exc)


class TestASTConversionErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = ASTConversionError("parse failed")
        assert repr(exc).startswith("ASTConversionError(")

    def test_repr_contains_context_when_set(self) -> None:
        exc = ASTConversionError(
            "failed",
            query_fragment="MATCH (n)",
            node_type="Foo",
        )
        r = repr(exc)
        assert "query_fragment='MATCH (n)'" in r
        assert "node_type='Foo'" in r


class TestGrammarTransformerSyncErrorRepr:
    def test_repr_contains_class_name(self) -> None:
        exc = GrammarTransformerSyncError(
            "out of sync",
            missing_node_type="FooBar",
        )
        assert repr(exc).startswith("GrammarTransformerSyncError(")

    def test_repr_contains_missing_node_type(self) -> None:
        exc = GrammarTransformerSyncError(
            "sync issue",
            missing_node_type="FooBar",
        )
        assert "missing_node_type='FooBar'" in repr(exc)
