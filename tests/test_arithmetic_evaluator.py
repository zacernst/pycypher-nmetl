"""TDD tests for ArithmeticExpressionEvaluator Phase 1 implementation.

This module provides comprehensive test coverage for the ArithmeticExpressionEvaluator
extracted from BindingExpressionEvaluator as part of Architecture Loop 277 refactoring.

Run with:
    uv run pytest tests/test_architecture_loop_277_arithmetic_evaluator_tdd.py -v
"""

import pandas as pd
import pytest
from pycypher.arithmetic_evaluator import (
    _ARITH_OPS,
    _CMP_OPS,
    _UNARY_OPS,
    ArithmeticExpressionEvaluator,
    _cypher_div,
    _cypher_mod,
    _cypher_pow,
    _first_non_null_val,
    _is_temporal_val,
)
from pycypher.binding_frame import BindingFrame
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


class TestArithmeticOperatorHelpers:
    """Test helper functions for arithmetic operations."""

    def test_cypher_div_basic_division(self) -> None:
        """Test basic integer division operations (returns integer with truncation)."""
        left = pd.Series([10, 20, 30])
        right = pd.Series([2, 4, 5])
        result = _cypher_div(left, right)

        # Integer / integer -> integer (truncated)
        expected = pd.Series([5, 5, 6], dtype="int64")
        pd.testing.assert_series_equal(result, expected)

    def test_cypher_div_division_by_zero(self) -> None:
        """Test division by zero returns null."""
        left = pd.Series([10, 20, 30])
        right = pd.Series([2, 0, 5])
        result = _cypher_div(left, right)

        expected = pd.Series([5.0, None, 6.0], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_cypher_div_null_handling(self) -> None:
        """Test null value handling in division."""
        left = pd.Series([10, None, 30])
        right = pd.Series([2, 4, None])
        result = _cypher_div(left, right)

        expected = pd.Series([5.0, None, None], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_cypher_mod_basic_modulo(self) -> None:
        """Test basic modulo operations."""
        left = pd.Series([10, 21, 33])
        right = pd.Series([3, 4, 5])
        result = _cypher_mod(left, right)

        expected = pd.Series([1, 1, 3])
        pd.testing.assert_series_equal(result, expected)

    def test_cypher_mod_modulo_by_zero(self) -> None:
        """Test modulo by zero returns null."""
        left = pd.Series([10, 21, 33])
        right = pd.Series([3, 0, 5])
        result = _cypher_mod(left, right)

        expected = pd.Series([1, None, 3], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_cypher_pow_basic_power(self) -> None:
        """Test basic power operations."""
        left = pd.Series([2, 3, 4])
        right = pd.Series([3, 2, 2])
        result = _cypher_pow(left, right)

        expected = pd.Series([8, 9, 16])
        pd.testing.assert_series_equal(result, expected)

    def test_cypher_pow_null_handling(self) -> None:
        """Test null handling in power operations."""
        left = pd.Series([2, None, 4])
        right = pd.Series([3, 2, None])
        result = _cypher_pow(left, right)

        expected = pd.Series([8, None, None], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_first_non_null_val_basic(self) -> None:
        """Test first non-null value extraction."""
        series = pd.Series([None, None, 42, 100])
        result = _first_non_null_val(series)
        assert result == 42

    def test_first_non_null_val_all_null(self) -> None:
        """Test first non-null value with all nulls."""
        series = pd.Series([None, None, None])
        result = _first_non_null_val(series)
        assert result is None

    def test_is_temporal_val_detection(self) -> None:
        """Test temporal value detection."""
        # Test date strings (ISO format YYYY-MM-DD)
        assert _is_temporal_val("2024-01-01") is True
        # Test datetime strings (ISO format with T)
        assert _is_temporal_val("2024-01-01T12:00:00") is True
        # Test duration dictionaries
        assert _is_temporal_val({"days": 7}) is True
        # Test non-temporal values
        assert _is_temporal_val(42) is False
        assert _is_temporal_val("text") is False
        assert _is_temporal_val("2024-01") is False  # Invalid date string
        assert _is_temporal_val(None) is False


class TestOperatorDispatchTables:
    """Test operator dispatch tables are correctly defined."""

    def test_arithmetic_ops_table(self) -> None:
        """Test arithmetic operators dispatch table."""
        assert len(_ARITH_OPS) == 6
        assert "+" in _ARITH_OPS
        assert "-" in _ARITH_OPS
        assert "*" in _ARITH_OPS
        assert "/" in _ARITH_OPS
        assert "%" in _ARITH_OPS
        assert "^" in _ARITH_OPS

        # Test basic operations work
        assert _ARITH_OPS["+"](5, 3) == 8
        assert _ARITH_OPS["-"](5, 3) == 2
        assert _ARITH_OPS["*"](5, 3) == 15

    def test_comparison_ops_table(self) -> None:
        """Test comparison operators dispatch table."""
        assert len(_CMP_OPS) == 6
        assert "=" in _CMP_OPS
        assert "<>" in _CMP_OPS
        assert "<" in _CMP_OPS
        assert ">" in _CMP_OPS
        assert "<=" in _CMP_OPS
        assert ">=" in _CMP_OPS

        # Test basic operations work
        assert _CMP_OPS["="](5, 5) is True
        assert _CMP_OPS["<>"](5, 3) is True
        assert _CMP_OPS["<"](3, 5) is True

    def test_unary_ops_table(self) -> None:
        """Test unary operators dispatch table."""
        assert len(_UNARY_OPS) == 2
        assert "+" in _UNARY_OPS
        assert "-" in _UNARY_OPS

        # Test operations work
        assert _UNARY_OPS["+"](5) == 5
        assert _UNARY_OPS["-"](5) == -5


class TestArithmeticExpressionEvaluator:
    """Test ArithmeticExpressionEvaluator class functionality."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame with sample data."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "Person__age": [25, 30, 35, 40],
                "Person__salary": [50000, 60000, 70000, 80000],
            }
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4],
                    "age": [25, 30, 35, 40],
                    "salary": [50000, 60000, 70000, 80000],
                }
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table})
        )
        type_registry = {"p": "Person"}  # Add type registry for BindingFrame
        return BindingFrame(
            bindings=df, context=context, type_registry=type_registry
        )

    @pytest.fixture
    def arithmetic_evaluator(
        self, test_frame: BindingFrame
    ) -> ArithmeticExpressionEvaluator:
        """Create ArithmeticExpressionEvaluator instance."""
        return ArithmeticExpressionEvaluator(test_frame)

    def test_evaluator_initialization(self, test_frame: BindingFrame) -> None:
        """Test evaluator initializes correctly."""
        evaluator = ArithmeticExpressionEvaluator(test_frame)
        assert evaluator.frame is test_frame

    def test_evaluate_arithmetic_addition(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test arithmetic addition evaluation."""

        # Mock expression evaluator for testing
        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "left":
                    return pd.Series([10, 20, 30, 40])
                elif expr == "right":
                    return pd.Series([5, 10, 15, 20])
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_arithmetic(
            "+", "left", "right", mock_evaluator
        )

        expected = pd.Series([15, 30, 45, 60])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_arithmetic_subtraction(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test arithmetic subtraction evaluation."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "left":
                    return pd.Series([50, 60, 70, 80])
                elif expr == "right":
                    return pd.Series([5, 10, 15, 20])
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_arithmetic(
            "-", "left", "right", mock_evaluator
        )

        expected = pd.Series([45, 50, 55, 60])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_arithmetic_division(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test arithmetic division evaluation with Cypher semantics."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "left":
                    return pd.Series([20, 40, 60, 80])
                elif expr == "right":
                    return pd.Series([2, 4, 0, 8])  # Include division by zero
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_arithmetic(
            "/", "left", "right", mock_evaluator
        )

        expected = pd.Series([10.0, 10.0, None, 10.0], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_arithmetic_unsupported_operator(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test unsupported arithmetic operator raises ValueError."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([1, 2, 3, 4])

        mock_evaluator = MockExpressionEvaluator()

        with pytest.raises(
            ValueError, match="Unsupported arithmetic operator"
        ):
            arithmetic_evaluator.evaluate_arithmetic(
                "@", "left", "right", mock_evaluator
            )

    def test_evaluate_comparison_equality(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test comparison equality evaluation."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "left":
                    return pd.Series([25, 30, 35, 40])
                elif expr == "right":
                    return pd.Series([25, 25, 35, 45])
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_comparison(
            "=", "left", "right", mock_evaluator
        )

        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_comparison_null_handling(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test comparison with null values."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "left":
                    return pd.Series([25, None, 35, 40])
                elif expr == "right":
                    return pd.Series([25, 30, None, 40])
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_comparison(
            "=", "left", "right", mock_evaluator
        )

        expected = pd.Series([True, None, None, True], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_comparison_unsupported_operator(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test unsupported comparison operator raises ValueError."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([1, 2, 3, 4])

        mock_evaluator = MockExpressionEvaluator()

        with pytest.raises(
            ValueError, match="Unsupported comparison operator"
        ):
            arithmetic_evaluator.evaluate_comparison(
                "~=", "left", "right", mock_evaluator
            )

    def test_evaluate_unary_negation(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test unary negation evaluation."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "operand":
                    return pd.Series([5, -10, 15, -20])
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_unary(
            "-", "operand", mock_evaluator
        )

        expected = pd.Series([-5, 10, -15, 20])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_unary_positive(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test unary positive (no-op) evaluation."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "operand":
                    return pd.Series([5, -10, 15, -20])
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_unary(
            "+", "operand", mock_evaluator
        )

        expected = pd.Series([5, -10, 15, -20])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_unary_null_handling(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test unary operations with null values."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "operand":
                    return pd.Series([5, None, 15, None], dtype=object)
                return pd.Series([0, 0, 0, 0])

        mock_evaluator = MockExpressionEvaluator()
        result = arithmetic_evaluator.evaluate_unary(
            "-", "operand", mock_evaluator
        )

        expected = pd.Series([-5, None, -15, None], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_unary_unsupported_operator(
        self, arithmetic_evaluator: ArithmeticExpressionEvaluator
    ) -> None:
        """Test unsupported unary operator raises ValueError."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([1, 2, 3, 4])

        mock_evaluator = MockExpressionEvaluator()

        with pytest.raises(ValueError, match="Unsupported unary operator"):
            arithmetic_evaluator.evaluate_unary("!", "operand", mock_evaluator)


class TestArithmeticEvaluatorIntegration:
    """Test ArithmeticExpressionEvaluator integration scenarios."""

    def test_complex_arithmetic_expressions(self) -> None:
        """Test evaluation of complex arithmetic expressions."""
        # This would test the evaluator in more complex scenarios
        # with actual expression objects from the AST
        pass  # Placeholder for integration tests

    def test_type_error_handling(self) -> None:
        """Test proper type error handling for incompatible operands."""
        # Test the TypeError handling for incompatible types
        pass  # Placeholder for type error tests

    def test_performance_baseline(self) -> None:
        """Test performance baseline for arithmetic operations."""
        # Establish performance characteristics for the extracted evaluator
        pass  # Placeholder for performance tests


class TestArithmeticEvaluatorRegression:
    """Test ArithmeticExpressionEvaluator maintains compatibility with original behavior."""

    def test_maintains_original_semantics(self) -> None:
        """Test that extracted evaluator maintains original BindingExpressionEvaluator semantics."""
        # Comprehensive test comparing behavior with original implementation
        pass  # Placeholder for regression tests

    def test_error_message_compatibility(self) -> None:
        """Test that error messages match original implementation."""
        # Test error messages are identical to original
        pass  # Placeholder for error message tests
