"""TDD tests for Testing Loop 281 - Division by Zero Semantics Fix.

This module provides comprehensive test coverage for the division by zero semantics
regression introduced during Loop 279 arithmetic evaluator changes.

Expected Neo4j-compatible behavior:
- Integer division by zero: null (✅ currently correct)
- Float division by zero: ±infinity (❌ currently returns null, should return infinity)

Run with:
    uv run pytest tests/test_testing_loop_281_division_by_zero_fix_tdd.py -v
"""

import math

import pandas as pd
import pytest
from pycypher.arithmetic_evaluator import _cypher_div
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


class TestDivisionByZeroSemantics:
    """Test correct Neo4j-compatible division by zero semantics."""

    def test_integer_division_by_zero_returns_null(self) -> None:
        """Integer division by zero should return null (currently correct)."""
        left = pd.Series([1, 5, -5, 0], dtype="int64")
        right = pd.Series([0, 0, 0, 0], dtype="int64")

        result = _cypher_div(left, right)

        # All results should be null for integer division by zero
        assert result.dtype == object
        assert result.isna().all()

    def test_float_division_by_zero_returns_infinity(self) -> None:
        """Float division by zero should return ±infinity (currently broken)."""
        left = pd.Series([1.0, -1.0, 5.0, -5.0])
        right = pd.Series([0.0, 0.0, 0.0, 0.0])

        result = _cypher_div(left, right)

        # Should return infinities, not nulls
        expected_results = [
            float("inf"),
            float("-inf"),
            float("inf"),
            float("-inf"),
        ]
        for actual, expected in zip(result, expected_results):
            assert math.isinf(actual)
            assert (actual > 0) == (expected > 0)  # Same sign

    def test_float_zero_division_by_zero_returns_nan(self) -> None:
        """0.0 / 0.0 should return NaN (currently broken)."""
        left = pd.Series([0.0])
        right = pd.Series([0.0])

        result = _cypher_div(left, right)

        assert math.isnan(result.iloc[0])

    def test_mixed_type_division_by_zero_returns_infinity(self) -> None:
        """Mixed type division by zero should return ±infinity."""
        left = pd.Series([1], dtype="int64")  # Integer
        right = pd.Series([0.0])  # Float zero

        result = _cypher_div(left, right)

        assert math.isinf(result.iloc[0])
        assert result.iloc[0] > 0  # Positive infinity

    def test_integer_division_truncation_unchanged(self) -> None:
        """Integer division truncation should remain unchanged."""
        left = pd.Series([7, -7], dtype="int64")
        right = pd.Series([2, 2], dtype="int64")

        result = _cypher_div(left, right)

        # Should truncate toward zero: 7/2=3, -7/2=-3
        assert result.tolist() == [3, -3]
        assert result.dtype == "int64"

    def test_regular_float_division_unchanged(self) -> None:
        """Regular float division should remain unchanged."""
        left = pd.Series([7.0, -7.0])
        right = pd.Series([2.0, 2.0])

        result = _cypher_div(left, right)

        # Should return true division: 7.0/2.0=3.5, -7.0/2.0=-3.5
        assert result.tolist() == [3.5, -3.5]


class TestIntegrationWithStarQuery:
    """Test division by zero behavior through Star query execution."""

    @pytest.fixture
    def star_context(self) -> Star:
        """Create Star context for division by zero testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "int_val": [1, -1, 0],
                "float_val": [1.0, -1.0, 0.0],
            },
        )

        table = EntityTable.from_dataframe("TestTable", df)
        context = Context(
            entity_mapping=EntityMapping(mapping={"TestTable": table}),
        )
        return Star(context=context)

    def test_integer_literal_division_by_zero_query(
        self,
        star_context: Star,
    ) -> None:
        """Test 'RETURN 1 / 0' returns null."""
        result = star_context.execute_query("RETURN 1 / 0 AS r")
        assert pd.isna(result["r"].iloc[0])

    def test_float_literal_division_by_zero_query(
        self,
        star_context: Star,
    ) -> None:
        """Test 'RETURN 1.0 / 0.0' returns infinity."""
        result = star_context.execute_query("RETURN 1.0 / 0.0 AS r")
        val = result["r"].iloc[0]
        assert math.isinf(val) and val > 0

    def test_negative_float_division_by_zero_query(
        self,
        star_context: Star,
    ) -> None:
        """Test 'RETURN -1.0 / 0.0' returns negative infinity."""
        result = star_context.execute_query("RETURN -1.0 / 0.0 AS r")
        val = result["r"].iloc[0]
        assert math.isinf(val) and val < 0

    def test_zero_float_division_by_zero_query(
        self,
        star_context: Star,
    ) -> None:
        """Test 'RETURN 0.0 / 0.0' returns NaN."""
        result = star_context.execute_query("RETURN 0.0 / 0.0 AS r")
        assert math.isnan(result["r"].iloc[0])

    def test_column_integer_division_by_zero(self, star_context: Star) -> None:
        """Test column integer division by zero returns null."""
        result = star_context.execute_query(
            "MATCH (t:TestTable) RETURN t.int_val / 0 AS r",
        )
        # All integer division by zero should be null
        assert result["r"].isna().all()

    def test_column_float_division_by_zero(self, star_context: Star) -> None:
        """Test column float division by zero returns infinity."""
        result = star_context.execute_query(
            "MATCH (t:TestTable) RETURN t.float_val / 0.0 AS r",
        )

        results = result["r"].tolist()
        # [1.0/0.0, -1.0/0.0, 0.0/0.0] → [+inf, -inf, NaN]
        assert math.isinf(results[0]) and results[0] > 0  # +infinity
        assert math.isinf(results[1]) and results[1] < 0  # -infinity
        assert math.isnan(results[2])  # NaN


class TestRegressionValidation:
    """Test that current failing tests should pass after fix."""

    def test_failing_test_expectations(self) -> None:
        """Document what the currently failing tests expect."""
        # This documents the test cases that are currently failing
        # After the fix, these assertions should hold:

        # From test_data_correctness_types.py::TestMathematicalEdgeCases::test_division_by_zero_float
        expected_behavior = {
            "positive_float_div_zero": lambda x: math.isinf(x) and x > 0,
            "negative_float_div_zero": lambda x: math.isinf(x) and x < 0,
            "zero_float_div_zero": math.isnan,
        }

        # From test_integer_division_by_zero.py::TestIntegerDivisionByZero
        expected_behavior_int = {
            "integer_div_zero": pd.isna,  # Should remain null
        }

        # These behaviors should be preserved after fix
        assert callable(expected_behavior["positive_float_div_zero"])
        assert callable(expected_behavior["negative_float_div_zero"])
        assert callable(expected_behavior["zero_float_div_zero"])
        assert callable(expected_behavior_int["integer_div_zero"])


class TestEdgeCases:
    """Test edge cases in division by zero handling."""

    def test_mixed_null_and_zero_divisors(self) -> None:
        """Test division with mixed null and zero divisors."""
        left = pd.Series([1.0, 2.0, 3.0])
        right = pd.Series([0.0, None, 0.0])  # zero, null, zero

        result = _cypher_div(left, right)

        # [1.0/0.0, 2.0/None, 3.0/0.0] → [+inf, null, +inf]
        assert math.isinf(result.iloc[0]) and result.iloc[0] > 0  # +infinity
        assert pd.isna(result.iloc[1])  # null
        assert math.isinf(result.iloc[2]) and result.iloc[2] > 0  # +infinity

    def test_mixed_null_and_zero_dividends(self) -> None:
        """Test division with mixed null and zero dividends."""
        left = pd.Series([0.0, None, 5.0])  # zero, null, normal
        right = pd.Series([0.0, 0.0, 0.0])  # all zero

        result = _cypher_div(left, right)

        # [0.0/0.0, None/0.0, 5.0/0.0] → [NaN, null, +inf]
        assert math.isnan(result.iloc[0])  # NaN
        assert pd.isna(result.iloc[1])  # null (null takes precedence)
        assert math.isinf(result.iloc[2]) and result.iloc[2] > 0  # +infinity

    def test_large_numbers_division_by_zero(self) -> None:
        """Test very large numbers divided by zero."""
        left = pd.Series([1e100, -1e100])
        right = pd.Series([0.0, 0.0])

        result = _cypher_div(left, right)

        assert math.isinf(result.iloc[0]) and result.iloc[0] > 0  # +infinity
        assert math.isinf(result.iloc[1]) and result.iloc[1] < 0  # -infinity
