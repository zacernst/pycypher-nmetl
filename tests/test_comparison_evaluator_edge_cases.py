"""Unit tests for :mod:`pycypher.comparison_evaluator` edge cases.

Focuses on edge cases that benefit from isolation:

- NULL handling in comparisons (three-valued logic)
- Type coercion scenarios
- Unsupported operator errors
- CASE expression edge cases (searched, simple, no-else, empty WHEN)
- Unary operator null propagation
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from pycypher.comparison_evaluator import ComparisonEvaluator
from pycypher.exceptions import UnsupportedOperatorError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_evaluator(n: int = 3) -> tuple[ComparisonEvaluator, MagicMock]:
    """Build a ComparisonEvaluator with a mocked frame of length n."""
    frame = MagicMock()
    frame.__len__ = MagicMock(return_value=n)
    return ComparisonEvaluator(frame), frame


def _mock_parent_evaluator(*series_list: pd.Series) -> MagicMock:
    """Build a mock ExpressionEvaluatorProtocol returning series in order."""
    parent = MagicMock()
    call_idx = 0

    def evaluate_side_effect(expr: Any) -> pd.Series:
        nonlocal call_idx
        result = series_list[call_idx]
        call_idx += 1
        return result

    parent.evaluate = MagicMock(side_effect=evaluate_side_effect)
    return parent


# ---------------------------------------------------------------------------
# evaluate_comparison — basic operators
# ---------------------------------------------------------------------------


class TestEvaluateComparisonBasic:
    """Test all six comparison operators with non-null data."""

    def test_eq_true(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 2, 3]), pd.Series([1, 2, 3]))
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert list(result) == [True, True, True]

    def test_eq_false(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 2, 3]), pd.Series([4, 5, 6]))
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert list(result) == [False, False, False]

    def test_neq(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 2, 3]), pd.Series([1, 5, 3]))
        result = ev.evaluate_comparison("<>", MagicMock(), MagicMock(), parent)
        assert list(result) == [False, True, False]

    def test_lt(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 5, 3]), pd.Series([2, 2, 3]))
        result = ev.evaluate_comparison("<", MagicMock(), MagicMock(), parent)
        assert list(result) == [True, False, False]

    def test_gt(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 5, 3]), pd.Series([2, 2, 3]))
        result = ev.evaluate_comparison(">", MagicMock(), MagicMock(), parent)
        assert list(result) == [False, True, False]

    def test_le(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 5, 3]), pd.Series([2, 2, 3]))
        result = ev.evaluate_comparison("<=", MagicMock(), MagicMock(), parent)
        assert list(result) == [True, False, True]

    def test_ge(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 5, 3]), pd.Series([2, 2, 3]))
        result = ev.evaluate_comparison(">=", MagicMock(), MagicMock(), parent)
        assert list(result) == [False, True, True]


# ---------------------------------------------------------------------------
# evaluate_comparison — NULL handling (three-valued logic)
# ---------------------------------------------------------------------------


class TestEvaluateComparisonNulls:
    """Comparison involving NULL → NULL result (three-valued logic)."""

    def test_left_null_propagates(self) -> None:
        ev, _ = _make_evaluator()
        left = pd.Series([None, 2, 3], dtype=object)
        right = pd.Series([1, 2, 3], dtype=object)
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert result.iloc[0] is None
        assert result.iloc[1] is True or result.iloc[1] == True  # noqa: E712
        assert result.iloc[2] is True or result.iloc[2] == True  # noqa: E712

    def test_right_null_propagates(self) -> None:
        ev, _ = _make_evaluator()
        left = pd.Series([1, 2, 3], dtype=object)
        right = pd.Series([1, None, 3], dtype=object)
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert result.iloc[1] is None

    def test_both_null_propagates(self) -> None:
        """NULL = NULL → NULL (not True!)."""
        ev, _ = _make_evaluator(1)
        left = pd.Series([None], dtype=object)
        right = pd.Series([None], dtype=object)
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert result.iloc[0] is None

    def test_all_null_comparison(self) -> None:
        ev, _ = _make_evaluator(3)
        left = pd.Series([None, None, None], dtype=object)
        right = pd.Series([None, None, None], dtype=object)
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("<>", MagicMock(), MagicMock(), parent)
        assert all(v is None for v in result)

    def test_no_nulls_no_object_cast(self) -> None:
        """When no nulls, result stays boolean dtype (not object)."""
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1, 2, 3]), pd.Series([1, 2, 3]))
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert result.dtype == bool


# ---------------------------------------------------------------------------
# evaluate_comparison — unsupported operator
# ---------------------------------------------------------------------------


class TestEvaluateComparisonErrors:
    """Test error handling for unsupported operators."""

    def test_unsupported_comparison_operator(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1]), pd.Series([2]))
        with pytest.raises(UnsupportedOperatorError) as exc_info:
            ev.evaluate_comparison("===", MagicMock(), MagicMock(), parent)
        assert exc_info.value.operator == "==="
        assert "comparison" in exc_info.value.category

    def test_unsupported_operator_includes_supported_list(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1]), pd.Series([2]))
        with pytest.raises(UnsupportedOperatorError) as exc_info:
            ev.evaluate_comparison("!!", MagicMock(), MagicMock(), parent)
        assert "=" in exc_info.value.supported_operators


# ---------------------------------------------------------------------------
# evaluate_null_check
# ---------------------------------------------------------------------------


class TestEvaluateNullCheck:
    """Test IS NULL and IS NOT NULL predicates."""

    def test_is_null(self) -> None:
        ev, _ = _make_evaluator()
        series = pd.Series([1, None, 3], dtype=object)
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_null_check("IS NULL", MagicMock(), parent)
        assert list(result) == [False, True, False]

    def test_is_not_null(self) -> None:
        ev, _ = _make_evaluator()
        series = pd.Series([1, None, 3], dtype=object)
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_null_check("IS NOT NULL", MagicMock(), parent)
        assert list(result) == [True, False, True]

    def test_all_null(self) -> None:
        ev, _ = _make_evaluator()
        series = pd.Series([None, None, None], dtype=object)
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_null_check("IS NULL", MagicMock(), parent)
        assert all(result)

    def test_no_nulls(self) -> None:
        ev, _ = _make_evaluator()
        series = pd.Series([1, 2, 3])
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_null_check("IS NULL", MagicMock(), parent)
        assert not any(result)

    def test_unsupported_null_check_operator(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1]))
        with pytest.raises(UnsupportedOperatorError) as exc_info:
            ev.evaluate_null_check("IS MISSING", MagicMock(), parent)
        assert "null check" in exc_info.value.category

    def test_nan_treated_as_null(self) -> None:
        """NaN values should be treated as null by IS NULL."""
        ev, _ = _make_evaluator()
        series = pd.Series([1.0, float("nan"), 3.0])
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_null_check("IS NULL", MagicMock(), parent)
        assert list(result) == [False, True, False]


# ---------------------------------------------------------------------------
# evaluate_unary
# ---------------------------------------------------------------------------


class TestEvaluateUnary:
    """Test unary arithmetic operators."""

    def test_unary_plus_noop(self) -> None:
        ev, _ = _make_evaluator()
        series = pd.Series([1, -2, 3])
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_unary("+", MagicMock(), parent)
        assert list(result) == [1, -2, 3]

    def test_unary_minus(self) -> None:
        ev, _ = _make_evaluator()
        series = pd.Series([1, -2, 3])
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_unary("-", MagicMock(), parent)
        assert list(result) == [-1, 2, -3]

    def test_unary_minus_with_null_object_dtype(self) -> None:
        """Negation on object-dtype Series with None triggers null-safe path."""
        ev, _ = _make_evaluator()
        series = pd.Series([1, None, 3], dtype=object)
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_unary("-", MagicMock(), parent)
        assert result.iloc[0] == -1
        assert result.iloc[1] is None
        assert result.iloc[2] == -3

    def test_unary_plus_with_null(self) -> None:
        """Unary + on object dtype with None — no null-safe path needed for +."""
        ev, _ = _make_evaluator()
        series = pd.Series([5, None, 10], dtype=object)
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_unary("+", MagicMock(), parent)
        # + is identity; null values should pass through
        assert result.iloc[0] == 5
        assert result.iloc[2] == 10

    def test_unary_minus_numeric_with_nan(self) -> None:
        """Negation on numeric dtype with NaN — standard numpy handles it."""
        ev, _ = _make_evaluator()
        series = pd.Series([1.0, float("nan"), 3.0])
        parent = _mock_parent_evaluator(series)
        result = ev.evaluate_unary("-", MagicMock(), parent)
        assert result.iloc[0] == -1.0
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == -3.0

    def test_unsupported_unary_operator(self) -> None:
        ev, _ = _make_evaluator()
        parent = _mock_parent_evaluator(pd.Series([1]))
        with pytest.raises(UnsupportedOperatorError) as exc_info:
            ev.evaluate_unary("~", MagicMock(), parent)
        assert "unary" in exc_info.value.category


# ---------------------------------------------------------------------------
# evaluate_case — searched CASE
# ---------------------------------------------------------------------------


class TestEvaluateCaseSearched:
    """Test searched CASE expressions (CASE WHEN cond THEN val ...)."""

    def test_single_when_clause(self) -> None:
        ev, _ = _make_evaluator()
        parent = MagicMock()

        # Condition: [True, False, True]
        # Then: [10, 10, 10]
        # Else: [0, 0, 0]
        call_results = iter([
            pd.Series([0, 0, 0]),         # else
            pd.Series([10, 10, 10]),       # then
            pd.Series([True, False, True]),  # condition (fillna applied)
        ])
        parent.evaluate = MagicMock(side_effect=lambda e: next(call_results))

        when = MagicMock()
        when.result = MagicMock()
        when.condition = MagicMock()

        result = ev.evaluate_case(
            case_expr=None,
            when_clauses=[when],
            else_expr=MagicMock(),
            evaluator=parent,
        )
        assert list(result) == [10, 0, 10]

    def test_no_else_defaults_to_null(self) -> None:
        ev, _ = _make_evaluator()
        parent = MagicMock()

        call_results = iter([
            pd.Series([99, 99, 99]),        # then
            pd.Series([False, False, False]),  # condition
        ])
        parent.evaluate = MagicMock(side_effect=lambda e: next(call_results))

        when = MagicMock()
        when.result = MagicMock()
        when.condition = MagicMock()

        result = ev.evaluate_case(
            case_expr=None,
            when_clauses=[when],
            else_expr=None,
            evaluator=parent,
        )
        # All conditions False, no else → all None
        assert all(v is None for v in result)

    def test_multiple_when_first_wins(self) -> None:
        """First matching WHEN clause wins when multiple match."""
        ev, _ = _make_evaluator()
        parent = MagicMock()

        # Reverse iteration: when2 applied first, then when1 overwrites
        call_results = iter([
            pd.Series([0, 0, 0]),             # else
            pd.Series([20, 20, 20]),           # when2 then
            pd.Series([True, True, False]),    # when2 condition
            pd.Series([10, 10, 10]),           # when1 then
            pd.Series([True, False, False]),   # when1 condition
        ])
        parent.evaluate = MagicMock(side_effect=lambda e: next(call_results))

        when1 = MagicMock()
        when1.result = MagicMock()
        when1.condition = MagicMock()

        when2 = MagicMock()
        when2.result = MagicMock()
        when2.condition = MagicMock()

        result = ev.evaluate_case(
            case_expr=None,
            when_clauses=[when1, when2],
            else_expr=MagicMock(),
            evaluator=parent,
        )
        # Row 0: when1 matches → 10; Row 1: when2 matches → 20; Row 2: else → 0
        assert list(result) == [10, 20, 0]


# ---------------------------------------------------------------------------
# evaluate_case — simple CASE
# ---------------------------------------------------------------------------


class TestEvaluateCaseSimple:
    """Test simple CASE expressions (CASE expr WHEN val THEN result ...)."""

    def test_simple_case_match(self) -> None:
        ev, _ = _make_evaluator()
        parent = MagicMock()

        # discriminant: [1, 2, 3]
        # else: [-1, -1, -1]
        # when val: [1, 1, 1] (match when discriminant == 1)
        # then: [100, 100, 100]
        call_results = iter([
            pd.Series([-1, -1, -1]),     # else
            pd.Series([1, 2, 3]),        # discriminant
            pd.Series([100, 100, 100]),  # then
            pd.Series([1, 1, 1]),        # match value
        ])
        parent.evaluate = MagicMock(side_effect=lambda e: next(call_results))

        when = MagicMock()
        when.result = MagicMock()
        when.condition = MagicMock()

        result = ev.evaluate_case(
            case_expr=MagicMock(),
            when_clauses=[when],
            else_expr=MagicMock(),
            evaluator=parent,
        )
        assert list(result) == [100, -1, -1]

    def test_empty_when_clauses_returns_else(self) -> None:
        ev, _ = _make_evaluator()
        parent = MagicMock()

        parent.evaluate = MagicMock(return_value=pd.Series([42, 42, 42]))

        result = ev.evaluate_case(
            case_expr=None,
            when_clauses=[],
            else_expr=MagicMock(),
            evaluator=parent,
        )
        assert list(result) == [42, 42, 42]


# ---------------------------------------------------------------------------
# Type coercion scenarios
# ---------------------------------------------------------------------------


class TestTypeCoercion:
    """Test comparison behavior with mixed types."""

    def test_int_vs_float_comparison(self) -> None:
        ev, _ = _make_evaluator()
        left = pd.Series([1, 2, 3])
        right = pd.Series([1.0, 2.0, 3.0])
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert list(result) == [True, True, True]

    def test_string_comparison(self) -> None:
        ev, _ = _make_evaluator()
        left = pd.Series(["alice", "bob", "charlie"])
        right = pd.Series(["alice", "BOB", "charlie"])
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert list(result) == [True, False, True]

    def test_empty_series_comparison(self) -> None:
        """Comparing empty series returns empty result."""
        ev, _ = _make_evaluator(0)
        left = pd.Series([], dtype=float)
        right = pd.Series([], dtype=float)
        parent = _mock_parent_evaluator(left, right)
        result = ev.evaluate_comparison("=", MagicMock(), MagicMock(), parent)
        assert len(result) == 0
