"""Tests for zero-coverage core evaluator modules.

Covers:
- arithmetic_evaluator.py  (div, mod, pow, temporal, class methods)
- boolean_evaluator.py     (kleene logic, class methods)
- comparison_evaluator.py  (comparison, null-check, unary, CASE)
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from pycypher.arithmetic_evaluator import (
    ArithmeticExpressionEvaluator,
    _cypher_div,
    _cypher_mod,
    _cypher_pow,
    _first_non_null_val,
    _is_date_str,
    _is_datetime_str,
    _is_duration_dict,
    _is_temporal_val,
    _temporal_arith_pair,
)
from pycypher.boolean_evaluator import (
    BooleanExpressionEvaluator,
    kleene_and,
    kleene_not,
    kleene_or,
    kleene_xor,
)
from pycypher.comparison_evaluator import ComparisonEvaluator
from pycypher.exceptions import UnsupportedOperatorError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_frame(n: int = 3) -> Any:
    """Build a minimal BindingFrame-like object for evaluator constructors."""
    frame = MagicMock()
    frame.__len__ = lambda self: n
    frame.bindings = pd.DataFrame({"x": range(n)})
    return frame


def _make_evaluator(lookup: dict[Any, pd.Series]) -> Any:
    """Build a mock expression evaluator that returns pre-built Series by key."""
    ev = MagicMock()
    ev.evaluate = lambda expr: lookup[expr]
    return ev


# ===================================================================
# arithmetic_evaluator — pure functions
# ===================================================================


class TestCypherDiv:
    """Tests for _cypher_div (integer truncation, null, div-by-zero)."""

    def test_int_div_truncates_toward_zero(self) -> None:
        left = pd.Series([7, -7, 7, -7])
        right = pd.Series([2, 2, -2, -2])
        result = _cypher_div(left, right)
        assert list(result) == [3, -3, -3, 3]

    def test_int_div_by_zero_yields_none(self) -> None:
        result = _cypher_div(pd.Series([10]), pd.Series([0]))
        assert result.iloc[0] is None

    def test_int_div_null_propagation(self) -> None:
        left = pd.Series([10, None], dtype=object).astype("Int64")
        right = pd.Series([2, 2]).astype("Int64")
        result = _cypher_div(left, right)
        assert result.iloc[1] is None

    def test_float_div_normal(self) -> None:
        result = _cypher_div(pd.Series([7.0]), pd.Series([2.0]))
        assert result.iloc[0] == pytest.approx(3.5)

    def test_float_div_null_propagation(self) -> None:
        left = pd.Series([1.0, None], dtype=object)
        right = pd.Series([2.0, 2.0])
        result = _cypher_div(left, right)
        assert result.iloc[1] is None


class TestCypherMod:
    """Tests for _cypher_mod (truncating remainder, Neo4j semantics)."""

    def test_positive_mod(self) -> None:
        result = _cypher_mod(pd.Series([7]), pd.Series([3]))
        assert result.iloc[0] == pytest.approx(1)

    def test_negative_dividend_truncating(self) -> None:
        # Neo4j: -5 % 3 = -2 (not 1 as in Python)
        result = _cypher_mod(pd.Series([-5]), pd.Series([3]))
        assert result.iloc[0] == pytest.approx(-2)

    def test_negative_divisor_truncating(self) -> None:
        # Neo4j: 5 % -3 = 2 (not -1 as in Python)
        result = _cypher_mod(pd.Series([5]), pd.Series([-3]))
        assert result.iloc[0] == pytest.approx(2)

    def test_mod_by_zero_yields_none(self) -> None:
        result = _cypher_mod(pd.Series([5]), pd.Series([0]))
        assert result.iloc[0] is None

    def test_mod_null_propagation(self) -> None:
        left = pd.Series([5, None], dtype=object)
        right = pd.Series([3, 3])
        result = _cypher_mod(left, right)
        assert result.iloc[1] is None

    def test_int_mod_preserves_dtype(self) -> None:
        result = _cypher_mod(pd.Series([10]), pd.Series([3]))
        assert pd.api.types.is_integer_dtype(result)


class TestCypherPow:
    """Tests for _cypher_pow (integer base with negative exponent, null)."""

    def test_int_positive_exponent(self) -> None:
        result = _cypher_pow(pd.Series([2]), pd.Series([3]))
        assert result.iloc[0] == 8

    def test_int_negative_exponent_returns_float(self) -> None:
        result = _cypher_pow(pd.Series([2]), pd.Series([-1]))
        assert result.iloc[0] == pytest.approx(0.5)

    def test_float_pow(self) -> None:
        result = _cypher_pow(pd.Series([4.0]), pd.Series([0.5]))
        assert result.iloc[0] == pytest.approx(2.0)

    def test_null_propagation(self) -> None:
        left = pd.Series([2, None], dtype=object)
        right = pd.Series([3, 3])
        result = _cypher_pow(left, right)
        assert result.iloc[1] is None


class TestHelperFunctions:
    """Tests for type-checking helpers and _first_non_null_val."""

    def test_first_non_null_val_returns_first(self) -> None:
        assert _first_non_null_val(pd.Series([None, 42, 99])) == 42

    def test_first_non_null_val_all_null(self) -> None:
        assert _first_non_null_val(pd.Series([None, None])) is None

    def test_is_date_str(self) -> None:
        assert _is_date_str("2024-01-15") is True
        assert _is_date_str("not-a-date") is False
        assert _is_date_str(42) is False

    def test_is_datetime_str(self) -> None:
        assert _is_datetime_str("2024-01-15T10:30:00") is True
        assert _is_datetime_str("2024-01-15") is False

    def test_is_duration_dict(self) -> None:
        assert _is_duration_dict({"days": 5, "hours": 3}) is True
        assert _is_duration_dict({"hours": 3}) is False
        assert _is_duration_dict("not a dict") is False

    def test_is_temporal_val(self) -> None:
        assert _is_temporal_val("2024-01-15") is True
        assert _is_temporal_val("2024-01-15T10:30:00") is True
        assert _is_temporal_val({"days": 1}) is True
        assert _is_temporal_val(42) is False


class TestTemporalArithPair:
    """Tests for _temporal_arith_pair."""

    def test_date_plus_duration(self) -> None:
        result = _temporal_arith_pair(
            "+",
            "2024-01-15",
            {
                "days": 10,
                "months": 0,
                "years": 0,
                "weeks": 0,
                "hours": 0,
                "minutes": 0,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0,
                "nanoseconds": 0,
            },
        )
        assert result == "2024-01-25"

    def test_date_minus_duration(self) -> None:
        result = _temporal_arith_pair(
            "-",
            "2024-01-15",
            {
                "days": 5,
                "months": 0,
                "years": 0,
                "weeks": 0,
                "hours": 0,
                "minutes": 0,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0,
                "nanoseconds": 0,
            },
        )
        assert result == "2024-01-10"

    def test_duration_plus_date(self) -> None:
        result = _temporal_arith_pair(
            "+",
            {
                "days": 10,
                "months": 0,
                "years": 0,
                "weeks": 0,
                "hours": 0,
                "minutes": 0,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0,
                "nanoseconds": 0,
            },
            "2024-01-15",
        )
        assert result == "2024-01-25"

    def test_date_minus_date(self) -> None:
        result = _temporal_arith_pair("-", "2024-01-20", "2024-01-15")
        assert isinstance(result, dict)
        assert result["days"] == 5

    def test_datetime_plus_duration(self) -> None:
        result = _temporal_arith_pair(
            "+",
            "2024-01-15T10:00:00",
            {
                "days": 1,
                "hours": 2,
                "months": 0,
                "years": 0,
                "weeks": 0,
                "minutes": 0,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0,
                "nanoseconds": 0,
            },
        )
        assert "2024-01-16T12:00:00" in result

    def test_datetime_minus_datetime(self) -> None:
        result = _temporal_arith_pair(
            "-", "2024-01-20T12:00:00", "2024-01-15T12:00:00"
        )
        assert isinstance(result, dict)
        assert result["days"] == 5

    def test_duration_plus_duration(self) -> None:
        d1 = {
            "days": 5,
            "hours": 3,
            "months": 0,
            "years": 0,
            "weeks": 0,
            "minutes": 0,
            "seconds": 0,
            "milliseconds": 0,
            "microseconds": 0,
            "nanoseconds": 0,
        }
        d2 = {
            "days": 2,
            "hours": 1,
            "months": 0,
            "years": 0,
            "weeks": 0,
            "minutes": 0,
            "seconds": 0,
            "milliseconds": 0,
            "microseconds": 0,
            "nanoseconds": 0,
        }
        result = _temporal_arith_pair("+", d1, d2)
        assert result["days"] == 7
        assert result["hours"] == 4

    def test_null_left_returns_none(self) -> None:
        assert _temporal_arith_pair("+", None, "2024-01-15") is None

    def test_null_right_returns_none(self) -> None:
        assert _temporal_arith_pair("+", "2024-01-15", None) is None

    def test_unsupported_combination_raises(self) -> None:
        from pycypher.exceptions import TemporalArithmeticError

        with pytest.raises(TemporalArithmeticError):
            _temporal_arith_pair("+", "2024-01-15", 7)


# ===================================================================
# arithmetic_evaluator — class methods
# ===================================================================


class TestArithmeticEvaluatorClass:
    """Tests for ArithmeticExpressionEvaluator class methods."""

    def test_evaluate_arithmetic_add(self) -> None:
        frame = _make_frame(3)
        ev = ArithmeticExpressionEvaluator(frame)
        left_s = pd.Series([1, 2, 3])
        right_s = pd.Series([10, 20, 30])
        mock_eval = _make_evaluator({"L": left_s, "R": right_s})
        result = ev.evaluate_arithmetic("+", "L", "R", mock_eval)
        assert list(result) == [11, 22, 33]

    def test_evaluate_arithmetic_unsupported_op(self) -> None:
        frame = _make_frame(1)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"L": pd.Series([1]), "R": pd.Series([2])})
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_arithmetic("@", "L", "R", mock_eval)

    def test_evaluate_arithmetic_type_error(self) -> None:
        frame = _make_frame(1)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator(
            {"L": pd.Series(["a"]), "R": pd.Series([1])}
        )
        with pytest.raises(TypeError, match="incompatible"):
            ev.evaluate_arithmetic("-", "L", "R", mock_eval)

    def test_evaluate_comparison(self) -> None:
        frame = _make_frame(3)
        ev = ArithmeticExpressionEvaluator(frame)
        left_s = pd.Series([1, 2, 3])
        right_s = pd.Series([2, 2, 2])
        mock_eval = _make_evaluator({"L": left_s, "R": right_s})
        result = ev.evaluate_comparison("<", "L", "R", mock_eval)
        assert list(result) == [True, False, False]

    def test_evaluate_comparison_null_three_valued(self) -> None:
        frame = _make_frame(2)
        ev = ArithmeticExpressionEvaluator(frame)
        left_s = pd.Series([1, None], dtype=object)
        right_s = pd.Series([1, 2])
        mock_eval = _make_evaluator({"L": left_s, "R": right_s})
        result = ev.evaluate_comparison("=", "L", "R", mock_eval)
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712
        assert result.iloc[1] is None

    def test_evaluate_comparison_unsupported_op(self) -> None:
        frame = _make_frame(1)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"L": pd.Series([1]), "R": pd.Series([2])})
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_comparison("!=", "L", "R", mock_eval)

    def test_evaluate_unary_neg(self) -> None:
        frame = _make_frame(3)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([1, -2, 3])})
        result = ev.evaluate_unary("-", "X", mock_eval)
        assert list(result) == [-1, 2, -3]

    def test_evaluate_unary_pos(self) -> None:
        frame = _make_frame(2)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([5, -3])})
        result = ev.evaluate_unary("+", "X", mock_eval)
        assert list(result) == [5, -3]

    def test_evaluate_unary_null_propagation(self) -> None:
        frame = _make_frame(2)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([1, None], dtype=object)})
        result = ev.evaluate_unary("-", "X", mock_eval)
        assert result.iloc[0] == -1
        assert result.iloc[1] is None

    def test_evaluate_unary_unsupported(self) -> None:
        frame = _make_frame(1)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([1])})
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_unary("~", "X", mock_eval)

    def test_evaluate_arithmetic_temporal(self) -> None:
        """Arithmetic dispatch detects temporal values and routes to temporal handler."""
        frame = _make_frame(1)
        ev = ArithmeticExpressionEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "L": pd.Series(["2024-01-15"]),
                "R": pd.Series(
                    [
                        {
                            "days": 5,
                            "months": 0,
                            "years": 0,
                            "weeks": 0,
                            "hours": 0,
                            "minutes": 0,
                            "seconds": 0,
                            "milliseconds": 0,
                            "microseconds": 0,
                            "nanoseconds": 0,
                        }
                    ]
                ),
            }
        )
        result = ev.evaluate_arithmetic("+", "L", "R", mock_eval)
        assert result.iloc[0] == "2024-01-20"


# ===================================================================
# boolean_evaluator — pure Kleene functions
# ===================================================================


class TestKleeneAnd:
    def test_true_and_true(self) -> None:
        r = kleene_and(pd.Series([True]), pd.Series([True]))
        assert r.iloc[0] == False or r.iloc[0] is False or r.iloc[0] == True  # noqa: E712
        # More precise:
        assert bool(r.iloc[0]) is True

    def test_true_and_false(self) -> None:
        r = kleene_and(pd.Series([True]), pd.Series([False]))
        assert bool(r.iloc[0]) is False

    def test_false_and_null(self) -> None:
        r = kleene_and(pd.Series([False]), pd.Series([None], dtype=object))
        assert bool(r.iloc[0]) is False

    def test_true_and_null(self) -> None:
        r = kleene_and(pd.Series([True]), pd.Series([None], dtype=object))
        assert r.iloc[0] is None

    def test_null_and_null(self) -> None:
        r = kleene_and(
            pd.Series([None], dtype=object), pd.Series([None], dtype=object)
        )
        assert r.iloc[0] is None


class TestKleeneOr:
    def test_false_or_false(self) -> None:
        r = kleene_or(pd.Series([False]), pd.Series([False]))
        assert bool(r.iloc[0]) is False

    def test_true_or_false(self) -> None:
        r = kleene_or(pd.Series([True]), pd.Series([False]))
        assert bool(r.iloc[0]) is True

    def test_null_or_true(self) -> None:
        r = kleene_or(pd.Series([None], dtype=object), pd.Series([True]))
        assert bool(r.iloc[0]) is True

    def test_null_or_false(self) -> None:
        r = kleene_or(pd.Series([None], dtype=object), pd.Series([False]))
        assert r.iloc[0] is None


class TestKleeneXor:
    def test_true_xor_false(self) -> None:
        r = kleene_xor(pd.Series([True]), pd.Series([False]))
        assert bool(r.iloc[0]) is True

    def test_true_xor_true(self) -> None:
        r = kleene_xor(pd.Series([True]), pd.Series([True]))
        assert bool(r.iloc[0]) is False

    def test_null_xor_anything(self) -> None:
        r = kleene_xor(pd.Series([None], dtype=object), pd.Series([True]))
        assert r.iloc[0] is None


class TestKleeneNot:
    def test_not_true(self) -> None:
        r = kleene_not(pd.Series([True]))
        assert bool(r.iloc[0]) is False

    def test_not_false(self) -> None:
        r = kleene_not(pd.Series([False]))
        assert bool(r.iloc[0]) is True

    def test_not_null(self) -> None:
        r = kleene_not(pd.Series([None], dtype=object))
        assert r.iloc[0] is None


# ===================================================================
# boolean_evaluator — class methods
# ===================================================================


class TestBooleanEvaluatorClass:
    def test_evaluate_and_all_true(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "a": pd.Series([True, True]),
                "b": pd.Series([True, False]),
            }
        )
        result = ev.evaluate_and(["a", "b"], mock_eval)
        assert bool(result.iloc[0]) is True
        assert bool(result.iloc[1]) is False

    def test_evaluate_and_empty(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        result = ev.evaluate_and([], MagicMock())
        assert list(result) == [True, True]

    def test_evaluate_or(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "a": pd.Series([False, False]),
                "b": pd.Series([True, False]),
            }
        )
        result = ev.evaluate_or(["a", "b"], mock_eval)
        assert bool(result.iloc[0]) is True
        assert bool(result.iloc[1]) is False

    def test_evaluate_or_empty(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        result = ev.evaluate_or([], MagicMock())
        assert list(result) == [False, False]

    def test_evaluate_not(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        mock_eval = _make_evaluator({"x": pd.Series([True, False])})
        result = ev.evaluate_not("x", mock_eval)
        assert bool(result.iloc[0]) is False
        assert bool(result.iloc[1]) is True

    def test_evaluate_xor(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "a": pd.Series([True, True]),
                "b": pd.Series([False, True]),
            }
        )
        result = ev.evaluate_xor(["a", "b"], mock_eval)
        assert bool(result.iloc[0]) is True
        assert bool(result.iloc[1]) is False

    def test_evaluate_xor_empty(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        result = ev.evaluate_xor([], MagicMock())
        assert list(result) == [False, False]

    def test_evaluate_bool_chain_and(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "a": pd.Series([True, True]),
                "b": pd.Series([True, False]),
            }
        )
        result = ev.evaluate_bool_chain("and", ["a", "b"], mock_eval)
        assert bool(result.iloc[0]) is True
        assert bool(result.iloc[1]) is False

    def test_evaluate_bool_chain_unsupported(self) -> None:
        frame = _make_frame(1)
        ev = BooleanExpressionEvaluator(frame)
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_bool_chain("nand", ["a"], MagicMock())

    def test_evaluate_bool_chain_empty(self) -> None:
        frame = _make_frame(2)
        ev = BooleanExpressionEvaluator(frame)
        result = ev.evaluate_bool_chain("or", [], MagicMock())
        assert list(result) == [False, False]


# ===================================================================
# comparison_evaluator — class methods
# ===================================================================


class TestComparisonEvaluatorClass:
    def test_evaluate_comparison_eq(self) -> None:
        frame = _make_frame(3)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "L": pd.Series([1, 2, 3]),
                "R": pd.Series([1, 9, 3]),
            }
        )
        result = ev.evaluate_comparison("=", "L", "R", mock_eval)
        assert list(result) == [True, False, True]

    def test_evaluate_comparison_null_propagation(self) -> None:
        frame = _make_frame(2)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator(
            {
                "L": pd.Series([1, None], dtype=object),
                "R": pd.Series([1, 2]),
            }
        )
        result = ev.evaluate_comparison("=", "L", "R", mock_eval)
        assert result.iloc[1] is None

    def test_evaluate_comparison_unsupported_op(self) -> None:
        frame = _make_frame(1)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator({"L": pd.Series([1]), "R": pd.Series([2])})
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_comparison("!=", "L", "R", mock_eval)

    def test_evaluate_null_check_is_null(self) -> None:
        frame = _make_frame(3)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator(
            {"X": pd.Series([1, None, 3], dtype=object)}
        )
        result = ev.evaluate_null_check("IS NULL", "X", mock_eval)
        assert list(result) == [False, True, False]

    def test_evaluate_null_check_is_not_null(self) -> None:
        frame = _make_frame(3)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator(
            {"X": pd.Series([1, None, 3], dtype=object)}
        )
        result = ev.evaluate_null_check("IS NOT NULL", "X", mock_eval)
        assert list(result) == [True, False, True]

    def test_evaluate_null_check_unsupported(self) -> None:
        frame = _make_frame(1)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([1])})
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_null_check("IS EMPTY", "X", mock_eval)

    def test_evaluate_unary_neg(self) -> None:
        frame = _make_frame(2)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([5, -3])})
        result = ev.evaluate_unary("-", "X", mock_eval)
        assert list(result) == [-5, 3]

    def test_evaluate_unary_pos(self) -> None:
        frame = _make_frame(2)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([5, -3])})
        result = ev.evaluate_unary("+", "X", mock_eval)
        assert list(result) == [5, -3]

    def test_evaluate_unary_null_propagation(self) -> None:
        frame = _make_frame(2)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([1, None], dtype=object)})
        result = ev.evaluate_unary("-", "X", mock_eval)
        assert result.iloc[1] is None

    def test_evaluate_unary_unsupported(self) -> None:
        frame = _make_frame(1)
        ev = ComparisonEvaluator(frame)
        mock_eval = _make_evaluator({"X": pd.Series([1])})
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate_unary("~", "X", mock_eval)

    def test_evaluate_case_searched(self) -> None:
        """Searched CASE: CASE WHEN cond THEN val ELSE default END."""
        frame = _make_frame(3)
        ev = ComparisonEvaluator(frame)

        cond_series = pd.Series([True, False, True])
        then_series = pd.Series(["yes", "yes", "yes"])
        else_series = pd.Series(["no", "no", "no"])

        clause = SimpleNamespace(
            condition="cond",
            result="then",
        )

        lookup = {
            "cond": cond_series,
            "then": then_series,
            "else": else_series,
        }
        mock_eval = _make_evaluator(lookup)

        result = ev.evaluate_case(None, [clause], "else", mock_eval)
        assert list(result) == ["yes", "no", "yes"]

    def test_evaluate_case_simple(self) -> None:
        """Simple CASE: CASE expr WHEN match THEN val END."""
        frame = _make_frame(3)
        ev = ComparisonEvaluator(frame)

        disc_series = pd.Series([1, 2, 3])
        match_series = pd.Series([2, 2, 2])
        then_series = pd.Series(["hit", "hit", "hit"])

        clause = SimpleNamespace(condition="match", result="then")
        lookup = {
            "disc": disc_series,
            "match": match_series,
            "then": then_series,
        }
        mock_eval = _make_evaluator(lookup)

        result = ev.evaluate_case("disc", [clause], None, mock_eval)
        assert pd.isna(result.iloc[0])  # 1 != 2
        assert result.iloc[1] == "hit"  # 2 == 2
        assert pd.isna(result.iloc[2])  # 3 != 2

    def test_evaluate_case_no_else_yields_none(self) -> None:
        """CASE with no ELSE and no matching WHEN → null."""
        frame = _make_frame(2)
        ev = ComparisonEvaluator(frame)

        cond_series = pd.Series([False, False])
        then_series = pd.Series(["x", "x"])
        clause = SimpleNamespace(condition="cond", result="then")
        mock_eval = _make_evaluator({"cond": cond_series, "then": then_series})

        result = ev.evaluate_case(None, [clause], None, mock_eval)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])

    def test_evaluate_case_multiple_whens_first_wins(self) -> None:
        """First matching WHEN clause wins."""
        frame = _make_frame(1)
        ev = ComparisonEvaluator(frame)

        clause1 = SimpleNamespace(condition="c1", result="t1")
        clause2 = SimpleNamespace(condition="c2", result="t2")
        lookup = {
            "c1": pd.Series([True]),
            "t1": pd.Series(["first"]),
            "c2": pd.Series([True]),
            "t2": pd.Series(["second"]),
        }
        mock_eval = _make_evaluator(lookup)

        result = ev.evaluate_case(None, [clause1, clause2], None, mock_eval)
        assert result.iloc[0] == "first"
