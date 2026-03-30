"""Dedicated unit tests for scalar_functions/math_functions.py.

Tests each math function directly via the ScalarFunctionRegistry,
covering normal cases, null handling, domain errors, and edge cases.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry


@pytest.fixture(scope="module")
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


# ---------------------------------------------------------------------------
# abs
# ---------------------------------------------------------------------------


class TestAbs:
    def test_positive(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("abs", [pd.Series([5.0])])
        assert result.iloc[0] == 5.0

    def test_negative(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("abs", [pd.Series([-5.0])])
        assert result.iloc[0] == 5.0

    def test_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("abs", [pd.Series([0.0])])
        assert result.iloc[0] == 0.0

    def test_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("abs", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# ceil / floor
# ---------------------------------------------------------------------------


class TestCeil:
    def test_ceil_positive(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("ceil", [pd.Series([1.1])])
        assert result.iloc[0] == 2.0

    def test_ceil_negative(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("ceil", [pd.Series([-1.9])])
        assert result.iloc[0] == -1.0

    def test_ceil_integer(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("ceil", [pd.Series([3.0])])
        assert result.iloc[0] == 3.0

    def test_ceil_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("ceil", [pd.Series([None])])
        assert result.iloc[0] is None


class TestFloor:
    def test_floor_positive(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("floor", [pd.Series([1.9])])
        assert result.iloc[0] == 1.0

    def test_floor_negative(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("floor", [pd.Series([-1.1])])
        assert result.iloc[0] == -2.0

    def test_floor_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("floor", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# round (1-arg, 2-arg, 3-arg)
# ---------------------------------------------------------------------------


class TestRound:
    def test_round_default(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("round", [pd.Series([2.5])])
        assert result.iloc[0] == 3.0  # HALF_UP: ties away from zero

    def test_round_with_precision(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("round", [pd.Series([1.567]), pd.Series([2])])
        assert abs(result.iloc[0] - 1.57) < 1e-10

    def test_round_half_even(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute(
            "round",
            [pd.Series([2.5]), pd.Series([0]), pd.Series(["HALF_EVEN"])],
        )
        assert result.iloc[0] == 2.0  # Banker's rounding: tie to even

    def test_round_invalid_mode(self, registry: ScalarFunctionRegistry) -> None:
        with pytest.raises(ValueError, match="Unknown rounding mode"):
            registry.execute(
                "round",
                [pd.Series([1.5]), pd.Series([0]), pd.Series(["INVALID"])],
            )

    def test_round_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("round", [pd.Series([None])])
        assert math.isnan(result.iloc[0]) or result.iloc[0] is None

    def test_round_zero_precision(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("round", [pd.Series([1.5]), pd.Series([0])])
        assert result.iloc[0] == 2.0  # HALF_UP default

    def test_round_ceiling_mode(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute(
            "round",
            [pd.Series([1.1]), pd.Series([0]), pd.Series(["CEILING"])],
        )
        assert result.iloc[0] == 2.0

    def test_round_floor_mode(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute(
            "round",
            [pd.Series([1.9]), pd.Series([0]), pd.Series(["FLOOR"])],
        )
        assert result.iloc[0] == 1.0


# ---------------------------------------------------------------------------
# sign
# ---------------------------------------------------------------------------


class TestSign:
    def test_positive(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sign", [pd.Series([42.0])])
        assert result.iloc[0] == 1.0

    def test_negative(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sign", [pd.Series([-3.0])])
        assert result.iloc[0] == -1.0

    def test_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sign", [pd.Series([0.0])])
        assert result.iloc[0] == 0.0

    def test_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sign", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# sqrt
# ---------------------------------------------------------------------------


class TestSqrt:
    def test_perfect_square(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sqrt", [pd.Series([9.0])])
        assert abs(result.iloc[0] - 3.0) < 1e-10

    def test_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sqrt", [pd.Series([0.0])])
        assert result.iloc[0] == 0.0

    def test_negative_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sqrt", [pd.Series([-1.0])])
        assert result.iloc[0] is None

    def test_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sqrt", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# cbrt
# ---------------------------------------------------------------------------


class TestCbrt:
    def test_positive(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cbrt", [pd.Series([27.0])])
        assert abs(result.iloc[0] - 3.0) < 1e-10

    def test_negative(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cbrt", [pd.Series([-8.0])])
        assert abs(result.iloc[0] - (-2.0)) < 1e-10

    def test_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cbrt", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# log / exp
# ---------------------------------------------------------------------------


class TestLog:
    def test_log_one(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log", [pd.Series([1.0])])
        assert abs(result.iloc[0]) < 1e-10

    def test_log_e(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log", [pd.Series([math.e])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_log_zero_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log", [pd.Series([0.0])])
        assert result.iloc[0] is None

    def test_log_negative_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log", [pd.Series([-1.0])])
        assert result.iloc[0] is None


class TestExp:
    def test_exp_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("exp", [pd.Series([0.0])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_exp_one(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("exp", [pd.Series([1.0])])
        assert abs(result.iloc[0] - math.e) < 1e-10

    def test_exp_overflow_returns_inf(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("exp", [pd.Series([1000.0])])
        assert result.iloc[0] == float("inf")

    def test_exp_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("exp", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# cot / haversin
# ---------------------------------------------------------------------------


class TestCot:
    def test_cot_pi_over_4(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cot", [pd.Series([math.pi / 4])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_cot_zero_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cot", [pd.Series([0.0])])
        assert result.iloc[0] is None  # Division by zero

    def test_cot_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cot", [pd.Series([None])])
        assert result.iloc[0] is None


class TestHaversin:
    def test_haversin_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("haversin", [pd.Series([0.0])])
        assert abs(result.iloc[0]) < 1e-10

    def test_haversin_pi(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("haversin", [pd.Series([math.pi])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_haversin_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("haversin", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# hypot / fmod
# ---------------------------------------------------------------------------


class TestHypot:
    def test_345_triangle(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("hypot", [pd.Series([3.0]), pd.Series([4.0])])
        assert abs(result.iloc[0] - 5.0) < 1e-10

    def test_null_propagation(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("hypot", [pd.Series([None]), pd.Series([4.0])])
        assert result.iloc[0] is None

    def test_length_mismatch(self, registry: ScalarFunctionRegistry) -> None:
        with pytest.raises(ValueError, match="same length"):
            registry.execute("hypot", [pd.Series([1.0, 2.0]), pd.Series([3.0])])


class TestFmod:
    def test_basic(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("fmod", [pd.Series([10.0]), pd.Series([3.0])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_division_by_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("fmod", [pd.Series([5.0]), pd.Series([0.0])])
        assert result.iloc[0] is None

    def test_null_propagation(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("fmod", [pd.Series([None]), pd.Series([3.0])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# log2
# ---------------------------------------------------------------------------


class TestLog2:
    def test_log2_8(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log2", [pd.Series([8.0])])
        assert abs(result.iloc[0] - 3.0) < 1e-10

    def test_log2_zero_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log2", [pd.Series([0.0])])
        assert result.iloc[0] is None

    def test_log2_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log2", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# Bitwise functions
# ---------------------------------------------------------------------------


class TestBitwise:
    def test_bit_and(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitAnd", [pd.Series([12]), pd.Series([10])])
        assert result.iloc[0] == 8

    def test_bit_or(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitOr", [pd.Series([12]), pd.Series([10])])
        assert result.iloc[0] == 14

    def test_bit_xor(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitXor", [pd.Series([12]), pd.Series([10])])
        assert result.iloc[0] == 6

    def test_bit_not(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitNot", [pd.Series([0])])
        assert result.iloc[0] == -1

    def test_bit_shift_left(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitShiftLeft", [pd.Series([1]), pd.Series([3])])
        assert result.iloc[0] == 8

    def test_bit_shift_right(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitShiftRight", [pd.Series([16]), pd.Series([2])])
        assert result.iloc[0] == 4

    def test_null_propagation(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("bitAnd", [pd.Series([None]), pd.Series([10])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# gcd / lcm
# ---------------------------------------------------------------------------


class TestGcdLcm:
    def test_gcd(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("gcd", [pd.Series([12]), pd.Series([8])])
        assert result.iloc[0] == 4

    def test_gcd_with_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("gcd", [pd.Series([0]), pd.Series([5])])
        assert result.iloc[0] == 5

    def test_gcd_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("gcd", [pd.Series([None]), pd.Series([5])])
        assert result.iloc[0] is None

    def test_lcm(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("lcm", [pd.Series([4]), pd.Series([6])])
        assert result.iloc[0] == 12

    def test_lcm_with_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("lcm", [pd.Series([0]), pd.Series([5])])
        assert result.iloc[0] == 0


# ---------------------------------------------------------------------------
# Vectorization: multiple rows
# ---------------------------------------------------------------------------


class TestVectorized:
    def test_abs_multiple_rows(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("abs", [pd.Series([-1.0, -2.0, 3.0, None])])
        assert result.iloc[0] == 1.0
        assert result.iloc[1] == 2.0
        assert result.iloc[2] == 3.0
        assert result.iloc[3] is None

    def test_head_multiple_rows(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("head", [pd.Series([[1, 2], [3, 4], None])])
        assert result.iloc[0] == 1
        assert result.iloc[1] == 3
        assert result.iloc[2] is None
