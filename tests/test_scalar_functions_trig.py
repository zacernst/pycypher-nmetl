"""Tests for trigonometric and advanced math scalar functions.

TDD red phase — verifies that sin, cos, tan, asin, acos, atan, atan2,
degrees, radians, pi, e, log10, and pow are registered and correct.
"""

import math

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry


@pytest.fixture(autouse=True)
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


def _s(*values: object) -> pd.Series:
    """Convenience: build a Series from the given values."""
    return pd.Series(list(values))


def approx_equal(
    series: pd.Series, expected: float, rel: float = 1e-9
) -> bool:
    """Return True if the single value in series is close to expected."""
    val = series.iloc[0]
    if math.isnan(expected):
        return math.isnan(float(val))
    return math.isclose(float(val), expected, rel_tol=rel)


# ─────────────────────────────────────────────────────────────────────────────
# sin / cos / tan
# ─────────────────────────────────────────────────────────────────────────────


class TestSinCosTan:
    def test_sin_zero(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("sin")
        result = registry.execute("sin", [_s(0.0)])
        assert approx_equal(result, 0.0)

    def test_sin_half_pi(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sin", [_s(math.pi / 2)])
        assert approx_equal(result, 1.0)

    def test_sin_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sin", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_cos_zero(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("cos")
        result = registry.execute("cos", [_s(0.0)])
        assert approx_equal(result, 1.0)

    def test_cos_pi(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cos", [_s(math.pi)])
        assert approx_equal(result, -1.0)

    def test_cos_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cos", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_tan_zero(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("tan")
        result = registry.execute("tan", [_s(0.0)])
        assert approx_equal(result, 0.0)

    def test_tan_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("tan", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_vectorized_sin(self, registry: ScalarFunctionRegistry) -> None:
        """sin works on multi-row Series; sin(π) is nearly 0 (floating-point)."""
        result = registry.execute("sin", [_s(0.0, math.pi / 2, math.pi)])
        assert approx_equal(result.iloc[[0]], 0.0)
        assert approx_equal(result.iloc[[1]], 1.0)
        # sin(π) ≈ 1.22e-16 due to floating-point — use loose tolerance
        assert abs(float(result.iloc[2])) < 1e-10


# ─────────────────────────────────────────────────────────────────────────────
# asin / acos / atan / atan2
# ─────────────────────────────────────────────────────────────────────────────


class TestInverseTrig:
    def test_asin_zero(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("asin")
        result = registry.execute("asin", [_s(0.0)])
        assert approx_equal(result, 0.0)

    def test_asin_one(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("asin", [_s(1.0)])
        assert approx_equal(result, math.pi / 2)

    def test_asin_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("asin", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_acos_one(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("acos")
        result = registry.execute("acos", [_s(1.0)])
        assert approx_equal(result, 0.0)

    def test_acos_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("acos", [_s(0.0)])
        assert approx_equal(result, math.pi / 2)

    def test_acos_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("acos", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_atan_zero(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("atan")
        result = registry.execute("atan", [_s(0.0)])
        assert approx_equal(result, 0.0)

    def test_atan_one(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("atan", [_s(1.0)])
        assert approx_equal(result, math.pi / 4)

    def test_atan_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("atan", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_atan2_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("atan2")

    def test_atan2_quadrant1(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("atan2", [_s(1.0), _s(1.0)])
        assert approx_equal(result, math.pi / 4)

    def test_atan2_positive_y_zero_x(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("atan2", [_s(1.0), _s(0.0)])
        assert approx_equal(result, math.pi / 2)

    def test_atan2_null_y(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("atan2", [_s(None), _s(1.0)])
        assert pd.isna(result.iloc[0])

    def test_atan2_null_x(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("atan2", [_s(1.0), _s(None)])
        assert pd.isna(result.iloc[0])


# ─────────────────────────────────────────────────────────────────────────────
# degrees / radians
# ─────────────────────────────────────────────────────────────────────────────


class TestDegreesRadians:
    def test_degrees_pi_is_180(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("degrees")
        result = registry.execute("degrees", [_s(math.pi)])
        assert approx_equal(result, 180.0)

    def test_degrees_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("degrees", [_s(0.0)])
        assert approx_equal(result, 0.0)

    def test_degrees_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("degrees", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_radians_180_is_pi(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("radians")
        result = registry.execute("radians", [_s(180.0)])
        assert approx_equal(result, math.pi)

    def test_radians_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("radians", [_s(0.0)])
        assert approx_equal(result, 0.0)

    def test_radians_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("radians", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_degrees_radians_roundtrip(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result_rad = registry.execute("radians", [_s(90.0)])
        result_deg = registry.execute("degrees", [result_rad])
        assert approx_equal(result_deg, 90.0)


# ─────────────────────────────────────────────────────────────────────────────
# pi / e  (zero-argument functions)
# ─────────────────────────────────────────────────────────────────────────────


class TestPiAndE:
    def test_pi_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("pi")

    def test_pi_value(self, registry: ScalarFunctionRegistry) -> None:
        # pi() takes no arguments — pass a dummy single-row series
        result = registry.execute("pi", [_s(0)])  # dummy arg ignored
        assert approx_equal(result, math.pi)

    def test_e_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("e")

    def test_e_value(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("e", [_s(0)])  # dummy arg ignored
        assert approx_equal(result, math.e)


# ─────────────────────────────────────────────────────────────────────────────
# log10
# ─────────────────────────────────────────────────────────────────────────────


class TestLog10:
    def test_log10_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("log10")

    def test_log10_100(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [_s(100.0)])
        assert approx_equal(result, 2.0)

    def test_log10_1(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [_s(1.0)])
        assert approx_equal(result, 0.0)

    def test_log10_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_log10_zero_returns_none(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """log10(0) is undefined; Cypher spec requires null (None), not NaN."""
        result = registry.execute("log10", [_s(0.0)])
        assert pd.isna(result.iloc[0])

    def test_log10_negative_returns_none(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """log10 of a negative number is undefined; must return null."""
        result = registry.execute("log10", [_s(-5.0)])
        assert pd.isna(result.iloc[0])

    def test_log10_vectorized(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [_s(1.0, 10.0, 100.0)])
        assert approx_equal(result.iloc[[0]], 0.0)
        assert approx_equal(result.iloc[[1]], 1.0)
        assert approx_equal(result.iloc[[2]], 2.0)


# ─────────────────────────────────────────────────────────────────────────────
# pow
# ─────────────────────────────────────────────────────────────────────────────


class TestPow:
    def test_pow_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("pow")

    def test_pow_2_10(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [_s(2.0), _s(10.0)])
        assert approx_equal(result, 1024.0)

    def test_pow_3_3(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [_s(3.0), _s(3.0)])
        assert approx_equal(result, 27.0)

    def test_pow_zero_exponent(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [_s(5.0), _s(0.0)])
        assert approx_equal(result, 1.0)

    def test_pow_null_base(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [_s(None), _s(2.0)])
        assert pd.isna(result.iloc[0])

    def test_pow_null_exp(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [_s(2.0), _s(None)])
        assert pd.isna(result.iloc[0])

    def test_pow_vectorized(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute(
            "pow", [_s(2.0, 3.0, 4.0), _s(2.0, 2.0, 2.0)]
        )
        assert approx_equal(result.iloc[[0]], 4.0)
        assert approx_equal(result.iloc[[1]], 9.0)
        assert approx_equal(result.iloc[[2]], 16.0)


# ─────────────────────────────────────────────────────────────────────────────
# Two-argument function length-mismatch guards
# ─────────────────────────────────────────────────────────────────────────────


class TestTwoArgLengthMismatch:
    """atan2() and pow() must raise ValueError when Series lengths differ.

    Both functions use zip() internally, which silently truncates to the
    shorter sequence.  This hidden truncation would produce wrong row counts
    without any signal to the caller.  A clear ValueError is far better.
    """

    def test_atan2_length_mismatch_raises(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """atan2 with mismatched Series lengths must raise ValueError."""
        with pytest.raises(ValueError, match="same length"):
            registry.execute("atan2", [_s(1.0, 2.0, 3.0), _s(1.0, 1.0)])

    def test_pow_length_mismatch_raises(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """pow with mismatched Series lengths must raise ValueError."""
        with pytest.raises(ValueError, match="same length"):
            registry.execute("pow", [_s(2.0, 3.0), _s(2.0, 2.0, 2.0)])

    def test_atan2_equal_lengths_still_works(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """Same-length Series must not be rejected."""
        result = registry.execute("atan2", [_s(1.0, 0.0), _s(1.0, 1.0)])
        assert len(result) == 2

    def test_pow_equal_lengths_still_works(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """Same-length Series must not be rejected."""
        result = registry.execute("pow", [_s(2.0, 3.0), _s(3.0, 2.0)])
        assert len(result) == 2
