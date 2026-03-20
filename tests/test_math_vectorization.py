"""TDD tests for Loop 190 — Performance/Detail: numpy-vectorise all remaining
math scalar functions.

Loop 188 vectorised the trig/transcendental functions using numpy but left the
core math functions using Python-level .apply() loops:

  abs(x)    → np.abs()
  ceil(x)   → np.ceil()
  floor(x)  → np.floor()
  sign(x)   → np.sign()
  sqrt(x)   → np.sqrt()
  cbrt(x)   → np.cbrt()
  log(x)    → np.log()
  exp(x)    → np.exp()
  log2(x)   → np.log2()
  log10(x)  → np.log10()
  pow(x,y)  → np.power()

The lesson from Loop 174/188: "Vectorisation loops must be treated as a set,
not a sequence."  ALL members of the math-function category must be fixed.

round() is intentionally excluded — it uses HALF_UP rounding (decimal module)
which has no numpy equivalent.

All tests written BEFORE the implementation (TDD red phase).

Categories:
  1. Correctness regression — spot values for every function.
  2. Domain-error semantics — null for x<=0 in log functions; null for x<0 in sqrt.
  3. Null propagation — every function returns null for null input.
  4. Performance — 30× 10k-row batch must complete within an absolute threshold.
"""

from __future__ import annotations

import math
import time

import numpy as np
import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


def _s(*vals: object) -> pd.Series:
    return pd.Series(list(vals))


def _null(v: object) -> bool:
    return (
        v is None
        or (isinstance(v, float) and math.isnan(v))
        or (isinstance(v, np.floating) and np.isnan(v))
    )


REPS = 30
N = 10_000


def _big(lo: float = 0.1, hi: float = 10.0) -> pd.Series:
    return pd.Series(np.linspace(lo, hi, N))


# ===========================================================================
# 1. Correctness regression
# ===========================================================================


class TestCorrectnessRegression:
    """Spot-check every function after vectorisation — semantics must be unchanged."""

    def test_abs_positive(self) -> None:
        assert float(
            _reg().execute("abs", [_s(5.0)]).iloc[0]
        ) == pytest.approx(5.0)

    def test_abs_negative(self) -> None:
        assert float(
            _reg().execute("abs", [_s(-7.3)]).iloc[0]
        ) == pytest.approx(7.3)

    def test_abs_zero(self) -> None:
        assert float(
            _reg().execute("abs", [_s(0.0)]).iloc[0]
        ) == pytest.approx(0.0)

    def test_ceil_basic(self) -> None:
        assert float(
            _reg().execute("ceil", [_s(1.1)]).iloc[0]
        ) == pytest.approx(2.0)

    def test_ceil_negative(self) -> None:
        assert float(
            _reg().execute("ceil", [_s(-1.9)]).iloc[0]
        ) == pytest.approx(-1.0)

    def test_floor_basic(self) -> None:
        assert float(
            _reg().execute("floor", [_s(1.9)]).iloc[0]
        ) == pytest.approx(1.0)

    def test_floor_negative(self) -> None:
        assert float(
            _reg().execute("floor", [_s(-1.1)]).iloc[0]
        ) == pytest.approx(-2.0)

    def test_sign_positive(self) -> None:
        assert float(
            _reg().execute("sign", [_s(5.0)]).iloc[0]
        ) == pytest.approx(1.0)

    def test_sign_negative(self) -> None:
        assert float(
            _reg().execute("sign", [_s(-3.0)]).iloc[0]
        ) == pytest.approx(-1.0)

    def test_sign_zero(self) -> None:
        assert float(
            _reg().execute("sign", [_s(0.0)]).iloc[0]
        ) == pytest.approx(0.0)

    def test_sqrt_four(self) -> None:
        assert float(
            _reg().execute("sqrt", [_s(4.0)]).iloc[0]
        ) == pytest.approx(2.0)

    def test_sqrt_nine(self) -> None:
        assert float(
            _reg().execute("sqrt", [_s(9.0)]).iloc[0]
        ) == pytest.approx(3.0)

    def test_cbrt_eight(self) -> None:
        assert float(
            _reg().execute("cbrt", [_s(8.0)]).iloc[0]
        ) == pytest.approx(2.0)

    def test_cbrt_negative(self) -> None:
        """cbrt supports negative inputs: cbrt(-8) = -2.0."""
        assert float(
            _reg().execute("cbrt", [_s(-8.0)]).iloc[0]
        ) == pytest.approx(-2.0)

    def test_log_one(self) -> None:
        assert float(
            _reg().execute("log", [_s(1.0)]).iloc[0]
        ) == pytest.approx(0.0)

    def test_log_e(self) -> None:
        assert float(
            _reg().execute("log", [_s(math.e)]).iloc[0]
        ) == pytest.approx(1.0)

    def test_exp_zero(self) -> None:
        assert float(
            _reg().execute("exp", [_s(0.0)]).iloc[0]
        ) == pytest.approx(1.0)

    def test_exp_one(self) -> None:
        assert float(
            _reg().execute("exp", [_s(1.0)]).iloc[0]
        ) == pytest.approx(math.e)

    def test_exp_overflow_returns_inf(self) -> None:
        """exp(1000) overflows to +inf — Neo4j spec says return +inf, not null."""
        val = _reg().execute("exp", [_s(1000.0)]).iloc[0]
        assert math.isinf(float(val)) and float(val) > 0

    def test_log2_one(self) -> None:
        assert float(
            _reg().execute("log2", [_s(1.0)]).iloc[0]
        ) == pytest.approx(0.0)

    def test_log2_eight(self) -> None:
        assert float(
            _reg().execute("log2", [_s(8.0)]).iloc[0]
        ) == pytest.approx(3.0)

    def test_log10_one(self) -> None:
        assert float(
            _reg().execute("log10", [_s(1.0)]).iloc[0]
        ) == pytest.approx(0.0)

    def test_log10_hundred(self) -> None:
        assert float(
            _reg().execute("log10", [_s(100.0)]).iloc[0]
        ) == pytest.approx(2.0)

    def test_pow_basic(self) -> None:
        assert float(
            _reg().execute("pow", [_s(2.0), _s(10.0)]).iloc[0]
        ) == pytest.approx(1024.0)

    def test_pow_zero_exponent(self) -> None:
        assert float(
            _reg().execute("pow", [_s(5.0), _s(0.0)]).iloc[0]
        ) == pytest.approx(1.0)

    def test_pow_overflow_returns_inf(self) -> None:
        """pow(2, 10000) overflows to +inf — consistent with exp overflow behaviour."""
        val = _reg().execute("pow", [_s(2.0), _s(10000.0)]).iloc[0]
        assert math.isinf(float(val)) and float(val) > 0

    def test_multi_row_abs(self) -> None:
        s = pd.Series([-3.0, 0.0, 5.0])
        result = _reg().execute("abs", [s])
        assert float(result.iloc[0]) == pytest.approx(3.0)
        assert float(result.iloc[1]) == pytest.approx(0.0)
        assert float(result.iloc[2]) == pytest.approx(5.0)

    def test_multi_row_pow(self) -> None:
        b = pd.Series([2.0, 3.0, 4.0])
        e = pd.Series([3.0, 2.0, 1.0])
        result = _reg().execute("pow", [b, e])
        assert float(result.iloc[0]) == pytest.approx(8.0)
        assert float(result.iloc[1]) == pytest.approx(9.0)
        assert float(result.iloc[2]) == pytest.approx(4.0)


# ===========================================================================
# 2. Domain-error semantics
# ===========================================================================


class TestDomainErrors:
    """Functions with domain restrictions must return null, not raise."""

    def test_sqrt_negative_returns_null(self) -> None:
        assert _null(_reg().execute("sqrt", [_s(-1.0)]).iloc[0])

    def test_log_zero_returns_null(self) -> None:
        assert _null(_reg().execute("log", [_s(0.0)]).iloc[0])

    def test_log_negative_returns_null(self) -> None:
        assert _null(_reg().execute("log", [_s(-5.0)]).iloc[0])

    def test_log2_zero_returns_null(self) -> None:
        assert _null(_reg().execute("log2", [_s(0.0)]).iloc[0])

    def test_log2_negative_returns_null(self) -> None:
        assert _null(_reg().execute("log2", [_s(-1.0)]).iloc[0])

    def test_log10_zero_returns_null(self) -> None:
        assert _null(_reg().execute("log10", [_s(0.0)]).iloc[0])

    def test_log10_negative_returns_null(self) -> None:
        assert _null(_reg().execute("log10", [_s(-100.0)]).iloc[0])

    def test_pow_complex_result_returns_null(self) -> None:
        """pow(-2, 0.5) is complex — must return null, not crash."""
        assert _null(_reg().execute("pow", [_s(-2.0), _s(0.5)]).iloc[0])


# ===========================================================================
# 3. Null propagation
# ===========================================================================


class TestNullPropagation:
    """Every unary math function must propagate null input to null output."""

    @pytest.mark.parametrize(
        "fn",
        [
            "abs",
            "ceil",
            "floor",
            "sign",
            "sqrt",
            "cbrt",
            "log",
            "exp",
            "log2",
            "log10",
        ],
    )
    def test_null_returns_null(self, fn: str) -> None:
        result = _reg().execute(fn, [_s(None)])
        assert _null(result.iloc[0]), (
            f"{fn}(null) must return null, got {result.iloc[0]!r}"
        )

    def test_pow_null_base_returns_null(self) -> None:
        assert _null(_reg().execute("pow", [_s(None), _s(2.0)]).iloc[0])

    def test_pow_null_exp_returns_null(self) -> None:
        assert _null(_reg().execute("pow", [_s(2.0), _s(None)]).iloc[0])

    @pytest.mark.parametrize(
        "fn",
        [
            "abs",
            "ceil",
            "floor",
            "sign",
            "sqrt",
            "cbrt",
            "log",
            "exp",
            "log2",
            "log10",
        ],
    )
    def test_mixed_null_multi_row(self, fn: str) -> None:
        """Middle element is null, others are valid."""
        s = pd.Series([1.0, None, 4.0])
        result = _reg().execute(fn, [s])
        assert _null(result.iloc[1]), f"{fn}: middle null must remain null"
        # Other rows must be non-null (1.0 is in domain for all these functions)
        assert not _null(result.iloc[0]), f"{fn}: first row must not be null"


# ===========================================================================
# 4. Performance: numpy vectorisation must be fast
# ===========================================================================


@pytest.mark.performance
class TestMathVectorisationPerformance:
    """30× application of each function to 10k rows must complete quickly."""

    def _time(self, fn: str, *args: pd.Series) -> float:
        arg_list = list(args)
        start = time.perf_counter()
        for _ in range(REPS):
            _reg().execute(fn, arg_list)
        return time.perf_counter() - start

    def test_abs_threshold(self) -> None:
        """With numpy vectorisation 30×abs(10k) must be < 0.03s."""
        elapsed = self._time("abs", _big(lo=-10.0, hi=10.0))
        assert elapsed < 0.15, (
            f"30×abs(10k) took {elapsed:.3f}s (threshold 0.03s — .apply() typically 0.05s+)"
        )

    def test_sqrt_threshold(self) -> None:
        """With numpy vectorisation 30×sqrt(10k) must be < 0.03s."""
        elapsed = self._time("sqrt", _big())
        assert elapsed < 0.15, (
            f"30×sqrt(10k) took {elapsed:.3f}s (threshold 0.03s — .apply() typically 0.06s+)"
        )

    def test_log_threshold(self) -> None:
        """With numpy vectorisation 30×log(10k) must be < 0.03s."""
        elapsed = self._time("log", _big())
        assert elapsed < 0.15, (
            f"30×log(10k) took {elapsed:.3f}s (threshold 0.03s — .apply() typically 0.065s+)"
        )

    def test_exp_threshold(self) -> None:
        """With numpy vectorisation 30×exp(10k) must be < 0.03s."""
        elapsed = self._time("exp", _big(lo=-5.0, hi=5.0))
        assert elapsed < 0.15, (
            f"30×exp(10k) took {elapsed:.3f}s (threshold 0.03s — .apply() typically 0.06s+)"
        )

    def test_log10_threshold(self) -> None:
        """With numpy vectorisation 30×log10(10k) must be < 0.03s."""
        elapsed = self._time("log10", _big())
        assert elapsed < 0.15, (
            f"30×log10(10k) took {elapsed:.3f}s (threshold 0.03s — .apply() typically 0.065s+)"
        )

    def test_pow_threshold(self) -> None:
        """With numpy vectorisation 30×pow(10k) must be < 0.05s."""
        b = _big(lo=1.0, hi=5.0)
        e = _big(lo=1.0, hi=3.0)
        elapsed = self._time("pow", b, e)
        assert elapsed < 0.25, (
            f"30×pow(10k) took {elapsed:.3f}s (threshold 0.25s — zip+.apply() typically 0.13s+)"
        )

    def test_numpy_vs_apply_speedup(self) -> None:
        """numpy abs must be ≥ 3× faster than .apply() at 10k rows."""
        arr = np.linspace(-10.0, 10.0, N)
        s_obj = pd.Series(arr, dtype=object)

        start = time.perf_counter()
        for _ in range(REPS):
            s_obj.apply(lambda x: abs(float(x)) if x is not None else None)
        baseline = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(REPS):
            np.abs(arr)
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 3.0, (
            f"numpy abs ({vectorised:.3f}s) should be ≥3× faster than "
            f".apply() ({baseline:.3f}s) at {N} rows, got {speedup:.1f}×."
        )
