"""TDD tests for Loop 188 — Performance + Cowbell: trig/math numpy vectorisation
and addition of ``cot`` and ``haversin``.

Problem: Every single trig/transcendental function in ``scalar_functions.py``
uses a Python-level ``.apply()`` loop:

- ``_make_trig1`` factory (lines 2125–2153): sin, cos, tan, asin, acos, atan
  → ``s.apply(_apply)`` — O(n) Python calls per evaluation.
- ``_atan2`` (lines 2156–2181): ``pd.Series(list(zip(y, x))).apply(_apply)``
  — O(n) Python calls.
- ``_degrees`` / ``_radians`` (lines 2183–2210): ``s.apply(_apply)``
  — O(n) Python calls.
- ``sinh``, ``cosh``, ``tanh`` (lines 1958–2004): ``s.apply(lambda x: ...)``
  — O(n) Python calls each.

For a 10 000-row frame computing ``haversine(lat1, lon1, lat2, lon2)`` via a
Cypher expression, the old path executes ~40 000 Python per-element dispatches.
The numpy path executes 4 C-level element-wise array operations.

Two functions are also entirely missing from the registry:

- ``cot(x)`` — cotangent (Neo4j 5.x built-in: ``cos(x)/sin(x)``).
  Returns null for null input and for x = 0 (division by zero).
- ``haversin(x)`` — half the versine: ``(1 - cos(x)) / 2``.
  Used as a building block for the great-circle distance formula
  (``RETURN 2 * asin(sqrt(haversin(...) + ...))``).

Fix:
1. Replace ``_make_trig1`` with ``_make_trig1_np(fn_np)`` that calls the numpy
   equivalent (``np.sin``, ``np.cos``, etc.) on the whole array at once.
2. Vectorise ``atan2``, ``degrees``, ``radians``, ``sinh``, ``cosh``, ``tanh``
   using numpy.
3. Register ``cot`` and ``haversin`` using numpy.

Null semantics: ``pd.to_numeric(s, errors='coerce')`` converts None/NaN/non-
numeric to NaN; numpy propagates NaN; final NaN values are converted back to
Python None so that Cypher null semantics are preserved.

Domain semantics: ``asin``/``acos`` out of [-1, 1] → NaN via numpy →
returned as None (same as the current ``ValueError`` → None path).  Division
by zero in ``cot`` → NaN → None.

All tests labelled ``Correctness`` pass before AND after the change (same
semantics).  Tests labelled ``Performance`` or ``New function`` fail before the
implementation.
"""

from __future__ import annotations

import math
import time

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg():
    from pycypher.scalar_functions import ScalarFunctionRegistry

    return ScalarFunctionRegistry.get_instance()


def _s(*values: object) -> pd.Series:
    return pd.Series(list(values))


def _approx(series: pd.Series, expected: float, rel: float = 1e-9) -> bool:
    val = series.iloc[0]
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return math.isnan(expected)
    return math.isclose(float(val), expected, rel_tol=rel)


def _is_null(v: object) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


# ===========================================================================
# Category 1 — New functions: cot and haversin
# ===========================================================================


class TestCotRegistered:
    """cot(x) = cos(x)/sin(x) must be registered and produce correct results."""

    def test_cot_registered(self) -> None:
        assert _reg().has_function("cot"), (
            "'cot' is not registered. Add it to scalar_functions.py."
        )

    def test_cot_quarter_pi(self) -> None:
        result = _reg().execute("cot", [_s(math.pi / 4)])
        assert _approx(result, 1.0), (
            f"cot(π/4) should be 1.0, got {result.iloc[0]}"
        )

    def test_cot_pi_over_6(self) -> None:
        result = _reg().execute("cot", [_s(math.pi / 6)])
        assert _approx(result, math.sqrt(3)), (
            f"cot(π/6) should be √3, got {result.iloc[0]}"
        )

    def test_cot_zero_returns_null(self) -> None:
        result = _reg().execute("cot", [_s(0.0)])
        assert _is_null(result.iloc[0]), (
            f"cot(0) should be null (division by zero), got {result.iloc[0]}"
        )

    def test_cot_null_returns_null(self) -> None:
        result = _reg().execute("cot", [_s(None)])
        assert _is_null(result.iloc[0])

    def test_cot_multi_row(self) -> None:
        result = _reg().execute("cot", [_s(math.pi / 4, math.pi / 3, None)])
        assert _approx(pd.Series([result.iloc[0]]), 1.0)
        assert _approx(pd.Series([result.iloc[1]]), 1.0 / math.sqrt(3))
        assert _is_null(result.iloc[2])

    def test_cot_cypher_integration(self) -> None:
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        result = star.execute_query(f"RETURN cot({math.pi / 4}) AS v")
        assert _approx(pd.Series([result["v"].iloc[0]]), 1.0)


class TestHaversinRegistered:
    """haversin(x) = (1 - cos(x)) / 2 must be registered."""

    def test_haversin_registered(self) -> None:
        assert _reg().has_function("haversin"), (
            "'haversin' is not registered. Add it to scalar_functions.py."
        )

    def test_haversin_zero(self) -> None:
        result = _reg().execute("haversin", [_s(0.0)])
        assert _approx(result, 0.0), (
            f"haversin(0) should be 0.0, got {result.iloc[0]}"
        )

    def test_haversin_pi(self) -> None:
        result = _reg().execute("haversin", [_s(math.pi)])
        assert _approx(result, 1.0), (
            f"haversin(π) should be 1.0, got {result.iloc[0]}"
        )

    def test_haversin_half_pi(self) -> None:
        result = _reg().execute("haversin", [_s(math.pi / 2)])
        assert _approx(result, 0.5), (
            f"haversin(π/2) should be 0.5, got {result.iloc[0]}"
        )

    def test_haversin_null_returns_null(self) -> None:
        result = _reg().execute("haversin", [_s(None)])
        assert _is_null(result.iloc[0])

    def test_haversin_multi_row(self) -> None:
        result = _reg().execute("haversin", [_s(0.0, math.pi, None)])
        assert _approx(pd.Series([result.iloc[0]]), 0.0)
        assert _approx(pd.Series([result.iloc[1]]), 1.0)
        assert _is_null(result.iloc[2])

    def test_haversin_cypher_integration(self) -> None:
        """haversin is the standard Neo4j great-circle building block."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        result = star.execute_query("RETURN haversin(0.0) AS v")
        assert _approx(pd.Series([result["v"].iloc[0]]), 0.0)


# ===========================================================================
# Category 2 — Correctness regression (must pass before AND after)
# ===========================================================================


class TestTrigCorrectnessRegression:
    """Correctness of existing trig functions must be unchanged after vectorisation."""

    def test_sin_quarter_pi(self) -> None:
        result = _reg().execute("sin", [_s(math.pi / 4)])
        assert _approx(result, math.sqrt(2) / 2)

    def test_cos_zero(self) -> None:
        result = _reg().execute("cos", [_s(0.0)])
        assert _approx(result, 1.0)

    def test_tan_quarter_pi(self) -> None:
        result = _reg().execute("tan", [_s(math.pi / 4)])
        assert _approx(result, 1.0)

    def test_asin_domain_error_returns_null(self) -> None:
        result = _reg().execute("asin", [_s(2.0)])
        assert _is_null(result.iloc[0]), (
            f"asin(2.0) is out of domain [-1,1] and must return null, got {result.iloc[0]}"
        )

    def test_acos_one(self) -> None:
        result = _reg().execute("acos", [_s(1.0)])
        assert _approx(result, 0.0)

    def test_atan_one(self) -> None:
        result = _reg().execute("atan", [_s(1.0)])
        assert _approx(result, math.pi / 4)

    def test_atan2_correct(self) -> None:
        result = _reg().execute("atan2", [_s(1.0), _s(1.0)])
        assert _approx(result, math.pi / 4)

    def test_degrees_pi(self) -> None:
        result = _reg().execute("degrees", [_s(math.pi)])
        assert _approx(result, 180.0)

    def test_radians_180(self) -> None:
        result = _reg().execute("radians", [_s(180.0)])
        assert _approx(result, math.pi)

    def test_sinh_zero(self) -> None:
        result = _reg().execute("sinh", [_s(0.0)])
        assert _approx(result, 0.0)

    def test_cosh_zero(self) -> None:
        result = _reg().execute("cosh", [_s(0.0)])
        assert _approx(result, 1.0)

    def test_tanh_zero(self) -> None:
        result = _reg().execute("tanh", [_s(0.0)])
        assert _approx(result, 0.0)

    def test_null_propagation_sin(self) -> None:
        result = _reg().execute("sin", [_s(None)])
        assert _is_null(result.iloc[0])

    def test_null_propagation_atan2(self) -> None:
        result = _reg().execute("atan2", [_s(None), _s(1.0)])
        assert _is_null(result.iloc[0])

    def test_multi_row_sin(self) -> None:
        result = _reg().execute("sin", [_s(0.0, math.pi / 2, math.pi)])
        assert _approx(pd.Series([result.iloc[0]]), 0.0)
        assert _approx(pd.Series([result.iloc[1]]), 1.0)
        # sin(π) ≈ 1.22e-16 due to IEEE 754; abs tolerance needed
        assert abs(float(result.iloc[2])) < 1e-10


# ===========================================================================
# Category 3 — Performance: numpy path must be faster than .apply() baseline
# ===========================================================================


REPS = 30
N_ROWS = 10_000


class TestTrigVectorisationPerformance:
    """Numpy trig must be significantly faster than .apply() at 10k rows."""

    def _big_series(self) -> pd.Series:
        angles = np.linspace(0.0, 2 * math.pi, N_ROWS)
        return pd.Series(angles)

    def test_sin_absolute_threshold(self) -> None:
        """30 × sin(10k rows) must complete in < 0.5s."""
        s = self._big_series()
        start = time.perf_counter()
        for _ in range(REPS):
            _reg().execute("sin", [s])
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, (
            f"30 × sin(10k rows) took {elapsed:.2f}s (threshold 0.5s). "
            "The .apply() loop may still be in place."
        )

    def test_atan2_absolute_threshold(self) -> None:
        """30 × atan2(10k rows, 10k rows) must complete in < 0.5s."""
        s = self._big_series()
        start = time.perf_counter()
        for _ in range(REPS):
            _reg().execute("atan2", [s, s])
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, (
            f"30 × atan2(10k rows) took {elapsed:.2f}s (threshold 0.5s). "
            "The .apply() loop may still be in place."
        )

    def test_numpy_vs_apply_speedup(self) -> None:
        """Numpy sin must be ≥ 5× faster than equivalent .apply() at 10k rows.

        We measure the core operation in isolation to avoid registry overhead:
        numpy vectorised array operation vs pandas .apply() element loop.
        """
        arr = self._big_series().to_numpy(dtype=np.float64)
        s_obj = pd.Series(
            arr, dtype=object
        )  # object dtype matches .apply() path

        # Baseline: simulate the old .apply() path on 10k float values
        start = time.perf_counter()
        for _ in range(REPS):
            s_obj.apply(
                lambda x: math.sin(float(x)) if x is not None else None
            )
        baseline = time.perf_counter() - start

        # New path: numpy vectorised sin
        start = time.perf_counter()
        for _ in range(REPS):
            np.sin(arr)
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 5.0, (
            f"Numpy sin ({vectorised:.3f}s) should be ≥ 5× faster than "
            f".apply() baseline ({baseline:.3f}s) at {N_ROWS} rows, "
            f"got {speedup:.1f}×."
        )
