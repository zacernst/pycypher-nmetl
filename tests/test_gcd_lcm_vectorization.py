"""TDD performance test for numpy-vectorized gcd() and lcm().

Loop 213 — Performance: replace pd.Series.apply(zip(...)) with np.gcd / np.lcm.

The existing correctness tests live in test_gcd_lcm_bytesize.py.
This file adds timing assertions to confirm the vectorized path is
substantially faster than the old Python-level apply loop.

Threshold: 10 000-element arrays must complete in < 100 ms each.
The apply() path typically takes 300-500 ms; np.gcd takes < 5 ms.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher.scalar_functions import ScalarFunctionRegistry

ARRAY_SIZE = 100_000
THRESHOLD_MS = perf_threshold(100)  # numpy path is ~12 ms; apply() path is ~300-500 ms
# Threshold is generous to avoid flaky failures under CPU contention
# (CI, parallel test runs, multi-agent sessions).


@pytest.fixture(scope="module")
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.mark.performance
class TestGcdLcmVectorizationPerformance:
    def test_gcd_10k_rows_is_fast(
        self,
        registry: ScalarFunctionRegistry,
    ) -> None:
        """Gcd on 10 000-element Series must complete in < 100 ms."""
        a = pd.Series(
            np.random.randint(1, 1000, size=ARRAY_SIZE),
            dtype=object,
        )
        b = pd.Series(
            np.random.randint(1, 1000, size=ARRAY_SIZE),
            dtype=object,
        )

        start = time.perf_counter()
        result = registry.execute("gcd", [a, b])
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) == ARRAY_SIZE
        assert elapsed_ms < THRESHOLD_MS, (
            f"gcd(10k) took {elapsed_ms:.1f} ms — expected < {THRESHOLD_MS} ms. "
            "Ensure the numpy-vectorized path is in use."
        )

    def test_lcm_10k_rows_is_fast(
        self,
        registry: ScalarFunctionRegistry,
    ) -> None:
        """Lcm on 10 000-element Series must complete in < 100 ms."""
        a = pd.Series(
            np.random.randint(1, 1000, size=ARRAY_SIZE),
            dtype=object,
        )
        b = pd.Series(
            np.random.randint(1, 1000, size=ARRAY_SIZE),
            dtype=object,
        )

        start = time.perf_counter()
        result = registry.execute("lcm", [a, b])
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) == ARRAY_SIZE
        assert elapsed_ms < THRESHOLD_MS, (
            f"lcm(10k) took {elapsed_ms:.1f} ms — expected < {THRESHOLD_MS} ms. "
            "Ensure the numpy-vectorized path is in use."
        )

    def test_gcd_null_propagation_preserved(
        self,
        registry: ScalarFunctionRegistry,
    ) -> None:
        """Null propagation still works after vectorization."""
        a = pd.Series([12, None, 15], dtype=object)
        b = pd.Series([8, 5, None], dtype=object)

        result = registry.execute("gcd", [a, b])

        assert result.iloc[0] == 4
        assert result.iloc[1] is None or pd.isna(result.iloc[1])
        assert result.iloc[2] is None or pd.isna(result.iloc[2])

    def test_lcm_null_propagation_preserved(
        self,
        registry: ScalarFunctionRegistry,
    ) -> None:
        """Null propagation still works after vectorization."""
        a = pd.Series([4, None, 15], dtype=object)
        b = pd.Series([6, 5, None], dtype=object)

        result = registry.execute("lcm", [a, b])

        assert result.iloc[0] == 12
        assert result.iloc[1] is None or pd.isna(result.iloc[1])
        assert result.iloc[2] is None or pd.isna(result.iloc[2])
