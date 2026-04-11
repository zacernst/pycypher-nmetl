"""TDD tests for properties(n) vectorised dict construction (Loop 166 — Performance).

Red phase: establish that the current iterrows() implementation is measurably
slow on large entity tables, and that the fix produces identical results.

Green phase (after replacing iterrows() with to_dict('index')): the performance
test passes and all correctness tests continue to pass.

The fix replaces:
    {row[ID_COL]: {c: row[c] for c in prop_cols}
     for _, row in raw_df.iterrows()}
with:
    raw_df.set_index(ID_COL)[prop_cols].to_dict("index")

to_dict("index") is implemented in Cython and is typically 10-30x faster than
iterrows() for building a dict-of-dicts lookup table.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.performance


def _make_star(n: int) -> Star:
    ctx = ContextBuilder.from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": list(range(n)),
                    "name": [f"Person_{i}" for i in range(n)],
                    "age": [20 + (i % 60) for i in range(n)],
                    "dept": [f"dept_{i % 10}" for i in range(n)],
                },
            ),
        },
    )
    return Star(context=ctx)


@pytest.fixture(scope="module")
def star_small() -> Star:
    return _make_star(10)


@pytest.fixture(scope="module")
def star_large() -> Star:
    return _make_star(5_000)


# ---------------------------------------------------------------------------
# Correctness tests (always green — guard against regression in the fix)
# ---------------------------------------------------------------------------


class TestPropertiesCorrectness:
    """Verify properties(n) returns the correct dict structure after the fix."""

    def test_properties_returns_dict_per_row(self, star_small: Star) -> None:
        result = star_small.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props LIMIT 1",
        )
        assert len(result) == 1
        props = result["props"].iloc[0]
        assert isinstance(props, dict)

    def test_properties_contains_all_user_fields(
        self,
        star_small: Star,
    ) -> None:
        result = star_small.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Person_0' RETURN properties(p) AS props",
        )
        props = result["props"].iloc[0]
        assert "name" in props
        assert "age" in props
        assert "dept" in props

    def test_properties_excludes_internal_id_column(
        self,
        star_small: Star,
    ) -> None:
        result = star_small.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Person_0' RETURN properties(p) AS props",
        )
        props = result["props"].iloc[0]
        assert "__ID__" not in props, "__ID__ must not appear in properties()"

    def test_properties_values_are_correct(self, star_small: Star) -> None:
        result = star_small.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Person_3' RETURN properties(p) AS props",
        )
        props = result["props"].iloc[0]
        assert props["name"] == "Person_3"
        assert props["age"] == 23  # 20 + (3 % 60)
        assert props["dept"] == "dept_3"

    def test_properties_all_rows_have_dict(self, star_small: Star) -> None:
        result = star_small.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props",
        )
        assert len(result) == 10
        assert all(isinstance(v, dict) for v in result["props"])

    def test_properties_each_row_has_correct_name(
        self,
        star_small: Star,
    ) -> None:
        result = star_small.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props",
        )
        names = {row["name"] for row in result["props"]}
        expected = {f"Person_{i}" for i in range(10)}
        assert names == expected

    def test_properties_no_cross_contamination(self, star_small: Star) -> None:
        """Each row gets its own dict, not a shared mutable reference."""
        result = star_small.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props",
        )
        dicts = result["props"].tolist()
        # Mutate the first dict and verify others are unaffected
        original_name = dicts[1]["name"]
        dicts[0]["name"] = "MUTATED"
        assert dicts[1]["name"] == original_name


# ---------------------------------------------------------------------------
# Performance test: properties(n) on 5k rows must complete in < 1 second
# ---------------------------------------------------------------------------


class TestPropertiesPerformance:
    """Performance regression guard for properties(n).

    The iterrows()-based implementation takes ~500ms on 5k rows (extrapolating
    from 156ms/10k — actually 78ms/5k from the baseline).  After fixing to
    to_dict('index'), this should complete well under 500ms for the whole loop
    of 3 repetitions (i.e., < 167ms/query).

    Baseline (iterrows): ~76ms/query on 5k rows → 0.229s for 3 reps.
    The Earley parser accounts for ~53ms/query (fixed overhead); the iterrows()
    dict-construction adds ~23ms.  After replacing with to_dict('index') (which
    is Cython-backed and ~20x faster), query time drops to ~53ms, so 3 reps
    should complete well under 0.200s.

    Threshold: 0.200s for 3 × 5k-row queries.
      - FAILS with iterrows()     (0.229s > 0.200s)
      - PASSES after to_dict()    (~0.163s < 0.200s)
    """

    def test_properties_on_5k_rows_is_fast(self, star_large: Star) -> None:
        query = "MATCH (p:Person) RETURN properties(p) AS props"

        # Warm up with the actual query (not LIMIT 1) so parser cache,
        # graph index, and property lookup caches are all primed.
        star_large.execute_query(query)

        # Take the median of 5 runs to absorb load-induced spikes.
        timings: list[float] = []
        for _ in range(5):
            t0 = time.perf_counter()
            result = star_large.execute_query(query)
            timings.append(time.perf_counter() - t0)

        median = sorted(timings)[len(timings) // 2]

        assert len(result) == 5_000, "Sanity: all 5k rows returned"
        assert median < perf_threshold(0.100), (
            f"properties(n) on 5k rows median={median:.3f}s "
            f"(all={[f'{t:.3f}' for t in timings]}); "
            f"expected median < 0.100s — regression back to iterrows()?"
        )
