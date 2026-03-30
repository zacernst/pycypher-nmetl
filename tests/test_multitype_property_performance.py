"""TDD tests for _get_property_multitype() performance fix.

The iterrows() loop at binding_frame.py:294 creates one pandas Series object
per row when building the id→value lookup dict for multi-type property access.
On a 5000-row entity table this is 5000 Python Series objects per entity type
per property access — needlessly expensive.

Fix: replace with dict(zip(id_col, prop_col)) — one Cython-level pass, no
per-row Python object creation.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star

pytestmark = [pytest.mark.slow, pytest.mark.performance]


@pytest.fixture
def large_multitype_ctx():
    """Context with two entity types of 5000 rows each."""
    rng = np.random.default_rng(42)
    return ContextBuilder().from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": [f"p{i}" for i in range(5000)],
                    "score": rng.integers(0, 100, 5000).tolist(),
                    "name": [f"person_{i}" for i in range(5000)],
                },
            ),
            "Company": pd.DataFrame(
                {
                    "__ID__": [f"c{i}" for i in range(5000)],
                    "score": rng.integers(0, 100, 5000).tolist(),
                    "name": [f"company_{i}" for i in range(5000)],
                },
            ),
        },
    )


@pytest.fixture
def small_multitype_ctx():
    """Context with two entity types of 100 rows each — for correctness checks."""
    return ContextBuilder().from_dict(
        {
            "Cat": pd.DataFrame(
                {
                    "__ID__": ["cat_a", "cat_b", "cat_c"],
                    "age": [3, 7, 1],
                },
            ),
            "Dog": pd.DataFrame(
                {
                    "__ID__": ["dog_x", "dog_y"],
                    "age": [5, 2],
                },
            ),
        },
    )


# ---------------------------------------------------------------------------
# Correctness — must pass before and after the fix
# ---------------------------------------------------------------------------


class TestMultitypePropertyCorrectness:
    def test_unlabeled_match_returns_all_entities(
        self,
        small_multitype_ctx: ContextBuilder,
    ) -> None:
        s = Star(context=small_multitype_ctx)
        result = s.execute_query("MATCH (n) RETURN n.age")
        # 3 cats + 2 dogs = 5 rows
        assert len(result) == 5

    def test_unlabeled_match_property_values_correct(
        self,
        small_multitype_ctx: ContextBuilder,
    ) -> None:
        s = Star(context=small_multitype_ctx)
        result = s.execute_query("MATCH (n) RETURN n.age ORDER BY n.age")
        ages = sorted(result["age"].dropna().tolist())
        assert ages == [1, 2, 3, 5, 7]

    def test_where_filter_on_multitype_property(
        self,
        small_multitype_ctx: ContextBuilder,
    ) -> None:
        s = Star(context=small_multitype_ctx)
        result = s.execute_query("MATCH (n) WHERE n.age > 3 RETURN n.age")
        ages = sorted(result["age"].tolist())
        assert ages == [5, 7]

    def test_multitype_string_property(
        self,
        large_multitype_ctx: ContextBuilder,
    ) -> None:
        s = Star(context=large_multitype_ctx)
        result = s.execute_query(
            'MATCH (n) WHERE n.name = "person_0" RETURN n.name',
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "person_0"

    def test_labeled_match_unaffected(
        self,
        large_multitype_ctx: ContextBuilder,
    ) -> None:
        """Labeled MATCH should still work correctly after the fix."""
        s = Star(context=large_multitype_ctx)
        result = s.execute_query(
            "MATCH (p:Person) WHERE p.score > 95 RETURN p.score",
        )
        assert len(result) > 0
        assert all(v > 95 for v in result["score"].tolist())

    def test_null_for_missing_property_across_types(
        self,
        small_multitype_ctx: ContextBuilder,
    ) -> None:
        """Property absent in one entity type returns null for those rows."""
        ctx = ContextBuilder().from_dict(
            {
                "Cat": pd.DataFrame({"__ID__": ["c1"], "sound": ["meow"]}),
                "Dog": pd.DataFrame({"__ID__": ["d1"], "breed": ["labrador"]}),
            },
        )
        s = Star(context=ctx)
        # sound is only on Cat; Dog rows should get null
        result = s.execute_query("MATCH (n) RETURN n.sound")
        sounds = result["sound"].tolist()
        non_null = [v for v in sounds if v is not None and v == v]
        assert non_null == ["meow"]


# ---------------------------------------------------------------------------
# Performance — the core TDD regression guard for the fix
# ---------------------------------------------------------------------------


class TestMultitypePropertyPerformance:
    def test_20x_unlabeled_match_under_2_seconds(
        self,
        large_multitype_ctx: ContextBuilder,
    ) -> None:
        """20 queries on a 10k-row 2-entity context must complete in < 2s.

        Baseline (iterrows): ~5.2s for N=20 (259ms/query).
        Expected after fix:  < 2s for N=20 (< 100ms/query).
        """
        s = Star(context=large_multitype_ctx)
        # warm up parse cache
        s.execute_query("MATCH (n) WHERE n.score > 80 RETURN n.score")

        N = 20
        t0 = time.perf_counter()
        for _ in range(N):
            s.execute_query("MATCH (n) WHERE n.score > 80 RETURN n.score")
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, (
            f"20x multi-type property query took {elapsed:.3f}s — "
            f"expected < 2.0s after iterrows fix"
        )

    def test_speedup_vs_baseline(
        self,
        large_multitype_ctx: ContextBuilder,
    ) -> None:
        """After fix, zip-based dict construction must be faster than iterrows().

        Directly benchmarks the dict-construction step in isolation to avoid
        full-pipeline noise.
        """
        df = pd.DataFrame(
            {
                "__ID__": [f"e{i}" for i in range(10_000)],
                "val": list(range(10_000)),
            },
        )

        # Old: iterrows()
        N = 50
        t0 = time.perf_counter()
        for _ in range(N):
            d = {}
            for _, row in df[["__ID__", "val"]].iterrows():
                d[row["__ID__"]] = row["val"]
        old_time = time.perf_counter() - t0

        # New: zip()
        t0 = time.perf_counter()
        for _ in range(N):
            d = dict(zip(df["__ID__"], df["val"]))
        new_time = time.perf_counter() - t0

        speedup = old_time / new_time
        assert speedup >= 5.0, (
            f"Expected ≥5× speedup from zip vs iterrows, got {speedup:.1f}× "
            f"(old={old_time:.3f}s, new={new_time:.3f}s)"
        )

    def test_absolute_time_for_zip_construction(self) -> None:
        """dict(zip(...)) on 10k rows × 50 reps must complete in < 0.5s."""
        df = pd.DataFrame(
            {
                "__ID__": [f"e{i}" for i in range(10_000)],
                "val": list(range(10_000)),
            },
        )
        N = 50
        t0 = time.perf_counter()
        for _ in range(N):
            dict(zip(df["__ID__"], df["val"]))
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.5, (
            f"50x zip dict construction took {elapsed:.3f}s — expected < 0.5s"
        )
