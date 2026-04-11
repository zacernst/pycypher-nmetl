"""TDD tests for Arrow→pandas conversion caching in property lookups.

The existing _property_lookup_cache caches raw_df.set_index(ID_COLUMN) to
avoid re-running set_index on every call.  However, _source_to_pandas() — the
Arrow→pandas table_to_dataframe conversion — was still called unconditionally
before the cache check.  For an Arrow-backed ContextBuilder, this means one
full Arrow→pandas round-trip per property lookup, even after the cache is warm.

Profile evidence (Loop 209):
    pyarrow.pandas_compat.table_to_dataframe: 100 calls, 0.251s/0.805s total
    for 20 read-only queries on a 1-entity 2000-row context.

After the fix:
    _source_to_pandas is called at most once per entity/relationship type
    (first cache miss).  All subsequent property lookups skip the conversion
    entirely.

    Same workload: ≤1 _source_to_pandas call for Person entity.
"""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd
import pycypher.binding_frame as bf_module
import pytest
from _perf_helpers import perf_threshold
from pycypher.binding_frame import _source_to_pandas
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ctx_arrow() -> ContextBuilder:
    """ContextBuilder.from_dict always produces Arrow-backed tables."""
    rng = np.random.default_rng(42)
    return ContextBuilder().from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": [f"p{i}" for i in range(500)],
                    "name": [f"Person{i}" for i in range(500)],
                    "age": rng.integers(20, 80, 500).tolist(),
                },
            ),
        },
    )


# ---------------------------------------------------------------------------
# Red-phase tests: _source_to_pandas must be called only once per entity type
# ---------------------------------------------------------------------------


class TestArrowToPandasCallCount:
    def test_source_to_pandas_called_once_for_first_query(
        self,
        ctx_arrow: ContextBuilder,
    ) -> None:
        """During the first query, _source_to_pandas must be called at most once
        for the Person entity type, regardless of how many properties are accessed.
        """
        star = Star(context=ctx_arrow)
        calls: list[int] = []
        original = _source_to_pandas

        def counting(obj: object) -> pd.DataFrame:
            calls.append(1)
            return original(obj)

        with patch.object(
            bf_module,
            "_source_to_pandas",
            side_effect=counting,
        ):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age",
            )

        # p.name and p.age both access Person — only 1 _source_to_pandas call needed
        assert len(calls) <= 1, (
            f"Expected ≤1 _source_to_pandas call for 2 properties on the same "
            f"entity type in one query, but got {len(calls)}. "
            "The Arrow→pandas conversion is not being cached before set_index."
        )

    def test_source_to_pandas_zero_calls_after_warmup(
        self,
        ctx_arrow: ContextBuilder,
    ) -> None:
        """After the first query warms the cache, subsequent read-only queries
        must call _source_to_pandas exactly 0 times.
        """
        star = Star(context=ctx_arrow)

        # Warm the cache
        star.execute_query("MATCH (p:Person) RETURN p.name")

        calls: list[int] = []
        original = _source_to_pandas

        def counting(obj: object) -> pd.DataFrame:
            calls.append(1)
            return original(obj)

        with patch.object(
            bf_module,
            "_source_to_pandas",
            side_effect=counting,
        ):
            for _ in range(10):
                star.execute_query(
                    "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age",
                )

        assert len(calls) == 0, (
            f"Expected 0 _source_to_pandas calls after cache warmup for 10 "
            f"read-only queries, but got {len(calls)}. "
            "The Arrow→pandas conversion cache is not persisting across queries."
        )

    def test_total_calls_across_many_queries(
        self,
        ctx_arrow: ContextBuilder,
    ) -> None:
        """Over 20 queries, total _source_to_pandas calls must be ≤1 (one miss)."""
        star = Star(context=ctx_arrow)
        calls: list[int] = []
        original = _source_to_pandas

        def counting(obj: object) -> pd.DataFrame:
            calls.append(1)
            return original(obj)

        with patch.object(
            bf_module,
            "_source_to_pandas",
            side_effect=counting,
        ):
            for _ in range(20):
                star.execute_query("MATCH (p:Person) RETURN p.name, p.age")

        # Before fix: 2 calls/query × 20 queries = 40 calls
        # After fix: 1 call (first query, first property; second property hits cache)
        assert len(calls) <= 1, (
            f"Expected ≤1 _source_to_pandas call total across 20 queries "
            f"(cache should persist after first miss), but got {len(calls)}."
        )

    def test_correctness_preserved(self, ctx_arrow: ContextBuilder) -> None:
        """Property values must be correct after the caching change."""
        star = Star(context=ctx_arrow)
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age "
            "ORDER BY p.age LIMIT 5",
        )
        assert "name" in r.columns
        assert "age" in r.columns
        ages = r["age"].tolist()
        assert all(a > 30 for a in ages)
        assert ages == sorted(ages)


# ---------------------------------------------------------------------------
# Performance verification
# ---------------------------------------------------------------------------


class TestArrowPandasCachePerformance:
    def test_20_warm_queries_under_2s(self, ctx_arrow: ContextBuilder) -> None:
        """20 warm read-only queries on 500-row Arrow context must finish < 2s."""
        import time

        star = Star(context=ctx_arrow)
        # Warm cache
        star.execute_query("MATCH (p:Person) RETURN p.name LIMIT 1")

        t0 = time.perf_counter()
        for _ in range(20):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age "
                "ORDER BY p.age LIMIT 10",
            )
        elapsed = time.perf_counter() - t0
        assert elapsed < perf_threshold(2.0), (
            f"20 warm queries took {elapsed:.2f}s (threshold 2.0s). "
            "Arrow→pandas caching may not be active."
        )
