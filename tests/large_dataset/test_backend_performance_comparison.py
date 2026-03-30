"""Performance comparison tests between PandasBackend and DuckDBBackend.

Systematically measures where each backend excels to validate the
auto-selection heuristic threshold (100K rows). These results inform
the select_backend() decision logic.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pycypher.backend_engine import (
    DuckDBBackend,
    PandasBackend,
    select_backend,
)

from .benchmark_utils import run_benchmark
from .dataset_generator import ID_COLUMN, generate_person_dataframe

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def small_df() -> pd.DataFrame:
    """1K rows — below auto-selection threshold."""
    return generate_person_dataframe(1_000, seed=42)


@pytest.fixture(scope="module")
def medium_df() -> pd.DataFrame:
    """50K rows — near auto-selection threshold."""
    return generate_person_dataframe(50_000, seed=42)


@pytest.fixture(scope="module")
def large_df() -> pd.DataFrame:
    """200K rows — above auto-selection threshold."""
    return generate_person_dataframe(200_000, seed=42)


@pytest.fixture
def pandas_be() -> PandasBackend:
    return PandasBackend()


@pytest.fixture
def duckdb_be() -> DuckDBBackend:
    return DuckDBBackend()


# ---------------------------------------------------------------------------
# Scan performance comparison
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestScanPerformanceComparison:
    """Compare scan performance across backends and data sizes."""

    def test_scan_small_pandas_competitive(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
        small_df: pd.DataFrame,
    ) -> None:
        """At 1K rows, pandas should be competitive with DuckDB."""
        p = run_benchmark(
            lambda: pandas_be.scan_entity(small_df, "Person"),
            iterations=5,
        )
        d = run_benchmark(
            lambda: duckdb_be.scan_entity(small_df, "Person"),
            iterations=5,
        )
        # Pandas should not be >10x slower at small scale
        ratio = p.median_time_s / max(d.median_time_s, 1e-9)
        assert ratio < 10, f"Pandas {ratio:.1f}x slower than DuckDB at 1K rows"

    def test_scan_large_both_complete(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
        large_df: pd.DataFrame,
    ) -> None:
        """At 200K rows, both backends should complete scan."""
        p = run_benchmark(
            lambda: pandas_be.scan_entity(large_df, "Person"),
            iterations=3,
        )
        d = run_benchmark(
            lambda: duckdb_be.scan_entity(large_df, "Person"),
            iterations=3,
        )
        # Both should complete in <5s
        p.assert_time_under(5.0)
        d.assert_time_under(5.0)


# ---------------------------------------------------------------------------
# Filter performance comparison
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestFilterPerformanceComparison:
    """Compare filter performance across backends."""

    def test_selective_filter_large(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
        large_df: pd.DataFrame,
    ) -> None:
        """Highly selective filter (returns ~1% of rows)."""
        mask = large_df["age"] > 75
        selectivity = mask.sum() / len(large_df)

        p = run_benchmark(
            lambda: pandas_be.filter(large_df, mask),
            iterations=5,
        )
        d = run_benchmark(
            lambda: duckdb_be.filter(large_df, mask),
            iterations=5,
        )

        # Record selectivity for debugging
        assert selectivity < 0.1
        # Both should complete quickly
        p.assert_time_under(2.0)
        d.assert_time_under(2.0)


# ---------------------------------------------------------------------------
# Join performance comparison
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestJoinPerformanceComparison:
    """Compare join performance — where DuckDB should shine."""

    def test_join_medium_scale(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
    ) -> None:
        """Join two 10K-row DataFrames."""
        left = pd.DataFrame(
            {
                "id": np.arange(10_000),
                "val": np.random.default_rng(42).standard_normal(10_000),
            },
        )
        right = pd.DataFrame(
            {
                "id": np.arange(5_000, 15_000),
                "score": np.random.default_rng(43).standard_normal(10_000),
            },
        )

        p = run_benchmark(
            lambda: pandas_be.join(left, right, on="id", how="inner"),
            iterations=5,
        )
        d = run_benchmark(
            lambda: duckdb_be.join(left, right, on="id", how="inner"),
            iterations=5,
        )

        # Both should complete in <2s
        p.assert_time_under(2.0)
        d.assert_time_under(2.0)

    def test_join_large_scale(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
    ) -> None:
        """Join two 100K-row DataFrames — DuckDB's sweet spot."""
        rng = np.random.default_rng(42)
        left = pd.DataFrame(
            {
                "id": np.arange(100_000),
                "val": rng.standard_normal(100_000),
            },
        )
        right = pd.DataFrame(
            {
                "id": np.arange(50_000, 150_000),
                "score": rng.standard_normal(100_000),
            },
        )

        p = run_benchmark(
            lambda: pandas_be.join(left, right, on="id", how="inner"),
            iterations=3,
        )
        d = run_benchmark(
            lambda: duckdb_be.join(left, right, on="id", how="inner"),
            iterations=3,
        )

        # Both should complete in <10s
        p.assert_time_under(10.0)
        d.assert_time_under(10.0)


# ---------------------------------------------------------------------------
# Aggregation performance comparison
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestAggregationPerformanceComparison:
    """Compare aggregation — DuckDB should excel at large grouped agg."""

    def test_grouped_agg_medium(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
        medium_df: pd.DataFrame,
    ) -> None:
        """Grouped count on 50K rows."""
        p = run_benchmark(
            lambda: pandas_be.aggregate(
                medium_df,
                group_cols=["city"],
                agg_specs={"cnt": (ID_COLUMN, "count")},
            ),
            iterations=5,
        )
        d = run_benchmark(
            lambda: duckdb_be.aggregate(
                medium_df,
                group_cols=["city"],
                agg_specs={"cnt": (ID_COLUMN, "count")},
            ),
            iterations=5,
        )

        p.assert_time_under(2.0)
        d.assert_time_under(2.0)

    def test_multi_group_agg_large(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
        large_df: pd.DataFrame,
    ) -> None:
        """Multi-column GROUP BY on 200K rows."""
        p = run_benchmark(
            lambda: pandas_be.aggregate(
                large_df,
                group_cols=["city", "dept"],
                agg_specs={
                    "cnt": (ID_COLUMN, "count"),
                    "avg_age": ("age", "mean"),
                },
            ),
            iterations=3,
        )
        d = run_benchmark(
            lambda: duckdb_be.aggregate(
                large_df,
                group_cols=["city", "dept"],
                agg_specs={
                    "cnt": (ID_COLUMN, "count"),
                    "avg_age": ("age", "mean"),
                },
            ),
            iterations=3,
        )

        p.assert_time_under(5.0)
        d.assert_time_under(5.0)


# ---------------------------------------------------------------------------
# Sort + Limit performance comparison
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestSortLimitPerformanceComparison:
    """Compare sort+limit — important for ORDER BY ... LIMIT N queries."""

    def test_sort_limit_large(
        self,
        pandas_be: PandasBackend,
        duckdb_be: DuckDBBackend,
        large_df: pd.DataFrame,
    ) -> None:
        """Sort 200K rows by age, then take top 100."""

        def pandas_sort_limit() -> pd.DataFrame:
            sorted_df = pandas_be.sort(large_df, by=["age"], ascending=[False])
            return pandas_be.limit(sorted_df, 100)

        def duckdb_sort_limit() -> pd.DataFrame:
            sorted_df = duckdb_be.sort(large_df, by=["age"], ascending=[False])
            return duckdb_be.limit(sorted_df, 100)

        p = run_benchmark(pandas_sort_limit, iterations=3)
        d = run_benchmark(duckdb_sort_limit, iterations=3)

        p.assert_time_under(5.0)
        d.assert_time_under(5.0)


# ---------------------------------------------------------------------------
# Auto-selection validation
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestAutoSelectionValidation:
    """Validate that the auto-selection heuristic picks reasonably."""

    def test_auto_selection_produces_correct_results(self) -> None:
        """Verify auto-selected backend produces correct results."""
        df = generate_person_dataframe(1_000, seed=42)

        # Auto should pick pandas for 1K rows
        be = select_backend(hint="auto", estimated_rows=1_000)
        assert be.name == "pandas"

        result = be.scan_entity(df, "Person")
        assert be.row_count(result) == 1_000

    def test_auto_selection_large_produces_correct_results(self) -> None:
        """Verify DuckDB produces same results as pandas."""
        df = generate_person_dataframe(1_000, seed=42)

        pandas_be = select_backend(hint="pandas")
        duckdb_be = select_backend(hint="duckdb")

        p_result = pandas_be.aggregate(
            df,
            group_cols=["city"],
            agg_specs={"cnt": (ID_COLUMN, "count")},
        )
        d_result = duckdb_be.aggregate(
            df,
            group_cols=["city"],
            agg_specs={"cnt": (ID_COLUMN, "count")},
        )

        p_sorted = p_result.sort_values("city").reset_index(drop=True)
        d_sorted = d_result.sort_values("city").reset_index(drop=True)

        assert list(p_sorted["city"]) == list(d_sorted["city"])
        assert list(p_sorted["cnt"]) == list(d_sorted["cnt"])

    def test_memory_estimate_scales_with_data(self) -> None:
        """Memory estimates should grow with data size."""
        be = PandasBackend()
        small = generate_person_dataframe(100, seed=42)
        large = generate_person_dataframe(10_000, seed=42)

        small_est = be.memory_estimate_bytes(small)
        large_est = be.memory_estimate_bytes(large)

        # 100x more rows should be at least 10x more memory
        assert large_est > small_est * 10
