"""Cross-backend equivalence tests at scale.

Validates that PandasBackend and DuckDBBackend produce identical
results for all operations at larger data sizes. This catches
numeric precision differences, ordering inconsistencies, and
type coercion issues that only manifest at scale.
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

from .dataset_generator import (
    ID_COLUMN,
    generate_person_dataframe,
    generate_relationship_dataframe,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def large_person_df() -> pd.DataFrame:
    """10K-person DataFrame for equivalence testing."""
    return generate_person_dataframe(10_000, seed=42)


@pytest.fixture(scope="module")
def large_rel_df() -> pd.DataFrame:
    """50K-relationship DataFrame for equivalence testing."""
    return generate_relationship_dataframe(50_000, 10_000, seed=43)


@pytest.fixture
def pandas_backend() -> PandasBackend:
    return PandasBackend()


@pytest.fixture
def duckdb_backend() -> DuckDBBackend:
    return DuckDBBackend()


# ---------------------------------------------------------------------------
# Scan equivalence
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestScanEquivalence:
    """Verify scan produces identical results across backends."""

    def test_scan_ids_match(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        pandas_result = pandas_backend.scan_entity(large_person_df, "Person")
        duckdb_result = duckdb_backend.scan_entity(large_person_df, "Person")

        assert len(pandas_result) == len(duckdb_result)
        assert sorted(pandas_result[ID_COLUMN].tolist()) == sorted(
            duckdb_result[ID_COLUMN].tolist(),
        )

    def test_scan_row_count(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p = pandas_backend.scan_entity(large_person_df, "Person")
        d = duckdb_backend.scan_entity(large_person_df, "Person")
        assert pandas_backend.row_count(p) == duckdb_backend.row_count(d)


# ---------------------------------------------------------------------------
# Filter equivalence
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestFilterEquivalence:
    """Verify filter produces identical results across backends."""

    def test_filter_by_age(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        mask = large_person_df["age"] > 50
        p = pandas_backend.filter(large_person_df, mask)
        d = duckdb_backend.filter(large_person_df, mask)

        assert len(p) == len(d)
        assert sorted(p[ID_COLUMN].tolist()) == sorted(d[ID_COLUMN].tolist())

    def test_filter_empty_result(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        mask = large_person_df["age"] > 200
        p = pandas_backend.filter(large_person_df, mask)
        d = duckdb_backend.filter(large_person_df, mask)
        assert len(p) == 0
        assert len(d) == 0


# ---------------------------------------------------------------------------
# Join equivalence
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestJoinEquivalence:
    """Verify join produces identical results across backends."""

    def test_inner_join_equivalence(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
    ) -> None:
        left = pd.DataFrame(
            {
                "id": np.arange(1000),
                "val_l": np.random.default_rng(42).standard_normal(1000),
            },
        )
        right = pd.DataFrame(
            {
                "id": np.arange(500, 1500),
                "val_r": np.random.default_rng(43).standard_normal(1000),
            },
        )

        p = pandas_backend.join(left, right, on="id", how="inner")
        d = duckdb_backend.join(left, right, on="id", how="inner")

        assert len(p) == len(d)
        # Sort both for deterministic comparison
        p_sorted = p.sort_values("id").reset_index(drop=True)
        d_sorted = d.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(p_sorted, d_sorted, check_dtype=False)

    def test_left_join_preserves_nulls(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
    ) -> None:
        left = pd.DataFrame({"id": [1, 2, 3], "a": ["x", "y", "z"]})
        right = pd.DataFrame({"id": [2, 3, 4], "b": ["p", "q", "r"]})

        p = pandas_backend.join(left, right, on="id", how="left")
        d = duckdb_backend.join(left, right, on="id", how="left")

        assert len(p) == len(d) == 3
        # Both should have null for id=1's right side
        p_sorted = p.sort_values("id").reset_index(drop=True)
        d_sorted = d.sort_values("id").reset_index(drop=True)
        assert pd.isna(p_sorted.loc[0, "b"])
        assert pd.isna(d_sorted.loc[0, "b"])


# ---------------------------------------------------------------------------
# Aggregation equivalence
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestAggregationEquivalence:
    """Verify aggregation produces identical results across backends."""

    def test_grouped_count_equivalence(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p = pandas_backend.aggregate(
            large_person_df,
            group_cols=["city"],
            agg_specs={"cnt": (ID_COLUMN, "count")},
        )
        d = duckdb_backend.aggregate(
            large_person_df,
            group_cols=["city"],
            agg_specs={"cnt": (ID_COLUMN, "count")},
        )

        p_sorted = p.sort_values("city").reset_index(drop=True)
        d_sorted = d.sort_values("city").reset_index(drop=True)

        assert list(p_sorted["city"]) == list(d_sorted["city"])
        assert list(p_sorted["cnt"]) == list(d_sorted["cnt"])

    def test_grouped_sum_equivalence(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p = pandas_backend.aggregate(
            large_person_df,
            group_cols=["dept"],
            agg_specs={"total_age": ("age", "sum")},
        )
        d = duckdb_backend.aggregate(
            large_person_df,
            group_cols=["dept"],
            agg_specs={"total_age": ("age", "sum")},
        )

        p_sorted = p.sort_values("dept").reset_index(drop=True)
        d_sorted = d.sort_values("dept").reset_index(drop=True)

        assert list(p_sorted["dept"]) == list(d_sorted["dept"])
        # Allow floating-point tolerance
        np.testing.assert_allclose(
            p_sorted["total_age"].values,
            d_sorted["total_age"].values,
            rtol=1e-10,
        )

    def test_full_table_aggregation_equivalence(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p = pandas_backend.aggregate(
            large_person_df,
            group_cols=[],
            agg_specs={
                "total": (ID_COLUMN, "count"),
                "avg_age": ("age", "mean"),
            },
        )
        d = duckdb_backend.aggregate(
            large_person_df,
            group_cols=[],
            agg_specs={
                "total": (ID_COLUMN, "count"),
                "avg_age": ("age", "mean"),
            },
        )

        assert p["total"].iloc[0] == d["total"].iloc[0]
        assert p["avg_age"].iloc[0] == pytest.approx(
            d["avg_age"].iloc[0],
            rel=1e-10,
        )


# ---------------------------------------------------------------------------
# Sort + Limit equivalence
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestSortLimitEquivalence:
    """Verify sort and limit produce identical results across backends."""

    def test_sort_ascending_equivalence(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p = pandas_backend.sort(large_person_df, by=["age"], ascending=[True])
        d = duckdb_backend.sort(large_person_df, by=["age"], ascending=[True])

        assert list(p["age"]) == list(d["age"])

    def test_limit_equivalence(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p = pandas_backend.limit(large_person_df, 100)
        d = duckdb_backend.limit(large_person_df, 100)

        assert len(p) == len(d) == 100


# ---------------------------------------------------------------------------
# Memory estimation equivalence
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestMemoryEstimationEquivalence:
    """Verify memory estimates are in the same order of magnitude."""

    def test_memory_estimate_same_order(
        self,
        pandas_backend: PandasBackend,
        duckdb_backend: DuckDBBackend,
        large_person_df: pd.DataFrame,
    ) -> None:
        p_est = pandas_backend.memory_estimate_bytes(large_person_df)
        d_est = duckdb_backend.memory_estimate_bytes(large_person_df)

        # Both should be within 10x of each other
        ratio = max(p_est, d_est) / max(min(p_est, d_est), 1)
        assert ratio < 10, (
            f"Memory estimates diverge: pandas={p_est}, duckdb={d_est}"
        )


# ---------------------------------------------------------------------------
# Backend selection at scale
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestBackendSelectionAtScale:
    """Verify auto-selection works correctly at different scales."""

    def test_auto_selects_pandas_for_small(self) -> None:
        be = select_backend(hint="auto", estimated_rows=1_000)
        assert be.name == "pandas"

    def test_auto_selects_polars_for_large(self) -> None:
        be = select_backend(hint="auto", estimated_rows=500_000)
        assert be.name == "polars"

    def test_auto_threshold_boundary(self) -> None:
        below = select_backend(hint="auto", estimated_rows=99_999)
        at = select_backend(hint="auto", estimated_rows=100_000)
        assert below.name == "pandas"
        assert at.name == "polars"
