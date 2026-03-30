"""Backend integration readiness tests.

Validates that the BackendEngine protocol is complete and robust
enough to replace direct pandas calls in Star.execute_query().
These tests simulate the query execution pipeline using BackendEngine
operations to verify protocol coverage.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.backend_engine import BackendEngine, DuckDBBackend, PandasBackend

from .dataset_generator import (
    ID_COLUMN,
    SOURCE_COLUMN,
    TARGET_COLUMN,
    generate_person_dataframe,
    generate_relationship_dataframe,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def person_df() -> pd.DataFrame:
    return generate_person_dataframe(5_000, seed=42)


@pytest.fixture(scope="module")
def knows_df() -> pd.DataFrame:
    return generate_relationship_dataframe(20_000, 5_000, seed=43)


@pytest.fixture(params=["pandas", "duckdb"])
def backend(request: pytest.FixtureRequest) -> BackendEngine:
    if request.param == "pandas":
        return PandasBackend()
    return DuckDBBackend()


# ---------------------------------------------------------------------------
# Simulated query pipeline: MATCH (p:Person) WHERE p.age > 30 RETURN p.name
# ---------------------------------------------------------------------------


class TestSimulatedScanFilterProject:
    """Simulate: MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age."""

    def test_scan_filter_project_pipeline(
        self,
        backend: BackendEngine,
        person_df: pd.DataFrame,
    ) -> None:
        """Full pipeline: scan → filter → project → sort → limit."""
        # Step 1: Scan entity IDs
        ids = backend.scan_entity(person_df, "Person")
        assert backend.row_count(ids) == 5_000

        # Step 2: Filter (simulating WHERE p.age > 30)
        # In real execution, the evaluator computes the mask
        mask = person_df["age"] > 30
        filtered = backend.filter(person_df, mask)
        assert backend.row_count(filtered) < 5_000
        assert backend.row_count(filtered) > 0

        # Step 3: Sort (ORDER BY age DESC)
        sorted_df = backend.sort(filtered, by=["age"], ascending=[False])
        result = backend.to_pandas(sorted_df)
        ages = list(result["age"])
        assert ages == sorted(ages, reverse=True)

        # Step 4: Limit
        limited = backend.limit(sorted_df, 10)
        assert backend.row_count(limited) == 10


# ---------------------------------------------------------------------------
# Simulated query pipeline: MATCH (a)-[r:KNOWS]->(b) RETURN a.name, b.name
# ---------------------------------------------------------------------------


class TestSimulatedJoinPipeline:
    """Simulate: MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN ..."""

    def test_join_pipeline(
        self,
        backend: BackendEngine,
        person_df: pd.DataFrame,
        knows_df: pd.DataFrame,
    ) -> None:
        """Full pipeline: scan entities → scan rels → join → project."""
        # Step 1: Scan relationship source/target
        rel_cols = knows_df[[ID_COLUMN, SOURCE_COLUMN, TARGET_COLUMN]]

        # Step 2: Join on source → person (a)
        person_ids = person_df[[ID_COLUMN, "name"]].rename(
            columns={ID_COLUMN: SOURCE_COLUMN, "name": "src_name"},
        )
        joined_src = backend.join(
            rel_cols,
            person_ids,
            on=SOURCE_COLUMN,
            how="inner",
        )

        # Step 3: Join on target → person (b)
        person_ids_tgt = person_df[[ID_COLUMN, "name"]].rename(
            columns={ID_COLUMN: TARGET_COLUMN, "name": "tgt_name"},
        )
        joined_both = backend.join(
            joined_src,
            person_ids_tgt,
            on=TARGET_COLUMN,
            how="inner",
        )

        result = backend.to_pandas(joined_both)
        assert len(result) > 0
        assert "src_name" in result.columns
        assert "tgt_name" in result.columns


# ---------------------------------------------------------------------------
# Simulated aggregation pipeline
# ---------------------------------------------------------------------------


class TestSimulatedAggregationPipeline:
    """Simulate: MATCH (p:Person) RETURN p.city, count(p), avg(p.age)."""

    def test_grouped_multi_agg_distinct_sources(
        self,
        backend: BackendEngine,
        person_df: pd.DataFrame,
    ) -> None:
        """Grouped aggregation with distinct source columns."""
        result = backend.aggregate(
            person_df,
            group_cols=["city"],
            agg_specs={
                "cnt": (ID_COLUMN, "count"),
                "avg_age": ("age", "mean"),
            },
        )
        result_pd = backend.to_pandas(result)

        assert len(result_pd) > 0
        assert set(result_pd.columns) >= {"city", "cnt", "avg_age"}
        # Sum of counts should equal total rows
        assert result_pd["cnt"].sum() == 5_000

    def test_grouped_multi_agg_same_source(
        self,
        backend: BackendEngine,
        person_df: pd.DataFrame,
    ) -> None:
        """Multiple aggs on same source column.

        NOTE: PandasBackend.aggregate() has a known bug where multiple
        agg specs targeting the same source column overwrite each other
        in agg_dict. This test documents the limitation. DuckDB handles
        it correctly via SQL.
        """
        if backend.name == "pandas":
            pytest.xfail(
                "PandasBackend.aggregate() cannot handle multiple aggs "
                "on the same source column (dict key collision)",
            )

        result = backend.aggregate(
            person_df,
            group_cols=["city"],
            agg_specs={
                "min_age": ("age", "min"),
                "max_age": ("age", "max"),
            },
        )
        result_pd = backend.to_pandas(result)

        assert set(result_pd.columns) >= {"city", "min_age", "max_age"}
        for _, row in result_pd.iterrows():
            assert row["min_age"] <= row["max_age"]

    def test_full_table_agg(
        self,
        backend: BackendEngine,
        person_df: pd.DataFrame,
    ) -> None:
        """Full-table aggregation (no GROUP BY)."""
        result = backend.aggregate(
            person_df,
            group_cols=[],
            agg_specs={
                "total": (ID_COLUMN, "count"),
                "avg_age": ("age", "mean"),
            },
        )
        result_pd = backend.to_pandas(result)
        assert result_pd["total"].iloc[0] == 5_000
        assert 18 < result_pd["avg_age"].iloc[0] < 80


# ---------------------------------------------------------------------------
# Protocol completeness checks
# ---------------------------------------------------------------------------


class TestProtocolCompleteness:
    """Verify all protocol methods handle edge cases correctly."""

    def test_empty_dataframe_operations(self, backend: BackendEngine) -> None:
        """All operations should handle empty DataFrames gracefully."""
        empty = pd.DataFrame(
            {
                ID_COLUMN: pd.array([], dtype="int64"),
                "name": pd.array([], dtype="str"),
            },
        )

        # Scan
        scanned = backend.scan_entity(empty, "Person")
        assert backend.row_count(scanned) == 0

        # Filter
        mask = pd.Series([], dtype="bool")
        filtered = backend.filter(empty, mask)
        assert backend.row_count(filtered) == 0

        # Sort
        sorted_df = backend.sort(empty, by=[ID_COLUMN])
        assert backend.row_count(sorted_df) == 0

        # Limit
        limited = backend.limit(empty, 10)
        assert backend.row_count(limited) == 0

        # Memory estimate
        est = backend.memory_estimate_bytes(empty)
        assert est >= 0

    def test_single_row_operations(self, backend: BackendEngine) -> None:
        """All operations should work with a single-row DataFrame."""
        single = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"], "age": [30]})

        scanned = backend.scan_entity(single, "Person")
        assert backend.row_count(scanned) == 1

        mask = single["age"] > 25
        filtered = backend.filter(single, mask)
        assert backend.row_count(filtered) == 1

        sorted_df = backend.sort(single, by=["age"])
        assert backend.row_count(sorted_df) == 1

        limited = backend.limit(single, 100)
        assert backend.row_count(limited) == 1

    def test_limit_larger_than_frame(self, backend: BackendEngine) -> None:
        """LIMIT N where N > row count should return all rows."""
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3]})
        result = backend.limit(df, 1000)
        assert backend.row_count(result) == 3

    def test_sort_multiple_columns(self, backend: BackendEngine) -> None:
        """Multi-column sort with mixed directions."""
        df = pd.DataFrame(
            {
                "city": ["NYC", "NYC", "LA", "LA"],
                "age": [30, 25, 35, 20],
                "name": ["A", "B", "C", "D"],
            },
        )
        result = backend.to_pandas(
            backend.sort(df, by=["city", "age"], ascending=[True, False]),
        )
        # LA before NYC (ascending), within each city age descending
        assert list(result["city"]) == ["LA", "LA", "NYC", "NYC"]
        la_ages = list(result[result["city"] == "LA"]["age"])
        assert la_ages == [35, 20]

    def test_cross_join_produces_cartesian(
        self,
        backend: BackendEngine,
    ) -> None:
        """Cross join should produce n*m rows."""
        left = pd.DataFrame({"a": [1, 2, 3]})
        right = pd.DataFrame({"b": [4, 5]})
        result = backend.join(left, right, on=[], how="cross")
        assert backend.row_count(result) == 6

    def test_left_join_preserves_all_left_rows(
        self,
        backend: BackendEngine,
    ) -> None:
        """Left join preserves all left-side rows with NULLs."""
        left = pd.DataFrame({"id": [1, 2, 3], "a": ["x", "y", "z"]})
        right = pd.DataFrame({"id": [2], "b": ["p"]})
        result = backend.to_pandas(
            backend.join(left, right, on="id", how="left"),
        )
        assert len(result) == 3
        # id=1 and id=3 should have null in column 'b'
        null_count = result["b"].isna().sum()
        assert null_count == 2


# ---------------------------------------------------------------------------
# Backend interchangeability validation
# ---------------------------------------------------------------------------


class TestBackendInterchangeability:
    """Verify that switching backends mid-pipeline produces correct results."""

    def test_pandas_result_equals_duckdb_result(self) -> None:
        """Full pipeline produces identical results from both backends."""
        df = generate_person_dataframe(5_000, seed=42)
        pandas_be = PandasBackend()
        duckdb_be = DuckDBBackend()

        # Run same pipeline on both
        for be in [pandas_be, duckdb_be]:
            mask = df["age"] > 40
            filtered = be.filter(df, mask)
            agg = be.aggregate(
                filtered,
                group_cols=["city"],
                agg_specs={"cnt": (ID_COLUMN, "count")},
            )
            sorted_result = be.sort(agg, by=["city"])
            result = be.to_pandas(sorted_result)

            if be is pandas_be:
                pandas_result = result
            else:
                duckdb_result = result

        pd.testing.assert_frame_equal(
            pandas_result.reset_index(drop=True),
            duckdb_result.reset_index(drop=True),
            check_dtype=False,
        )

    def test_to_pandas_always_returns_dataframe(self) -> None:
        """to_pandas() must return pd.DataFrame from any backend."""
        df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["A", "B"]})
        for be in [PandasBackend(), DuckDBBackend()]:
            result = be.to_pandas(be.scan_entity(df, "Test"))
            assert isinstance(result, pd.DataFrame)
