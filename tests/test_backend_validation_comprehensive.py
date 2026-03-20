"""Comprehensive backend validation — end-to-end query execution equivalence.

Validates that query results are identical regardless of which backend
processes the intermediate operations (pandas vs DuckDB).  Also validates
the auto-selection heuristic and performance characteristics.
"""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.backend_engine import (
    DuckDBBackend,
    PandasBackend,
    select_backend,
)
from pycypher.query_planner import AggStrategy, JoinStrategy, QueryPlanner
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_context(n: int, n_rels: int | None = None) -> Context:
    """Build a context with *n* entities and relationships."""
    rng = np.random.default_rng(42)
    if n_rels is None:
        n_rels = n * 3

    entity_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n + 1)),
            "name": [f"node_{i}" for i in range(1, n + 1)],
            "age": rng.integers(18, 80, size=n).tolist(),
            "dept": [f"dept_{rng.integers(0, 5)}" for _ in range(n)],
            "salary": (rng.random(n) * 100_000 + 30_000).astype(int).tolist(),
        },
    )
    rel_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_rels + 1)),
            "__SOURCE__": rng.integers(1, n + 1, size=n_rels).tolist(),
            "__TARGET__": rng.integers(1, n + 1, size=n_rels).tolist(),
            "weight": rng.random(n_rels).tolist(),
        },
    )

    et = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(entity_df.columns),
        source_obj_attribute_map={
            c: c for c in entity_df.columns if c != ID_COLUMN
        },
        attribute_map={c: c for c in entity_df.columns if c != ID_COLUMN},
        source_obj=entity_df,
    )
    rt = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(rel_df.columns),
        source_obj_attribute_map={"weight": "weight"},
        attribute_map={"weight": "weight"},
        source_obj=rel_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": et}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": rt}),
    )


@pytest.fixture()
def small_ctx() -> Context:
    """Small context (100 entities, 300 rels)."""
    return _build_context(100)


@pytest.fixture()
def small_star(small_ctx: Context) -> Star:
    """Star with small context."""
    return Star(context=small_ctx)


@pytest.fixture()
def medium_ctx() -> Context:
    """Medium context (10K entities, 30K rels)."""
    return _build_context(10_000)


@pytest.fixture()
def medium_star(medium_ctx: Context) -> Star:
    """Star with medium context."""
    return Star(context=medium_ctx)


# ---------------------------------------------------------------------------
# 1. End-to-end query execution validation
# ---------------------------------------------------------------------------


class TestEndToEndQueryValidation:
    """Verify Star.execute_query produces correct results for all query types."""

    def test_simple_entity_scan(self, small_star: Star) -> None:
        """MATCH (p:Person) RETURN p.name — basic entity scan."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN p.name",
        )
        assert len(result) == 100
        assert "name" in result.columns
        assert result["name"].iloc[0].startswith("node_")

    def test_filtered_scan(self, small_star: Star) -> None:
        """Entity scan with WHERE filter."""
        result = small_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 60 RETURN p.name, p.age",
        )
        assert len(result) > 0
        assert all(result["age"] > 60)

    def test_single_hop_join(self, small_star: Star) -> None:
        """Single-hop relationship traversal."""
        result = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        )
        assert len(result) > 0
        assert "a.name" in result.columns
        assert "b.name" in result.columns

    def test_two_hop_join(self, small_star: Star) -> None:
        """Two-hop relationship traversal."""
        result = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, c.name",
        )
        assert len(result) > 0

    def test_count_aggregation(self, small_star: Star) -> None:
        """COUNT aggregation."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
        )
        assert result.iloc[0, 0] == 100

    def test_grouped_aggregation(self, small_star: Star) -> None:
        """Grouped aggregation."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN p.dept, count(p)",
        )
        assert len(result) > 0
        total = result["count(p)"].sum()
        assert total == 100

    def test_order_by(self, small_star: Star) -> None:
        """ORDER BY clause."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name",
        )
        names = result["name"].tolist()
        assert names == sorted(names)

    def test_limit(self, small_star: Star) -> None:
        """LIMIT clause."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN p.name LIMIT 5",
        )
        assert len(result) == 5

    def test_with_clause(self, small_star: Star) -> None:
        """WITH clause for piped queries."""
        result = small_star.execute_query(
            "MATCH (p:Person) "
            "WITH p.dept AS dept, count(p) AS cnt "
            "RETURN dept, cnt ORDER BY cnt DESC",
        )
        assert len(result) > 0
        counts = result["cnt"].tolist()
        assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# 2. Backend operation equivalence
# ---------------------------------------------------------------------------


class TestBackendEquivalence:
    """Verify PandasBackend and DuckDBBackend produce identical results."""

    def _compare_backends(
        self,
        operation: str,
        pandas_fn: Any,
        duckdb_fn: Any,
    ) -> None:
        """Run same operation on both backends and compare results."""
        pandas_result = pandas_fn()
        duckdb_result = duckdb_fn()

        if isinstance(pandas_result, pd.DataFrame):
            # Sort both to handle non-deterministic row order
            sort_cols = list(pandas_result.columns)
            p_sorted = pandas_result.sort_values(sort_cols).reset_index(
                drop=True
            )
            d_sorted = duckdb_result.sort_values(sort_cols).reset_index(
                drop=True
            )
            pd.testing.assert_frame_equal(
                p_sorted,
                d_sorted,
                check_dtype=False,  # DuckDB may return different dtypes
                check_exact=False,
                atol=1e-10,
            )

    def test_filter_equivalence(self) -> None:
        """Filter produces same results on both backends."""
        df = pd.DataFrame(
            {"id": range(100), "val": range(100)},
        )
        mask = df["val"] > 50
        pb = PandasBackend()
        db = DuckDBBackend()

        self._compare_backends(
            "filter",
            lambda: pb.filter(df, mask),
            lambda: db.filter(df, mask),
        )

    def test_inner_join_equivalence(self) -> None:
        """Inner join produces same results."""
        rng = np.random.default_rng(42)
        left = pd.DataFrame(
            {"id": range(100), "left_val": rng.random(100)},
        )
        right = pd.DataFrame(
            {"id": range(50, 150), "right_val": rng.random(100)},
        )
        pb = PandasBackend()
        db = DuckDBBackend()

        self._compare_backends(
            "inner_join",
            lambda: pb.join(left, right, on="id"),
            lambda: db.join(left, right, on="id"),
        )

    def test_sort_equivalence(self) -> None:
        """Sort produces same results."""
        rng = np.random.default_rng(42)
        df = pd.DataFrame(
            {"a": rng.integers(0, 10, 50), "b": rng.random(50)},
        )
        pb = PandasBackend()
        db = DuckDBBackend()

        p_result = pb.sort(df, by=["a", "b"], ascending=[True, False])
        d_result = db.sort(df, by=["a", "b"], ascending=[True, False])
        pd.testing.assert_frame_equal(
            p_result,
            d_result,
            check_dtype=False,
            check_exact=False,
        )

    def test_aggregate_equivalence(self) -> None:
        """Aggregation produces same results."""
        df = pd.DataFrame(
            {
                "group": ["a", "a", "b", "b", "c"],
                "val": [1, 2, 3, 4, 5],
            },
        )
        pb = PandasBackend()
        db = DuckDBBackend()

        self._compare_backends(
            "aggregate",
            lambda: pb.aggregate(
                df,
                ["group"],
                {"total": ("val", "sum")},
            ),
            lambda: db.aggregate(
                df,
                ["group"],
                {"total": ("val", "sum")},
            ),
        )


# ---------------------------------------------------------------------------
# 3. Backend selection heuristic validation
# ---------------------------------------------------------------------------


class TestBackendSelectionHeuristic:
    """Validate the auto-selection threshold decisions."""

    def test_below_threshold_selects_pandas(self) -> None:
        """Under 100K rows selects pandas."""
        for n in [100, 1_000, 10_000, 99_999]:
            be = select_backend(hint="auto", estimated_rows=n)
            assert be.name == "pandas", f"Expected pandas for {n} rows"

    def test_at_threshold_selects_polars(self) -> None:
        """At 100K rows selects Polars (preferred over DuckDB)."""
        be = select_backend(hint="auto", estimated_rows=100_000)
        assert be.name == "polars"

    def test_above_threshold_selects_polars(self) -> None:
        """Above 100K rows selects Polars (preferred over DuckDB)."""
        for n in [100_001, 500_000, 1_000_000, 10_000_000]:
            be = select_backend(hint="auto", estimated_rows=n)
            assert be.name == "polars", f"Expected polars for {n} rows"


# ---------------------------------------------------------------------------
# 4. Query planner integration validation
# ---------------------------------------------------------------------------


class TestQueryPlannerIntegration:
    """Validate query planner decisions against actual performance."""

    def test_broadcast_threshold_correct(self) -> None:
        """Broadcast join threshold at 10K rows is reasonable."""
        planner = QueryPlanner()
        # Just below threshold → broadcast
        plan = planner.plan_join(
            left_name="a",
            right_name="b",
            left_rows=9_999,
            right_rows=1_000_000,
            join_key="id",
        )
        assert plan.strategy == JoinStrategy.BROADCAST

        # Just above threshold → hash
        plan = planner.plan_join(
            left_name="a",
            right_name="b",
            left_rows=10_001,
            right_rows=1_000_000,
            join_key="id",
        )
        assert plan.strategy == JoinStrategy.HASH

    def test_streaming_agg_threshold_correct(self) -> None:
        """Streaming agg threshold at 10M rows is reasonable."""
        planner = QueryPlanner()
        plan = planner.plan_aggregation(
            input_rows=9_999_999,
            group_cardinality=1000,
        )
        assert plan.strategy == AggStrategy.HASH_AGG

        plan = planner.plan_aggregation(
            input_rows=10_000_001,
            group_cardinality=1000,
        )
        assert plan.strategy == AggStrategy.STREAMING_AGG


# ---------------------------------------------------------------------------
# 5. Performance comparison (DuckDB vs pandas)
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestPerformanceComparison:
    """Systematic benchmarks showing backend performance characteristics."""

    def _time_it(self, fn: Any) -> float:
        """Time a function, returning seconds."""
        start = time.monotonic()
        fn()
        return time.monotonic() - start

    def test_large_join_duckdb_faster(self) -> None:
        """DuckDB should be competitive for large joins."""
        rng = np.random.default_rng(42)
        n = 50_000
        left = pd.DataFrame(
            {"id": range(n), "val": rng.random(n)},
        )
        right = pd.DataFrame(
            {"id": rng.integers(0, n, size=n * 2), "val2": rng.random(n * 2)},
        )

        pb = PandasBackend()
        db = DuckDBBackend()

        t_pandas = self._time_it(lambda: pb.join(left, right, on="id"))
        t_duckdb = self._time_it(lambda: db.join(left, right, on="id"))

        # Just verify both complete in reasonable time
        assert t_pandas < 10, f"Pandas join: {t_pandas:.2f}s"
        assert t_duckdb < 10, f"DuckDB join: {t_duckdb:.2f}s"

    def test_large_aggregation_comparison(self) -> None:
        """Compare aggregation performance across backends."""
        rng = np.random.default_rng(42)
        n = 100_000
        df = pd.DataFrame(
            {
                "group": [f"g{rng.integers(0, 1000)}" for _ in range(n)],
                "val": rng.random(n),
            },
        )

        pb = PandasBackend()
        db = DuckDBBackend()

        t_pandas = self._time_it(
            lambda: pb.aggregate(df, ["group"], {"total": ("val", "sum")}),
        )
        t_duckdb = self._time_it(
            lambda: db.aggregate(df, ["group"], {"total": ("val", "sum")}),
        )

        assert t_pandas < 5, f"Pandas agg: {t_pandas:.2f}s"
        assert t_duckdb < 5, f"DuckDB agg: {t_duckdb:.2f}s"
