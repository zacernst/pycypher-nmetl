"""End-to-end integration tests for the Phase 1 pipeline.

Validates the full BackendEngine → BindingFrame → Star pipeline with
both PandasBackend and DuckDBBackend.  Focuses on memory-intensive
scenarios that prove large dataset capabilities and verifies that
backend selection via Context(backend=...) produces correct results.
"""

from __future__ import annotations

import gc

import pandas as pd
import pytest
from pycypher import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
    Star,
)
from pycypher.backend_engine import DuckDBBackend, PandasBackend

from .benchmark_utils import _get_process_memory_mb, run_benchmark
from .dataset_generator import SCALE_SMALL, SCALE_TINY, generate_social_graph
from _perf_helpers import perf_threshold

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_context(
    person_df: pd.DataFrame,
    knows_df: pd.DataFrame,
    *,
    backend: str | None = None,
) -> Context:
    """Build a Context with an optional backend hint."""
    person_table = EntityTable.from_dataframe("Person", person_df)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            "__SOURCE__",
            "__TARGET__",
            "since",
            "weight",
        ],
        source_obj_attribute_map={"since": "since", "weight": "weight"},
        attribute_map={"since": "since", "weight": "weight"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    kwargs: dict = {
        "entity_mapping": EntityMapping(mapping={"Person": person_table}),
        "relationship_mapping": RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    }
    if backend is not None:
        kwargs["backend"] = backend
    return Context(**kwargs)


def _build_star(
    person_df: pd.DataFrame,
    knows_df: pd.DataFrame,
    *,
    backend: str | None = None,
) -> Star:
    ctx = _build_context(person_df, knows_df, backend=backend)
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def social_tiny() -> tuple[pd.DataFrame, pd.DataFrame]:
    return generate_social_graph(SCALE_TINY)


@pytest.fixture(scope="module")
def social_small() -> tuple[pd.DataFrame, pd.DataFrame]:
    return generate_social_graph(SCALE_SMALL)


# ---------------------------------------------------------------------------
# Backend wiring validation
# ---------------------------------------------------------------------------


class TestBackendWiring:
    """Verify Context(backend=...) correctly wires the backend engine."""

    def test_default_backend_is_pandas(
        self,
        social_tiny: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Default Context uses PandasBackend."""
        person_df, knows_df = social_tiny
        ctx = _build_context(person_df, knows_df)
        assert isinstance(ctx.backend, PandasBackend)

    def test_explicit_pandas_backend(
        self,
        social_tiny: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        ctx = _build_context(*social_tiny, backend="pandas")
        assert isinstance(ctx.backend, PandasBackend)

    def test_explicit_duckdb_backend(
        self,
        social_tiny: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        ctx = _build_context(*social_tiny, backend="duckdb")
        assert isinstance(ctx.backend, DuckDBBackend)

    def test_auto_backend_small_picks_pandas(
        self,
        social_tiny: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Auto selection with small data should pick pandas."""
        ctx = _build_context(*social_tiny, backend="auto")
        assert ctx.backend.name == "pandas"

    def test_pre_constructed_backend(
        self,
        social_tiny: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Passing a BackendEngine instance directly."""
        be = DuckDBBackend()
        person_df, knows_df = social_tiny
        ctx = _build_context(person_df, knows_df, backend=be)
        assert ctx.backend is be


# ---------------------------------------------------------------------------
# Cross-backend correctness: same query, same results
# ---------------------------------------------------------------------------


CORRECTNESS_QUERIES = {
    "simple_scan": "MATCH (n:Person) RETURN n.name",
    "filtered_scan": "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
    "single_hop": (
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name"
    ),
    "aggregation": ("MATCH (n:Person) RETURN n.city, count(n) AS cnt"),
    "order_limit": (
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC LIMIT 5"
    ),
}


class TestCrossBackendCorrectness:
    """Both backends must produce identical results for the same query."""

    @pytest.mark.parametrize("query_name", list(CORRECTNESS_QUERIES))
    def test_pandas_equals_duckdb(
        self,
        query_name: str,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        query = CORRECTNESS_QUERIES[query_name]
        person_df, knows_df = social_small

        pandas_star = _build_star(person_df, knows_df, backend="pandas")
        duckdb_star = _build_star(person_df, knows_df, backend="duckdb")

        pandas_result = pandas_star.execute_query(query)
        duckdb_result = duckdb_star.execute_query(query)

        # Same shape
        assert pandas_result.shape == duckdb_result.shape, (
            f"Shape mismatch for {query_name}: "
            f"pandas={pandas_result.shape}, duckdb={duckdb_result.shape}"
        )
        # Same columns
        assert list(pandas_result.columns) == list(duckdb_result.columns)

        # Sort both for deterministic comparison
        sort_cols = list(pandas_result.columns)
        p_sorted = pandas_result.sort_values(sort_cols).reset_index(drop=True)
        d_sorted = duckdb_result.sort_values(sort_cols).reset_index(drop=True)

        pd.testing.assert_frame_equal(
            p_sorted,
            d_sorted,
            check_dtype=False,
        )


# ---------------------------------------------------------------------------
# Memory-intensive join scenarios
# ---------------------------------------------------------------------------


class TestMemoryIntensiveJoins:
    """Validate join-heavy queries work correctly with both backends."""

    def test_single_hop_at_scale(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """10K-person single hop join completes without error."""
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
            )
            assert len(result) > 0, f"{backend} returned empty result"
            assert list(result.columns) == ["a.name", "b.name"]

    def test_filtered_hop_at_scale(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Filtered single-hop join produces subset of full join."""
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            full = star.execute_query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
            )
            filtered = star.execute_query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) "
                "WHERE a.age > 40 "
                "RETURN a.name, b.name",
            )
            assert len(filtered) < len(full), (
                f"{backend}: filtered ({len(filtered)}) >= full ({len(full)})"
            )
            assert len(filtered) > 0

    def test_vlp_with_limit_at_scale(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Variable-length path + LIMIT at 10K scale."""
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
                "RETURN a.name, b.name LIMIT 50",
            )
            assert len(result) == 50, (
                f"{backend}: expected 50 rows, got {len(result)}"
            )


# ---------------------------------------------------------------------------
# Aggregation pipeline end-to-end
# ---------------------------------------------------------------------------


class TestAggregationPipeline:
    """Validate aggregation queries produce correct results with both backends."""

    def test_count_aggregation(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (n:Person) RETURN n.city, count(n) AS cnt",
            )
            assert len(result) > 0
            assert result["cnt"].sum() == len(person_df), (
                f"{backend}: count sum {result['cnt'].sum()} != {len(person_df)}"
            )

    def test_avg_aggregation(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (n:Person) RETURN avg(n.age) AS avg_age",
            )
            assert len(result) == 1
            avg_age = result["avg_age"].iloc[0]
            assert 18 < avg_age < 80, (
                f"{backend}: avg_age {avg_age} out of range"
            )


# ---------------------------------------------------------------------------
# WITH pipeline end-to-end
# ---------------------------------------------------------------------------


class TestWithPipeline:
    """WITH clause processing end-to-end with both backends."""

    def test_with_passthrough(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (n:Person) "
                "WITH n.name AS name, n.age AS age "
                "WHERE age > 30 "
                "RETURN name, age "
                "ORDER BY age DESC LIMIT 10",
            )
            assert len(result) <= 10
            assert len(result) > 0
            ages = list(result["age"])
            assert ages == sorted(ages, reverse=True)
            assert all(a > 30 for a in ages)


# ---------------------------------------------------------------------------
# Memory stability under repeated execution
# ---------------------------------------------------------------------------


class TestMemoryStability:
    """Verify no memory leaks across repeated query executions."""

    def test_repeated_queries_stable_memory(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """20 repeated queries should not leak memory."""
        person_df, knows_df = social_small
        star = _build_star(person_df, knows_df, backend="pandas")
        query = "MATCH (n:Person) RETURN n.name, n.age"

        # Warmup
        for _ in range(3):
            star.execute_query(query)
        gc.collect()
        baseline = _get_process_memory_mb()

        for _ in range(20):
            star.execute_query(query)

        gc.collect()
        growth = _get_process_memory_mb() - baseline
        assert growth < perf_threshold(200), (
            f"Memory grew by {growth:.1f}MB over 20 iterations"
        )

    def test_backend_switching_no_leak(
        self,
        social_tiny: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Creating stars with different backends shouldn't leak."""
        person_df, knows_df = social_tiny
        query = "MATCH (n:Person) RETURN n.name"

        # Warmup
        for backend in ["pandas", "duckdb"]:
            _build_star(person_df, knows_df, backend=backend).execute_query(
                query,
            )
        gc.collect()
        baseline = _get_process_memory_mb()

        for _ in range(10):
            for backend in ["pandas", "duckdb"]:
                star = _build_star(person_df, knows_df, backend=backend)
                star.execute_query(query)

        gc.collect()
        growth = _get_process_memory_mb() - baseline
        assert growth < perf_threshold(100), f"Backend switching leaked {growth:.1f}MB"


# ---------------------------------------------------------------------------
# LIMIT pushdown integration
# ---------------------------------------------------------------------------


class TestLimitPushdownIntegration:
    """LIMIT pushdown through the full Star pipeline."""

    def test_limit_respected_across_backends(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (n:Person) RETURN n.name LIMIT 7",
            )
            assert len(result) == 7, (
                f"{backend}: LIMIT 7 returned {len(result)} rows"
            )

    def test_limit_on_vlp_across_backends(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        person_df, knows_df = social_small
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            result = star.execute_query(
                "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
                "RETURN a.name, b.name LIMIT 25",
            )
            assert len(result) == 25, (
                f"{backend}: VLP LIMIT 25 returned {len(result)}"
            )


# ---------------------------------------------------------------------------
# Performance: DuckDB join advantage at scale
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestBackendPerformanceAtScale:
    """Verify backend performance characteristics at 10K scale."""

    def test_join_completes_both_backends(
        self,
        social_small: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """Single-hop join at 10K completes in reasonable time."""
        person_df, knows_df = social_small
        query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
        for backend in ["pandas", "duckdb"]:
            star = _build_star(person_df, knows_df, backend=backend)
            bench = run_benchmark(
                lambda s=star: s.execute_query(query),
                iterations=3,
            )
            bench.assert_time_under(10.0)
