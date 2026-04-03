"""Performance baseline tests for current pandas implementation.

Establishes baseline measurements at multiple dataset scales to
detect performance regressions and validate optimization targets.
These tests use the 'performance' marker and are excluded from
fast test runs.
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

from .benchmark_utils import run_benchmark
from .dataset_generator import (
    SCALE_MEDIUM,
    SCALE_SMALL,
    SCALE_TINY,
    generate_social_graph,
)

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures: reusable contexts at different scales
# ---------------------------------------------------------------------------


def _build_star(
    person_df: pd.DataFrame,
    knows_df: pd.DataFrame,
) -> Star:
    """Build a Star instance from person and knows DataFrames."""
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
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )
    return Star(context=ctx)


@pytest.fixture(scope="module")
def tiny_star() -> Star:
    """Star with ~100 persons, ~200 relationships."""
    person_df, knows_df = generate_social_graph(SCALE_TINY)
    return _build_star(person_df, knows_df)


@pytest.fixture(scope="module")
def small_star() -> Star:
    """Star with ~10K persons, ~50K relationships."""
    person_df, knows_df = generate_social_graph(SCALE_SMALL)
    return _build_star(person_df, knows_df)


@pytest.fixture(scope="module")
def medium_star() -> Star:
    """Star with ~100K persons, ~500K relationships."""
    person_df, knows_df = generate_social_graph(SCALE_MEDIUM)
    return _build_star(person_df, knows_df)


# ---------------------------------------------------------------------------
# Baseline: Entity scan + filter + projection
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestEntityScanBaseline:
    """Baseline performance for entity scan queries."""

    def test_simple_scan_tiny(self, tiny_star: Star) -> None:
        """Baseline: scan + filter + project on tiny dataset."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
        result = run_benchmark(
            lambda: tiny_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_TINY.person_rows,
            iterations=5,
        )
        result.assert_time_under(SCALE_TINY.max_query_time_s)

    def test_simple_scan_small(self, small_star: Star) -> None:
        """Baseline: scan + filter + project on small dataset."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.person_rows,
            iterations=5,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)

    @pytest.mark.slow
    def test_simple_scan_medium(self, medium_star: Star) -> None:
        """Baseline: scan + filter + project on medium dataset."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
        result = run_benchmark(
            lambda: medium_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_MEDIUM.person_rows,
            iterations=3,
        )
        result.assert_time_under(SCALE_MEDIUM.max_query_time_s)


# ---------------------------------------------------------------------------
# Baseline: Relationship join
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestJoinBaseline:
    """Baseline performance for relationship join queries."""

    def test_single_hop_join_tiny(self, tiny_star: Star) -> None:
        query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt"
        result = run_benchmark(
            lambda: tiny_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_TINY.relationship_rows,
            iterations=5,
        )
        result.assert_time_under(SCALE_TINY.max_query_time_s)

    def test_single_hop_join_small(self, small_star: Star) -> None:
        query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt"
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.relationship_rows,
            iterations=3,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)


# ---------------------------------------------------------------------------
# Baseline: Aggregation
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestAggregationBaseline:
    """Baseline performance for aggregation queries."""

    def test_count_aggregation_tiny(self, tiny_star: Star) -> None:
        query = "MATCH (p:Person) RETURN count(p) AS cnt"
        result = run_benchmark(
            lambda: tiny_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_TINY.person_rows,
            iterations=5,
        )
        result.assert_time_under(SCALE_TINY.max_query_time_s)

    def test_grouped_aggregation_small(self, small_star: Star) -> None:
        query = "MATCH (p:Person) RETURN p.city AS city, count(p) AS cnt ORDER BY cnt DESC"
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.person_rows,
            iterations=3,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)

    def test_sum_aggregation_small(self, small_star: Star) -> None:
        query = (
            "MATCH (p:Person) RETURN p.dept AS dept, sum(p.age) AS total_age"
        )
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.person_rows,
            iterations=3,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)


# ---------------------------------------------------------------------------
# Baseline: WITH clause pipeline
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestWithClauseBaseline:
    """Baseline performance for WITH clause pipelines."""

    def test_with_filter_pipeline_small(self, small_star: Star) -> None:
        query = (
            "MATCH (p:Person) "
            "WITH p.name AS name, p.age AS age "
            "WHERE age > 40 "
            "RETURN name"
        )
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.person_rows,
            iterations=3,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)


# ---------------------------------------------------------------------------
# Baseline: ORDER BY + LIMIT + SKIP
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestOrderByBaseline:
    """Baseline performance for ORDER BY with LIMIT/SKIP."""

    def test_order_limit_small(self, small_star: Star) -> None:
        query = (
            "MATCH (p:Person) "
            "RETURN p.name AS name, p.age AS age "
            "ORDER BY p.age DESC LIMIT 10"
        )
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.person_rows,
            iterations=5,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)

    def test_order_skip_limit_small(self, small_star: Star) -> None:
        query = (
            "MATCH (p:Person) "
            "RETURN p.name AS name, p.age AS age "
            "ORDER BY p.age ASC SKIP 100 LIMIT 50"
        )
        result = run_benchmark(
            lambda: small_star.execute_query(query),
            query=query,
            dataset_rows=SCALE_SMALL.person_rows,
            iterations=5,
        )
        result.assert_time_under(SCALE_SMALL.max_query_time_s)


# ---------------------------------------------------------------------------
# Baseline: Scaling behavior (measures how time grows with data size)
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestScalingBehavior:
    """Verify that query execution scales linearly (not quadratically)."""

    def test_scan_scales_linearly(self) -> None:
        """Verify scan performance scales ~linearly with row count."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
        times = []

        for scale in [SCALE_TINY, SCALE_SMALL]:
            person_df, knows_df = generate_social_graph(scale)
            star = _build_star(person_df, knows_df)
            bench = run_benchmark(
                lambda s=star: s.execute_query(query),
                query=query,
                dataset_rows=scale.person_rows,
                iterations=3,
                warmup=1,
            )
            times.append((scale.person_rows, bench.median_time_s))
            del star, person_df, knows_df
            gc.collect()

        # Check that scaling factor is sub-quadratic
        # If 100x data increase → <200x time increase, it's sub-quadratic
        rows_ratio = times[1][0] / times[0][0]
        time_ratio = times[1][1] / max(times[0][1], 1e-6)
        # Allow up to 3x the linear scaling factor as a generous bound
        max_acceptable_ratio = rows_ratio * 3
        assert time_ratio < max_acceptable_ratio, (
            f"Scaling appears super-linear: "
            f"{rows_ratio:.0f}x rows → {time_ratio:.1f}x time "
            f"(max acceptable: {max_acceptable_ratio:.1f}x)"
        )
