"""Query planner validation tests.

Validates the Phase 2 QueryPlanner: join strategy selection,
aggregation strategy, memory estimation, and cross-join safety.
"""

from __future__ import annotations

import pytest
from pycypher.query_planner import (
    AggPlan,
    AggStrategy,
    JoinPlan,
    JoinStrategy,
    QueryPlan,
    QueryPlanner,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def planner() -> QueryPlanner:
    return QueryPlanner()


@pytest.fixture()
def tight_planner() -> QueryPlanner:
    """Planner with a tight 100MB memory budget."""
    return QueryPlanner(memory_budget_bytes=100 * 1024 * 1024)


# ---------------------------------------------------------------------------
# Join strategy selection
# ---------------------------------------------------------------------------


class TestJoinStrategySelection:
    """Verify join strategy is selected correctly based on data characteristics."""

    def test_broadcast_for_small_table(self, planner: QueryPlanner) -> None:
        """Small table (<10K rows) should use broadcast join."""
        plan = planner.plan_join(
            left_name="Person",
            right_name="City",
            left_rows=1_000_000,
            right_rows=100,
            join_key="city_id",
        )
        assert plan.strategy == JoinStrategy.BROADCAST

    def test_broadcast_symmetry(self, planner: QueryPlanner) -> None:
        """Broadcast should be selected regardless of which side is smaller."""
        plan = planner.plan_join(
            left_name="City",
            right_name="Person",
            left_rows=100,
            right_rows=1_000_000,
            join_key="city_id",
        )
        assert plan.strategy == JoinStrategy.BROADCAST

    def test_hash_for_large_tables(self, planner: QueryPlanner) -> None:
        """Two large tables should use hash join."""
        plan = planner.plan_join(
            left_name="Person",
            right_name="Order",
            left_rows=100_000,
            right_rows=500_000,
            join_key="person_id",
        )
        assert plan.strategy == JoinStrategy.HASH

    def test_merge_for_sorted_tables(self, planner: QueryPlanner) -> None:
        """Both-sorted tables should use merge join."""
        plan = planner.plan_join(
            left_name="Person",
            right_name="Order",
            left_rows=100_000,
            right_rows=500_000,
            join_key="id",
            left_sorted=True,
            right_sorted=True,
        )
        assert plan.strategy == JoinStrategy.MERGE

    def test_hash_when_only_one_sorted(self, planner: QueryPlanner) -> None:
        """Only one side sorted → should NOT use merge join."""
        plan = planner.plan_join(
            left_name="A",
            right_name="B",
            left_rows=100_000,
            right_rows=100_000,
            join_key="id",
            left_sorted=True,
            right_sorted=False,
        )
        assert plan.strategy != JoinStrategy.MERGE

    def test_memory_warning_on_large_hash(
        self, tight_planner: QueryPlanner
    ) -> None:
        """Hash join exceeding budget should produce warning in notes."""
        plan = tight_planner.plan_join(
            left_name="A",
            right_name="B",
            left_rows=10_000_000,
            right_rows=10_000_000,
            join_key="id",
        )
        assert plan.strategy == JoinStrategy.HASH
        assert "exceeds budget" in plan.notes


# ---------------------------------------------------------------------------
# Aggregation strategy selection
# ---------------------------------------------------------------------------


class TestAggregationStrategySelection:
    """Verify aggregation strategy is selected correctly."""

    def test_hash_agg_for_moderate_data(self, planner: QueryPlanner) -> None:
        """Moderate data should use hash aggregation."""
        plan = planner.plan_aggregation(
            input_rows=100_000,
            group_cardinality=1_000,
        )
        assert plan.strategy == AggStrategy.HASH_AGG

    def test_sort_agg_for_sorted_input(self, planner: QueryPlanner) -> None:
        """Pre-sorted input should use sort aggregation."""
        plan = planner.plan_aggregation(
            input_rows=100_000,
            group_cardinality=1_000,
            is_sorted=True,
        )
        assert plan.strategy == AggStrategy.SORT_AGG

    def test_streaming_agg_for_large_data(self, planner: QueryPlanner) -> None:
        """Very large data (>10M rows) should use streaming aggregation."""
        plan = planner.plan_aggregation(
            input_rows=50_000_000,
            group_cardinality=10_000,
        )
        assert plan.strategy == AggStrategy.STREAMING_AGG

    def test_sorted_takes_precedence_over_streaming(
        self, planner: QueryPlanner
    ) -> None:
        """Sorted input prefers sort-agg even for very large data."""
        plan = planner.plan_aggregation(
            input_rows=50_000_000,
            group_cardinality=10_000,
            is_sorted=True,
        )
        assert plan.strategy == AggStrategy.SORT_AGG


# ---------------------------------------------------------------------------
# Cross join planning
# ---------------------------------------------------------------------------


class TestCrossJoinPlanning:
    """Verify cross join plans with safety warnings."""

    def test_cross_join_calculates_output_rows(
        self, planner: QueryPlanner
    ) -> None:
        plan = planner.plan_cross_join(
            left_name="A",
            right_name="B",
            left_rows=100,
            right_rows=200,
        )
        assert plan.estimated_rows == 20_000  # noqa: PLR2004
        assert plan.strategy == JoinStrategy.NESTED_LOOP

    def test_cross_join_warning_for_large_output(
        self, planner: QueryPlanner
    ) -> None:
        """Cross join producing >100K rows should warn."""
        plan = planner.plan_cross_join(
            left_name="A",
            right_name="B",
            left_rows=1_000,
            right_rows=1_000,
        )
        assert plan.estimated_rows == 1_000_000  # noqa: PLR2004
        assert "WARNING" in plan.notes

    def test_cross_join_small_no_warning(self, planner: QueryPlanner) -> None:
        """Small cross join should not warn."""
        plan = planner.plan_cross_join(
            left_name="A",
            right_name="B",
            left_rows=10,
            right_rows=10,
        )
        assert plan.estimated_rows == 100  # noqa: PLR2004
        assert "WARNING" not in plan.notes


# ---------------------------------------------------------------------------
# Memory estimation
# ---------------------------------------------------------------------------


class TestMemoryEstimation:
    """Verify memory estimation and budget tracking."""

    def test_empty_plan_within_budget(self, planner: QueryPlanner) -> None:
        plan = QueryPlan()
        est = planner.estimate_memory(plan)
        assert est["within_budget"] is True
        assert est["total_estimated_bytes"] == 0

    def test_plan_with_joins_accumulates_memory(
        self, planner: QueryPlanner
    ) -> None:
        plan = QueryPlan(
            joins=[
                JoinPlan(
                    left_name="A",
                    right_name="B",
                    join_key="id",
                    strategy=JoinStrategy.HASH,
                    estimated_memory_bytes=500 * 1024 * 1024,
                ),
                JoinPlan(
                    left_name="C",
                    right_name="D",
                    join_key="id",
                    strategy=JoinStrategy.HASH,
                    estimated_memory_bytes=300 * 1024 * 1024,
                ),
            ]
        )
        est = planner.estimate_memory(plan)
        assert est["total_estimated_mb"] == pytest.approx(800, abs=1)

    def test_plan_with_aggregation(self, planner: QueryPlanner) -> None:
        plan = QueryPlan(
            aggregation=AggPlan(
                strategy=AggStrategy.HASH_AGG,
                estimated_memory_bytes=100 * 1024 * 1024,
            ),
        )
        est = planner.estimate_memory(plan)
        assert est["total_estimated_mb"] == pytest.approx(100, abs=1)

    def test_budget_exceeded_detection(
        self, tight_planner: QueryPlanner
    ) -> None:
        """Plan exceeding budget should be detected."""
        plan = QueryPlan(
            joins=[
                JoinPlan(
                    left_name="A",
                    right_name="B",
                    join_key="id",
                    strategy=JoinStrategy.HASH,
                    estimated_memory_bytes=200 * 1024 * 1024,
                ),
            ]
        )
        est = tight_planner.estimate_memory(plan)
        assert est["within_budget"] is False
        assert est["utilisation_pct"] > 100  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Planner consistency
# ---------------------------------------------------------------------------


class TestPlannerConsistency:
    """Verify planner produces consistent, deterministic results."""

    def test_same_input_same_plan(self, planner: QueryPlanner) -> None:
        """Identical inputs should produce identical plans."""
        kwargs = {
            "left_name": "A",
            "right_name": "B",
            "left_rows": 50_000,
            "right_rows": 50_000,
            "join_key": "id",
        }
        plan1 = planner.plan_join(**kwargs)
        plan2 = planner.plan_join(**kwargs)
        assert plan1.strategy == plan2.strategy
        assert plan1.estimated_memory_bytes == plan2.estimated_memory_bytes

    def test_all_strategies_reachable(self, planner: QueryPlanner) -> None:
        """All join strategies should be reachable with appropriate inputs."""
        broadcast = planner.plan_join(
            left_name="A",
            right_name="B",
            left_rows=1_000_000,
            right_rows=100,
            join_key="id",
        )
        hash_join = planner.plan_join(
            left_name="A",
            right_name="B",
            left_rows=100_000,
            right_rows=100_000,
            join_key="id",
        )
        merge = planner.plan_join(
            left_name="A",
            right_name="B",
            left_rows=100_000,
            right_rows=100_000,
            join_key="id",
            left_sorted=True,
            right_sorted=True,
        )
        cross = planner.plan_cross_join(
            left_name="A",
            right_name="B",
            left_rows=100,
            right_rows=100,
        )

        strategies = {
            broadcast.strategy,
            hash_join.strategy,
            merge.strategy,
            cross.strategy,
        }
        assert strategies == {
            JoinStrategy.BROADCAST,
            JoinStrategy.HASH,
            JoinStrategy.MERGE,
            JoinStrategy.NESTED_LOOP,
        }
