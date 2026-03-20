"""Tests for the QueryPlanner — join strategy and aggregation planning.

Validates that the planner selects optimal strategies based on
data characteristics (size, sort order, cardinality).
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
    """Default query planner."""
    return QueryPlanner()


@pytest.fixture()
def tight_planner() -> QueryPlanner:
    """Planner with a very tight memory budget (100MB)."""
    return QueryPlanner(memory_budget_bytes=100 * 1024 * 1024)


# ---------------------------------------------------------------------------
# Join strategy selection
# ---------------------------------------------------------------------------


class TestJoinStrategySelection:
    """Verify the planner selects the correct join strategy."""

    def test_small_right_gets_broadcast(self, planner: QueryPlanner) -> None:
        """Small right table triggers broadcast join."""
        plan = planner.plan_join(
            left_name="entities",
            right_name="rels",
            left_rows=1_000_000,
            right_rows=100,
            join_key="__ID__",
        )
        assert plan.strategy == JoinStrategy.BROADCAST
        assert "Broadcast" in plan.notes

    def test_small_left_gets_broadcast(self, planner: QueryPlanner) -> None:
        """Small left table also triggers broadcast join."""
        plan = planner.plan_join(
            left_name="rels",
            right_name="entities",
            left_rows=50,
            right_rows=500_000,
            join_key="__ID__",
        )
        assert plan.strategy == JoinStrategy.BROADCAST

    def test_both_sorted_gets_merge(self, planner: QueryPlanner) -> None:
        """Both sides sorted triggers merge join."""
        plan = planner.plan_join(
            left_name="sorted_a",
            right_name="sorted_b",
            left_rows=100_000,
            right_rows=100_000,
            join_key="__ID__",
            left_sorted=True,
            right_sorted=True,
        )
        assert plan.strategy == JoinStrategy.MERGE
        assert "Merge" in plan.notes

    def test_large_unsorted_gets_hash(self, planner: QueryPlanner) -> None:
        """Large unsorted tables get hash join."""
        plan = planner.plan_join(
            left_name="big_a",
            right_name="big_b",
            left_rows=500_000,
            right_rows=500_000,
            join_key="__ID__",
        )
        assert plan.strategy == JoinStrategy.HASH
        assert "Hash" in plan.notes

    def test_memory_budget_warning(self, tight_planner: QueryPlanner) -> None:
        """Hash join exceeding budget produces warning in notes."""
        plan = tight_planner.plan_join(
            left_name="huge_a",
            right_name="huge_b",
            left_rows=5_000_000,
            right_rows=5_000_000,
            join_key="__ID__",
            avg_row_bytes=200,
        )
        assert plan.strategy == JoinStrategy.HASH
        assert "exceeds budget" in plan.notes

    def test_join_key_preserved(self, planner: QueryPlanner) -> None:
        """Join key is preserved in the plan."""
        plan = planner.plan_join(
            left_name="a",
            right_name="b",
            left_rows=100,
            right_rows=100,
            join_key=["id1", "id2"],
        )
        assert plan.join_key == ["id1", "id2"]


# ---------------------------------------------------------------------------
# Cross join planning
# ---------------------------------------------------------------------------


class TestCrossJoinPlanning:
    """Verify cross join safety checks."""

    def test_small_cross_join(self, planner: QueryPlanner) -> None:
        """Small cross join produces no warning."""
        plan = planner.plan_cross_join(
            left_name="a",
            right_name="b",
            left_rows=10,
            right_rows=10,
        )
        assert plan.strategy == JoinStrategy.NESTED_LOOP
        assert plan.estimated_rows == 100
        assert "WARNING" not in plan.notes

    def test_large_cross_join_warns(self, planner: QueryPlanner) -> None:
        """Large cross join produces warning."""
        plan = planner.plan_cross_join(
            left_name="a",
            right_name="b",
            left_rows=1000,
            right_rows=1000,
        )
        assert plan.estimated_rows == 1_000_000
        assert "WARNING" in plan.notes


# ---------------------------------------------------------------------------
# Aggregation strategy selection
# ---------------------------------------------------------------------------


class TestAggregationPlanning:
    """Verify aggregation strategy selection."""

    def test_sorted_input_gets_sort_agg(self, planner: QueryPlanner) -> None:
        """Pre-sorted input triggers sort-based aggregation."""
        plan = planner.plan_aggregation(
            input_rows=100_000,
            group_cardinality=1000,
            is_sorted=True,
        )
        assert plan.strategy == AggStrategy.SORT_AGG

    def test_large_dataset_gets_streaming(self, planner: QueryPlanner) -> None:
        """Very large input triggers streaming aggregation."""
        plan = planner.plan_aggregation(
            input_rows=50_000_000,
            group_cardinality=10_000,
        )
        assert plan.strategy == AggStrategy.STREAMING_AGG
        assert "Streaming" in plan.notes

    def test_normal_dataset_gets_hash_agg(self, planner: QueryPlanner) -> None:
        """Normal-sized input gets hash aggregation."""
        plan = planner.plan_aggregation(
            input_rows=100_000,
            group_cardinality=1000,
        )
        assert plan.strategy == AggStrategy.HASH_AGG


# ---------------------------------------------------------------------------
# Memory estimation
# ---------------------------------------------------------------------------


class TestMemoryEstimation:
    """Verify memory budget calculations."""

    def test_within_budget(self, planner: QueryPlanner) -> None:
        """Small plan fits within default 2GB budget."""
        plan = QueryPlan(
            joins=[
                JoinPlan(
                    left_name="a",
                    right_name="b",
                    join_key="id",
                    strategy=JoinStrategy.HASH,
                    estimated_memory_bytes=100 * 1024 * 1024,  # 100MB
                ),
            ],
        )
        est = planner.estimate_memory(plan)
        assert est["within_budget"]
        assert est["total_estimated_mb"] == pytest.approx(100, abs=1)

    def test_over_budget(self, tight_planner: QueryPlanner) -> None:
        """Large plan exceeds tight budget."""
        plan = QueryPlan(
            joins=[
                JoinPlan(
                    left_name="a",
                    right_name="b",
                    join_key="id",
                    strategy=JoinStrategy.HASH,
                    estimated_memory_bytes=200 * 1024 * 1024,  # 200MB
                ),
            ],
        )
        est = tight_planner.estimate_memory(plan)
        assert not est["within_budget"]

    def test_aggregation_included(self, planner: QueryPlanner) -> None:
        """Memory estimate includes aggregation overhead."""
        plan = QueryPlan(
            joins=[],
            aggregation=AggPlan(
                strategy=AggStrategy.HASH_AGG,
                group_cardinality=10_000,
                estimated_memory_bytes=50 * 1024 * 1024,
            ),
        )
        est = planner.estimate_memory(plan)
        assert est["total_estimated_mb"] == pytest.approx(50, abs=1)

    def test_utilisation_pct(self, planner: QueryPlanner) -> None:
        """Utilisation percentage is calculated correctly."""
        budget_mb = 2048  # 2GB
        used_bytes = 1024 * 1024 * 1024  # 1GB
        plan = QueryPlan(
            joins=[
                JoinPlan(
                    left_name="a",
                    right_name="b",
                    join_key="id",
                    strategy=JoinStrategy.HASH,
                    estimated_memory_bytes=used_bytes,
                ),
            ],
        )
        est = planner.estimate_memory(plan)
        assert est["utilisation_pct"] == pytest.approx(50, abs=1)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_zero_rows_left(self, planner: QueryPlanner) -> None:
        """Empty left side still produces a valid plan."""
        plan = planner.plan_join(
            left_name="empty",
            right_name="full",
            left_rows=0,
            right_rows=100_000,
            join_key="id",
        )
        assert plan.strategy == JoinStrategy.BROADCAST
        assert plan.estimated_memory_bytes == 0

    def test_both_zero_rows(self, planner: QueryPlanner) -> None:
        """Both sides empty produces valid plan."""
        plan = planner.plan_join(
            left_name="a",
            right_name="b",
            left_rows=0,
            right_rows=0,
            join_key="id",
        )
        assert plan.strategy == JoinStrategy.BROADCAST

    def test_one_sorted_not_enough_for_merge(
        self,
        planner: QueryPlanner,
    ) -> None:
        """Only one side sorted doesn't trigger merge join."""
        plan = planner.plan_join(
            left_name="sorted",
            right_name="unsorted",
            left_rows=100_000,
            right_rows=100_000,
            join_key="id",
            left_sorted=True,
            right_sorted=False,
        )
        assert plan.strategy == JoinStrategy.HASH
