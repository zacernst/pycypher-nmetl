"""TDD tests for query planner accuracy metrics.

Validates that QueryMetrics tracks planner memory estimates vs actual
memory usage, plus planning time, so operators can assess whether
the planner's predictions reflect reality.
"""

from __future__ import annotations

import pytest
from shared.metrics import QUERY_METRICS


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """Reset metrics before each test."""
    QUERY_METRICS.reset()


class TestPlannerAccuracyRecording:
    """Verify record_query accepts planner metrics."""

    def test_record_query_accepts_estimated_memory(self) -> None:
        QUERY_METRICS.record_query(
            query_id="p1",
            elapsed_s=0.05,
            rows=10,
            memory_delta_mb=2.0,
            estimated_memory_mb=3.0,
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.total_queries == 1

    def test_planner_accuracy_ratio_in_snapshot(self) -> None:
        QUERY_METRICS.record_query(
            query_id="p1",
            elapsed_s=0.05,
            rows=10,
            memory_delta_mb=2.0,
            estimated_memory_mb=4.0,
        )
        snap = QUERY_METRICS.snapshot()
        # Ratio = estimated / actual = 4.0 / 2.0 = 2.0
        assert snap.planner_accuracy_ratio_p50 > 0.0

    def test_planner_accuracy_with_multiple_queries(self) -> None:
        # Over-estimate: 4.0 estimated, 2.0 actual -> ratio 2.0
        QUERY_METRICS.record_query(
            query_id="p1",
            elapsed_s=0.05,
            rows=10,
            memory_delta_mb=2.0,
            estimated_memory_mb=4.0,
        )
        # Under-estimate: 1.0 estimated, 2.0 actual -> ratio 0.5
        QUERY_METRICS.record_query(
            query_id="p2",
            elapsed_s=0.05,
            rows=10,
            memory_delta_mb=2.0,
            estimated_memory_mb=1.0,
        )
        snap = QUERY_METRICS.snapshot()
        # Mean absolute error: avg(|4-2|, |1-2|) = avg(2, 1) = 1.5
        assert snap.planner_mae_mb > 0.0

    def test_planner_accuracy_zero_actual_handled(self) -> None:
        """When actual memory delta is 0, ratio should be 0 (not divide by zero)."""
        QUERY_METRICS.record_query(
            query_id="p1",
            elapsed_s=0.05,
            rows=10,
            memory_delta_mb=0.0,
            estimated_memory_mb=1.0,
        )
        snap = QUERY_METRICS.snapshot()
        # Should not crash; ratio is 0.0 when actual is 0
        assert snap.planner_accuracy_ratio_p50 == 0.0


class TestPlanTimeTracking:
    """Verify plan_time_ms is tracked separately."""

    def test_record_query_accepts_plan_time(self) -> None:
        QUERY_METRICS.record_query(
            query_id="pt1",
            elapsed_s=0.05,
            rows=10,
            plan_time_ms=2.5,
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.plan_time_p50_ms > 0.0

    def test_plan_time_percentile_ordering(self) -> None:
        for i in range(20):
            QUERY_METRICS.record_query(
                query_id=f"pt{i}",
                elapsed_s=0.01,
                rows=1,
                plan_time_ms=float(i + 1),
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.plan_time_p50_ms <= snap.plan_time_p90_ms
        assert snap.plan_time_p90_ms <= snap.plan_time_max_ms


class TestPlannerMetricsReset:
    """Verify reset clears planner accuracy data."""

    def test_reset_clears_planner_metrics(self) -> None:
        QUERY_METRICS.record_query(
            query_id="r1",
            elapsed_s=0.01,
            rows=1,
            memory_delta_mb=1.0,
            estimated_memory_mb=2.0,
            plan_time_ms=5.0,
        )
        QUERY_METRICS.reset()
        snap = QUERY_METRICS.snapshot()
        assert snap.planner_accuracy_ratio_p50 == 0.0
        assert snap.planner_mae_mb == 0.0
        assert snap.plan_time_p50_ms == 0.0


class TestPlannerMetricsNoneHandling:
    """Backward compat when planner metrics are not provided."""

    def test_no_estimated_memory_still_works(self) -> None:
        QUERY_METRICS.record_query(
            query_id="n1",
            elapsed_s=0.01,
            rows=1,
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.planner_accuracy_ratio_p50 == 0.0
        assert snap.planner_mae_mb == 0.0

    def test_no_plan_time_still_works(self) -> None:
        QUERY_METRICS.record_query(
            query_id="n2",
            elapsed_s=0.01,
            rows=1,
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.plan_time_p50_ms == 0.0
