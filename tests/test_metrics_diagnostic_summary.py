"""TDD tests for MetricsSnapshot diagnostic summary and health status.

Validates that operators get human-readable summaries and automated
health classification from the metrics system.
"""

from __future__ import annotations

import pytest
from shared.metrics import QUERY_METRICS


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """Reset metrics before each test."""
    QUERY_METRICS.reset()


class TestSummaryMethod:
    """Verify MetricsSnapshot.summary() returns a readable string."""

    def test_summary_returns_string(self) -> None:
        snap = QUERY_METRICS.snapshot()
        result = snap.summary()
        assert isinstance(result, str)

    def test_summary_includes_query_count(self) -> None:
        for i in range(5):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=10
            )
        snap = QUERY_METRICS.snapshot()
        summary = snap.summary()
        assert "5" in summary

    def test_summary_includes_error_rate(self) -> None:
        QUERY_METRICS.record_query(query_id="q1", elapsed_s=0.01, rows=1)
        QUERY_METRICS.record_error(
            query_id="e1", error_type="ValueError", elapsed_s=0.01
        )
        snap = QUERY_METRICS.snapshot()
        summary = snap.summary()
        assert "error" in summary.lower()

    def test_summary_includes_timing(self) -> None:
        QUERY_METRICS.record_query(query_id="q1", elapsed_s=0.05, rows=1)
        snap = QUERY_METRICS.snapshot()
        summary = snap.summary()
        assert "p50" in summary.lower() or "ms" in summary.lower()

    def test_summary_multiline(self) -> None:
        QUERY_METRICS.record_query(query_id="q1", elapsed_s=0.01, rows=1)
        snap = QUERY_METRICS.snapshot()
        summary = snap.summary()
        assert "\n" in summary


class TestHealthStatus:
    """Verify MetricsSnapshot.health_status() returns correct classification."""

    def test_healthy_when_no_errors(self) -> None:
        for i in range(10):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.health_status() == "healthy"

    def test_healthy_when_empty(self) -> None:
        snap = QUERY_METRICS.snapshot()
        assert snap.health_status() == "healthy"

    def test_degraded_when_high_error_rate(self) -> None:
        # 9 successes + 1 error = 10% error rate -> degraded
        for i in range(9):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        QUERY_METRICS.record_error(
            query_id="e1", error_type="ValueError", elapsed_s=0.01
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.health_status() == "degraded"

    def test_unhealthy_when_very_high_error_rate(self) -> None:
        # 3 successes + 2 errors = 40% error rate -> unhealthy
        for i in range(3):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        for i in range(2):
            QUERY_METRICS.record_error(
                query_id=f"e{i}", error_type="ValueError", elapsed_s=0.01
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.health_status() == "unhealthy"

    def test_degraded_when_slow_queries_present(self) -> None:
        # 10 queries, 2 slow = 20% slow rate -> degraded
        for i in range(8):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        for i in range(2):
            QUERY_METRICS.record_query(query_id=f"s{i}", elapsed_s=2.0, rows=1)
        snap = QUERY_METRICS.snapshot()
        assert snap.health_status() in ("degraded", "unhealthy")

    def test_health_status_returns_string(self) -> None:
        snap = QUERY_METRICS.snapshot()
        result = snap.health_status()
        assert result in ("healthy", "degraded", "unhealthy")
