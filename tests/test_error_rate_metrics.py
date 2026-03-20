"""TDD tests for error rate trend detection in QueryMetrics.

Validates that MetricsSnapshot provides error_rate (overall) and
recent_error_rate (windowed) so operators can detect error spikes.
"""

from __future__ import annotations

import pytest
from shared.metrics import QUERY_METRICS


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """Reset metrics before each test."""
    QUERY_METRICS.reset()


class TestErrorRate:
    """Overall error rate as a fraction of total operations."""

    def test_error_rate_zero_when_no_queries(self) -> None:
        snap = QUERY_METRICS.snapshot()
        assert snap.error_rate == 0.0

    def test_error_rate_zero_when_all_succeed(self) -> None:
        for i in range(5):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.error_rate == 0.0

    def test_error_rate_one_when_all_fail(self) -> None:
        for i in range(5):
            QUERY_METRICS.record_error(
                query_id=f"e{i}", error_type="ValueError", elapsed_s=0.01
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.error_rate == 1.0

    def test_error_rate_mixed(self) -> None:
        for i in range(8):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        for i in range(2):
            QUERY_METRICS.record_error(
                query_id=f"e{i}", error_type="TypeError", elapsed_s=0.01
            )
        snap = QUERY_METRICS.snapshot()
        assert abs(snap.error_rate - 0.2) < 1e-9


class TestRecentErrorRate:
    """Windowed error rate over last 60s."""

    def test_recent_error_rate_zero_when_no_errors(self) -> None:
        QUERY_METRICS.record_query(query_id="q1", elapsed_s=0.01, rows=1)
        snap = QUERY_METRICS.snapshot()
        assert snap.recent_error_rate == 0.0

    def test_recent_error_rate_after_errors(self) -> None:
        for i in range(3):
            QUERY_METRICS.record_query(
                query_id=f"q{i}", elapsed_s=0.01, rows=1
            )
        QUERY_METRICS.record_error(
            query_id="e1", error_type="ValueError", elapsed_s=0.01
        )
        snap = QUERY_METRICS.snapshot()
        # 1 error out of 4 total ops in the window
        assert abs(snap.recent_error_rate - 0.25) < 1e-9

    def test_recent_error_rate_zero_when_no_operations(self) -> None:
        snap = QUERY_METRICS.snapshot()
        assert snap.recent_error_rate == 0.0


class TestErrorRateReset:
    """Verify reset clears error rate data."""

    def test_reset_clears_error_timestamps(self) -> None:
        QUERY_METRICS.record_error(
            query_id="e1", error_type="ValueError", elapsed_s=0.01
        )
        QUERY_METRICS.reset()
        snap = QUERY_METRICS.snapshot()
        assert snap.error_rate == 0.0
        assert snap.recent_error_rate == 0.0
