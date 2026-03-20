"""Tests for query throughput rate tracking in MetricsSnapshot (Task #14).

Verifies that MetricsSnapshot includes:
1. ``queries_per_second`` — derived from total_queries / uptime_s
2. ``rows_per_second`` — derived from total_rows_returned / uptime_s
3. ``recent_queries_per_second`` — time-windowed rate (queries in last 60s)

TDD: These tests are written BEFORE the implementation.

Run with:
    uv run pytest tests/test_throughput_rate_tracking.py -v
"""

from __future__ import annotations

import pytest
from shared.metrics import QueryMetrics

# ---------------------------------------------------------------------------
# Unit tests — derived throughput fields
# ---------------------------------------------------------------------------


class TestQueriesPerSecond:
    """Verify queries_per_second is computed from total_queries / uptime_s."""

    def test_zero_queries_returns_zero(self) -> None:
        m = QueryMetrics()
        snap = m.snapshot()
        assert snap.queries_per_second == 0.0

    def test_queries_per_second_is_positive_after_recording(self) -> None:
        m = QueryMetrics()
        for i in range(10):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=5)
        snap = m.snapshot()
        assert snap.queries_per_second > 0.0

    def test_queries_per_second_equals_total_over_uptime(self) -> None:
        m = QueryMetrics()
        for i in range(20):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=1)
        snap = m.snapshot()
        expected = snap.total_queries / snap.uptime_s
        assert snap.queries_per_second == pytest.approx(expected, rel=0.01)


class TestRowsPerSecond:
    """Verify rows_per_second is computed from total_rows_returned / uptime_s."""

    def test_zero_rows_returns_zero(self) -> None:
        m = QueryMetrics()
        snap = m.snapshot()
        assert snap.rows_per_second == 0.0

    def test_rows_per_second_is_positive_after_recording(self) -> None:
        m = QueryMetrics()
        for i in range(5):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=100)
        snap = m.snapshot()
        assert snap.rows_per_second > 0.0

    def test_rows_per_second_equals_total_over_uptime(self) -> None:
        m = QueryMetrics()
        for i in range(10):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=50)
        snap = m.snapshot()
        expected = snap.total_rows_returned / snap.uptime_s
        assert snap.rows_per_second == pytest.approx(expected, rel=0.01)


class TestRecentQueriesPerSecond:
    """Verify recent_queries_per_second tracks queries in a sliding window."""

    def test_zero_when_no_queries(self) -> None:
        m = QueryMetrics()
        snap = m.snapshot()
        assert snap.recent_queries_per_second == 0.0

    def test_positive_after_recent_queries(self) -> None:
        m = QueryMetrics()
        for i in range(10):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=1)
        snap = m.snapshot()
        assert snap.recent_queries_per_second > 0.0

    def test_recent_rate_reflects_recent_activity(self) -> None:
        m = QueryMetrics()
        # Record 10 queries — all should be "recent"
        for i in range(10):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=1)
        snap = m.snapshot()
        # All 10 queries happened within the window, so rate should be >= 10/60
        assert snap.recent_queries_per_second >= 10.0 / 60.0


class TestResetClearsThroughputState:
    """Verify reset clears the windowed query timestamps."""

    def test_reset_zeros_recent_rate(self) -> None:
        m = QueryMetrics()
        for i in range(10):
            m.record_query(query_id=f"q{i}", elapsed_s=0.001, rows=1)
        m.reset()
        snap = m.snapshot()
        assert snap.recent_queries_per_second == 0.0
        assert snap.queries_per_second == 0.0
        assert snap.rows_per_second == 0.0


class TestSnapshotHasThroughputFields:
    """Verify MetricsSnapshot has all new throughput attributes."""

    def test_snapshot_has_queries_per_second(self) -> None:
        m = QueryMetrics()
        snap = m.snapshot()
        assert hasattr(snap, "queries_per_second")

    def test_snapshot_has_rows_per_second(self) -> None:
        m = QueryMetrics()
        snap = m.snapshot()
        assert hasattr(snap, "rows_per_second")

    def test_snapshot_has_recent_queries_per_second(self) -> None:
        m = QueryMetrics()
        snap = m.snapshot()
        assert hasattr(snap, "recent_queries_per_second")
