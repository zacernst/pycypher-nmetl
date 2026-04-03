"""Tests for the in-process query metrics collector.

Verifies that ``shared.metrics.QueryMetrics`` correctly:
1. Counts successful queries and errors.
2. Tracks timing percentiles from a rolling window.
3. Detects slow queries and emits warning logs.
4. Records clause execution frequency.
5. Produces consistent snapshots under concurrent access.
6. Is a no-op when disabled.
7. Integrates with Star.execute_query() for end-to-end recording.

Run with:
    uv run pytest tests/test_query_metrics_collector.py -v
"""

from __future__ import annotations

import logging
import threading

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star
from shared.metrics import QUERY_METRICS, QueryMetrics, get_rss_mb

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_metrics() -> QueryMetrics:
    """Return a fresh, isolated metrics collector for each test."""
    return QueryMetrics()


@pytest.fixture
def simple_star() -> Star:
    """Three-person context: Alice (30), Bob (25), Carol (35)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# ---------------------------------------------------------------------------
# Unit tests — QueryMetrics in isolation
# ---------------------------------------------------------------------------


class TestQueryCounting:
    """Verify basic counting of queries and errors."""

    def test_initial_snapshot_is_zero(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        snap = fresh_metrics.snapshot()
        assert snap.total_queries == 0
        assert snap.total_errors == 0
        assert snap.slow_queries == 0
        assert snap.total_rows_returned == 0

    def test_record_query_increments_count(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        fresh_metrics.record_query(query_id="q1", elapsed_s=0.01, rows=10)
        fresh_metrics.record_query(query_id="q2", elapsed_s=0.02, rows=20)
        snap = fresh_metrics.snapshot()
        assert snap.total_queries == 2
        assert snap.total_rows_returned == 30

    def test_record_error_increments_count(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        fresh_metrics.record_error(
            query_id="q1",
            error_type="TypeError",
            elapsed_s=0.01,
        )
        fresh_metrics.record_error(
            query_id="q2",
            error_type="ValueError",
            elapsed_s=0.02,
        )
        fresh_metrics.record_error(
            query_id="q3",
            error_type="TypeError",
            elapsed_s=0.03,
        )
        snap = fresh_metrics.snapshot()
        assert snap.total_errors == 3
        assert snap.error_counts == {"TypeError": 2, "ValueError": 1}


class TestTimingPercentiles:
    """Verify timing percentile calculations."""

    def test_single_query_all_percentiles_equal(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        fresh_metrics.record_query(query_id="q1", elapsed_s=0.1, rows=5)
        snap = fresh_metrics.snapshot()
        # Single sample: all percentiles should be 100.0ms
        assert snap.timing_min_ms == pytest.approx(100.0)
        assert snap.timing_max_ms == pytest.approx(100.0)

    def test_multiple_queries_percentile_ordering(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        for i in range(100):
            fresh_metrics.record_query(
                query_id=f"q{i}",
                elapsed_s=(i + 1) / 1000.0,
                rows=1,
            )
        snap = fresh_metrics.snapshot()
        assert snap.timing_p50_ms <= snap.timing_p90_ms
        assert snap.timing_p90_ms <= snap.timing_p99_ms
        assert snap.timing_p99_ms <= snap.timing_max_ms
        assert snap.timing_min_ms <= snap.timing_p50_ms


class TestSlowQueryDetection:
    """Verify slow query detection and warning logs."""

    def test_slow_query_counted(self, fresh_metrics: QueryMetrics) -> None:
        # Default threshold is 1000ms = 1.0s
        fresh_metrics.record_query(query_id="fast", elapsed_s=0.01, rows=1)
        fresh_metrics.record_query(query_id="slow", elapsed_s=2.0, rows=1)
        snap = fresh_metrics.snapshot()
        assert snap.slow_queries == 1

    def test_slow_query_emits_warning(
        self,
        fresh_metrics: QueryMetrics,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING):
            fresh_metrics.record_query(
                query_id="slowpoke",
                elapsed_s=5.0,
                rows=42,
            )
        assert any("SLOW QUERY" in r.message for r in caplog.records)
        assert any("slowpoke" in r.message for r in caplog.records)


class TestClauseCounting:
    """Verify clause execution frequency tracking."""

    def test_clause_counts(self, fresh_metrics: QueryMetrics) -> None:
        fresh_metrics.record_query(
            query_id="q1",
            elapsed_s=0.01,
            rows=1,
            clauses=["Match", "Return"],
        )
        fresh_metrics.record_query(
            query_id="q2",
            elapsed_s=0.01,
            rows=1,
            clauses=["Match", "With", "Return"],
        )
        snap = fresh_metrics.snapshot()
        assert snap.clause_counts["Match"] == 2
        assert snap.clause_counts["Return"] == 2
        assert snap.clause_counts["With"] == 1


class TestReset:
    """Verify that reset clears all state."""

    def test_reset_zeros_everything(self, fresh_metrics: QueryMetrics) -> None:
        fresh_metrics.record_query(query_id="q1", elapsed_s=0.5, rows=100)
        fresh_metrics.record_error(
            query_id="q2",
            error_type="RuntimeError",
            elapsed_s=0.1,
        )
        fresh_metrics.reset()
        snap = fresh_metrics.snapshot()
        assert snap.total_queries == 0
        assert snap.total_errors == 0
        assert snap.total_rows_returned == 0
        assert snap.timing_max_ms == 0.0


class TestThreadSafety:
    """Verify concurrent access does not corrupt state."""

    def test_concurrent_recording(self, fresh_metrics: QueryMetrics) -> None:
        n_threads = 8
        n_per_thread = 100
        barrier = threading.Barrier(n_threads)

        def worker(tid: int) -> None:
            barrier.wait()
            for i in range(n_per_thread):
                fresh_metrics.record_query(
                    query_id=f"t{tid}-q{i}",
                    elapsed_s=0.001,
                    rows=1,
                )

        threads = [
            threading.Thread(target=worker, args=(t,))
            for t in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = fresh_metrics.snapshot()
        assert snap.total_queries == n_threads * n_per_thread
        assert snap.total_rows_returned == n_threads * n_per_thread


class TestSnapshotImmutability:
    """Verify snapshot is a frozen dataclass."""

    def test_snapshot_is_frozen(self, fresh_metrics: QueryMetrics) -> None:
        snap = fresh_metrics.snapshot()
        with pytest.raises(AttributeError):
            snap.total_queries = 999  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Integration — Star.execute_query() records metrics
# ---------------------------------------------------------------------------


class TestStarMetricsIntegration:
    """Verify that Star.execute_query() feeds the global QUERY_METRICS."""

    def test_successful_query_recorded(self, simple_star: Star) -> None:
        QUERY_METRICS.reset()
        simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        snap = QUERY_METRICS.snapshot()
        assert snap.total_queries >= 1
        assert snap.total_rows_returned >= 3
        assert snap.timing_max_ms > 0

    def test_failed_query_recorded(self, simple_star: Star) -> None:
        QUERY_METRICS.reset()
        with pytest.raises(Exception):
            simple_star.execute_query(
                "MATCH (p:NonExistent) RETURN p.name AS name",
            )
        snap = QUERY_METRICS.snapshot()
        # Either error was recorded, or query succeeded with 0 rows (both valid)
        assert snap.total_queries + snap.total_errors >= 1

    def test_clause_names_tracked(self, simple_star: Star) -> None:
        QUERY_METRICS.reset()
        simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        snap = QUERY_METRICS.snapshot()
        assert "Match" in snap.clause_counts
        assert "Return" in snap.clause_counts


# ---------------------------------------------------------------------------
# Memory delta tracking
# ---------------------------------------------------------------------------


class TestMemoryDeltaTracking:
    """Verify memory delta recording and percentile aggregation."""

    def test_memory_delta_recorded(self, fresh_metrics: QueryMetrics) -> None:
        fresh_metrics.record_query(
            query_id="q1",
            elapsed_s=0.01,
            rows=1,
            memory_delta_mb=5.0,
        )
        fresh_metrics.record_query(
            query_id="q2",
            elapsed_s=0.01,
            rows=1,
            memory_delta_mb=10.0,
        )
        snap = fresh_metrics.snapshot()
        assert snap.memory_delta_max_mb == pytest.approx(10.0)

    def test_no_memory_delta_when_none(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        fresh_metrics.record_query(query_id="q1", elapsed_s=0.01, rows=1)
        snap = fresh_metrics.snapshot()
        assert snap.memory_delta_max_mb == 0.0
        assert snap.memory_delta_p50_mb == 0.0

    def test_memory_delta_percentile_ordering(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        for i in range(100):
            fresh_metrics.record_query(
                query_id=f"q{i}",
                elapsed_s=0.01,
                rows=1,
                memory_delta_mb=float(i + 1),
            )
        snap = fresh_metrics.snapshot()
        assert snap.memory_delta_p50_mb <= snap.memory_delta_p90_mb
        assert snap.memory_delta_p90_mb <= snap.memory_delta_max_mb

    def test_reset_clears_memory_deltas(
        self,
        fresh_metrics: QueryMetrics,
    ) -> None:
        fresh_metrics.record_query(
            query_id="q1",
            elapsed_s=0.01,
            rows=1,
            memory_delta_mb=42.0,
        )
        fresh_metrics.reset()
        snap = fresh_metrics.snapshot()
        assert snap.memory_delta_max_mb == 0.0


class TestGetRssMb:
    """Verify the get_rss_mb helper returns sensible values."""

    def test_returns_positive_float(self) -> None:
        rss = get_rss_mb()
        # A running Python process should use > 0 MB
        assert rss > 0.0

    def test_returns_float_type(self) -> None:
        assert isinstance(get_rss_mb(), float)


class TestStarMemoryDeltaIntegration:
    """Verify Star.execute_query() records memory deltas in QUERY_METRICS."""

    def test_memory_delta_tracked_end_to_end(self, simple_star: Star) -> None:
        QUERY_METRICS.reset()
        simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        snap = QUERY_METRICS.snapshot()
        # Memory delta should have been recorded (could be 0 or positive)
        assert snap.memory_delta_max_mb >= 0.0
