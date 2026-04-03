"""Test flakiness detection and isolation tests.

These tests validate that common sources of test flakiness
(timing, signal state, tracemalloc state, GC pressure) are
properly handled across the test suite.
"""

from __future__ import annotations

import gc
import signal
import time
import tracemalloc

import pandas as pd
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star
from _perf_helpers import perf_threshold


class TestSignalStateIsolation:
    """Verify that pending SIGALRM from timeout tests don't leak."""

    @pytest.mark.timeout(0)  # Disable pytest-timeout for this test
    def test_no_pending_sigalrm_after_timeout_query(self) -> None:
        """After a timeout-equipped query, no SIGALRM should be pending."""
        signal.alarm(0)  # Clear any pre-existing alarm from pytest-timeout
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": [1, 2], "name": ["A", "B"]},
                ),
            },
        )
        star = Star(context=ctx)
        star.execute_query("MATCH (p:Person) RETURN p.name AS name")

        # Cancel any pending alarm
        remaining = signal.alarm(0)
        # No alarm should be pending after normal query
        assert remaining == 0

    @pytest.mark.timeout(0)  # Disable pytest-timeout for this test
    def test_sigalrm_cleanup_after_multiple_queries(self) -> None:
        """Multiple sequential queries should not accumulate alarms."""
        signal.alarm(0)  # Clear any pre-existing alarm from pytest-timeout
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": [1], "name": ["A"]},
                ),
            },
        )
        star = Star(context=ctx)
        for _ in range(10):
            star.execute_query("MATCH (p:Person) RETURN p.name AS name")

        assert signal.alarm(0) == 0


class TestTraceMallocIsolation:
    """Verify tracemalloc state is properly managed between tests."""

    def test_tracemalloc_start_stop_idempotent(self) -> None:
        """Starting tracemalloc when already started should not crash."""
        was_tracing = tracemalloc.is_tracing()
        if was_tracing:
            tracemalloc.stop()

        tracemalloc.start()
        tracemalloc.start()  # Double-start should be safe
        assert tracemalloc.is_tracing()
        tracemalloc.stop()

        if was_tracing:
            tracemalloc.start()

    def test_tracemalloc_stop_when_not_started(self) -> None:
        """Stopping tracemalloc when not started should not crash."""
        was_tracing = tracemalloc.is_tracing()
        if was_tracing:
            tracemalloc.stop()

        # Double-stop should not raise
        try:
            tracemalloc.stop()
        except RuntimeError:
            pass  # Expected on some Python versions

        if was_tracing:
            tracemalloc.start()


class TestGCPressureResilience:
    """Tests that query execution is resilient to GC pressure."""

    def test_query_after_gc_collect(self) -> None:
        """Queries should work correctly after forced GC collection."""
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": list(range(100)), "name": [f"P{i}" for i in range(100)]},
                ),
            },
        )
        star = Star(context=ctx)

        gc.collect()
        result = star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert int(result["cnt"].iloc[0]) == 100

    def test_gc_during_query_stream(self) -> None:
        """GC collection between queries should not corrupt state."""
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": [1, 2, 3], "name": ["A", "B", "C"]},
                ),
            },
        )
        star = Star(context=ctx)

        for i in range(20):
            if i % 5 == 0:
                gc.collect()
            result = star.execute_query(
                "MATCH (p:Person) RETURN count(p) AS cnt"
            )
            assert int(result["cnt"].iloc[0]) == 3


class TestTimingSensitiveResilience:
    """Tests that timing-sensitive operations don't cause flakiness."""

    def test_rapid_query_execution(self) -> None:
        """Rapid sequential queries should all succeed."""
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": [1], "name": ["A"]},
                ),
            },
        )
        star = Star(context=ctx)

        start = time.monotonic()
        for _ in range(100):
            result = star.execute_query(
                "MATCH (p:Person) RETURN p.name AS name"
            )
            assert result["name"].iloc[0] == "A"
        elapsed = time.monotonic() - start

        # 100 simple queries should complete in under 30 seconds
        assert elapsed < perf_threshold(30.0)

    def test_query_timing_deterministic(self) -> None:
        """Query execution time should be reasonably consistent."""
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {
                        "__ID__": list(range(500)),
                        "name": [f"P{i}" for i in range(500)],
                        "age": [20 + (i % 60) for i in range(500)],
                    },
                ),
            },
        )
        star = Star(context=ctx)

        # Warm up with exact same query to prime all caches
        query = "MATCH (p:Person) WHERE p.age > 40 RETURN p.name AS name"
        star.execute_query(query)
        star.execute_query(query)

        times = []
        for _ in range(10):
            t0 = time.monotonic()
            star.execute_query(query)
            times.append(time.monotonic() - t0)

        # Variance should be bounded — no query should take >50x the median
        # (generous bound to avoid flakiness from GC or OS scheduling)
        median_time = sorted(times)[len(times) // 2]
        max_allowed = max(median_time * 50, 0.5)  # At least 500ms allowed
        for t in times:
            assert t < max_allowed, (
                f"Query time {t:.3f}s is too far from median {median_time:.3f}s"
            )
