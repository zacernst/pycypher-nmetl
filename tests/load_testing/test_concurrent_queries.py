"""Load tests: concurrent query execution stress testing.

Verifies that PyCypher handles concurrent query execution correctly,
including thread safety of caches, metrics collection, and resource
limit enforcement under contention.

These tests use Python 3.14's free-threaded interpreter to exercise
true parallelism where available.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest
from pycypher.star import Star

from .load_generator import (
    QUERY_WORKLOAD,
    SCALE_SMALL,
    build_graph,
    execute_workload,
)


@pytest.fixture(scope="module")
def small_star() -> Star:
    """Module-scoped Star with a small social graph."""
    ctx = build_graph(SCALE_SMALL)
    return Star(ctx)


class TestConcurrentQueryExecution:
    """Concurrent query execution must be safe and correct."""

    def test_parallel_reads_return_consistent_results(
        self,
        small_star: Star,
    ) -> None:
        """Multiple threads executing the same query must get identical results."""
        query = "MATCH (p:Person) RETURN count(p)"
        results: list[int] = []
        errors: list[Exception] = []

        def run_query() -> None:
            try:
                df = small_star.execute_query(query, timeout_seconds=10.0)
                results.append(int(df.iloc[0, 0]))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=run_query) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Concurrent queries raised errors: {errors}"
        assert len(results) == 8
        # All threads must see the same count.
        assert len(set(results)) == 1, f"Inconsistent results: {results}"

    def test_thread_pool_workload(self, small_star: Star) -> None:
        """A mixed workload executed via ThreadPoolExecutor must complete."""
        queries = QUERY_WORKLOAD[:6]  # Use the cheaper queries.
        results: list[tuple[str, bool]] = []

        def run_one(qdef: dict[str, Any]) -> tuple[str, bool]:
            try:
                small_star.execute_query(
                    qdef["query"],
                    timeout_seconds=10.0,
                )
                return (qdef["name"], True)
            except Exception:
                return (qdef["name"], False)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(run_one, q) for q in queries * 3]
            for fut in as_completed(futures):
                results.append(fut.result())

        success_count = sum(1 for _, ok in results if ok)
        # At least 80% should succeed (some may timeout under contention).
        assert success_count >= len(results) * 0.8, (
            f"Too many failures: {success_count}/{len(results)}"
        )

    def test_cache_thread_safety(self, small_star: Star) -> None:
        """AST and result caches must not corrupt under concurrent access."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name LIMIT 5"
        barrier = threading.Barrier(4, timeout=10)
        errors: list[str] = []

        def hammer_cache() -> None:
            try:
                barrier.wait()
                for _ in range(20):
                    small_star.execute_query(query, timeout_seconds=5.0)
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")

        threads = [threading.Thread(target=hammer_cache) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Cache corruption detected: {errors}"

    def test_concurrent_different_queries(self, small_star: Star) -> None:
        """Different queries running concurrently must not interfere."""
        query_a = "MATCH (p:Person) RETURN p.name LIMIT 5"
        query_b = "MATCH (p:Person) RETURN p.age LIMIT 5"
        results_a: list[list[str]] = []
        results_b: list[list[int]] = []

        def run_a() -> None:
            for _ in range(10):
                df = small_star.execute_query(query_a, timeout_seconds=5.0)
                results_a.append(df.iloc[:, 0].tolist())

        def run_b() -> None:
            for _ in range(10):
                df = small_star.execute_query(query_b, timeout_seconds=5.0)
                results_b.append(df.iloc[:, 0].tolist())

        ta = threading.Thread(target=run_a)
        tb = threading.Thread(target=run_b)
        ta.start()
        tb.start()
        ta.join(timeout=30)
        tb.join(timeout=30)

        # Each query type must return consistent results.
        assert len(results_a) == 10
        assert len(results_b) == 10
        # All runs of query_a should produce identical results.
        for r in results_a[1:]:
            assert r == results_a[0], (
                "Query A results inconsistent across threads"
            )
        for r in results_b[1:]:
            assert r == results_b[0], (
                "Query B results inconsistent across threads"
            )


class TestThroughputUnderLoad:
    """Throughput must remain reasonable under sustained load."""

    def test_sustained_workload_throughput(self, small_star: Star) -> None:
        """Execute the full workload 3x and verify minimum throughput."""
        report = execute_workload(
            small_star,
            QUERY_WORKLOAD[:6],
            iterations=3,
            timeout_per_query=10.0,
        )
        # At minimum we should complete all queries (even if some timeout).
        assert report.total_queries == 18
        assert report.success_rate >= 0.8
        # Throughput: at least 0.5 query/second on any reasonable hardware.
        # Uses a conservative threshold to avoid flakiness when running
        # alongside the full test suite (gc.collect() per query + parser
        # init overhead + system load can reduce apparent throughput).
        assert report.throughput_qps >= 0.5, (
            f"Throughput too low: {report.throughput_qps:.2f} qps"
        )

    def test_latency_percentiles(self, small_star: Star) -> None:
        """p50 and p99 latency must be within acceptable bounds."""
        report = execute_workload(
            small_star,
            QUERY_WORKLOAD[:4],
            iterations=5,
            timeout_per_query=10.0,
        )
        # p50 under 2 seconds for small-scale queries.
        assert report.p50_latency_s < 2.0, (
            f"p50 latency too high: {report.p50_latency_s:.3f}s"
        )
        # p99 under 10 seconds.
        assert report.p99_latency_s < 10.0, (
            f"p99 latency too high: {report.p99_latency_s:.3f}s"
        )
