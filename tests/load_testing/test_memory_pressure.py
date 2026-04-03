"""Load tests: memory pressure and leak detection.

Verifies that PyCypher does not leak memory under sustained query
workloads and that memory usage scales predictably with data size.
"""

from __future__ import annotations

import gc
import signal
import tracemalloc

import pytest
from pycypher.star import Star

from _perf_helpers import perf_threshold
from .load_generator import (
    QUERY_WORKLOAD,
    SCALE_MICRO,
    SCALE_SMALL,
    build_graph,
)


@pytest.fixture(scope="module")
def micro_star() -> Star:
    """Module-scoped Star with micro graph (fast tests)."""
    ctx = build_graph(SCALE_MICRO)
    return Star(ctx)


@pytest.fixture(scope="module")
def small_star() -> Star:
    """Module-scoped Star with small graph."""
    ctx = build_graph(SCALE_SMALL)
    return Star(ctx)


class TestMemoryLeakDetection:
    """Repeated query execution must not leak memory."""

    def test_repeated_queries_no_leak(self, micro_star: Star) -> None:
        """Running the same query 100 times must not grow memory unboundedly."""
        query = "MATCH (p:Person) RETURN p.name, p.age"

        gc.collect()
        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()

        for _ in range(100):
            micro_star.execute_query(query, timeout_seconds=5.0)

        gc.collect()
        snapshot_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Compare top allocations.
        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        total_growth_mb = sum(s.size_diff for s in stats) / (1024 * 1024)

        # Allow up to 50MB growth for 100 iterations of a tiny query.
        # This is generous — real leaks grow linearly without bound.
        assert total_growth_mb < perf_threshold(50), (
            f"Memory grew by {total_growth_mb:.1f}MB over 100 iterations — "
            "possible leak"
        )

    def test_diverse_queries_no_leak(self, micro_star: Star) -> None:
        """Running diverse queries must not accumulate leaked memory."""
        queries = [q["query"] for q in QUERY_WORKLOAD[:6]]

        gc.collect()
        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()

        for _ in range(20):
            for q in queries:
                try:
                    micro_star.execute_query(q, timeout_seconds=5.0)
                except Exception:
                    pass  # Some queries may fail on micro graph — that's fine.

        gc.collect()
        snapshot_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        total_growth_mb = sum(s.size_diff for s in stats) / (1024 * 1024)

        assert total_growth_mb < perf_threshold(100), (
            f"Memory grew by {total_growth_mb:.1f}MB over diverse workload — "
            "possible leak"
        )

    def test_cache_bounded_memory(self) -> None:
        """AST cache must not grow beyond its configured limit."""
        from pycypher.grammar_parser import GrammarParser

        # Cancel any pending SIGALRM from prior timeout-based tests.
        signal.alarm(0)

        parser = GrammarParser()
        parser._ast_cache_max = 20

        gc.collect()
        # Ensure clean tracemalloc state (prior tests may leave it running).
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        tracemalloc.start()

        # Parse 500 unique queries.
        for i in range(500):
            parser.parse_to_ast(f"RETURN {i}")

        gc.collect()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Cache must be bounded.
        assert len(parser._ast_cache) <= 20
        # Peak memory must be reasonable (under 100MB for parse-only).
        assert peak / (1024 * 1024) < 100


class TestMemoryScaling:
    """Memory usage must scale predictably with data size."""

    def test_entity_scan_memory_proportional_to_data(self) -> None:
        """Entity scan memory should scale roughly linearly with entity count."""
        measurements: list[tuple[int, float]] = []

        for scale in [SCALE_MICRO, SCALE_SMALL]:
            ctx = build_graph(scale)
            star = Star(ctx)

            gc.collect()
            tracemalloc.start()

            star.execute_query(
                "MATCH (p:Person) RETURN p.name, p.age",
                timeout_seconds=10.0,
            )

            gc.collect()
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            measurements.append((scale.person_count, peak / (1024 * 1024)))

        # Larger dataset should use more memory (basic sanity check).
        if len(measurements) == 2:
            small_persons, small_mem = measurements[0]
            large_persons, large_mem = measurements[1]

            scale_ratio = large_persons / small_persons
            mem_ratio = large_mem / max(small_mem, 0.01)

            # Memory should not grow worse than O(n^2) relative to data size.
            assert mem_ratio < scale_ratio**2, (
                f"Memory scaling is worse than quadratic: "
                f"data {scale_ratio:.0f}x, memory {mem_ratio:.1f}x"
            )


class TestGarbageCollectionInteraction:
    """GC must be able to reclaim query result memory."""

    def test_result_frames_reclaimable(self, micro_star: Star) -> None:
        """DataFrame results must be reclaimable by GC."""
        import weakref

        query = "MATCH (p:Person) RETURN p.name"
        result = micro_star.execute_query(query, timeout_seconds=5.0)

        # Create a weak reference to the result.
        ref = weakref.ref(result)
        assert ref() is not None

        # Delete the strong reference and collect.
        del result
        gc.collect()

        # The weak reference should now be dead.
        assert ref() is None, "Query result was not reclaimed by GC"
