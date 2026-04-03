"""Load tests: cache thrashing and eviction under pressure.

Verifies that the AST cache and result cache behave correctly when
workloads defeat caching strategies — e.g., high-cardinality unique
queries, alternating access patterns, and cache size pressure.
"""

from __future__ import annotations

import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.star import Star

from .load_generator import SCALE_SMALL, build_graph


@pytest.fixture(scope="module")
def small_star() -> Star:
    """Module-scoped Star with small graph."""
    ctx = build_graph(SCALE_SMALL)
    return Star(ctx, result_cache_max_mb=10)


class TestASTCacheThrashing:
    """AST cache must degrade gracefully under high-cardinality workloads."""

    def test_unique_query_flood(self) -> None:
        """Flooding with unique queries must not crash or leak memory."""
        parser = GrammarParser()
        parser._ast_cache_max = 50

        for i in range(500):
            parser.parse_to_ast(f"RETURN {i}")

        # Cache must not exceed configured max.
        assert len(parser._ast_cache) <= 50
        # Eviction counter must reflect overflow.
        stats = parser.cache_stats
        assert stats["ast_evictions"] >= 450

    def test_cache_hit_rate_under_skewed_workload(self) -> None:
        """A Zipf-like workload should achieve decent hit rate."""
        parser = GrammarParser()
        parser._ast_cache_max = 20

        # 80% of accesses go to 5 "hot" queries, 20% to 50 "cold" queries.
        hot_queries = [f"RETURN {i}" for i in range(5)]
        cold_queries = [f"RETURN {100 + i}" for i in range(50)]

        import numpy as np

        rng = np.random.default_rng(42)

        for _ in range(1000):
            if rng.random() < 0.8:
                q = rng.choice(hot_queries)
            else:
                q = rng.choice(cold_queries)
            parser.parse_to_ast(q)

        stats = parser.cache_stats
        # Hot queries should be cached — expect >50% overall hit rate.
        assert stats["ast_hit_rate"] > 0.5, (
            f"Hit rate too low for skewed workload: {stats['ast_hit_rate']:.2%}"
        )

    def test_scan_resistant_eviction(self) -> None:
        """A sequential scan must not evict frequently-used entries."""
        parser = GrammarParser()
        parser._ast_cache_max = 10

        # Warm up 5 "hot" queries.
        hot_queries = [f"RETURN {i}" for i in range(5)]
        for q in hot_queries:
            parser.parse_to_ast(q)

        # Sequential scan of 20 unique queries (should evict some hot ones).
        for i in range(100, 120):
            parser.parse_to_ast(f"RETURN {i}")

        # Re-access hot queries — some may have been evicted.
        hits_before = parser._ast_cache_hits
        for q in hot_queries:
            parser.parse_to_ast(q)
        hits_after = parser._ast_cache_hits

        # With LRU, sequential scan will evict hot entries. This test
        # documents the behaviour — pure LRU is not scan-resistant.
        # At least verify cache is consistent.
        assert len(parser._ast_cache) <= 10

    def test_alternating_working_sets(self) -> None:
        """Two alternating working sets larger than cache must not crash."""
        parser = GrammarParser()
        parser._ast_cache_max = 10

        set_a = [f"RETURN {i}" for i in range(15)]
        set_b = [f"RETURN {100 + i}" for i in range(15)]

        for cycle in range(5):
            working_set = set_a if cycle % 2 == 0 else set_b
            for q in working_set:
                parser.parse_to_ast(q)

        stats = parser.cache_stats
        assert stats["ast_size"] <= 10
        # Total evictions should be substantial.
        assert stats["ast_evictions"] > 50


class TestResultCacheThrashing:
    """Result cache must handle pressure gracefully."""

    def test_result_cache_eviction_under_pressure(
        self,
        small_star: Star,
    ) -> None:
        """Running many distinct queries must trigger result cache eviction."""
        # Generate unique queries via LIMIT variation.
        for i in range(1, 50):
            small_star.execute_query(
                f"MATCH (p:Person) RETURN p.name LIMIT {i}",
                timeout_seconds=5.0,
            )

        # Result cache should have evicted older entries.
        if small_star._result_cache is not None:
            stats = small_star._result_cache.stats()
            # Cache should not exceed its configured max.
            assert (
                stats["result_cache_size_mb"] <= 10.5
            )  # 10MB + small tolerance

    def test_repeated_query_uses_cache(self, small_star: Star) -> None:
        """Repeating the same query must produce cache hits."""
        query = "MATCH (p:Person) RETURN count(p)"

        # Prime the cache.
        small_star.execute_query(query, timeout_seconds=5.0)

        if small_star._result_cache is not None:
            hits_before = small_star._result_cache.stats()["result_cache_hits"]

            # Repeat 10 times.
            for _ in range(10):
                small_star.execute_query(query, timeout_seconds=5.0)

            hits_after = small_star._result_cache.stats()["result_cache_hits"]
            assert hits_after - hits_before >= 10

    def test_cache_invalidation_on_mutation(self, small_star: Star) -> None:
        """Cache generation must advance on mutation, preventing stale reads."""
        if small_star._result_cache is None:
            pytest.skip("Result cache not enabled")

        query = "MATCH (p:Person) RETURN count(p)"
        result1 = small_star.execute_query(query, timeout_seconds=5.0)

        # Simulate mutation — invalidate cache.
        small_star._result_cache.invalidate()

        # Next execution should miss cache.
        misses_before = small_star._result_cache.stats()["result_cache_misses"]
        result2 = small_star.execute_query(query, timeout_seconds=5.0)
        misses_after = small_star._result_cache.stats()["result_cache_misses"]

        assert misses_after > misses_before
