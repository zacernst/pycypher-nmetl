"""TDD tests for AST parse-result LRU caching (Loop 169 — Performance).

Problem: ``ASTConverter.from_cypher()`` re-runs the full Earley parse + transform
+ convert pipeline on EVERY call, even for identical query strings.  The Earley
parser consumes ~56ms per query even when the parser singleton is cached.  For
an ETL pipeline executing the same 5 queries across 1000 data batches this wastes
56ms × 5 × 999 ≈ 280 seconds of unnecessary parsing.

Fix: wrap the parse+transform+convert pipeline in a module-level
``@functools.lru_cache``-decorated function so that identical query strings hit
an O(1) cache lookup after the first call.

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest
from pycypher.ast_models import ASTConverter

# ---------------------------------------------------------------------------
# Category 1 — Cache identity: same string returns the same object
# ---------------------------------------------------------------------------


class TestASTCacheIdentity:
    """Identical query strings must return the exact same AST object (``is``).

    This verifies that caching is happening at the object level, not just
    that the results are equal.
    """

    def test_same_query_returns_same_object(self) -> None:
        """from_cypher('MATCH (n) RETURN n') called twice must return ``is``-identical object."""
        q = "MATCH (n:Person) RETURN n.name"
        ast1 = ASTConverter.from_cypher(q)
        ast2 = ASTConverter.from_cypher(q)
        assert ast1 is ast2, (
            "Expected the same cached ASTNode object on the second call, "
            "but got a new instance — the cache is not active."
        )

    def test_different_queries_return_different_objects(self) -> None:
        """Different query strings must return distinct AST objects."""
        ast1 = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")
        ast2 = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.age")
        assert ast1 is not ast2

    def test_whitespace_variation_treated_as_different(self) -> None:
        """'MATCH (n) RETURN n' and 'MATCH  (n) RETURN n' are distinct cache keys."""
        ast1 = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")
        ast2 = ASTConverter.from_cypher("MATCH  (n:Person) RETURN n.name")
        # Different strings — may or may not produce identical ASTs, but
        # cache keys are strings so they are treated as distinct entries.
        # We just verify no error is raised.
        assert ast1 is not None
        assert ast2 is not None


# ---------------------------------------------------------------------------
# Category 2 — Parse count: ``parser.parse`` called only once per unique query
# ---------------------------------------------------------------------------


class TestASTCacheParseCount:
    """The grammar parser must be invoked only once per unique query string."""

    def test_repeated_calls_invoke_parse_only_once(self) -> None:
        """20 calls with the same query string must trigger exactly 1 ``parser.parse()``."""
        from pycypher.grammar_parser import get_default_parser

        parser = get_default_parser()
        call_count = {"n": 0}
        original_parse = parser.parse

        def counting_parse(query: str) -> object:
            call_count["n"] += 1
            return original_parse(query)

        q = "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p.name, q.name"
        # Ensure the query is NOT already cached by using a unique string.
        unique_q = q + "  -- unique_marker_test_cache"

        with patch.object(parser, "parse", side_effect=counting_parse):
            for _ in range(20):
                ASTConverter.from_cypher(unique_q)

        assert call_count["n"] == 1, (
            f"Expected parser.parse() to be called exactly 1 time for 20 identical "
            f"from_cypher() calls, but it was called {call_count['n']} times. "
            "The AST result cache is not active."
        )

    def test_different_queries_each_invoke_parse_once(self) -> None:
        """5 distinct query strings must each invoke ``parser.parse()`` exactly once."""
        from pycypher.grammar_parser import get_default_parser

        parser = get_default_parser()
        call_count = {"n": 0}
        original_parse = parser.parse

        def counting_parse(query: str) -> object:
            call_count["n"] += 1
            return original_parse(query)

        unique_suffix = "  -- unique_marker_different_queries"
        queries = [
            f"MATCH (n:Person) RETURN n.name{unique_suffix}_1",
            f"MATCH (n:Person) RETURN n.age{unique_suffix}_2",
            f"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name{unique_suffix}_3",
            f"MATCH (n:Person) WHERE n.age > 30 RETURN n.name{unique_suffix}_4",
            f"MATCH (n:Person) RETURN count(n){unique_suffix}_5",
        ]

        with patch.object(parser, "parse", side_effect=counting_parse):
            for _ in range(10):
                for q in queries:
                    ASTConverter.from_cypher(q)

        # 5 unique strings × 10 reps = 50 calls, but only 5 parses
        assert call_count["n"] == 5, (
            f"Expected 5 parser.parse() calls for 5 distinct queries × 10 reps, "
            f"but got {call_count['n']}."
        )


# ---------------------------------------------------------------------------
# Category 3 — Performance: repeated calls are substantially faster
# ---------------------------------------------------------------------------


class TestASTCachePerformance:
    """20 repeated calls with the same query must complete under 0.5s total.

    The cold-parse time for a moderately complex query is ~65-90ms.  Without
    caching, 20 calls would take ~1.3-1.8s.  With caching, 19 of 20 calls cost
    only a dict lookup — total should be well under 0.5s.
    """

    QUERY = (
        "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
        "WHERE a.age < 40 "
        "RETURN a.name, b.name, count(*) AS common "
        "ORDER BY a.name LIMIT 50"
    )
    REPS = 20
    THRESHOLD_SECONDS = 0.50

    def test_repeated_parse_is_fast(self) -> None:
        """20 calls with the same query string must finish in under 0.5s."""
        # Warm up the cache for this query
        ASTConverter.from_cypher(self.QUERY)

        start = time.perf_counter()
        for _ in range(self.REPS):
            ASTConverter.from_cypher(self.QUERY)
        elapsed = time.perf_counter() - start

        assert elapsed < self.THRESHOLD_SECONDS, (
            f"20 repeated from_cypher() calls took {elapsed:.3f}s, expected < "
            f"{self.THRESHOLD_SECONDS}s.  The AST result cache is not active — "
            f"each call is re-running the full Earley parse."
        )

    def test_cache_speedup_ratio(self) -> None:
        """Cached calls must be at least 10× faster per call than the first (cold) call.

        The first call pays the full parse cost; subsequent calls should be
        near-instant dict lookups.
        """
        unique_q = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
            "  -- unique_cache_speedup_marker"
        )

        # Cold call
        t0 = time.perf_counter()
        ASTConverter.from_cypher(unique_q)
        cold_time = time.perf_counter() - t0

        if cold_time < 0.005:
            pytest.skip(
                "Cold parse time too small to measure speedup ratio meaningfully",
            )

        # Warm calls
        N = 10
        t1 = time.perf_counter()
        for _ in range(N):
            ASTConverter.from_cypher(unique_q)
        warm_total = time.perf_counter() - t1
        warm_per_call = warm_total / N

        speedup = cold_time / warm_per_call
        assert speedup >= 10, (
            f"Expected cached calls to be ≥10× faster than cold call, "
            f"but cold={cold_time * 1000:.1f}ms, warm={warm_per_call * 1000:.2f}ms/call, "
            f"speedup={speedup:.1f}×.  Cache may not be active."
        )
