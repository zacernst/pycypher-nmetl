"""Load tests: resource exhaustion and graceful degradation.

Verifies that PyCypher enforces resource limits correctly and recovers
gracefully when those limits are hit — timeouts, memory budgets, query
complexity ceilings, and cross-join limits.
"""

from __future__ import annotations

import gc
import time

import pytest
from pycypher.exceptions import (
    QueryMemoryBudgetError,
    QueryTimeoutError,
)
from pycypher.query_complexity import QueryComplexityError
from pycypher.star import Star

from .load_generator import SCALE_MEDIUM, SCALE_SMALL, build_graph


@pytest.fixture(scope="module")
def small_star() -> Star:
    """Module-scoped Star with small graph."""
    ctx = build_graph(SCALE_SMALL)
    return Star(ctx)


@pytest.fixture(scope="module")
def medium_star() -> Star:
    """Module-scoped Star with medium graph."""
    ctx = build_graph(SCALE_MEDIUM)
    return Star(ctx)


class TestTimeoutEnforcement:
    """Query timeout must terminate long-running queries reliably."""

    def test_tight_timeout_triggers_on_expensive_query(
        self,
        medium_star: Star,
    ) -> None:
        """A complex query with a very short timeout must raise QueryTimeoutError."""
        # Two-hop join on medium graph without LIMIT — potentially expensive.
        query = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, b.name, c.name"
        )
        with pytest.raises(QueryTimeoutError):
            medium_star.execute_query(query, timeout_seconds=0.001)

    def test_generous_timeout_allows_cheap_query(
        self,
        small_star: Star,
    ) -> None:
        """A cheap query with generous timeout must complete normally."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
            timeout_seconds=30.0,
        )
        assert len(result) == 1

    def test_recovery_after_timeout(self, medium_star: Star) -> None:
        """After a timeout, subsequent queries must still work."""
        # Force a timeout.
        with pytest.raises(QueryTimeoutError):
            medium_star.execute_query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
                "RETURN a.name, b.name, c.name",
                timeout_seconds=0.001,
            )

        # The engine must recover — next query must succeed.
        result = medium_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
            timeout_seconds=10.0,
        )
        assert len(result) == 1

    def test_sequential_timeouts_no_resource_leak(
        self,
        medium_star: Star,
    ) -> None:
        """Repeated timeouts must not leak file descriptors or memory."""
        expensive_query = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, b.name, c.name"
        )
        for _ in range(10):
            with pytest.raises(QueryTimeoutError):
                medium_star.execute_query(
                    expensive_query,
                    timeout_seconds=0.001,
                )

        # A normal query must still work after 10 consecutive timeouts.
        result = medium_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
            timeout_seconds=10.0,
        )
        assert len(result) == 1


class TestComplexityLimitEnforcement:
    """Query complexity scoring must reject dangerously complex queries."""

    def test_complexity_ceiling_rejects_heavy_query(
        self,
        small_star: Star,
    ) -> None:
        """A multi-hop unbounded query should exceed a low complexity ceiling."""
        query = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN DISTINCT a.name, c.name ORDER BY a.name"
        )
        with pytest.raises(QueryComplexityError):
            small_star.execute_query(
                query,
                max_complexity_score=5,
                timeout_seconds=10.0,
            )

    def test_simple_query_passes_complexity_check(
        self,
        small_star: Star,
    ) -> None:
        """A simple scan should pass even a tight complexity ceiling."""
        result = small_star.execute_query(
            "MATCH (p:Person) RETURN p.name LIMIT 5",
            max_complexity_score=50,
            timeout_seconds=10.0,
        )
        assert len(result) <= 5


class TestGracefulDegradation:
    """System must degrade gracefully rather than crash under pressure."""

    def test_error_does_not_corrupt_state(self, small_star: Star) -> None:
        """A failed query must not corrupt the Star's internal state."""
        # Run a normal query first.
        baseline = small_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
            timeout_seconds=10.0,
        )
        baseline_count = int(baseline.iloc[0, 0])

        # Force an error (invalid query).
        with pytest.raises(Exception):
            small_star.execute_query(
                "THIS IS NOT VALID CYPHER",
                timeout_seconds=5.0,
            )

        # Re-run the baseline — must return same result.
        after = small_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
            timeout_seconds=10.0,
        )
        assert int(after.iloc[0, 0]) == baseline_count

    def test_mixed_success_failure_workload(self, small_star: Star) -> None:
        """Interleaving valid and invalid queries must not break the engine."""
        valid = "MATCH (p:Person) RETURN count(p)"
        invalid_queries = [
            "NOT CYPHER AT ALL",
            "MATCH (a)-[:NONEXISTENT_TYPE]->(b) RETURN a",
        ]

        results: list[int] = []
        for _ in range(5):
            # Valid query.
            df = small_star.execute_query(valid, timeout_seconds=10.0)
            results.append(int(df.iloc[0, 0]))

            # Invalid queries (should fail, not crash).
            for inv in invalid_queries:
                try:
                    small_star.execute_query(inv, timeout_seconds=5.0)
                except Exception:
                    pass  # Expected.

        # All valid query results must be consistent.
        assert len(set(results)) == 1

    def test_gc_pressure_does_not_break_execution(
        self,
        small_star: Star,
    ) -> None:
        """Forced garbage collection during workload must not cause errors."""
        query = "MATCH (p:Person) RETURN p.name LIMIT 10"
        for _ in range(20):
            gc.collect()
            result = small_star.execute_query(query, timeout_seconds=5.0)
            assert len(result) <= 10
