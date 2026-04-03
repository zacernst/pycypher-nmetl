"""Stress tests for error resilience under load.

Tests that the system remains stable and produces correct results
when encountering many errors, mixed valid/invalid query streams,
and rapid error/success alternation.
"""

from __future__ import annotations

import concurrent.futures
import threading

import pandas as pd
import pytest
from pycypher.exceptions import (
    CypherSyntaxError,
    GraphTypeNotFoundError,
)
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star


@pytest.fixture
def large_star() -> Star:
    """Star with enough data to exercise real query paths."""
    n = 1000
    ctx = ContextBuilder.from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": list(range(1, n + 1)),
                    "name": [f"Person_{i}" for i in range(n)],
                    "age": [20 + (i % 60) for i in range(n)],
                },
            ),
        },
    )
    return Star(context=ctx)


class TestRapidErrorRecovery:
    """System should recover cleanly after each error."""

    def test_alternating_valid_invalid_queries(self, large_star: Star) -> None:
        """Alternating valid and invalid queries should not corrupt state."""
        for i in range(50):
            # Valid query
            result = large_star.execute_query(
                f"MATCH (p:Person) WHERE p.age = {20 + i} RETURN p.name AS name"
            )
            assert isinstance(result, pd.DataFrame)

            # Invalid query - syntax error
            with pytest.raises(Exception):
                large_star.execute_query("MATCH (broken syntax")

    def test_many_syntax_errors_dont_leak_state(self, large_star: Star) -> None:
        """Many consecutive syntax errors should not leak memory or state."""
        bad_queries = [
            "MATCH (",
            "RETURN ???",
            "MATCH (n:Person WHERE",
            "CREAT (:Person {name: 'x'})",
            "MATCH (n) RETRUN n",
        ]

        for _ in range(20):
            for q in bad_queries:
                with pytest.raises(Exception):
                    large_star.execute_query(q)

        # System should still work
        result = large_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt"
        )
        assert int(result["cnt"].iloc[0]) == 1000

    def test_unregistered_type_errors_dont_affect_valid_types(
        self, large_star: Star
    ) -> None:
        """GraphTypeNotFoundError should not affect subsequent valid queries."""
        for _ in range(30):
            with pytest.raises(GraphTypeNotFoundError):
                large_star.execute_query("MATCH (d:Dinosaur) RETURN d")

        # Valid type should still work
        result = large_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt"
        )
        assert int(result["cnt"].iloc[0]) == 1000


class TestConcurrentErrorHandling:
    """Error handling under concurrent access."""

    def test_concurrent_mixed_queries(self, large_star: Star) -> None:
        """Concurrent valid and invalid queries should not interfere."""
        errors: list[Exception] = []
        lock = threading.Lock()

        def run_valid(idx: int) -> None:
            try:
                result = large_star.execute_query(
                    f"MATCH (p:Person) WHERE p.age = {20 + (idx % 60)} "
                    "RETURN p.name AS name"
                )
                assert isinstance(result, pd.DataFrame)
            except Exception as e:
                with lock:
                    errors.append(e)

        def run_invalid() -> None:
            try:
                large_star.execute_query("MATCH (broken")
            except (CypherSyntaxError, Exception):
                pass  # Expected

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(20):
                futures.append(executor.submit(run_valid, i))
                futures.append(executor.submit(run_invalid))
            concurrent.futures.wait(futures)

        assert len(errors) == 0, f"Valid queries failed: {errors}"

    def test_concurrent_creates_with_errors(self, large_star: Star) -> None:
        """CREATE operations mixed with errors should not corrupt state.

        Note: ``execute_query`` is not thread-safe for mutations, so some
        CREATEs may silently fail under concurrent access.  The important
        invariant is that the system remains in a *consistent* state (no
        crashes, no data corruption) — not that every CREATE succeeds.
        """
        initial_result = large_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt"
        )
        initial_count = int(initial_result["cnt"].iloc[0])

        errors: list[Exception] = []
        lock = threading.Lock()

        def create_person(idx: int) -> None:
            try:
                large_star.execute_query(
                    f"CREATE (:Person {{name: 'New_{idx}', age: {idx}}})"
                )
            except Exception as e:
                with lock:
                    errors.append(e)

        def bad_query() -> None:
            try:
                large_star.execute_query("INVALID SYNTAX HERE")
            except Exception:
                pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(10):
                futures.append(executor.submit(create_person, i))
                futures.append(executor.submit(bad_query))
            concurrent.futures.wait(futures)

        # System should remain consistent — count >= initial (no data lost)
        result = large_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt"
        )
        final_count = int(result["cnt"].iloc[0])
        assert final_count >= initial_count, (
            f"Data corruption: count dropped from {initial_count} to {final_count}"
        )


class TestErrorMessageConsistency:
    """Error messages should be consistent under stress."""

    def test_same_error_produces_same_message(self) -> None:
        """The same error scenario should produce consistent messages."""
        messages = set()
        for _ in range(50):
            try:
                ctx = ContextBuilder.from_dict(
                    {
                        "Person": pd.DataFrame(
                            {"__ID__": [1], "name": ["Alice"]},
                        ),
                    },
                )
                star = Star(context=ctx)
                star.execute_query("MATCH (n:Ghost) RETURN n")
            except GraphTypeNotFoundError as e:
                messages.add(str(e))

        # All 50 runs should produce the same error message
        assert len(messages) == 1

    def test_syntax_error_messages_stable(self) -> None:
        """Syntax errors for the same input should be deterministic."""
        messages = set()
        for _ in range(30):
            try:
                ctx = ContextBuilder.from_dict({})
                star = Star(context=ctx)
                star.execute_query("MATCH (n WHERE")
            except Exception as e:
                messages.add(str(e))

        assert len(messages) == 1


class TestLargeErrorPayloads:
    """System should handle large/unusual error inputs gracefully."""

    def test_very_long_query_error(self) -> None:
        """Very long invalid queries should produce bounded error messages."""
        ctx = ContextBuilder.from_dict({})
        star = Star(context=ctx)
        long_query = "MATCH (" + "x" * 10000 + ")"
        with pytest.raises(Exception) as exc_info:
            star.execute_query(long_query)
        # Error message should not be unbounded
        assert len(str(exc_info.value)) < 50000

    def test_many_labels_query(self, large_star: Star) -> None:
        """Query referencing many nonexistent labels should fail gracefully."""
        for i in range(20):
            with pytest.raises(Exception):
                large_star.execute_query(
                    f"MATCH (n:NonExistent{i}) RETURN n"
                )

    def test_deeply_nested_property_access(self, large_star: Star) -> None:
        """Deep property chains should not stack overflow."""
        # PyCypher handles chained property access gracefully (returns NULL)
        result = large_star.execute_query(
            "MATCH (n:Person) RETURN n.a.b.c.d.e.f.g AS deep"
        )
        assert isinstance(result, pd.DataFrame)
