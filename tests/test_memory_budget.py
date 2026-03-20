"""Tests for query memory budget enforcement.

Verifies that ``execute_query(memory_budget_bytes=...)`` raises
``QueryMemoryBudgetError`` when the query planner estimates memory usage
exceeds the budget, and that queries within budget complete normally.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star
from pycypher.exceptions import QueryMemoryBudgetError


@pytest.fixture
def star() -> Star:
    """Return a Star with a small Person entity table."""
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    context = ContextBuilder().add_entity("Person", df).build()
    return Star(context=context)


class TestMemoryBudgetEnforcement:
    """Verify memory budget blocks oversized queries."""

    def test_no_budget_succeeds(self, star: Star) -> None:
        """Without a budget, queries always run."""
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 3

    def test_generous_budget_succeeds(self, star: Star) -> None:
        """A large budget allows normal queries."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name",
            memory_budget_bytes=10 * 1024 * 1024 * 1024,  # 10 GB
        )
        assert len(result) == 3

    def test_tiny_budget_raises(self, star: Star) -> None:
        """A 1-byte budget should reject any query."""
        with pytest.raises(QueryMemoryBudgetError) as exc_info:
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                memory_budget_bytes=1,
            )
        err = exc_info.value
        assert err.budget_bytes == 1
        assert err.estimated_bytes > 0


class TestQueryMemoryBudgetErrorAttributes:
    """Verify QueryMemoryBudgetError diagnostic attributes."""

    def test_error_attributes(self) -> None:
        err = QueryMemoryBudgetError(
            estimated_bytes=4 * 1024 * 1024 * 1024,
            budget_bytes=2 * 1024 * 1024 * 1024,
        )
        assert err.estimated_bytes == 4 * 1024 * 1024 * 1024
        assert err.budget_bytes == 2 * 1024 * 1024 * 1024
        assert isinstance(err, MemoryError)
        assert "4096MB" in str(err)
        assert "2048MB" in str(err)

    def test_custom_suggestion(self) -> None:
        err = QueryMemoryBudgetError(
            estimated_bytes=1000,
            budget_bytes=500,
            suggestion="Try adding LIMIT 10",
        )
        assert "Try adding LIMIT 10" in str(err)

    def test_default_suggestion(self) -> None:
        err = QueryMemoryBudgetError(estimated_bytes=1000, budget_bytes=500)
        assert "LIMIT" in str(err)


class TestBudgetCleanup:
    """Ensure budget rejection doesn't leave dirty state."""

    def test_subsequent_query_works_after_rejection(self, star: Star) -> None:
        """After a budget rejection, normal queries succeed."""
        with pytest.raises(QueryMemoryBudgetError):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name", memory_budget_bytes=1
            )
        # Should work fine without budget constraint
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 3
