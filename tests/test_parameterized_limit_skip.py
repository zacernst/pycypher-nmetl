"""Tests for parameterized LIMIT and SKIP clauses.

``LIMIT $n`` and ``SKIP $n`` must work with query parameters, enabling
reusable pagination queries without string interpolation.

Prior to this fix the parameter expression was silently dropped in the
ASTConverter (``_convert_Return``), and WITH threw a Pydantic validation
error trying to coerce a ``Parameter`` dict to ``int``.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    """Five persons ordered by ascending age."""
    persons = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3", "p4", "p5"],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [10, 20, 30, 40, 50],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": persons}))


class TestReturnLimitSkipParameters:
    """RETURN clause LIMIT/SKIP with query parameters."""

    def test_return_limit_parameter(self, star: Star) -> None:
        """LIMIT $n in RETURN restricts row count to the parameter value."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm ORDER BY p.age ASC LIMIT $n",
            parameters={"n": 3},
        )
        assert len(result) == 3
        assert list(result["nm"]) == ["Alice", "Bob", "Carol"]

    def test_return_skip_parameter(self, star: Star) -> None:
        """SKIP $n in RETURN skips the first n rows."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm ORDER BY p.age ASC SKIP $s",
            parameters={"s": 2},
        )
        assert len(result) == 3
        assert list(result["nm"]) == ["Carol", "Dave", "Eve"]

    def test_return_skip_and_limit_parameters(self, star: Star) -> None:
        """SKIP $s LIMIT $l together implement a pagination window."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm ORDER BY p.age ASC SKIP $s LIMIT $l",
            parameters={"s": 1, "l": 2},
        )
        # Skip 1 → start at Bob; Limit 2 → Bob, Carol
        assert len(result) == 2
        assert list(result["nm"]) == ["Bob", "Carol"]

    def test_return_limit_integer_literal_still_works(
        self,
        star: Star,
    ) -> None:
        """Integer literal LIMIT continues to work after the fix (regression)."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm ORDER BY p.age ASC LIMIT 2",
        )
        assert len(result) == 2
        assert list(result["nm"]) == ["Alice", "Bob"]

    def test_return_skip_integer_literal_still_works(self, star: Star) -> None:
        """Integer literal SKIP continues to work after the fix (regression)."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm ORDER BY p.age ASC SKIP 4",
        )
        assert len(result) == 1
        assert list(result["nm"]) == ["Eve"]


class TestWithLimitSkipParameters:
    """WITH clause LIMIT/SKIP with query parameters."""

    def test_with_limit_parameter(self, star: Star) -> None:
        """LIMIT $n in WITH restricts rows before the following RETURN."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS nm ORDER BY p.age ASC LIMIT $n RETURN nm",
            parameters={"n": 2},
        )
        assert len(result) == 2
        assert list(result["nm"]) == ["Alice", "Bob"]

    def test_with_skip_parameter(self, star: Star) -> None:
        """SKIP $n in WITH skips rows before passing them on."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS nm ORDER BY p.age ASC SKIP $s RETURN nm",
            parameters={"s": 3},
        )
        assert len(result) == 2
        assert list(result["nm"]) == ["Dave", "Eve"]

    def test_with_limit_integer_literal_still_works(self, star: Star) -> None:
        """Integer literal LIMIT in WITH continues to work (regression)."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS nm ORDER BY p.age ASC LIMIT 3 RETURN nm",
        )
        assert len(result) == 3

    def test_limit_zero_returns_empty(self, star: Star) -> None:
        """LIMIT $n with n=0 returns an empty result."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm LIMIT $n",
            parameters={"n": 0},
        )
        assert len(result) == 0

    def test_limit_exceeds_row_count(self, star: Star) -> None:
        """LIMIT $n with n > row count returns all rows (no error)."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm LIMIT $n",
            parameters={"n": 100},
        )
        assert len(result) == 5
