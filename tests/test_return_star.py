"""Tests for RETURN * (return all variables).

``RETURN *`` returns every variable in scope from the preceding MATCH/WITH.

    MATCH (p:Person) RETURN *          -> returns 'p' column (entity IDs)
    MATCH (p:Person) WITH p.name AS name RETURN *  -> returns 'name' column
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "dept": ["eng", "sales", "eng"],
        }
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


class TestReturnStar:
    """RETURN * — return all in-scope variables."""

    def test_return_star_after_match_has_rows(self, star: Star) -> None:
        """RETURN * after MATCH must return at least one row per entity."""
        result = star.execute_query("MATCH (p:Person) RETURN *")
        assert len(result) == 3

    def test_return_star_after_with_alias(self, star: Star) -> None:
        """RETURN * after WITH alias returns the aliased columns."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name RETURN * ORDER BY name ASC"
        )
        assert "name" in result.columns
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]

    def test_return_star_after_two_with_aliases(self, star: Star) -> None:
        """RETURN * returns all aliases introduced by WITH."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, p.age AS age "
            "RETURN * "
            "ORDER BY name ASC"
        )
        assert "name" in result.columns
        assert "age" in result.columns
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]
        assert list(result["age"]) == [30, 25, 35]

    def test_return_star_with_order_by(self, star: Star) -> None:
        """RETURN * ORDER BY works correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.age AS age "
            "RETURN * ORDER BY age ASC"
        )
        assert list(result["name"]) == ["Bob", "Alice", "Carol"]

    def test_return_star_with_limit(self, star: Star) -> None:
        """RETURN * LIMIT works correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name ORDER BY name ASC RETURN * LIMIT 2"
        )
        assert len(result) == 2

    def test_return_star_with_where_before(self, star: Star) -> None:
        """RETURN * only returns rows passing WHERE filter."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'eng' "
            "WITH p.name AS name RETURN * ORDER BY name ASC"
        )
        assert list(result["name"]) == ["Alice", "Carol"]
