"""Tests for MapLiteral expressions: {key: value, key2: expr}.

Map literals ``{name: p.name, count: size(p.tags)}`` produce a dict
per row and can appear in RETURN, WITH, and WHERE clauses.
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
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


class TestMapLiteral:
    """MapLiteral — {key: expr} in query context."""

    def test_static_map_literal(self, star: Star) -> None:
        """A map literal with constant values returns a dict."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN {greeting: 'hello', num: 42} AS m",
        )
        row = result["m"].iloc[0]
        assert row["greeting"] == "hello"
        assert row["num"] == 42

    def test_map_with_property_values(self, star: Star) -> None:
        """A map literal referencing node properties evaluates correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN {n: p.name, a: p.age} AS m",
        )
        row = result["m"].iloc[0]
        assert row["n"] == "Alice"
        assert row["a"] == 30

    def test_map_with_computed_value(self, star: Star) -> None:
        """A map literal with an arithmetic expression evaluates the expression."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN {double: p.age * 2} AS m",
        )
        assert result["m"].iloc[0]["double"] == 60

    def test_map_in_with_clause(self, star: Star) -> None:
        """Map literals work as WITH aliases."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH {name: p.name} AS info "
            "RETURN info",
        )
        assert result["info"].iloc[0] == {"name": "Alice"}

    def test_map_all_rows(self, star: Star) -> None:
        """Map literal evaluated for all rows produces correct dicts."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN {name: p.name} AS m ORDER BY p.name ASC",
        )
        names = [row["name"] for row in result["m"]]
        assert names == ["Alice", "Bob", "Carol"]

    def test_empty_map(self, star: Star) -> None:
        """An empty map literal {} returns an empty dict."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN {} AS m",
        )
        assert result["m"].iloc[0] == {}
