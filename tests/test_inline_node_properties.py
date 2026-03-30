"""Tests for inline node property filters in MATCH patterns.

Cypher supports inline property filters directly in node patterns:

    MATCH (p:Person {name: 'Alice'}) RETURN p.name
    MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(q:Person) RETURN q.name
    MATCH (p:Person {name: 'Alice', age: 30}) RETURN p.name

These must behave identically to their equivalent WHERE clauses.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    """Alice→Bob, Alice→Carol."""
    return pd.DataFrame(
        {
            "__ID__": [10, 11],
            "__SOURCE__": [1, 1],
            "__TARGET__": [2, 3],
        },
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


@pytest.fixture
def star_with_edges(people_df: pd.DataFrame, knows_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict(
        {"Person": people_df, "KNOWS": knows_df},
    )
    return Star(context=context)


# ===========================================================================
# Single-node inline property filter
# ===========================================================================


class TestInlineNodePropertyFilter:
    """MATCH (p:Label {prop: val}) returns only matching rows."""

    def test_single_string_filter(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_single_integer_filter(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person {age: 25}) RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Bob"]

    def test_filter_returns_no_rows_on_mismatch(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person {name: 'Dave'}) RETURN p.name AS name",
        )
        assert len(result) == 0

    def test_multiple_inline_filters(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice', age: 30}) RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_multiple_inline_filters_no_match_when_one_fails(
        self,
        star: Star,
    ) -> None:
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice', age: 99}) RETURN p.name AS name",
        )
        assert len(result) == 0

    def test_inline_filter_equivalent_to_where(self, star: Star) -> None:
        """Inline filter and WHERE filter return identical results."""
        inline = star.execute_query(
            "MATCH (p:Person {age: 35}) RETURN p.name AS name",
        )
        where = star.execute_query(
            "MATCH (p:Person) WHERE p.age = 35 RETURN p.name AS name",
        )
        assert set(inline["name"]) == set(where["name"])


# ===========================================================================
# Inline property filter on start node in a relationship pattern
# ===========================================================================


class TestInlineFilterWithRelationship:
    """MATCH (p:Person {prop: val})-[:REL]->(q) returns correct rows."""

    def test_inline_filter_on_source_node(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(q:Person) "
            "RETURN q.name AS name ORDER BY q.name ASC",
        )
        assert list(result["name"]) == ["Bob", "Carol"]

    def test_inline_filter_on_source_node_no_match(
        self,
        star_with_edges: Star,
    ) -> None:
        """No outgoing KNOWS for Carol."""
        result = star_with_edges.execute_query(
            "MATCH (p:Person {name: 'Carol'})-[:KNOWS]->(q:Person) "
            "RETURN q.name AS name",
        )
        assert len(result) == 0

    def test_inline_filter_on_target_node(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person {name: 'Bob'}) RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_inline_filter_on_both_nodes(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(q:Person {name: 'Bob'}) "
            "RETURN p.name AS src, q.name AS tgt",
        )
        assert list(result["src"]) == ["Alice"]
        assert list(result["tgt"]) == ["Bob"]

    def test_inline_filter_equivalent_to_where_in_relationship(
        self,
        star_with_edges: Star,
    ) -> None:
        inline = star_with_edges.execute_query(
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(q:Person) "
            "RETURN q.name AS name ORDER BY q.name ASC",
        )
        where_equiv = star_with_edges.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.name = 'Alice' "
            "RETURN q.name AS name ORDER BY q.name ASC",
        )
        assert list(inline["name"]) == list(where_equiv["name"])
