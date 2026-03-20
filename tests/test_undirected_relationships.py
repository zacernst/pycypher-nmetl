"""Tests for undirected relationship matching.

Cypher supports three relationship-direction forms:

  Directed outgoing:    MATCH (a)-[:KNOWS]->(b)
  Directed incoming:    MATCH (a)<-[:KNOWS]-(b)
  Undirected:           MATCH (a)-[:KNOWS]-(b)   ← both directions
  Any type, undirected: MATCH (a)--(b)           ← all types, both directions
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
def simple_graph() -> Star:
    """Alice -[:KNOWS]-> Bob  (A knows B, not the reverse).
    Carol -[:KNOWS]-> Dave
    """
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
        }
    )
    knows = pd.DataFrame(
        {"__ID__": [10, 11], "__SOURCE__": [1, 3], "__TARGET__": [2, 4]}
    )
    ctx = ContextBuilder.from_dict({"Person": people, "KNOWS": knows})
    return Star(context=ctx)


@pytest.fixture
def multi_type_graph() -> Star:
    """Alice -[:KNOWS]-> Bob
    Alice -[:LIKES]-> Carol
    """
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    knows = pd.DataFrame(
        {"__ID__": [10], "__SOURCE__": [1], "__TARGET__": [2]}
    )
    likes = pd.DataFrame(
        {"__ID__": [20], "__SOURCE__": [1], "__TARGET__": [3]}
    )
    ctx = ContextBuilder.from_dict(
        {"Person": people, "KNOWS": knows, "LIKES": likes}
    )
    return Star(context=ctx)


@pytest.fixture
def undirected_graph() -> Star:
    """Alice -[:KNOWS]-> Bob  AND  Carol -[:KNOWS]-> Bob.
    Bob is reachable from both Alice (forward) and Carol (forward).
    In undirected mode Bob should be found from both; Alice/Carol found from Bob.
    """
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    knows = pd.DataFrame(
        {"__ID__": [10, 11], "__SOURCE__": [1, 3], "__TARGET__": [2, 2]}
    )
    ctx = ContextBuilder.from_dict({"Person": people, "KNOWS": knows})
    return Star(context=ctx)


# ===========================================================================
# Typed undirected: MATCH (a)-[:KNOWS]-(b)
# ===========================================================================


class TestTypedUndirected:
    """MATCH (a)-[:TYPE]-(b) — specific type, either direction."""

    def test_typed_undirected_finds_both_directions(
        self, simple_graph: Star
    ) -> None:
        """Undirected should match both (A)->(B) and treat it as (A)-(B) meaning B can find A."""
        result = simple_graph.execute_query(
            "MATCH (a:Person)-[:KNOWS]-(b:Person) "
            "RETURN a.name AS a, b.name AS b "
            "ORDER BY a.name ASC, b.name ASC"
        )
        pairs = set(zip(result["a"], result["b"]))
        # Alice->(Bob): found as (Alice, Bob) and also (Bob, Alice) via reverse direction
        assert ("Alice", "Bob") in pairs
        assert ("Bob", "Alice") in pairs

    def test_typed_undirected_row_count(self, simple_graph: Star) -> None:
        """MATCH (a)-[:KNOWS]-(b) should return 2 rows for 1 directed edge (both orientations)."""
        result = simple_graph.execute_query(
            "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b"
        )
        # 2 directed edges (Alice→Bob, Carol→Dave) → 4 undirected rows
        assert len(result) == 4

    def test_typed_undirected_no_self_loops(self, simple_graph: Star) -> None:
        """A node should never be found as its own neighbor."""
        result = simple_graph.execute_query(
            "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b"
        )
        for _, row in result.iterrows():
            assert row["a"] != row["b"]

    def test_typed_undirected_with_where(self, undirected_graph: Star) -> None:
        """WHERE should filter undirected results."""
        result = undirected_graph.execute_query(
            "MATCH (a:Person)-[:KNOWS]-(b:Person) "
            "WHERE a.age > 26 "
            "RETURN a.name AS a, b.name AS b "
            "ORDER BY a.name ASC"
        )
        # Alice (30) and Carol (35) have age > 26; Bob (25) and entries where a=Bob are excluded
        names_a = set(result["a"])
        assert "Alice" in names_a or "Carol" in names_a
        assert "Bob" not in names_a

    def test_typed_undirected_named_rel_variable(
        self, simple_graph: Star
    ) -> None:
        """Named relationship variable in undirected pattern should not crash."""
        result = simple_graph.execute_query(
            "MATCH (a:Person)-[r:KNOWS]-(b:Person) "
            "RETURN a.name AS a, b.name AS b "
            "ORDER BY a.name ASC, b.name ASC"
        )
        # Should not raise; should find same pairs as the anonymous version
        assert len(result) == 4


# ===========================================================================
# Any-type undirected: MATCH (a)--(b)
# ===========================================================================


class TestAnyTypeUndirected:
    """MATCH (a)--(b) — any relationship type, either direction."""

    def test_anon_undirected_single_type(self, simple_graph: Star) -> None:
        """(a)--(b) with one rel type should match same pairs as typed undirected."""
        typed = simple_graph.execute_query(
            "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b"
        )
        anon = simple_graph.execute_query(
            "MATCH (a:Person)--(b:Person) RETURN a.name AS a, b.name AS b"
        )
        assert set(zip(typed["a"], typed["b"])) == set(
            zip(anon["a"], anon["b"])
        )

    def test_anon_undirected_multi_type(self, multi_type_graph: Star) -> None:
        """(a)--(b) should match across ALL relationship types."""
        result = multi_type_graph.execute_query(
            "MATCH (a:Person)--(b:Person) RETURN a.name AS a, b.name AS b"
        )
        pairs = set(zip(result["a"], result["b"]))
        # Alice→Bob via KNOWS and Alice→Carol via LIKES → 4 undirected pairs
        assert ("Alice", "Bob") in pairs
        assert ("Bob", "Alice") in pairs
        assert ("Alice", "Carol") in pairs
        assert ("Carol", "Alice") in pairs

    def test_anon_undirected_no_self_loops(
        self, multi_type_graph: Star
    ) -> None:
        """No self-loop rows expected."""
        result = multi_type_graph.execute_query(
            "MATCH (a:Person)--(b:Person) RETURN a.name AS a, b.name AS b"
        )
        for _, row in result.iterrows():
            assert row["a"] != row["b"]

    def test_anon_undirected_with_property_return(
        self, simple_graph: Star
    ) -> None:
        """Properties should be accessible after undirected traversal."""
        result = simple_graph.execute_query(
            "MATCH (a:Person)--(b:Person) "
            "RETURN a.name AS a_name, b.age AS b_age "
            "ORDER BY a.name ASC, b.name ASC"
        )
        assert "a_name" in result.columns
        assert "b_age" in result.columns
        assert len(result) > 0
