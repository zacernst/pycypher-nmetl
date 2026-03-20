"""Tests for startNode() and endNode() graph introspection functions.

``startNode(r)`` returns the ID of the source node of a relationship.
``endNode(r)`` returns the ID of the target node.

Both follow the pre-evaluation intercept pattern used by ``labels()``,
``type()``, ``keys()``, and ``properties()``.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    persons = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3"],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    knows = pd.DataFrame(
        {
            "__SOURCE__": ["p1", "p2"],
            "__TARGET__": ["p2", "p3"],
            "since": [2020, 2021],
        }
    )
    return Star(
        context=ContextBuilder.from_dict({"Person": persons, "KNOWS": knows})
    )


class TestStartNodeEndNode:
    """startNode(r) and endNode(r) return source/target node IDs."""

    def test_startnode_returns_source_id(self, star: Star) -> None:
        """startNode(r) returns the source node ID for each relationship."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN startNode(r) AS src ORDER BY a.name ASC"
        )
        assert list(result["src"]) == ["p1", "p2"]

    def test_endnode_returns_target_id(self, star: Star) -> None:
        """endNode(r) returns the target node ID for each relationship."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN endNode(r) AS tgt ORDER BY a.name ASC"
        )
        assert list(result["tgt"]) == ["p2", "p3"]

    def test_startnode_matches_source_variable(self, star: Star) -> None:
        """startNode(r) equals the a variable in MATCH (a)-[r]->(b)."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN startNode(r) AS src, a AS a_id ORDER BY a.name ASC"
        )
        for _, row in result.iterrows():
            assert row["src"] == row["a_id"]

    def test_endnode_matches_target_variable(self, star: Star) -> None:
        """endNode(r) equals the b variable in MATCH (a)-[r]->(b)."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN endNode(r) AS tgt, b AS b_id ORDER BY a.name ASC"
        )
        for _, row in result.iterrows():
            assert row["tgt"] == row["b_id"]

    def test_startnode_in_where_filter(self, star: Star) -> None:
        """startNode(r) can be used in a WHERE predicate."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE startNode(r) = 'p1' "
            "RETURN b.name AS friend"
        )
        assert list(result["friend"]) == ["Bob"]

    def test_endnode_in_where_filter(self, star: Star) -> None:
        """endNode(r) can be used in a WHERE predicate."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE endNode(r) = 'p3' "
            "RETURN a.name AS person"
        )
        assert list(result["person"]) == ["Bob"]

    def test_startnode_with_clause(self, star: Star) -> None:
        """startNode(r) works as a WITH alias."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WITH startNode(r) AS src_id "
            "RETURN src_id ORDER BY src_id ASC"
        )
        assert list(result["src_id"]) == ["p1", "p2"]

    def test_endnode_all_rows(self, star: Star) -> None:
        """endNode(r) returns the correct target for every row."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN endNode(r) AS tgt, b.name AS name ORDER BY a.name ASC"
        )
        assert len(result) == 2
        # First edge: p1→p2 (Alice→Bob)
        assert result["tgt"].iloc[0] == "p2"
        assert result["name"].iloc[0] == "Bob"
