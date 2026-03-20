"""Tests for property access on unlabeled node variables in relationship patterns.

When a node in a MATCH pattern has no label, e.g. MATCH (p:Person)-[:KNOWS]->(q),
the variable 'q' inherits its entity type implicitly from the relationship traversal.
get_property() must still resolve the correct entity table by auto-detecting which
table contains the IDs present in the binding frame.
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
        }
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    """Alice→Bob, Alice→Carol."""
    return pd.DataFrame(
        {
            "__ID__": [10, 11],
            "__SOURCE__": [1, 1],
            "__TARGET__": [2, 3],
        }
    )


@pytest.fixture
def star_with_edges(people_df: pd.DataFrame, knows_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict(
        {"Person": people_df, "KNOWS": knows_df}
    )
    return Star(context=context)


# ===========================================================================
# Basic unlabeled target node
# ===========================================================================


class TestUnlabeledNodePropertyAccess:
    """MATCH (p:Person)-[:KNOWS]->(q) — q has no label but should be accessible."""

    def test_unlabeled_target_node_name(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q) "
            "RETURN p.name AS src, q.name AS tgt "
            "ORDER BY q.name ASC"
        )
        assert list(result["tgt"]) == ["Bob", "Carol"]
        assert all(s == "Alice" for s in result["src"])

    def test_unlabeled_target_node_age(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q) RETURN q.age AS age ORDER BY q.age ASC"
        )
        assert list(result["age"]) == [25, 35]

    def test_unlabeled_source_node(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p)-[:KNOWS]->(q:Person) RETURN p.name AS src ORDER BY p.name ASC"
        )
        assert list(result["src"]) == ["Alice", "Alice"]

    def test_both_nodes_unlabeled(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p)-[:KNOWS]->(q) "
            "RETURN p.name AS src, q.name AS tgt "
            "ORDER BY q.name ASC"
        )
        assert list(result["tgt"]) == ["Bob", "Carol"]

    def test_unlabeled_node_where_filter(self, star_with_edges: Star) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q) WHERE q.age > 30 RETURN q.name AS name"
        )
        assert list(result["name"]) == ["Carol"]

    def test_unlabeled_node_with_passthrough(
        self, star_with_edges: Star
    ) -> None:
        """WITH + second MATCH using unlabeled target."""
        result = star_with_edges.execute_query(
            "MATCH (p:Person) WHERE p.age > 27 "
            "WITH p "
            "MATCH (p)-[:KNOWS]->(q) "
            "RETURN p.name AS src, q.name AS tgt "
            "ORDER BY q.name ASC"
        )
        assert list(result["tgt"]) == ["Bob", "Carol"]
        assert all(s == "Alice" for s in result["src"])
