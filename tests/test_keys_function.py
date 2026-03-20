"""Tests for the keys() graph introspection function.

``keys(n)`` returns the list of property names of a node or relationship.
Internal columns (``__ID__``, ``__SOURCE__``, ``__TARGET__``) must be excluded.
The function must work for both entity variables and relationship variables.
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
            "weight": [1.0, 0.8],
        }
    )
    return Star(
        context=ContextBuilder.from_dict({"Person": persons, "KNOWS": knows})
    )


class TestKeysFunction:
    """keys() returns property name lists for nodes and relationships."""

    def test_keys_for_entity_node(self, star: Star) -> None:
        """keys(n) returns the list of user-visible property names for a node."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN keys(p) AS ks"
        )
        ks = result["ks"].iloc[0]
        assert set(ks) == {"name", "age"}

    def test_keys_excludes_id_column(self, star: Star) -> None:
        """__ID__ must NOT appear in keys(n)."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN keys(p) AS ks"
        )
        assert "__ID__" not in result["ks"].iloc[0]

    def test_keys_for_relationship(self, star: Star) -> None:
        """keys(r) returns the user-visible property names of a relationship."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN keys(r) AS ks ORDER BY a.name ASC"
        )
        ks = result["ks"].iloc[0]
        assert set(ks) == {"since", "weight"}

    def test_keys_relationship_excludes_internal_columns(
        self, star: Star
    ) -> None:
        """__ID__, __SOURCE__, and __TARGET__ must NOT appear in keys(r)."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN keys(r) AS ks"
        )
        for ks in result["ks"]:
            assert "__ID__" not in ks
            assert "__SOURCE__" not in ks
            assert "__TARGET__" not in ks

    def test_keys_result_is_a_list(self, star: Star) -> None:
        """keys() always returns a list, never a set or other iterable."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN keys(p) AS ks"
        )
        assert isinstance(result["ks"].iloc[0], list)

    def test_keys_for_relationship_result_is_a_list(self, star: Star) -> None:
        """keys(r) returns a list, consistent with keys(n)."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN keys(r) AS ks"
        )
        for ks in result["ks"]:
            assert isinstance(ks, list)

    def test_keys_same_for_all_rows(self, star: Star) -> None:
        """All rows of the same entity type return the same key list."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS ks ORDER BY p.name ASC"
        )
        key_sets = [frozenset(ks) for ks in result["ks"]]
        assert len(set(key_sets)) == 1, (
            "All Person rows should have the same keys"
        )

    def test_keys_matches_properties_keys(self, star: Star) -> None:
        """The keys returned by keys(r) match those in properties(r)."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN keys(r) AS ks, properties(r) AS props ORDER BY a.name ASC"
        )
        row = result.iloc[0]
        assert set(row["ks"]) == set(row["props"].keys())
