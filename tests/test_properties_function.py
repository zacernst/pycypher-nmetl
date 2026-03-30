"""Tests for the properties() graph introspection function.

``properties(n)`` returns all non-internal properties of a node or
relationship as a dict per row, consistent with the openCypher spec.
Internal columns (``__ID__``, ``__SOURCE__``, ``__TARGET__``) are excluded.
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
        },
    )
    knows = pd.DataFrame(
        {
            "__SOURCE__": ["p1", "p2"],
            "__TARGET__": ["p2", "p3"],
            "since": [2020, 2021],
        },
    )
    return Star(
        context=ContextBuilder.from_dict({"Person": persons, "KNOWS": knows}),
    )


class TestPropertiesFunction:
    """properties() — returns all user-visible properties as a map."""

    def test_properties_returns_dict(self, star: Star) -> None:
        """properties(p) returns a dict containing the node's properties."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN properties(p) AS props",
        )
        props = result["props"].iloc[0]
        assert isinstance(props, dict)
        assert props["name"] == "Alice"
        assert props["age"] == 30

    def test_properties_excludes_id_column(self, star: Star) -> None:
        """__ID__ must NOT appear in the properties() result."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN properties(p) AS props",
        )
        props = result["props"].iloc[0]
        assert "__ID__" not in props

    def test_properties_all_rows(self, star: Star) -> None:
        """properties(p) produces a distinct dict for each row."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props ORDER BY p.name ASC",
        )
        names = [r["name"] for r in result["props"]]
        assert names == ["Alice", "Bob", "Carol"]

    def test_properties_keys_match_keys_function(self, star: Star) -> None:
        """The keys in properties(n) match those returned by keys(n)."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN properties(p) AS props, keys(p) AS ks",
        )
        props = result["props"].iloc[0]
        ks = result["ks"].iloc[0]
        assert set(props.keys()) == set(ks)

    def test_properties_for_relationship(self, star: Star) -> None:
        """properties(r) works on relationship variables."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN properties(r) AS props ORDER BY a.name ASC",
        )
        props_list = list(result["props"])
        assert props_list[0]["since"] == 2020
        assert props_list[1]["since"] == 2021

    def test_properties_relationship_excludes_source_target(
        self,
        star: Star,
    ) -> None:
        """__SOURCE__ and __TARGET__ must NOT appear in properties(r)."""
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN properties(r) AS props",
        )
        for props in result["props"]:
            assert "__SOURCE__" not in props
            assert "__TARGET__" not in props

    def test_properties_in_with_clause(self, star: Star) -> None:
        """properties(n) is usable as a WITH alias."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' "
            "WITH properties(p) AS info "
            "RETURN info",
        )
        assert result["info"].iloc[0]["age"] == 35

    def test_properties_size(self, star: Star) -> None:
        """size(keys(properties(n))) equals the number of user columns."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN properties(p) AS props",
        )
        props = result["props"].iloc[0]
        # Person has 'name' and 'age' — exactly 2 user-visible properties
        assert len(props) == 2
