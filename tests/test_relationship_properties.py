"""Tests for accessing relationship properties in MATCH/RETURN/WHERE.

MATCH (a)-[r:KNOWS]->(b) RETURN r.since  — accesses a property stored
on the relationship itself, not on the connected nodes.

This is a core Cypher feature that must be supported alongside the
existing node property access path.
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
    """Alice→Bob (friends since 2020), Alice→Carol (friends since 2022)."""
    return pd.DataFrame(
        {
            "__ID__": [10, 11],
            "__SOURCE__": [1, 1],
            "__TARGET__": [2, 3],
            "since": [2020, 2022],
            "strength": [0.8, 0.5],
        }
    )


@pytest.fixture
def star(people_df: pd.DataFrame, knows_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict(
        {"Person": people_df, "KNOWS": knows_df}
    )
    return Star(context=context)


# ===========================================================================
# Basic relationship property RETURN
# ===========================================================================


class TestRelationshipPropertyReturn:
    """MATCH (a)-[r:KNOWS]->(b) RETURN r.prop — reads edge attributes."""

    def test_return_single_rel_property(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN r.since AS since ORDER BY r.since ASC"
        )
        assert list(result["since"]) == [2020, 2022]

    def test_return_multiple_rel_properties(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN r.since AS since, r.strength AS strength "
            "ORDER BY r.since ASC"
        )
        assert list(result["since"]) == [2020, 2022]
        assert list(result["strength"]) == [0.8, 0.5]

    def test_return_rel_and_node_properties_together(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt, r.since AS since "
            "ORDER BY since ASC"
        )
        assert list(result["src"]) == ["Alice", "Alice"]
        assert list(result["tgt"]) == ["Bob", "Carol"]
        assert list(result["since"]) == [2020, 2022]

    def test_missing_rel_property_returns_null(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.nonexistent AS val"
        )
        assert len(result) == 2
        assert result["val"].isna().all()


# ===========================================================================
# Relationship property in WHERE clause
# ===========================================================================


class TestRelationshipPropertyWhere:
    """Filter edges by their own properties."""

    def test_where_rel_property_equality(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE r.since = 2020 "
            "RETURN b.name AS name"
        )
        assert list(result["name"]) == ["Bob"]

    def test_where_rel_property_comparison(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE r.since > 2020 "
            "RETURN b.name AS name"
        )
        assert list(result["name"]) == ["Carol"]

    def test_where_rel_and_node_property(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE r.strength > 0.6 AND b.age < 30 "
            "RETURN b.name AS name"
        )
        assert list(result["name"]) == ["Bob"]
