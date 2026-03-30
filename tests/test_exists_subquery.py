"""Tests for EXISTS with full subquery form.

EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > 25 RETURN f }

This is the correlated subquery form of EXISTS — the outer frame's bound
variables are visible inside the subquery, and the subquery is evaluated
once per outer row.  It returns True iff the subquery produces at least
one result row.

Contrast with the pattern-only form (already supported):
  EXISTS { (p)-[:KNOWS]->(f) }

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def social_context() -> Context:
    """Alice (age 30) knows Bob (age 25) and Carol (age 35)."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102],
            "__SOURCE__": [1, 1],  # Alice knows Bob and Carol
            "__TARGET__": [2, 3],
        },
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture
def isolated_context() -> Context:
    """Dave knows nobody; Eve knows Frank."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [4, 5, 6],
            "name": ["Dave", "Eve", "Frank"],
            "age": [40, 28, 32],
        },
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [201],
            "__SOURCE__": [5],  # Eve knows Frank
            "__TARGET__": [6],
        },
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


# ---------------------------------------------------------------------------
# Basic EXISTS { MATCH ... RETURN ... } — correlated subquery
# ---------------------------------------------------------------------------


class TestExistsSubqueryBasic:
    """EXISTS { MATCH ... RETURN ... } evaluates per outer row."""

    def test_exists_subquery_filters_correctly(
        self,
        social_context: Context,
    ) -> None:
        """Persons with at least one outgoing KNOWS edge pass the EXISTS filter."""
        star = Star(context=social_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name",
        )
        # Only Alice has outgoing KNOWS edges
        assert result["name"].tolist() == ["Alice"]

    def test_not_exists_subquery(self, social_context: Context) -> None:
        """NOT EXISTS { MATCH ... RETURN ... } keeps rows with no subquery match."""
        star = Star(context=social_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name ORDER BY p.name",
        )
        # Bob and Carol have no outgoing KNOWS edges
        assert set(result["name"].tolist()) == {"Bob", "Carol"}

    def test_exists_subquery_with_where_inside(
        self,
        social_context: Context,
    ) -> None:
        """EXISTS subquery can filter with WHERE on inner variables."""
        star = Star(context=social_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { "
            "  MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > 30 RETURN f "
            "} "
            "RETURN p.name AS name",
        )
        # Alice knows Carol (age 35 > 30), so Alice passes; Bob and Carol have no outgoing edges
        assert result["name"].tolist() == ["Alice"]

    def test_exists_subquery_no_match_returns_false(
        self,
        isolated_context: Context,
    ) -> None:
        """EXISTS returns False for rows where the subquery finds no match."""
        star = Star(context=isolated_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name",
        )
        # Only Eve has outgoing KNOWS edges
        assert result["name"].tolist() == ["Eve"]

    def test_exists_subquery_does_not_raise_not_implemented(
        self,
        social_context: Context,
    ) -> None:
        """Regression: EXISTS with full subquery must not raise NotImplementedError."""
        star = Star(context=social_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Combining EXISTS subquery with other predicates
# ---------------------------------------------------------------------------


class TestExistsSubqueryCombined:
    """EXISTS subquery works in combination with other WHERE predicates."""

    def test_exists_and_age_filter(self, social_context: Context) -> None:
        """EXISTS combined with age filter via AND."""
        star = Star(context=social_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE p.age > 25 "
            "AND EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name",
        )
        # Alice (age 30) has outgoing edges; Bob (age 25) does not qualify on age;
        # Carol (age 35) has no outgoing edges
        assert result["name"].tolist() == ["Alice"]

    def test_all_rows_pass_when_all_match(
        self,
        social_context: Context,
    ) -> None:
        """EXISTS returns True for all rows when the subquery always matches."""
        star = Star(context=social_context)
        # Every person knows themselves in this contrived example?
        # No — use a broader EXISTS that is always True
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (q:Person) RETURN q } "
            "RETURN p.name AS name ORDER BY p.name",
        )
        # All rows pass because the inner MATCH always finds persons
        assert set(result["name"].tolist()) == {"Alice", "Bob", "Carol"}
