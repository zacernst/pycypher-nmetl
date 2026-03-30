"""Tests for EXISTS subquery predicate evaluation.

EXISTS { pattern } returns True for rows where at least one graph path
matches the given pattern, anchored on currently-bound variables.

TDD: tests written before implementation.
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


@pytest.fixture
def knows_context() -> Context:
    """Three people; Alice knows Bob and Carol; Bob knows Carol."""
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
    # Alice(1)->Bob(2), Alice(1)->Carol(3), Bob(2)->Carol(3)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103],
            "__SOURCE__": [1, 1, 2],
            "__TARGET__": [2, 3, 3],
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


class TestExistsPredicate:
    """EXISTS { pattern } in WHERE clause."""

    def test_exists_returns_true_for_rows_with_matches(
        self,
        knows_context: Context,
    ) -> None:
        """Alice and Bob have outgoing KNOWS — EXISTS returns True for them."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->(f) } RETURN p.name AS name",
        )
        names = set(result["name"].tolist())
        assert names == {"Alice", "Bob"}

    def test_exists_returns_false_for_rows_without_matches(
        self,
        knows_context: Context,
    ) -> None:
        """Carol has no outgoing KNOWS — she is excluded."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->(f) } RETURN p.name AS name",
        )
        assert "Carol" not in result["name"].tolist()

    def test_not_exists_inverts_the_predicate(
        self,
        knows_context: Context,
    ) -> None:
        """NOT EXISTS selects rows where no match exists."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT EXISTS { (p)-[:KNOWS]->(f) } RETURN p.name AS name",
        )
        assert result["name"].tolist() == ["Carol"]

    def test_exists_in_and_combination(self, knows_context: Context) -> None:
        """EXISTS can be combined with other predicates via AND."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE p.age < 31 AND EXISTS { (p)-[:KNOWS]->(f) } "
            "RETURN p.name AS name",
        )
        # Alice (age=30, has KNOWS) passes; Bob (age=25, has KNOWS) passes;
        # Carol (age=35) fails age filter; no one else
        names = set(result["name"].tolist())
        assert names == {"Alice", "Bob"}

    def test_exists_does_not_raise_not_implemented(
        self,
        knows_context: Context,
    ) -> None:
        """EXISTS must not raise NotImplementedError."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->(f) } RETURN p.name AS name",
        )
        assert result is not None
