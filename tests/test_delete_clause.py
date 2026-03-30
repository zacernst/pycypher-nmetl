"""Tests for DELETE and DETACH DELETE clause execution.

DELETE p removes entity rows from the entity tables.
DETACH DELETE p also removes all relationships involving those entities.

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


@pytest.fixture
def social_context() -> Context:
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


class TestDeleteNode:
    """DELETE p removes matched node rows from the entity table."""

    def test_delete_removes_node(self, social_context: Context) -> None:
        """Deleting a node removes it from subsequent MATCH results."""
        star = Star(context=social_context)
        star.execute_query("MATCH (p:Person) WHERE p.name = 'Carol' DELETE p")
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert "Carol" not in result["name"].tolist()

    def test_delete_preserves_other_nodes(
        self,
        social_context: Context,
    ) -> None:
        """Deleting one node does not affect other nodes."""
        star = Star(context=social_context)
        star.execute_query("MATCH (p:Person) WHERE p.name = 'Carol' DELETE p")
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert "Alice" in result["name"].tolist()
        assert "Bob" in result["name"].tolist()

    def test_delete_reduces_count(self, social_context: Context) -> None:
        """After DELETE, the number of matching nodes decreases."""
        star = Star(context=social_context)
        before = len(
            star.execute_query("MATCH (p:Person) RETURN p.name AS name"),
        )
        star.execute_query("MATCH (p:Person) WHERE p.name = 'Alice' DELETE p")
        after = len(
            star.execute_query("MATCH (p:Person) RETURN p.name AS name"),
        )
        assert after == before - 1

    def test_delete_all_nodes(self, social_context: Context) -> None:
        """Deleting all matching nodes leaves an empty result."""
        star = Star(context=social_context)
        star.execute_query("MATCH (p:Person) DELETE p")
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert len(result) == 0

    def test_delete_does_not_raise_not_implemented(
        self,
        social_context: Context,
    ) -> None:
        """Regression: DELETE must not raise NotImplementedError."""
        star = Star(context=social_context)
        # Should not raise:
        star.execute_query("MATCH (p:Person) WHERE p.name = 'Bob' DELETE p")

    def test_delete_atomicity_failure_does_not_persist(
        self,
        social_context: Context,
    ) -> None:
        """A failing query after DELETE does not persist the deletion."""
        star = Star(context=social_context)
        with pytest.raises(Exception):
            star.execute_query(
                "MATCH (p:Person) WHERE p.name = 'Alice' "
                "DELETE p "
                "RETURN nosuchvar.prop AS x",
            )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert "Alice" in result["name"].tolist()


class TestDetachDelete:
    """DETACH DELETE p removes the node AND all its relationships."""

    def test_detach_delete_removes_relationships(
        self,
        social_context: Context,
    ) -> None:
        """DETACH DELETE removes the entity and all its KNOWS relationships."""
        star = Star(context=social_context)
        # Alice has 2 outgoing KNOWS edges (to Bob and Carol)
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' DETACH DELETE p",
        )
        # Those relationships should be gone
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src",
        )
        assert "Alice" not in result["src"].tolist()

    def test_detach_delete_removes_incoming_relationships(
        self,
        social_context: Context,
    ) -> None:
        """DETACH DELETE also removes incoming relationships to the deleted node."""
        star = Star(context=social_context)
        # Carol has 2 incoming KNOWS edges
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' DETACH DELETE p",
        )
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS tgt",
        )
        assert "Carol" not in result["tgt"].tolist()

    def test_detach_delete_preserves_unrelated_relationships(
        self,
        social_context: Context,
    ) -> None:
        """Relationships not involving the deleted node are preserved."""
        star = Star(context=social_context)
        # Delete Alice; Bob->Carol should survive
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' DETACH DELETE p",
        )
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt",
        )
        assert len(result) == 1
        assert result["src"].iloc[0] == "Bob"
        assert result["tgt"].iloc[0] == "Carol"
