"""TDD tests for SET on relationship variable properties.

    MATCH (p)-[r:KNOWS]->(f) SET r.weight = 0.9

Currently raises ``KeyError: 'KNOWS'`` because BindingFrame.mutate() looks up
the variable's type in ``context.entity_mapping`` but relationship tables live
in ``context.relationship_mapping``.

All tests written before the fix (TDD step 1).
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
def social_star() -> Star:
    """Alice -KNOWS[w=0.8]-> Bob -KNOWS[w=0.5]-> Carol."""
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
            "weight": [0.8, 0.5],
        },
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "weight"],
        source_obj_attribute_map={"weight": "weight"},
        attribute_map={"weight": "weight"},
        source_obj=knows_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        ),
    )


class TestSetRelationshipProperty:
    """SET r.prop = value must work for relationship variables."""

    def test_set_rel_property_does_not_raise(self, social_star: Star) -> None:
        """SET r.weight = 1.0 must not raise KeyError."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 1.0",
        )

    def test_set_rel_property_updates_value(self, social_star: Star) -> None:
        """After SET r.weight = 1.0, reading r.weight returns 1.0."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 1.0",
        )
        r = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN r.weight ORDER BY p.name",
        )
        # Column is named 'weight' (no variable prefix) per pycypher convention
        assert list(r["weight"]) == [1.0, 1.0]

    def test_set_rel_property_with_where(self, social_star: Star) -> None:
        """SET r.weight = 0.0 WHERE p.name = 'Alice' updates only that edge."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) "
            "WHERE p.name = 'Alice' SET r.weight = 0.0",
        )
        r = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN p.name, r.weight ORDER BY p.name",
        )
        weights = dict(zip(r["name"], r["weight"]))
        assert weights["Alice"] == 0.0
        assert weights["Bob"] == 0.5  # unchanged

    def test_set_new_rel_property(self, social_star: Star) -> None:
        """SET r.label = 'friend' adds a new property to the relationship."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.label = 'friend'",
        )
        r = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN r.label ORDER BY p.name",
        )
        assert list(r["label"]) == ["friend", "friend"]

    def test_set_rel_property_read_back_in_where(
        self,
        social_star: Star,
    ) -> None:
        """After SET, WHERE on updated property filters correctly."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) "
            "WHERE p.name = 'Alice' SET r.weight = 0.0",
        )
        r = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) WHERE r.weight > 0.0 RETURN p.name",
        )
        assert list(r["name"]) == ["Bob"]

    def test_set_rel_existing_property_preserved_on_unmatched(
        self,
        social_star: Star,
    ) -> None:
        """Unmatched rows keep their original weight after a conditional SET."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) "
            "WHERE p.name = 'Bob' SET r.weight = 0.1",
        )
        r = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN p.name, r.weight ORDER BY p.name",
        )
        weights = dict(zip(r["name"], r["weight"]))
        assert weights["Alice"] == 0.8  # unchanged
        assert weights["Bob"] == 0.1  # updated
