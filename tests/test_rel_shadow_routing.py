"""TDD tests for correct relationship property mutation shadow-layer routing.

BindingFrame.mutate() must write relationship property mutations to
``context._shadow_rels``, not to ``context._shadow``.  Before the fix, every
SET on a relationship variable silently polluted ``entity_mapping`` with a
spurious relationship-type entry and left ``relationship_mapping.source_obj``
permanently stale.

All tests in this file are written *before* the fix (TDD red phase).
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
# Shared fixture
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Shadow-layer routing tests
# ---------------------------------------------------------------------------


class TestRelationshipShadowRouting:
    """SET on a relationship variable must route to _shadow_rels, not _shadow."""

    def test_entity_mapping_not_polluted_after_set_rel_property(
        self,
        social_star: Star,
    ) -> None:
        """After SET r.weight = 1.0, entity_mapping must NOT gain a 'KNOWS' entry."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 1.0",
        )
        assert "KNOWS" not in social_star.context.entity_mapping.mapping, (
            "relationship type 'KNOWS' was incorrectly added to entity_mapping after "
            "SET r.weight — mutations on relationship variables must go to _shadow_rels"
        )

    def test_relationship_mapping_source_obj_updated_after_commit(
        self,
        social_star: Star,
    ) -> None:
        """After SET r.weight = 1.0 and commit, relationship_mapping.source_obj
        must reflect the updated weight.
        """
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 1.0",
        )
        knows_source = social_star.context.relationship_mapping.mapping[
            "KNOWS"
        ].source_obj
        if isinstance(knows_source, pd.DataFrame):
            weights = set(knows_source["weight"].dropna().tolist())
        else:
            weights = set(
                pd.DataFrame(knows_source)["weight"].dropna().tolist(),
            )
        assert weights == {1.0}, (
            f"relationship_mapping.source_obj still has stale weights {weights}; "
            "commit_query must promote _shadow_rels to relationship_mapping"
        )

    def test_relationship_shadow_cleared_after_rollback(
        self,
        social_star: Star,
    ) -> None:
        """After a failed query (rollback), relationship_mapping must be unchanged."""
        original_weights = list(
            pd.DataFrame(
                social_star.context.relationship_mapping.mapping["KNOWS"].source_obj,
            )["weight"],
        )
        # Force a rollback by making the query fail after the SET via invalid RETURN
        try:
            social_star.execute_query(
                "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 99.0 "
                "RETURN nonexistent_function_that_does_not_exist_xyz()",
            )
        except Exception:
            pass

        # relationship_mapping should not have been updated
        after_weights = list(
            pd.DataFrame(
                social_star.context.relationship_mapping.mapping["KNOWS"].source_obj,
            )["weight"],
        )
        assert after_weights == original_weights, (
            "Relationship source_obj was mutated despite rollback"
        )

    def test_entity_shadow_still_used_for_entity_mutations(
        self,
        social_star: Star,
    ) -> None:
        """SET on an entity (node) variable must still route to _shadow, not _shadow_rels."""
        social_star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 42",
        )
        # Entity mapping must be updated
        person_source = social_star.context.entity_mapping.mapping["Person"].source_obj
        if isinstance(person_source, pd.DataFrame):
            df = person_source
        else:
            df = pd.DataFrame(person_source)
        alice_age = df.loc[df["name"] == "Alice", "age"]
        assert not alice_age.empty and alice_age.iloc[0] == 42, (
            "SET on entity variable did not update entity_mapping after commit"
        )
        # Relationship mapping must NOT be modified
        assert "Person" not in social_star.context.relationship_mapping.mapping

    def test_commit_does_not_add_relationship_type_to_entity_mapping(
        self,
        social_star: Star,
    ) -> None:
        """commit_query() must never create EntityTable entries for relationship types."""
        initial_entity_keys = set(
            social_star.context.entity_mapping.mapping.keys(),
        )

        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 2.5",
        )

        final_entity_keys = set(
            social_star.context.entity_mapping.mapping.keys(),
        )
        assert final_entity_keys == initial_entity_keys, (
            f"entity_mapping grew from {initial_entity_keys} to {final_entity_keys}; "
            "commit_query must not register relationship types as entities"
        )


# ---------------------------------------------------------------------------
# Correctness regression tests (must still pass after the fix)
# ---------------------------------------------------------------------------


class TestRelPropertySetCorrectness:
    """Observable correctness of SET on relationship properties."""

    def test_set_rel_property_value_visible_in_same_star(
        self,
        social_star: Star,
    ) -> None:
        """Updated relationship property is readable in a follow-up query."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 1.0",
        )
        result = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN r.weight",
        )
        assert set(result["weight"].tolist()) == {1.0}

    def test_partial_set_rel_property_preserves_other_rows(
        self,
        social_star: Star,
    ) -> None:
        """SET filtered by WHERE only updates the matched edge."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) "
            "WHERE p.name = 'Alice' SET r.weight = 0.0",
        )
        result = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) "
            "RETURN p.name, r.weight ORDER BY p.name",
        )
        weights = dict(zip(result["name"], result["weight"]))
        assert weights["Alice"] == 0.0
        assert weights["Bob"] == 0.5  # unchanged

    def test_two_sequential_sets_on_same_relationship(
        self,
        social_star: Star,
    ) -> None:
        """Chaining two SET queries on the same relationship gives the second value."""
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 1.0",
        )
        social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) SET r.weight = 2.0",
        )
        result = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN r.weight",
        )
        assert set(result["weight"].tolist()) == {2.0}
