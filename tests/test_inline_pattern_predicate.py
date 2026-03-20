"""TDD tests for inline pattern predicates in WHERE clauses.

Standard openCypher supports:
    MATCH (p:Person) WHERE (p)-[:KNOWS]->() RETURN p.name

as shorthand for:
    MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->() } RETURN p.name

The current grammar raises ``UnexpectedCharacters`` when it encounters the
relationship-pattern token after what it parsed as a parenthesised expression.

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


@pytest.fixture()
def social_star() -> Star:
    """Alice -KNOWS-> Bob -KNOWS-> Carol, Dave has no KNOWS edges."""
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Dave"]}
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    likes_df = pd.DataFrame(
        {ID_COLUMN: [20], "__SOURCE__": [4], "__TARGET__": [1]}
    )
    likes_table = RelationshipTable(
        relationship_type="LIKES",
        identifier="LIKES",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=likes_df,
    )
    knows_df = pd.DataFrame(
        {ID_COLUMN: [10, 11], "__SOURCE__": [1, 2], "__TARGET__": [2, 3]}
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table, "LIKES": likes_table}
            ),
        )
    )


class TestInlinePatternPredicate:
    """WHERE (n)-[:REL]->() shorthand for WHERE EXISTS { (n)-[:REL]->() }."""

    def test_does_not_raise(self, social_star: Star) -> None:
        """Grammar must accept inline pattern predicates without error."""
        social_star.execute_query(
            "MATCH (p:Person) WHERE (p)-[:KNOWS]->() RETURN p.name"
        )

    def test_basic_outgoing_pattern(self, social_star: Star) -> None:
        """Only nodes that have an outgoing KNOWS edge are returned."""
        r = social_star.execute_query(
            "MATCH (p:Person) WHERE (p)-[:KNOWS]->() RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Alice", "Bob"]

    def test_negated_pattern(self, social_star: Star) -> None:
        """NOT (p)-[:KNOWS]->() returns nodes with NO outgoing KNOWS edge."""
        r = social_star.execute_query(
            "MATCH (p:Person) WHERE NOT (p)-[:KNOWS]->() RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Carol", "Dave"]

    def test_same_result_as_exists(self, social_star: Star) -> None:
        """Inline predicate and EXISTS { } must return identical results."""
        r_inline = social_star.execute_query(
            "MATCH (p:Person) WHERE (p)-[:KNOWS]->() RETURN p.name ORDER BY p.name"
        )
        r_exists = social_star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->() } RETURN p.name ORDER BY p.name"
        )
        assert list(r_inline["name"]) == list(r_exists["name"])

    def test_incoming_pattern(self, social_star: Star) -> None:
        """Pattern with incoming arrow: ()-[:KNOWS]->(p)."""
        r = social_star.execute_query(
            "MATCH (p:Person) WHERE ()-[:KNOWS]->(p) RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Bob", "Carol"]

    def test_with_typed_neighbour(self, social_star: Star) -> None:
        """Inline predicate with label on neighbour node."""
        r = social_star.execute_query(
            "MATCH (p:Person) WHERE (p)-[:KNOWS]->(:Person) RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Alice", "Bob"]

    def test_combined_with_property_filter(self, social_star: Star) -> None:
        """Inline predicate AND scalar filter both applied."""
        r = social_star.execute_query(
            "MATCH (p:Person) WHERE (p)-[:KNOWS]->() AND p.name = 'Alice' RETURN p.name"
        )
        assert list(r["name"]) == ["Alice"]

    def test_combined_with_or(self, social_star: Star) -> None:
        """(p)-[:KNOWS]->() OR (p)-[:LIKES]->() returns union of both predicates."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p)-[:KNOWS]->() OR (p)-[:LIKES]->() "
            "RETURN p.name ORDER BY p.name"
        )
        # Alice has KNOWS, Dave has LIKES -> Alice, Bob, Dave
        assert list(r["name"]) == ["Alice", "Bob", "Dave"]

    def test_two_hop_inline_predicate(self, social_star: Star) -> None:
        """Two-hop inline predicate (p)-[:KNOWS]->()-[:KNOWS]->()."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p)-[:KNOWS]->()-[:KNOWS]->() "
            "RETURN p.name ORDER BY p.name"
        )
        # Alice -> Bob -> Carol (two hops), Bob -> Carol -> nobody (Bob is source only up to 2)
        assert list(r["name"]) == ["Alice"]
