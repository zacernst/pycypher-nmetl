"""Tests that exercise both EXISTS code paths simultaneously.

These tests document the expected behavior after the _eval_exists refactor
(Loop 125) that extracts the per-row subquery execution loop into a shared
private helper.  Both code paths (Query-form EXISTS and multi-hop Pattern
fallback) must agree in their results.

Some tests here combine multiple EXISTS predicates in a single WHERE clause,
which guarantees both the single-hop pattern-comprehension path AND the
multi-hop / query execution path are exercised in the same query evaluation.
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
def chain_star() -> Star:
    """Alice -KNOWS-> Bob -KNOWS-> Carol -KNOWS-> Dave."""
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
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 4],
        }
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
                mapping={"KNOWS": knows_table}
            ),
        )
    )


class TestExistsComplexPaths:
    """Combined EXISTS predicates that exercise multiple evaluation paths."""

    def test_inline_and_query_exists_combined(self, chain_star: Star) -> None:
        """AND of inline pattern predicate and full EXISTS {} gives correct result."""
        r = chain_star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p)-[:KNOWS]->() "
            "AND EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Bob' } "
            "RETURN p.name ORDER BY p.name"
        )
        # Alice knows Bob, Bob knows Carol, Carol knows Dave; only Alice-KNOWS-Bob match
        assert list(r["name"]) == ["Alice"]

    def test_two_hop_inline_and_single_hop_query_exists(
        self, chain_star: Star
    ) -> None:
        """Two-hop inline predicate AND single-hop Query EXISTS."""
        r = chain_star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p)-[:KNOWS]->()-[:KNOWS]->() "
            "AND EXISTS { (p)-[:KNOWS]->() } "
            "RETURN p.name ORDER BY p.name"
        )
        # Alice: 2-hop (Alice->Bob->Carol) and 1-hop (Alice->Bob) — both true
        # Bob: 2-hop (Bob->Carol->Dave) and 1-hop (Bob->Carol) — both true
        assert list(r["name"]) == ["Alice", "Bob"]

    def test_not_two_hop_inline(self, chain_star: Star) -> None:
        """NOT on a two-hop inline predicate."""
        r = chain_star.execute_query(
            "MATCH (p:Person) "
            "WHERE NOT (p)-[:KNOWS]->()-[:KNOWS]->() "
            "RETURN p.name ORDER BY p.name"
        )
        # Carol: 1 hop (Carol->Dave), no 2-hop
        # Dave: no outgoing edges
        assert list(r["name"]) == ["Carol", "Dave"]

    def test_inline_or_exists_subquery(self, chain_star: Star) -> None:
        """OR of inline pattern and full EXISTS {}."""
        r = chain_star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p)-[:KNOWS]->()-[:KNOWS]->() "
            "OR EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Dave' } "
            "RETURN p.name ORDER BY p.name"
        )
        # Alice: 2-hop True
        # Bob: 2-hop True
        # Carol: 2-hop False, BUT knows Dave → True
        assert list(r["name"]) == ["Alice", "Bob", "Carol"]

    def test_three_hop_inline_pattern(self, chain_star: Star) -> None:
        """Three-hop inline predicate using multi-hop EXISTS fallback."""
        r = chain_star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p)-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->() "
            "RETURN p.name"
        )
        # Alice->Bob->Carol->Dave: 3 hops — only Alice
        assert list(r["name"]) == ["Alice"]
