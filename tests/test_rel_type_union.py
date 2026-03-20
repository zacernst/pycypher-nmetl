"""Tests for relationship type union in MATCH patterns.

``MATCH (p)-[:A|:B]->(q)`` (colon-pipe) and ``MATCH (p)-[:A|B]->(q)``
(pipe-only, Neo4j 5.x style) should both match relationships of type A
**or** type B.  Previously, `star.py` silently used only the first type in
the union, and the no-colon ``|TYPE`` form caused a parse error.

TDD: all tests written before the implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture — two types of relationships
# ---------------------------------------------------------------------------


@pytest.fixture()
def multi_rel_ctx() -> Context:
    """Person nodes with two relationship types: KNOWS and LIKES."""
    pdf = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}
    )
    ptable = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=pdf,
    )
    # Alice -[KNOWS]-> Bob
    kdf = pd.DataFrame(
        {
            ID_COLUMN: [10],
            RELATIONSHIP_SOURCE_COLUMN: [1],
            RELATIONSHIP_TARGET_COLUMN: [2],
        }
    )
    ktable = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=kdf,
    )
    # Alice -[LIKES]-> Carol
    ldf = pd.DataFrame(
        {
            ID_COLUMN: [20],
            RELATIONSHIP_SOURCE_COLUMN: [1],
            RELATIONSHIP_TARGET_COLUMN: [3],
        }
    )
    ltable = RelationshipTable(
        relationship_type="LIKES",
        identifier="LIKES",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=ldf,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": ptable}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": ktable, "LIKES": ltable}
        ),
    )


# ---------------------------------------------------------------------------
# Colon-pipe syntax  [:A|:B]
# ---------------------------------------------------------------------------


class TestColonPipeUnion:
    """[:KNOWS|:LIKES] matches both relationship types."""

    def test_colon_pipe_union_returns_both_targets(
        self, multi_rel_ctx: Context
    ) -> None:
        """[:KNOWS|:LIKES] returns Bob and Carol as targets of Alice."""
        star = Star(context=multi_rel_ctx)
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS|:LIKES]->(q:Person) "
            "RETURN q.name AS name ORDER BY name"
        )
        assert sorted(result["name"].tolist()) == ["Bob", "Carol"]

    def test_colon_pipe_union_row_count(self, multi_rel_ctx: Context) -> None:
        """Two matched rows for the two relationship types."""
        star = Star(context=multi_rel_ctx)
        result = star.execute_query(
            "MATCH (p:Person)-[:KNOWS|:LIKES]->(q:Person) "
            "RETURN p.name AS p, q.name AS q"
        )
        assert len(result) == 2

    def test_single_type_still_works(self, multi_rel_ctx: Context) -> None:
        """[:KNOWS] alone still only matches KNOWS edges."""
        star = Star(context=multi_rel_ctx)
        result = star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN q.name AS name"
        )
        assert result["name"].tolist() == ["Bob"]


# ---------------------------------------------------------------------------
# No-colon pipe syntax  [:A|B]
# ---------------------------------------------------------------------------


class TestNocolonPipeUnion:
    """[:KNOWS|LIKES] (no second colon) parses and matches both types."""

    def test_no_colon_union_parses_without_error(
        self, multi_rel_ctx: Context
    ) -> None:
        """[:KNOWS|LIKES] must not raise a parse error."""
        star = Star(context=multi_rel_ctx)
        # Should not raise
        star.execute_query(
            "MATCH (p:Person)-[:KNOWS|LIKES]->(q:Person) RETURN q.name AS name"
        )

    def test_no_colon_union_returns_both_targets(
        self, multi_rel_ctx: Context
    ) -> None:
        """[:KNOWS|LIKES] returns Bob and Carol (both relationship types)."""
        star = Star(context=multi_rel_ctx)
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS|LIKES]->(q:Person) "
            "RETURN q.name AS name ORDER BY name"
        )
        assert sorted(result["name"].tolist()) == ["Bob", "Carol"]
