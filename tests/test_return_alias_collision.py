"""TDD tests for RETURN/WITH alias collision disambiguation.

When two expressions share the same inferred column name (e.g. ``p.name``
and ``f.name`` both infer to ``name``), pycypher must not silently drop one
column.  Instead it should automatically qualify the ambiguous aliases to
their full ``var.prop`` form.

These tests are written before the fix (TDD step 1) and must fail until
the disambiguation logic is added to ``_return_from_frame``.
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
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def friends_star() -> Star:
    """Two Person nodes connected by a KNOWS relationship."""
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    rels = pd.DataFrame(
        {"__ID__": [10, 11], "__SOURCE__": [1, 2], "__TARGET__": [2, 3]},
    )
    pt = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=persons,
    )
    rt = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=["__ID__", "__SOURCE__", "__TARGET__"],
        source_obj=rels,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": pt}),
            relationship_mapping=RelationshipMapping(mapping={"KNOWS": rt}),
        ),
    )


# ---------------------------------------------------------------------------
# Collision disambiguation tests
# ---------------------------------------------------------------------------


class TestReturnAliasCollision:
    """When two PropertyLookup items share a name, both must appear in result."""

    def test_both_columns_present(self, friends_star: Star) -> None:
        """RETURN p.name, f.name must produce two columns, not one."""
        r = friends_star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name",
        )
        assert len(r.columns) == 2, (
            f"Expected 2 columns, got {len(r.columns)}: {r.columns.tolist()}"
        )

    def test_qualified_column_names_for_collision(
        self,
        friends_star: Star,
    ) -> None:
        """Ambiguous aliases must be qualified to 'p.name' and 'f.name'."""
        r = friends_star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name",
        )
        assert "p.name" in r.columns, f"Missing 'p.name' in {r.columns.tolist()}"
        assert "f.name" in r.columns, f"Missing 'f.name' in {r.columns.tolist()}"

    def test_correct_values_in_both_columns(self, friends_star: Star) -> None:
        """Values in both columns must be correct after disambiguation."""
        r = friends_star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) "
            "RETURN p.name, f.name ORDER BY p.name",
        )
        assert list(r["p.name"]) == ["Alice", "Bob"], r.to_dict()
        assert list(r["f.name"]) == ["Bob", "Carol"], r.to_dict()

    def test_triple_collision_all_qualified(self, friends_star: Star) -> None:
        """Three variables with same property → all three get qualified aliases."""
        # p-[:KNOWS]->f (same person table for q via OPTIONAL MATCH)
        r = friends_star.execute_query("MATCH (p:Person) RETURN p.name, p.age")
        # p.name and p.age are different — no collision → stay unqualified
        assert "name" in r.columns
        assert "age" in r.columns

    def test_no_collision_keeps_short_alias(self, friends_star: Star) -> None:
        """When there is no collision, short aliases remain unchanged."""
        r = friends_star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.age",
        )
        # name appears once, age appears once → no collision
        assert "name" in r.columns
        assert "age" in r.columns

    def test_explicit_alias_unaffected(self, friends_star: Star) -> None:
        """Explicit AS alias always wins — disambiguation must not override it."""
        r = friends_star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) "
            "RETURN p.name AS pname, f.name AS fname",
        )
        assert "pname" in r.columns
        assert "fname" in r.columns
        assert "p.name" not in r.columns
        assert "f.name" not in r.columns

    def test_optional_match_both_columns_present(
        self,
        friends_star: Star,
    ) -> None:
        """OPTIONAL MATCH with same-named property must not drop outer column."""
        r = friends_star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "RETURN p.name, f.name",
        )
        assert len(r.columns) == 2, (
            f"Expected 2 columns, got {len(r.columns)}: {r.columns.tolist()}"
        )
        assert "p.name" in r.columns
        assert "f.name" in r.columns

    def test_optional_match_correct_nulls(self, friends_star: Star) -> None:
        """OPTIONAL MATCH must preserve all outer rows with nulls for non-matches."""
        r = friends_star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "RETURN p.name, f.name ORDER BY p.name",
        )
        # All 3 persons; only Alice→Bob and Bob→Carol exist
        assert list(r["p.name"]) == ["Alice", "Bob", "Carol"]
        assert r["f.name"].iloc[0] == "Bob"
        assert r["f.name"].iloc[1] == "Carol"
        assert pd.isna(r["f.name"].iloc[2])  # Carol has no outgoing KNOWS
