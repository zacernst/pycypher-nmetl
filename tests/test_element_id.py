"""TDD tests for elementId() scalar function.

Neo4j 5.x replaced id() with elementId() for Neo4j Fabric compatibility.
elementId() should behave identically to id() — returning the internal
identifier for a node or relationship.

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
)
from pycypher.star import Star


@pytest.fixture
def star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [10, 20, 30],
            "name": ["Alice", "Bob", "Carol"],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


class TestElementId:
    def test_registered(self, star: Star) -> None:
        assert "elementid" in star.available_functions()

    def test_returns_id_for_node(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN elementId(p) AS r",
        )
        assert r["r"].iloc[0] == 10

    def test_matches_id_function(self, star: Star) -> None:
        """elementId(p) must return the same value as id(p)."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN id(p) AS a, elementId(p) AS b ORDER BY p.name",
        )
        assert list(r["a"]) == list(r["b"])

    def test_all_rows(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) RETURN elementId(p) AS r ORDER BY r",
        )
        assert list(r["r"]) == [10, 20, 30]

    def test_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN elementId(null) AS r")
        assert pd.isna(r["r"].iloc[0])
