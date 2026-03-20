"""Tests for PropertyLookup on non-Variable expression targets.

PropertyLookup currently only handles ``Variable`` targets.  This module tests
the general case where the sub-expression evaluates to a dict/map value — for
example when the target is a MapLiteral, MapProjection, or any other expression
that produces dicts.

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


@pytest.fixture()
def person_context() -> Context:
    """Two people: Alice (age=30) and Bob (age=25)."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
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
            ID_COLUMN: [101],
            "__SOURCE__": [1],
            "__TARGET__": [2],
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
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


class TestPropertyLookupOnMapLiteral:
    """PropertyLookup on inline MapLiteral expressions."""

    def test_map_literal_property_returns_value(
        self, person_context: Context
    ) -> None:
        """Accessing a key on a MapLiteral returns the corresponding value."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN {key: 'hello', num: 42}.key AS label"
        )
        assert len(result) == 1
        assert result["label"].iloc[0] == "hello"

    def test_map_literal_numeric_property(
        self, person_context: Context
    ) -> None:
        """Accessing a numeric key from MapLiteral works correctly."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN {key: 'hello', num: 42}.num AS value"
        )
        assert result["value"].iloc[0] == 42

    def test_map_literal_missing_key_returns_null(
        self, person_context: Context
    ) -> None:
        """Accessing a missing key on a MapLiteral returns None (null)."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN {key: 'hello'}.missing AS val"
        )
        assert result["val"].iloc[0] is None

    def test_map_literal_with_dynamic_values(
        self, person_context: Context
    ) -> None:
        """MapLiteral keys with runtime expression values support property lookup."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN {name: p.name, age: p.age}.name AS extracted"
        )
        assert result["extracted"].iloc[0] == "Alice"

    def test_map_literal_does_not_raise_not_implemented(
        self, person_context: Context
    ) -> None:
        """Regression: MapLiteral property lookup must not raise NotImplementedError."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN {key: 'v'}.key AS k"
        )
        assert result is not None


class TestPropertyLookupOnMapProjection:
    """PropertyLookup on map-projection expressions (n {.prop1, .prop2})."""

    def test_map_projection_property_access(
        self, person_context: Context
    ) -> None:
        """Accessing a property on a MapProjection result returns the right value."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p {.name, .age}.name AS extracted"
        )
        assert result["extracted"].iloc[0] == "Alice"

    def test_map_projection_multiple_rows(
        self, person_context: Context
    ) -> None:
        """MapProjection property lookup works across multiple rows."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p {.name, .age}.age AS age ORDER BY age"
        )
        assert len(result) == 2
        ages = result["age"].tolist()
        assert ages == [25, 30]

    def test_map_projection_missing_key_returns_null(
        self, person_context: Context
    ) -> None:
        """Accessing a key not in the projection returns None."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p {.name}.missing AS val"
        )
        assert result["val"].iloc[0] is None

    def test_map_projection_does_not_raise_not_implemented(
        self, person_context: Context
    ) -> None:
        """Regression: MapProjection property lookup must not raise NotImplementedError."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p {.name}.name AS n"
        )
        assert result is not None
