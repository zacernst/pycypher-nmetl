"""Tests for map property access via [] subscript operator.

``map['key']`` should return the value associated with that key, just like
``map.key`` property access.  Previously, the evaluator called ``int(i)``
unconditionally for the index, causing a ``ValueError`` when the key was
a string.

TDD: all tests written before the implementation fix.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture()
def item_ctx() -> Context:
    """Single row for map/list access tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1],
            "name": ["Alice"],
            "age": [30],
        }
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


class TestMapKeyAccess:
    """Map subscript access with string key: map['key'] → value."""

    def test_map_string_key_access_returns_value(
        self, item_ctx: Context
    ) -> None:
        """{'name': 'Alice'}['name'] returns 'Alice'."""
        star = Star(context=item_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN {name: i.name}['name'] AS v"
        )
        assert result["v"].iloc[0] == "Alice"

    def test_map_string_key_access_integer_value(
        self, item_ctx: Context
    ) -> None:
        """{'age': 30}['age'] returns 30."""
        star = Star(context=item_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN {age: i.age}['age'] AS v"
        )
        assert result["v"].iloc[0] == 30

    def test_map_missing_key_returns_null(self, item_ctx: Context) -> None:
        """{'name': 'Alice'}['missing'] returns null (not an error)."""
        star = Star(context=item_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN {name: i.name}['missing'] AS v"
        )
        val = result["v"].iloc[0]
        assert val is None or (isinstance(val, float) and pd.isna(val))

    def test_map_key_access_does_not_raise(self, item_ctx: Context) -> None:
        """Regression: map['key'] must not raise ValueError."""
        star = Star(context=item_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN {x: 1}['x'] AS v")
        assert result is not None


class TestListIntIndexAccess:
    """List integer subscript access still works after the fix."""

    def test_list_int_index_zero(self, item_ctx: Context) -> None:
        """[10, 20, 30][0] returns 10."""
        star = Star(context=item_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN [10, 20, 30][0] AS v"
        )
        assert result["v"].iloc[0] == 10

    def test_list_int_index_last(self, item_ctx: Context) -> None:
        """[10, 20, 30][2] returns 30."""
        star = Star(context=item_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN [10, 20, 30][2] AS v"
        )
        assert result["v"].iloc[0] == 30

    def test_list_negative_index(self, item_ctx: Context) -> None:
        """[10, 20, 30][-1] returns 30 (Python negative index)."""
        star = Star(context=item_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN [10, 20, 30][-1] AS v"
        )
        assert result["v"].iloc[0] == 30
