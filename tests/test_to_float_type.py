"""TDD tests ensuring toFloat() always returns a float-dtype value.

Neo4j/openCypher: toFloat(30) → 30.0 (float).  The current implementation
uses pd.to_numeric which preserves integer dtype for integer inputs, causing
toFloat(integer_column) → int64 instead of float64.

All tests written before the fix (TDD step 1).
"""

from __future__ import annotations

import numpy as np
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


@pytest.fixture()
def int_star() -> Star:
    df = pd.DataFrame({ID_COLUMN: [1, 2], "score": [30, 75]})
    t = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=[ID_COLUMN, "score"],
        source_obj_attribute_map={"score": "score"},
        attribute_map={"score": "score"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Item": t}),
            relationship_mapping=RelationshipMapping(),
        )
    )


@pytest.fixture()
def empty_star() -> Star:
    return Star(
        context=Context(
            entity_mapping=EntityMapping(),
            relationship_mapping=RelationshipMapping(),
        )
    )


class TestToFloatAlwaysFloat:
    """toFloat() must return float dtype regardless of input type."""

    def test_integer_literal_returns_float(self, empty_star: Star) -> None:
        """toFloat(30) → 30.0 (float, not int)."""
        r = empty_star.execute_query("RETURN toFloat(30) AS f")
        val = r["f"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_integer_literal_value_correct(self, empty_star: Star) -> None:
        """toFloat(30) == 30.0."""
        r = empty_star.execute_query("RETURN toFloat(30) AS f")
        assert float(r["f"].iloc[0]) == 30.0

    def test_integer_column_returns_float(self, int_star: Star) -> None:
        """toFloat(integer_property_column) → float dtype."""
        r = int_star.execute_query(
            "MATCH (i:Item) RETURN toFloat(i.score) AS f ORDER BY i.score"
        )
        val = r["f"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_integer_column_values_correct(self, int_star: Star) -> None:
        """toFloat on integer column preserves values."""
        r = int_star.execute_query(
            "MATCH (i:Item) RETURN toFloat(i.score) AS f ORDER BY f"
        )
        assert float(r["f"].iloc[0]) == 30.0
        assert float(r["f"].iloc[1]) == 75.0

    def test_float_string_still_works(self, empty_star: Star) -> None:
        """toFloat('3.14') → 3.14 (regression)."""
        r = empty_star.execute_query("RETURN toFloat('3.14') AS f")
        assert abs(float(r["f"].iloc[0]) - 3.14) < 1e-6

    def test_invalid_string_returns_null(self, empty_star: Star) -> None:
        """toFloat('abc') → null (regression)."""
        r = empty_star.execute_query("RETURN toFloat('abc') AS f")
        assert r["f"].iloc[0] is None or pd.isna(r["f"].iloc[0])

    def test_null_input_returns_null(self, empty_star: Star) -> None:
        """toFloat(null) → null (regression)."""
        r = empty_star.execute_query("RETURN toFloat(null) AS f")
        assert r["f"].iloc[0] is None or pd.isna(r["f"].iloc[0])
