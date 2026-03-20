"""TDD tests ensuring floor() and ceil() always return float64 dtype.

Neo4j/openCypher specification: floor() and ceil() always return a Float,
regardless of input type.  The current implementation uses math.floor / math.ceil
which return Python int, so pd.Series.apply() produces an int64 column.

    floor(3.7)  → 3.0   (float, not int)
    ceil(3.2)   → 4.0   (float, not int)

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
def empty_star() -> Star:
    return Star(
        context=Context(
            entity_mapping=EntityMapping(),
            relationship_mapping=RelationshipMapping(),
        )
    )


@pytest.fixture()
def num_star() -> Star:
    df = pd.DataFrame({ID_COLUMN: [1, 2], "val": [3.7, -2.1]})
    t = EntityTable(
        entity_type="Num",
        identifier="Num",
        column_names=[ID_COLUMN, "val"],
        source_obj_attribute_map={"val": "val"},
        attribute_map={"val": "val"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Num": t}),
            relationship_mapping=RelationshipMapping(),
        )
    )


class TestFloorDtype:
    """floor() must return float dtype regardless of input type."""

    def test_floor_float_input_returns_float_type(
        self, empty_star: Star
    ) -> None:
        """floor(3.7) → 3.0 (float, not int)."""
        r = empty_star.execute_query("RETURN floor(3.7) AS v")
        val = r["v"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_floor_float_input_value_correct(self, empty_star: Star) -> None:
        """floor(3.7) == 3.0."""
        r = empty_star.execute_query("RETURN floor(3.7) AS v")
        assert float(r["v"].iloc[0]) == 3.0

    def test_floor_negative_value_correct(self, empty_star: Star) -> None:
        """floor(-3.7) == -4.0."""
        r = empty_star.execute_query("RETURN floor(-3.7) AS v")
        assert float(r["v"].iloc[0]) == -4.0

    def test_floor_integer_input_returns_float_type(
        self, empty_star: Star
    ) -> None:
        """floor(3) → 3.0 (float)."""
        r = empty_star.execute_query("RETURN floor(3) AS v")
        val = r["v"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_floor_column_returns_float_type(self, num_star: Star) -> None:
        """floor(n.val) on a float column → float dtype."""
        r = num_star.execute_query(
            "MATCH (n:Num) RETURN floor(n.val) AS v ORDER BY n.val"
        )
        val = r["v"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_floor_column_values_correct(self, num_star: Star) -> None:
        """floor(3.7) == 3.0, floor(-2.1) == -3.0."""
        r = num_star.execute_query(
            "MATCH (n:Num) RETURN floor(n.val) AS v ORDER BY n.val"
        )
        assert float(r["v"].iloc[0]) == -3.0
        assert float(r["v"].iloc[1]) == 3.0


class TestCeilDtype:
    """ceil() must return float dtype regardless of input type."""

    def test_ceil_float_input_returns_float_type(
        self, empty_star: Star
    ) -> None:
        """ceil(3.2) → 4.0 (float, not int)."""
        r = empty_star.execute_query("RETURN ceil(3.2) AS v")
        val = r["v"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_ceil_float_input_value_correct(self, empty_star: Star) -> None:
        """ceil(3.2) == 4.0."""
        r = empty_star.execute_query("RETURN ceil(3.2) AS v")
        assert float(r["v"].iloc[0]) == 4.0

    def test_ceil_negative_value_correct(self, empty_star: Star) -> None:
        """ceil(-3.7) == -3.0."""
        r = empty_star.execute_query("RETURN ceil(-3.7) AS v")
        assert float(r["v"].iloc[0]) == -3.0

    def test_ceil_integer_input_returns_float_type(
        self, empty_star: Star
    ) -> None:
        """ceil(3) → 3.0 (float)."""
        r = empty_star.execute_query("RETURN ceil(3) AS v")
        val = r["v"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_ceil_column_returns_float_type(self, num_star: Star) -> None:
        """ceil(n.val) on a float column → float dtype."""
        r = num_star.execute_query(
            "MATCH (n:Num) RETURN ceil(n.val) AS v ORDER BY n.val"
        )
        val = r["v"].iloc[0]
        assert isinstance(val, (float, np.floating)), (
            f"Expected float, got {type(val).__name__}: {val!r}"
        )

    def test_ceil_column_values_correct(self, num_star: Star) -> None:
        """ceil(-2.1) == -2.0, ceil(3.7) == 4.0."""
        r = num_star.execute_query(
            "MATCH (n:Num) RETURN ceil(n.val) AS v ORDER BY n.val"
        )
        assert float(r["v"].iloc[0]) == -2.0
        assert float(r["v"].iloc[1]) == 4.0
