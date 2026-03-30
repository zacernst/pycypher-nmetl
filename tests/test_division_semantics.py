"""Consolidated tests for openCypher division semantics.

Covers:
- Integer division: truncation toward zero (not Python's floor division)
- Division by zero: integer/0 -> null, float/0.0 -> +/-infinity
- Float division: preserved as float
- Type preservation: int/int -> int, float/any -> float

Consolidated from: test_integer_division.py, test_integer_division_by_zero.py
"""

from __future__ import annotations

import math

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
def int_star() -> Star:
    """Entities with integer and float properties."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [25, 30, 7],  # integer column
            "score": [7.5, 2.0, 3.0],  # float column
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "score"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
        },
        attribute_map={"name": "name", "age": "age", "score": "score"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


@pytest.fixture
def div_zero_star() -> Star:
    """Entities for division-by-zero tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "val": [10, 0, -5],
        },
    )
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "val"],
        source_obj_attribute_map={"val": "val"},
        attribute_map={"val": "val"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# ===========================================================================
# Integer division types
# ===========================================================================


class TestIntegerDivisionTypes:
    """Integer-divided-by-integer must produce an integer result."""

    def test_int_div_int_returns_integer_type(self, int_star: Star) -> None:
        r = int_star.execute_query(
            "MATCH (p:Person {name: 'Bob'}) RETURN p.age / 2 AS d",
        )
        val = r["d"].iloc[0]
        assert isinstance(val, (int, __import__("numpy").integer)), (
            f"Expected int, got {type(val).__name__}: {val}"
        )

    def test_int_div_int_truncates(self, int_star: Star) -> None:
        """25 / 2 = 12 (truncate toward zero, not 12.5)."""
        r = int_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age / 2 AS d",
        )
        assert r["d"].iloc[0] == 12

    def test_int_div_int_exact(self, int_star: Star) -> None:
        r = int_star.execute_query(
            "MATCH (p:Person {name: 'Bob'}) RETURN p.age / 2 AS d",
        )
        assert r["d"].iloc[0] == 15

    def test_int_div_int_negative_truncates_toward_zero(
        self,
        int_star: Star,
    ) -> None:
        """-7 / 2 = -3 (truncation toward zero, not -4 from floor)."""
        r = int_star.execute_query(
            "MATCH (p:Person {name: 'Carol'}) RETURN -(p.age) / 2 AS d",
        )
        assert r["d"].iloc[0] == -3

    def test_int_literal_div_int_literal(self, int_star: Star) -> None:
        r = int_star.execute_query("RETURN 7 / 2 AS d")
        assert r["d"].iloc[0] == 3

    def test_int_literal_div_int_literal_type(self, int_star: Star) -> None:
        r = int_star.execute_query("RETURN 7 / 2 AS d")
        val = r["d"].iloc[0]
        assert isinstance(val, (int, __import__("numpy").integer)), (
            f"Expected int, got {type(val).__name__}"
        )


# ===========================================================================
# Float division (preserved)
# ===========================================================================


class TestFloatDivisionUnchanged:
    """Float division must remain float."""

    def test_float_div_int_returns_float(self, int_star: Star) -> None:
        r = int_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.score / 2 AS d",
        )
        val = r["d"].iloc[0]
        assert isinstance(val, float)
        assert val == pytest.approx(3.75)

    def test_int_div_float_literal_returns_float(self, int_star: Star) -> None:
        r = int_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age / 2.0 AS d",
        )
        val = r["d"].iloc[0]
        assert isinstance(val, float)
        assert val == pytest.approx(12.5)

    def test_float_literal_div_float_literal(self, int_star: Star) -> None:
        r = int_star.execute_query("RETURN 7.0 / 2.0 AS d")
        val = r["d"].iloc[0]
        assert isinstance(val, float)
        assert val == pytest.approx(3.5)

    def test_modulo_not_affected(self, int_star: Star) -> None:
        r = int_star.execute_query("RETURN 7 % 3 AS m")
        assert r["m"].iloc[0] == 1


# ===========================================================================
# Division by zero
# ===========================================================================


class TestIntegerDivisionByZero:
    """Integer division by zero returns null; float by zero returns infinity."""

    def test_one_div_zero_is_null(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN 1 / 0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_five_div_zero_is_null(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN 5 / 0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_neg_five_div_zero_is_null(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN -5 / 0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_zero_div_zero_is_null(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN 0 / 0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_normal_integer_division_unchanged(
        self,
        div_zero_star: Star,
    ) -> None:
        r = div_zero_star.execute_query("RETURN 10 / 2 AS r")
        assert r["r"].iloc[0] == 5

    def test_truncating_integer_division(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN 7 / 2 AS r")
        assert r["r"].iloc[0] == 3

    def test_negative_truncating_division(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN -7 / 2 AS r")
        assert r["r"].iloc[0] == -3

    def test_div_zero_excluded_in_where(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query(
            "MATCH (n:N) WHERE n.val / 0 > 1 RETURN n.val",
        )
        assert len(r) == 0

    def test_column_div_zero_all_null(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query(
            "MATCH (n:N) RETURN n.val / 0 AS r",
        )
        assert r["r"].isna().all()

    def test_float_div_zero_is_infinity(self, div_zero_star: Star) -> None:
        r = div_zero_star.execute_query("RETURN 1.0 / 0.0 AS r")
        val = r["r"].iloc[0]
        assert math.isinf(val) and val > 0

    def test_neg_float_div_zero_is_neg_infinity(
        self,
        div_zero_star: Star,
    ) -> None:
        r = div_zero_star.execute_query("RETURN -1.0 / 0.0 AS r")
        val = r["r"].iloc[0]
        assert math.isinf(val) and val < 0
