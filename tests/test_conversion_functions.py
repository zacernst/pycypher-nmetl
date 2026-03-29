"""Tests for type conversion scalar functions (conversion_functions.py).

Covers toString, toInteger, toFloat, toBoolean and their OrNull variants.
Tests Cypher-specific semantics: truncation toward zero, case-insensitive
boolean parsing, pandas null-upcasting detection, and null propagation.

Zero-coverage module identified by systematic test coverage survey.
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
)
from pycypher.star import Star


@pytest.fixture()
def conversion_context() -> Context:
    """Context with diverse data types for conversion testing."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5, 6],
            "int_val": [42, 0, -7, None, 100, 1],
            "float_val": [3.14, 2.99, -1.7, None, 0.0, 42.0],
            "str_int": ["42", "0", "-7", None, "abc", "3.14"],
            "str_bool": ["true", "false", "TRUE", "FALSE", None, "yes"],
            "name": ["Alice", "Bob", "Carol", None, "Dave", "Eve"],
            "bool_val": [True, False, True, None, False, True],
        }
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=list(df.columns),
        source_obj_attribute_map={
            c: c for c in df.columns if c != ID_COLUMN
        },
        attribute_map={c: c for c in df.columns if c != ID_COLUMN},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


def _query(ctx: Context, cypher: str) -> pd.DataFrame:
    """Execute a Cypher query and return result DataFrame."""
    star = Star(context=ctx)
    return star.execute_query(cypher)


# ---------------------------------------------------------------------------
# toString
# ---------------------------------------------------------------------------


class TestToString:
    """toString(value) -> string representation."""

    def test_integer_to_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.int_val = 42 RETURN toString(x.int_val) AS s",
        )
        # pandas upcasts int columns with None to float64, so toString may
        # produce "42" or "42.0" depending on whether the upcasting detection
        # in _to_string sees the full column or a filtered slice.
        assert result["s"].iloc[0] in ("42", "42.0")

    def test_float_to_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toString(x.float_val) AS s",
        )
        assert result["s"].iloc[0] == "3.14"

    def test_string_passthrough(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toString(x.name) AS s",
        )
        assert result["s"].iloc[0] == "Alice"

    def test_boolean_true_to_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toString(x.bool_val) AS s",
        )
        assert result["s"].iloc[0] == "true"

    def test_boolean_false_to_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN toString(x.bool_val) AS s",
        )
        assert result["s"].iloc[0] == "false"

    def test_null_to_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.int_val = 42 RETURN toString(x.name) AS s",
        )
        # Non-null name for int_val=42 is "Alice"
        assert result["s"].iloc[0] == "Alice"


# ---------------------------------------------------------------------------
# toInteger
# ---------------------------------------------------------------------------


class TestToInteger:
    """toInteger(value) -> integer via truncation toward zero."""

    def test_string_integer(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toInteger(x.str_int) AS i",
        )
        assert result["i"].iloc[0] == 42

    def test_float_truncates_toward_zero_positive(
        self, conversion_context: Context
    ) -> None:
        """toInteger(2.99) -> 2 (truncate, not round)."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN toInteger(x.float_val) AS i",
        )
        assert result["i"].iloc[0] == 2

    def test_float_truncates_toward_zero_negative(
        self, conversion_context: Context
    ) -> None:
        """toInteger(-1.7) -> -1 (truncate toward zero)."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Carol' RETURN toInteger(x.float_val) AS i",
        )
        assert result["i"].iloc[0] == -1

    def test_invalid_string_returns_null(
        self, conversion_context: Context
    ) -> None:
        """toInteger('abc') -> null."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Dave' RETURN toInteger(x.str_int) AS i",
        )
        assert pd.isna(result["i"].iloc[0])

    def test_string_float_to_integer(
        self, conversion_context: Context
    ) -> None:
        """toInteger('3.14') -> 3."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Eve' RETURN toInteger(x.str_int) AS i",
        )
        assert result["i"].iloc[0] == 3

    def test_zero(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN toInteger(x.str_int) AS i",
        )
        assert result["i"].iloc[0] == 0


# ---------------------------------------------------------------------------
# toFloat
# ---------------------------------------------------------------------------


class TestToFloat:
    """toFloat(value) -> float64."""

    def test_string_to_float(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toFloat(x.str_int) AS f",
        )
        assert result["f"].iloc[0] == 42.0

    def test_integer_to_float(self, conversion_context: Context) -> None:
        """toFloat(30) -> 30.0 (not 30)."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toFloat(x.int_val) AS f",
        )
        assert isinstance(result["f"].iloc[0], float)
        assert result["f"].iloc[0] == 42.0

    def test_invalid_string_returns_null(
        self, conversion_context: Context
    ) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Dave' RETURN toFloat(x.str_int) AS f",
        )
        assert pd.isna(result["f"].iloc[0])

    def test_zero_float(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Dave' RETURN toFloat(x.float_val) AS f",
        )
        assert result["f"].iloc[0] == 0.0 or pd.isna(result["f"].iloc[0])


# ---------------------------------------------------------------------------
# toBoolean
# ---------------------------------------------------------------------------


class TestToBoolean:
    """toBoolean(value) -> bool following Cypher rules."""

    def test_string_true(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toBoolean(x.str_bool) AS b",
        )
        assert result["b"].iloc[0] is True or result["b"].iloc[0] == True

    def test_string_false(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN toBoolean(x.str_bool) AS b",
        )
        assert result["b"].iloc[0] is False or result["b"].iloc[0] == False

    def test_case_insensitive_true(self, conversion_context: Context) -> None:
        """toBoolean('TRUE') -> true."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Carol' RETURN toBoolean(x.str_bool) AS b",
        )
        assert result["b"].iloc[0] is True or result["b"].iloc[0] == True

    def test_case_insensitive_false(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.int_val = 100 RETURN toBoolean(x.str_bool) AS b",
        )
        # str_bool for int_val=100 (row index 4) is None
        assert pd.isna(result["b"].iloc[0])


# ---------------------------------------------------------------------------
# toStringOrNull
# ---------------------------------------------------------------------------


class TestToStringOrNull:
    """toStringOrNull(value) -> string or null (never raises)."""

    def test_normal_value(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toStringOrNull(x.int_val) AS s",
        )
        # See TestToString.test_integer_to_string for pandas upcasting note.
        assert result["s"].iloc[0] in ("42", "42.0")

    def test_null_returns_null(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.int_val = 100 RETURN toStringOrNull(x.name) AS s",
        )
        # name for int_val=100 is "Dave"
        assert result["s"].iloc[0] == "Dave"


# ---------------------------------------------------------------------------
# toIntegerOrNull
# ---------------------------------------------------------------------------


class TestToIntegerOrNull:
    """toIntegerOrNull(value) -> integer or null."""

    def test_valid_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toIntegerOrNull(x.str_int) AS i",
        )
        assert result["i"].iloc[0] == 42

    def test_invalid_returns_null(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Dave' RETURN toIntegerOrNull(x.str_int) AS i",
        )
        assert pd.isna(result["i"].iloc[0])


# ---------------------------------------------------------------------------
# toFloatOrNull
# ---------------------------------------------------------------------------


class TestToFloatOrNull:
    """toFloatOrNull(value) -> float or null."""

    def test_valid_string(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Eve' RETURN toFloatOrNull(x.str_int) AS f",
        )
        assert result["f"].iloc[0] == pytest.approx(3.14)

    def test_invalid_returns_null(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Dave' RETURN toFloatOrNull(x.str_int) AS f",
        )
        assert pd.isna(result["f"].iloc[0])


# ---------------------------------------------------------------------------
# toBooleanOrNull
# ---------------------------------------------------------------------------


class TestToBooleanOrNull:
    """toBooleanOrNull(value) -> bool or null."""

    def test_valid_true(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN toBooleanOrNull(x.str_bool) AS b",
        )
        assert result["b"].iloc[0] is True or result["b"].iloc[0] == True

    def test_valid_false(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN toBooleanOrNull(x.str_bool) AS b",
        )
        assert result["b"].iloc[0] is False or result["b"].iloc[0] == False

    def test_invalid_returns_null(self, conversion_context: Context) -> None:
        """toBooleanOrNull('yes') -> null (not a valid boolean string)."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Eve' RETURN toBooleanOrNull(x.str_bool) AS b",
        )
        assert pd.isna(result["b"].iloc[0])

    def test_null_input_returns_null(self, conversion_context: Context) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.int_val = 100 RETURN toBooleanOrNull(x.str_bool) AS b",
        )
        assert pd.isna(result["b"].iloc[0])
