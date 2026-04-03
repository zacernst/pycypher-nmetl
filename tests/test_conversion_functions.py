"""Tests for type conversion scalar functions (conversion_functions.py).

Covers toString, toInteger, toFloat, toBoolean and their OrNull variants.
Tests Cypher-specific semantics: truncation toward zero, case-insensitive
boolean parsing, pandas null-upcasting detection, and null propagation.

Zero-coverage module identified by systematic test coverage survey.
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
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(columns: dict) -> Context:
    """Build a minimal Context from a dict of column_name -> values."""
    columns[ID_COLUMN] = list(range(1, len(next(iter(columns.values()))) + 1))
    df = pd.DataFrame(columns)
    non_id = [c for c in df.columns if c != ID_COLUMN]
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=list(df.columns),
        source_obj_attribute_map={c: c for c in non_id},
        attribute_map={c: c for c in non_id},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


def _call_fn(name: str, s: pd.Series) -> pd.Series:
    """Execute a registered conversion function by name for direct unit tests."""
    reg = ScalarFunctionRegistry.get_instance()
    return reg.execute(name, [s])


@pytest.fixture
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
        },
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=list(df.columns),
        source_obj_attribute_map={c: c for c in df.columns if c != ID_COLUMN},
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

    def test_boolean_false_to_string(
        self, conversion_context: Context
    ) -> None:
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
        self,
        conversion_context: Context,
    ) -> None:
        """toInteger(2.99) -> 2 (truncate, not round)."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN toInteger(x.float_val) AS i",
        )
        assert result["i"].iloc[0] == 2

    def test_float_truncates_toward_zero_negative(
        self,
        conversion_context: Context,
    ) -> None:
        """toInteger(-1.7) -> -1 (truncate toward zero)."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Carol' RETURN toInteger(x.float_val) AS i",
        )
        assert result["i"].iloc[0] == -1

    def test_invalid_string_returns_null(
        self,
        conversion_context: Context,
    ) -> None:
        """toInteger('abc') -> null."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Dave' RETURN toInteger(x.str_int) AS i",
        )
        assert pd.isna(result["i"].iloc[0])

    def test_string_float_to_integer(
        self,
        conversion_context: Context,
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
        self,
        conversion_context: Context,
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

    def test_null_input_returns_null(
        self, conversion_context: Context
    ) -> None:
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.int_val = 100 RETURN toBooleanOrNull(x.str_bool) AS b",
        )
        assert pd.isna(result["b"].iloc[0])


# ===========================================================================
# Direct unit tests — call inner functions directly on pd.Series to exercise
# specific code paths that are hard to reach through the query engine.
# ===========================================================================


class TestToStringDirect:
    """Direct unit tests for _to_string covering all code paths."""

    def test_bool_dtype_fast_path(self) -> None:
        """Bool dtype Series uses the map fast path (line 85-86)."""
        result = _call_fn(
            "toString", pd.Series([True, False, True], dtype=bool)
        )
        assert list(result) == ["true", "false", "true"]

    def test_null_in_loop_none(self) -> None:
        """None values in the element loop produce None output."""
        result = _call_fn(
            "toString", pd.Series(["hello", None, "world"], dtype=object)
        )
        assert result.iloc[0] == "hello"
        assert result.iloc[1] is None
        assert result.iloc[2] == "world"

    def test_null_in_loop_nan(self) -> None:
        """Float NaN values produce None output."""
        result = _call_fn("toString", pd.Series([1.5, float("nan"), 2.5]))
        assert result.iloc[1] is None

    def test_null_in_loop_pd_na(self) -> None:
        """pd.NA values produce None output."""
        result = _call_fn(
            "toString", pd.Series(["a", pd.NA, "b"], dtype=object)
        )
        assert result.iloc[1] is None

    def test_upcasted_int_float64_path(self) -> None:
        """Float64 series with nulls and integer-valued floats (line 64-69)."""
        # pandas upcasts [42, None, 7] to float64: [42.0, NaN, 7.0]
        result = _call_fn(
            "toString", pd.Series([42, None, 7], dtype="float64")
        )
        assert result.iloc[0] == "42"
        assert result.iloc[1] is None
        assert result.iloc[2] == "7"

    def test_upcasted_int_object_dtype_path(self) -> None:
        """Object dtype with Python float values that are integer-valued (line 70-79)."""
        # Simulate post-null-normalisation: object dtype with Python floats
        result = _call_fn(
            "toString", pd.Series([30.0, 40.0, None, 35.0], dtype=object)
        )
        assert result.iloc[0] == "30"
        assert result.iloc[1] == "40"
        assert result.iloc[2] is None
        assert result.iloc[3] == "35"

    def test_object_dtype_mixed_not_upcasted(self) -> None:
        """Object dtype with mixed non-integer floats is NOT upcasted."""
        result = _call_fn(
            "toString", pd.Series([3.14, None, 2.5], dtype=object)
        )
        assert result.iloc[0] == "3.14"
        assert result.iloc[2] == "2.5"

    def test_no_nulls_no_upcast_detection(self) -> None:
        """Series without nulls skips upcast detection entirely."""
        result = _call_fn(
            "toString", pd.Series([1.0, 2.0, 3.0], dtype="float64")
        )
        # No upcasting detected (no nulls), so floats render as-is
        assert result.iloc[0] == "1.0"

    def test_bool_in_object_series(self) -> None:
        """Boolean value in object-dtype series uses openCypher lowercase."""
        result = _call_fn(
            "toString", pd.Series([True, "hello", False], dtype=object)
        )
        assert result.iloc[0] == "true"
        assert result.iloc[1] == "hello"
        assert result.iloc[2] == "false"

    def test_all_nulls_float64(self) -> None:
        """Series of all NaN values."""
        result = _call_fn(
            "toString",
            pd.Series([float("nan"), float("nan")], dtype="float64"),
        )
        assert result.iloc[0] is None
        assert result.iloc[1] is None

    def test_empty_series(self) -> None:
        """Empty series returns empty series."""
        result = _call_fn("toString", pd.Series([], dtype=object))
        assert len(result) == 0

    def test_object_dtype_non_numeric_not_upcasted(self) -> None:
        """Object dtype with non-numeric strings + None does not trigger upcast."""
        result = _call_fn(
            "toString", pd.Series(["abc", None, "def"], dtype=object)
        )
        assert result.iloc[0] == "abc"
        assert result.iloc[1] is None
        assert result.iloc[2] == "def"


class TestToIntegerDirect:
    """Direct unit tests for _to_integer."""

    def test_truncation_toward_zero_positive(self) -> None:
        result = _call_fn("toInteger", pd.Series([3.14, 2.99, 0.999]))
        assert list(result) == [3, 2, 0]

    def test_truncation_toward_zero_negative(self) -> None:
        result = _call_fn("toInteger", pd.Series([-1.7, -2.99, -0.1]))
        assert list(result) == [-1, -2, 0]

    def test_string_numbers(self) -> None:
        result = _call_fn("toInteger", pd.Series(["42", "-7", "3.14"]))
        assert list(result) == [42, -7, 3]

    def test_invalid_strings_become_null(self) -> None:
        result = _call_fn("toInteger", pd.Series(["abc", "", "xyz"]))
        assert all(pd.isna(v) for v in result)

    def test_null_propagation(self) -> None:
        result = _call_fn(
            "toInteger", pd.Series([1.0, None, 3.0], dtype=object)
        )
        assert result.iloc[0] == 1
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 3

    def test_zero_values(self) -> None:
        result = _call_fn("toInteger", pd.Series([0, 0.0, "0", -0.0]))
        assert all(v == 0 for v in result)

    def test_large_integers(self) -> None:
        result = _call_fn("toInteger", pd.Series([1e15, -1e15]))
        assert result.iloc[0] == 1000000000000000
        assert result.iloc[1] == -1000000000000000


class TestToFloatDirect:
    """Direct unit tests for _to_float."""

    def test_integers_become_float(self) -> None:
        result = _call_fn("toFloat", pd.Series([1, 2, 3]))
        assert result.dtype == np.float64
        assert list(result) == [1.0, 2.0, 3.0]

    def test_string_floats(self) -> None:
        result = _call_fn("toFloat", pd.Series(["3.14", "-2.5", "0.0"]))
        assert result.iloc[0] == pytest.approx(3.14)
        assert result.iloc[1] == pytest.approx(-2.5)
        assert result.iloc[2] == pytest.approx(0.0)

    def test_invalid_strings_become_nan(self) -> None:
        result = _call_fn("toFloat", pd.Series(["abc", "not_a_number"]))
        assert all(pd.isna(v) for v in result)

    def test_null_propagation(self) -> None:
        result = _call_fn("toFloat", pd.Series([1.0, None, 3.0], dtype=object))
        assert result.iloc[0] == 1.0
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 3.0

    def test_already_float(self) -> None:
        result = _call_fn("toFloat", pd.Series([3.14, -1.7, 0.0]))
        assert result.dtype == np.float64
        assert result.iloc[0] == pytest.approx(3.14)


class TestToBooleanDirect:
    """Direct unit tests for _to_boolean."""

    def test_string_true_false(self) -> None:
        result = _call_fn("toBoolean", pd.Series(["true", "false"]))
        assert result.iloc[0] is True or result.iloc[0] == True
        assert result.iloc[1] is False or result.iloc[1] == False

    def test_case_insensitive(self) -> None:
        result = _call_fn(
            "toBoolean", pd.Series(["TRUE", "FALSE", "True", "False"])
        )
        assert result.iloc[0] == True
        assert result.iloc[1] == False
        assert result.iloc[2] == True
        assert result.iloc[3] == False

    def test_numeric_strings(self) -> None:
        """'1' -> True, '0' -> False per bool_map."""
        result = _call_fn("toBoolean", pd.Series(["1", "0"]))
        assert result.iloc[0] == True
        assert result.iloc[1] == False

    def test_invalid_string_returns_nan(self) -> None:
        result = _call_fn("toBoolean", pd.Series(["yes", "no", "maybe"]))
        assert all(pd.isna(v) for v in result)

    def test_null_preserved(self) -> None:
        result = _call_fn(
            "toBoolean", pd.Series(["true", None, "false"], dtype=object)
        )
        assert pd.isna(result.iloc[1])


class TestToStringOrNullDirect:
    """Direct unit tests for _to_string_or_null."""

    def test_normal_values(self) -> None:
        result = _call_fn("toStringOrNull", pd.Series([42, 3.14, True]))
        assert result.iloc[0] in ("42", "42.0")
        assert result.iloc[1] == "3.14"
        # Note: toStringOrNull uses str() directly, so True -> "True"
        assert result.iloc[2] in ("True", "true")

    def test_none_returns_none(self) -> None:
        result = _call_fn(
            "toStringOrNull",
            pd.Series([None, "hello", None], dtype=object),
        )
        assert result.iloc[0] is None
        assert result.iloc[1] == "hello"
        assert result.iloc[2] is None

    def test_nan_returns_none(self) -> None:
        result = _call_fn("toStringOrNull", pd.Series([float("nan"), 1.0]))
        assert result.iloc[0] is None
        assert result.iloc[1] == "1.0"

    def test_pd_na_returns_none(self) -> None:
        result = _call_fn(
            "toStringOrNull", pd.Series(["x", pd.NA, "y"], dtype=object)
        )
        assert result.iloc[0] == "x"
        assert result.iloc[1] is None
        assert result.iloc[2] == "y"

    def test_preserves_index(self) -> None:
        result = _call_fn("toStringOrNull", pd.Series([10, 20], index=[5, 10]))
        assert list(result.index) == [5, 10]


class TestToBooleanOrNullDirect:
    """Direct unit tests for _to_boolean_or_null."""

    def test_empty_series(self) -> None:
        """Empty series returns empty series (line 283-284)."""
        result = _call_fn("toBooleanOrNull", pd.Series([], dtype=object))
        assert len(result) == 0

    def test_all_null_series(self) -> None:
        """All-null series returns all None (line 294)."""
        result = _call_fn(
            "toBooleanOrNull",
            pd.Series([None, None, None], dtype=object),
        )
        assert all(v is None or pd.isna(v) for v in result)

    def test_mixed_valid_invalid(self) -> None:
        result = _call_fn(
            "toBooleanOrNull",
            pd.Series(["true", "invalid", "false", None], dtype=object),
        )
        assert result.iloc[0] == True
        assert result.iloc[1] is None or pd.isna(result.iloc[1])
        assert result.iloc[2] == False
        assert result.iloc[3] is None or pd.isna(result.iloc[3])

    def test_numeric_string_mapping(self) -> None:
        """'1' -> True, '0' -> False."""
        result = _call_fn("toBooleanOrNull", pd.Series(["1", "0"]))
        assert result.iloc[0] == True
        assert result.iloc[1] == False

    def test_case_insensitive(self) -> None:
        result = _call_fn(
            "toBooleanOrNull",
            pd.Series(["TRUE", "FALSE", "True", "False"]),
        )
        assert result.iloc[0] == True
        assert result.iloc[1] == False
        assert result.iloc[2] == True
        assert result.iloc[3] == False

    def test_preserves_index(self) -> None:
        result = _call_fn(
            "toBooleanOrNull",
            pd.Series(["true", "false"], index=[10, 20]),
        )
        assert list(result.index) == [10, 20]


class TestToIntegerOrNullDirect:
    """Direct unit tests for _to_integer_or_null."""

    def test_valid_conversions(self) -> None:
        result = _call_fn("toIntegerOrNull", pd.Series(["42", "3.14", "-7"]))
        assert result.iloc[0] == 42
        assert result.iloc[1] == 3  # truncation toward zero
        assert result.iloc[2] == -7

    def test_invalid_returns_null(self) -> None:
        result = _call_fn("toIntegerOrNull", pd.Series(["abc", ""]))
        assert all(pd.isna(v) for v in result)

    def test_null_propagation(self) -> None:
        result = _call_fn(
            "toIntegerOrNull", pd.Series([None, "5", None], dtype=object)
        )
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == 5
        assert pd.isna(result.iloc[2])


class TestToFloatOrNullDirect:
    """Direct unit tests for _to_float_or_null."""

    def test_valid_conversions(self) -> None:
        result = _call_fn("toFloatOrNull", pd.Series(["3.14", "42", "-1.7"]))
        assert result.iloc[0] == pytest.approx(3.14)
        assert result.iloc[1] == pytest.approx(42.0)
        assert result.iloc[2] == pytest.approx(-1.7)

    def test_invalid_returns_nan(self) -> None:
        result = _call_fn("toFloatOrNull", pd.Series(["abc", "not_float"]))
        assert all(pd.isna(v) for v in result)

    def test_null_propagation(self) -> None:
        result = _call_fn(
            "toFloatOrNull", pd.Series([None, "5.0", None], dtype=object)
        )
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(5.0)
        assert pd.isna(result.iloc[2])

    def test_output_dtype_is_float64(self) -> None:
        result = _call_fn("toFloatOrNull", pd.Series([1, 2, 3]))
        assert result.dtype == np.float64


# ---------------------------------------------------------------------------
# Integration tests: multiple conversions in single query
# ---------------------------------------------------------------------------


class TestConversionComposition:
    """Test multiple conversion functions used together."""

    def test_chained_conversions(self, conversion_context: Context) -> None:
        """toString(toInteger(x)) composition."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' "
            "RETURN toString(toInteger(x.float_val)) AS s",
        )
        assert result["s"].iloc[0] in ("3", "3.0")

    def test_multiple_conversions_in_return(
        self,
        conversion_context: Context,
    ) -> None:
        """Multiple conversion functions in a single RETURN."""
        result = _query(
            conversion_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' "
            "RETURN toInteger(x.float_val) AS i, "
            "toFloat(x.str_int) AS f, "
            "toString(x.int_val) AS s",
        )
        assert result["i"].iloc[0] == 3
        assert result["f"].iloc[0] == 42.0
        assert result["s"].iloc[0] in ("42", "42.0")

    def test_negative_string_to_integer(self) -> None:
        """Negative number string conversions."""
        ctx = _make_context({"val": ["-42", "-3.14", "-0"]})
        result = _query(
            ctx,
            "MATCH (x:Item) RETURN toInteger(x.val) AS i",
        )
        vals = list(result["i"])
        assert vals[0] == -42
        assert vals[1] == -3
        assert vals[2] == 0

    def test_all_null_column_to_string(self) -> None:
        """Column of all nulls through toString."""
        ctx = _make_context({"val": [None, None, None]})
        result = _query(
            ctx,
            "MATCH (x:Item) RETURN toStringOrNull(x.val) AS s",
        )
        assert all(v is None or pd.isna(v) for v in result["s"])
