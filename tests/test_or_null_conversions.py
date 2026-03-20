"""TDD tests for *OrNull conversion functions.

Neo4j provides null-safe conversion variants that return null instead of
raising or returning NaN for invalid inputs:

    toBooleanOrNull(x)  -- True/False for valid booleans, null otherwise
    toIntegerOrNull(x)  -- integer for valid integers, null otherwise
    toFloatOrNull(x)    -- float for valid floats, null otherwise

These mirror the existing ``toStringOrNull()`` pattern already in the registry.

All tests written before implementation (TDD step 1).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture()
def empty_star() -> Star:
    return Star(context=Context(entity_mapping=EntityMapping(mapping={})))


# ---------------------------------------------------------------------------
# toBooleanOrNull
# ---------------------------------------------------------------------------


class TestToBooleanOrNull:
    """toBooleanOrNull returns True/False for valid inputs, null otherwise."""

    def test_true_string(self, empty_star: Star) -> None:
        """toBooleanOrNull('true') returns True."""
        r = empty_star.execute_query("RETURN toBooleanOrNull('true') AS b")
        assert r["b"].iloc[0] is True or bool(r["b"].iloc[0]) is True

    def test_false_string(self, empty_star: Star) -> None:
        """toBooleanOrNull('false') returns False."""
        r = empty_star.execute_query("RETURN toBooleanOrNull('false') AS b")
        assert r["b"].iloc[0] is False or bool(r["b"].iloc[0]) is False

    def test_true_capital(self, empty_star: Star) -> None:
        """toBooleanOrNull('True') returns True (case-insensitive)."""
        r = empty_star.execute_query("RETURN toBooleanOrNull('True') AS b")
        assert bool(r["b"].iloc[0]) is True

    def test_invalid_returns_null(self, empty_star: Star) -> None:
        """toBooleanOrNull('yes') returns null (not a valid Cypher boolean)."""
        r = empty_star.execute_query("RETURN toBooleanOrNull('yes') AS b")
        assert r["b"].iloc[0] is None or pd.isna(r["b"].iloc[0])

    def test_null_input_returns_null(self, empty_star: Star) -> None:
        """toBooleanOrNull(null) returns null."""
        r = empty_star.execute_query("RETURN toBooleanOrNull(null) AS b")
        assert r["b"].iloc[0] is None or pd.isna(r["b"].iloc[0])

    def test_integer_one_returns_true(self, empty_star: Star) -> None:
        """toBooleanOrNull(1) returns True."""
        r = empty_star.execute_query("RETURN toBooleanOrNull(1) AS b")
        assert bool(r["b"].iloc[0]) is True

    def test_integer_zero_returns_false(self, empty_star: Star) -> None:
        """toBooleanOrNull(0) returns False."""
        r = empty_star.execute_query("RETURN toBooleanOrNull(0) AS b")
        assert bool(r["b"].iloc[0]) is False

    def test_does_not_raise(self, empty_star: Star) -> None:
        """toBooleanOrNull must not raise for any valid input."""
        empty_star.execute_query("RETURN toBooleanOrNull('true') AS b")


# ---------------------------------------------------------------------------
# toIntegerOrNull
# ---------------------------------------------------------------------------


class TestToIntegerOrNull:
    """toIntegerOrNull returns an integer for valid inputs, null otherwise."""

    def test_valid_integer_string(self, empty_star: Star) -> None:
        """toIntegerOrNull('42') returns 42."""
        r = empty_star.execute_query("RETURN toIntegerOrNull('42') AS n")
        assert int(r["n"].iloc[0]) == 42

    def test_invalid_returns_null(self, empty_star: Star) -> None:
        """toIntegerOrNull('abc') returns null."""
        r = empty_star.execute_query("RETURN toIntegerOrNull('abc') AS n")
        assert r["n"].iloc[0] is None or pd.isna(r["n"].iloc[0])

    def test_null_input_returns_null(self, empty_star: Star) -> None:
        """toIntegerOrNull(null) returns null."""
        r = empty_star.execute_query("RETURN toIntegerOrNull(null) AS n")
        assert r["n"].iloc[0] is None or pd.isna(r["n"].iloc[0])

    def test_float_string_truncates(self, empty_star: Star) -> None:
        """toIntegerOrNull('3.9') returns 3 (truncates toward zero)."""
        r = empty_star.execute_query("RETURN toIntegerOrNull('3.9') AS n")
        # Valid float string → convert to int (truncate)
        assert int(r["n"].iloc[0]) == 3

    def test_negative_valid(self, empty_star: Star) -> None:
        """toIntegerOrNull('-7') returns -7."""
        r = empty_star.execute_query("RETURN toIntegerOrNull('-7') AS n")
        assert int(r["n"].iloc[0]) == -7

    def test_does_not_raise(self, empty_star: Star) -> None:
        """toIntegerOrNull must not raise for invalid inputs."""
        empty_star.execute_query("RETURN toIntegerOrNull('bad') AS n")


# ---------------------------------------------------------------------------
# toFloatOrNull
# ---------------------------------------------------------------------------


class TestToFloatOrNull:
    """toFloatOrNull returns a float for valid inputs, null otherwise."""

    def test_valid_float_string(self, empty_star: Star) -> None:
        """toFloatOrNull('3.14') returns 3.14."""
        r = empty_star.execute_query("RETURN toFloatOrNull('3.14') AS f")
        assert abs(float(r["f"].iloc[0]) - 3.14) < 1e-6

    def test_integer_string(self, empty_star: Star) -> None:
        """toFloatOrNull('42') returns 42.0."""
        r = empty_star.execute_query("RETURN toFloatOrNull('42') AS f")
        assert float(r["f"].iloc[0]) == 42.0

    def test_invalid_returns_null(self, empty_star: Star) -> None:
        """toFloatOrNull('abc') returns null."""
        r = empty_star.execute_query("RETURN toFloatOrNull('abc') AS f")
        assert r["f"].iloc[0] is None or pd.isna(r["f"].iloc[0])

    def test_null_input_returns_null(self, empty_star: Star) -> None:
        """toFloatOrNull(null) returns null."""
        r = empty_star.execute_query("RETURN toFloatOrNull(null) AS f")
        assert r["f"].iloc[0] is None or pd.isna(r["f"].iloc[0])

    def test_negative_float(self, empty_star: Star) -> None:
        """toFloatOrNull('-1.5') returns -1.5."""
        r = empty_star.execute_query("RETURN toFloatOrNull('-1.5') AS f")
        assert float(r["f"].iloc[0]) == -1.5

    def test_does_not_raise(self, empty_star: Star) -> None:
        """toFloatOrNull must not raise for invalid inputs."""
        empty_star.execute_query("RETURN toFloatOrNull('bad') AS f")
