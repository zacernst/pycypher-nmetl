"""Tests for nullIf, isNaN, and randomUUID scalar utility functions.

Neo4j built-in functions not yet in the registry:

  * ``nullIf(v1, v2)``    — return null when v1 == v2, otherwise return v1.
  * ``isNaN(x)``          — boolean: True when x is IEEE 754 NaN.
  * ``randomUUID()``      — return a fresh RFC 4122 UUID string each call.

TDD: all tests written before implementation.
"""

from __future__ import annotations

import re

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def one_row_ctx() -> Context:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1],
            "score": [0.0],
            "label": ["default"],
        },
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=[ID_COLUMN, "score", "label"],
        source_obj_attribute_map={"score": "score", "label": "label"},
        attribute_map={"score": "score", "label": "label"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


# ---------------------------------------------------------------------------
# nullIf tests
# ---------------------------------------------------------------------------


class TestNullIf:
    """nullIf(v1, v2) returns null when v1 equals v2, else returns v1."""

    def test_nullif_equal_values_returns_null(
        self,
        one_row_ctx: Context,
    ) -> None:
        """nullIf(0, 0) returns null."""
        star = Star(context=one_row_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN nullIf(0, 0) AS v")
        val = result["v"].iloc[0]
        assert val is None or (isinstance(val, float) and pd.isna(val))

    def test_nullif_unequal_values_returns_first(
        self,
        one_row_ctx: Context,
    ) -> None:
        """nullIf(1, 0) returns 1."""
        star = Star(context=one_row_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN nullIf(1, 0) AS v")
        assert result["v"].iloc[0] == 1

    def test_nullif_strings_equal(self, one_row_ctx: Context) -> None:
        """nullIf('a', 'a') returns null."""
        star = Star(context=one_row_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN nullIf('hello', 'hello') AS v",
        )
        val = result["v"].iloc[0]
        assert val is None or (isinstance(val, float) and pd.isna(val))

    def test_nullif_strings_unequal(self, one_row_ctx: Context) -> None:
        """nullIf('a', 'b') returns 'a'."""
        star = Star(context=one_row_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN nullIf('hello', 'world') AS v",
        )
        assert result["v"].iloc[0] == "hello"

    def test_nullif_with_property(self, one_row_ctx: Context) -> None:
        """nullIf(i.label, 'default') nullifies the sentinel value."""
        star = Star(context=one_row_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN nullIf(i.label, 'default') AS v",
        )
        val = result["v"].iloc[0]
        assert val is None or (isinstance(val, float) and pd.isna(val))

    def test_nullif_does_not_raise(self, one_row_ctx: Context) -> None:
        """Regression: nullIf must not raise NotImplementedError."""
        star = Star(context=one_row_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN nullIf(1, 2) AS v")
        assert result is not None


# ---------------------------------------------------------------------------
# isNaN tests
# ---------------------------------------------------------------------------


class TestIsNaN:
    """isNaN(x) returns True for IEEE 754 NaN, False for finite numbers."""

    def test_isnan_literal_nan_returns_true(
        self,
        one_row_ctx: Context,
    ) -> None:
        """isNaN(0.0/0.0) returns True (NaN from division)."""
        star = Star(context=one_row_ctx)
        # 0.0/0.0 in Cypher is typically NaN; alternatively use toFloat('NaN')
        result = star.execute_query(
            "MATCH (i:Item) RETURN isNaN(toFloat('NaN')) AS v",
        )
        assert result["v"].iloc[0] is True or result["v"].iloc[0] == True

    def test_isnan_integer_returns_false(self, one_row_ctx: Context) -> None:
        """isNaN(42) returns False."""
        star = Star(context=one_row_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN isNaN(42) AS v")
        assert result["v"].iloc[0] is False or result["v"].iloc[0] == False

    def test_isnan_float_returns_false(self, one_row_ctx: Context) -> None:
        """isNaN(3.14) returns False."""
        star = Star(context=one_row_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN isNaN(3.14) AS v")
        assert not result["v"].iloc[0]

    def test_isnan_does_not_raise(self, one_row_ctx: Context) -> None:
        """Regression: isNaN must not raise NotImplementedError."""
        star = Star(context=one_row_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN isNaN(1) AS v")
        assert result is not None


# ---------------------------------------------------------------------------
# randomUUID tests
# ---------------------------------------------------------------------------


class TestRandomUUID:
    """randomUUID() returns a fresh UUID string for every call."""

    _UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )

    def test_randomuuid_looks_like_uuid(self, one_row_ctx: Context) -> None:
        """randomUUID() produces a string matching the UUID v4 pattern."""
        star = Star(context=one_row_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN randomUUID() AS uid",
        )
        val = str(result["uid"].iloc[0])
        assert self._UUID_RE.match(val), f"Not a UUID: {val!r}"

    def test_randomuuid_produces_different_values(
        self,
        one_row_ctx: Context,
    ) -> None:
        """Two calls to randomUUID() produce different values."""
        star = Star(context=one_row_ctx)
        r1 = star.execute_query("MATCH (i:Item) RETURN randomUUID() AS uid")
        r2 = star.execute_query("MATCH (i:Item) RETURN randomUUID() AS uid")
        assert r1["uid"].iloc[0] != r2["uid"].iloc[0]

    def test_randomuuid_does_not_raise(self, one_row_ctx: Context) -> None:
        """Regression: randomUUID() must not raise NotImplementedError."""
        star = Star(context=one_row_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN randomUUID() AS uid",
        )
        assert result is not None
