"""Null-safety regression tests for functions being refactored to use _is_null().

Each inline ``val is None or (isinstance(val, float) and math.isnan(val))``
pattern is replaced with the ``_is_null(val)`` helper.  These tests pin the
expected null-propagation behaviour so that the refactoring cannot silently
change semantics.

Functions covered:
  * toStringOrNull  (scalar_functions.py line ~532)
  * lpad            (scalar_functions.py line ~908)
  * rpad            (scalar_functions.py line ~945)
  * indexOf         (scalar_functions.py line ~1018)
  * date            (scalar_functions.py line ~1469)
  * datetime        (scalar_functions.py line ~1502)
  * duration        (scalar_functions.py line ~1571)

TDD: all tests written before the refactoring.
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

# ---------------------------------------------------------------------------
# Shared fixture — one row whose relevant column is null (None)
# ---------------------------------------------------------------------------


@pytest.fixture
def null_ctx() -> Context:
    """Single row with a null string/date column for null-propagation tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1],
            "name": [None],
            "score": [float("nan")],
            "dstr": [None],
        },
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=[ID_COLUMN, "name", "score", "dstr"],
        source_obj_attribute_map={
            "name": "name",
            "score": "score",
            "dstr": "dstr",
        },
        attribute_map={
            "name": "name",
            "score": "score",
            "dstr": "dstr",
        },
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


def _is_null_value(v: object) -> bool:
    """Helper: True if v is None or float NaN."""
    return v is None or (isinstance(v, float) and math.isnan(v))


# ---------------------------------------------------------------------------
# toStringOrNull — null in → null out
# ---------------------------------------------------------------------------


class TestToStringOrNullNullSafety:
    """toStringOrNull propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """toStringOrNull(null column) → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN toStringOrNull(i.name) AS v",
        )
        assert _is_null_value(result["v"].iloc[0])

    def test_nan_input_returns_null(self, null_ctx: Context) -> None:
        """toStringOrNull(NaN column) → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN toStringOrNull(i.score) AS v",
        )
        assert _is_null_value(result["v"].iloc[0])


# ---------------------------------------------------------------------------
# lpad — null in → null out
# ---------------------------------------------------------------------------


class TestLpadNullSafety:
    """lpad propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """lpad(null, 5, '*') → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN lpad(i.name, 5, '*') AS v",
        )
        assert _is_null_value(result["v"].iloc[0])

    def test_nan_score_returns_null(self, null_ctx: Context) -> None:
        """lpad(NaN column, 5, '*') → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN lpad(i.score, 5, '*') AS v",
        )
        assert _is_null_value(result["v"].iloc[0])


# ---------------------------------------------------------------------------
# rpad — null in → null out
# ---------------------------------------------------------------------------


class TestRpadNullSafety:
    """rpad propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """rpad(null, 5, '-') → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN rpad(i.name, 5, '-') AS v",
        )
        assert _is_null_value(result["v"].iloc[0])

    def test_nan_score_returns_null(self, null_ctx: Context) -> None:
        """rpad(NaN column, 5, '-') → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN rpad(i.score, 5, '-') AS v",
        )
        assert _is_null_value(result["v"].iloc[0])


# ---------------------------------------------------------------------------
# indexOf — null in → null out
# ---------------------------------------------------------------------------


class TestIndexOfNullSafety:
    """indexOf propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """indexOf(null, 'x') → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN indexOf(i.name, 'x') AS v",
        )
        assert _is_null_value(result["v"].iloc[0])

    def test_nan_input_returns_null(self, null_ctx: Context) -> None:
        """indexOf(NaN, 'x') → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN indexOf(i.score, 'x') AS v",
        )
        assert _is_null_value(result["v"].iloc[0])


# ---------------------------------------------------------------------------
# date — null in → null out
# ---------------------------------------------------------------------------


class TestDateNullSafety:
    """date() propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """date(null column) → null."""
        star = Star(context=null_ctx)
        result = star.execute_query("MATCH (i:Item) RETURN date(i.dstr) AS v")
        assert _is_null_value(result["v"].iloc[0])


# ---------------------------------------------------------------------------
# datetime — null in → null out
# ---------------------------------------------------------------------------


class TestDatetimeNullSafety:
    """datetime() propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """datetime(null column) → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN datetime(i.dstr) AS v",
        )
        assert _is_null_value(result["v"].iloc[0])


# ---------------------------------------------------------------------------
# duration — null in → null out
# ---------------------------------------------------------------------------


class TestDurationNullSafety:
    """duration() propagates null inputs to null outputs."""

    def test_none_input_returns_null(self, null_ctx: Context) -> None:
        """duration(null column) → null."""
        star = Star(context=null_ctx)
        result = star.execute_query(
            "MATCH (i:Item) RETURN duration(i.dstr) AS v",
        )
        assert _is_null_value(result["v"].iloc[0])
