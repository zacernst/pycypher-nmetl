"""Tests for indexOf and toStringOrNull scalar functions.

Neo4j built-in functions not yet in the registry:

  * ``indexOf(original, search)`` — position of first occurrence of *search*
    in *original*, or ``-1`` if not found.  Analogous to Python's ``str.find``.
  * ``indexOf(original, search, from)`` — same but starting at *from*.
  * ``toStringOrNull(value)`` — like ``toString`` but returns ``null`` for
    values that cannot be converted rather than raising.

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
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def one_person_ctx() -> Context:
    df = pd.DataFrame(
        {ID_COLUMN: [1], "name": ["Alice"], "code": ["hello world"]},
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "code"],
        source_obj_attribute_map={"name": "name", "code": "code"},
        attribute_map={"name": "name", "code": "code"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# indexOf tests
# ---------------------------------------------------------------------------


class TestIndexOf:
    """indexOf(str, search) returns first occurrence position or -1."""

    def test_indexof_found_returns_position(
        self,
        one_person_ctx: Context,
    ) -> None:
        """IndexOf finds the first occurrence position (0-based)."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN indexOf(p.code, 'world') AS pos",
        )
        assert result["pos"].iloc[0] == 6

    def test_indexof_not_found_returns_minus_one(
        self,
        one_person_ctx: Context,
    ) -> None:
        """IndexOf returns -1 when search string is not present."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN indexOf(p.code, 'xyz') AS pos",
        )
        assert result["pos"].iloc[0] == -1

    def test_indexof_at_start_returns_zero(
        self,
        one_person_ctx: Context,
    ) -> None:
        """IndexOf returns 0 when substring is at the beginning."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN indexOf(p.code, 'hello') AS pos",
        )
        assert result["pos"].iloc[0] == 0

    def test_indexof_with_from_position(self, one_person_ctx: Context) -> None:
        """indexOf(str, search, from) starts search at *from*."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            # 'hello world' searching for 'o' from position 6 → finds 'world' at 7
            "MATCH (p:Person) RETURN indexOf(p.code, 'o', 6) AS pos",
        )
        assert result["pos"].iloc[0] == 7

    def test_indexof_literal_string(self, one_person_ctx: Context) -> None:
        """IndexOf works on literal string arguments."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN indexOf('abcde', 'cd') AS pos",
        )
        assert result["pos"].iloc[0] == 2

    def test_indexof_does_not_raise(self, one_person_ctx: Context) -> None:
        """Regression: indexOf must not raise NotImplementedError."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN indexOf(p.name, 'A') AS pos",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# toStringOrNull tests
# ---------------------------------------------------------------------------


class TestToStringOrNull:
    """toStringOrNull(x) converts to string, returning null on failure."""

    def test_toStringOrNull_integer(self, one_person_ctx: Context) -> None:
        """ToStringOrNull converts integer to string."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN toStringOrNull(42) AS s",
        )
        assert str(result["s"].iloc[0]) == "42"

    def test_toStringOrNull_string_passthrough(
        self,
        one_person_ctx: Context,
    ) -> None:
        """ToStringOrNull passes through string values unchanged."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN toStringOrNull('hello') AS s",
        )
        assert result["s"].iloc[0] == "hello"

    def test_toStringOrNull_null_returns_null(
        self,
        one_person_ctx: Context,
    ) -> None:
        """toStringOrNull(null) returns null."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN toStringOrNull(null) AS s",
        )
        val = result["s"].iloc[0]
        assert val is None or (isinstance(val, float) and pd.isna(val))

    def test_toStringOrNull_does_not_raise(
        self,
        one_person_ctx: Context,
    ) -> None:
        """Regression: toStringOrNull must not raise NotImplementedError."""
        star = Star(context=one_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN toStringOrNull(p.name) AS s",
        )
        assert result is not None
