"""Tests for standalone WITH clauses (no preceding MATCH).

In openCypher, ``WITH 1 AS x RETURN x`` is valid — WITH can open a
query the same way a standalone RETURN can.  Previously, the execution
engine raised ``ValueError: WITH clause requires preceding MATCH clause``
whenever ``current_frame is None`` at a WITH clause.

TDD: all tests written before the implementation.
"""

from __future__ import annotations

import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture()
def empty_ctx() -> Context:
    """Context with no entity tables."""
    return Context(entity_mapping=EntityMapping(mapping={}))


class TestStandaloneWith:
    """Standalone WITH (no preceding MATCH) should work like UNWIND or RETURN."""

    def test_with_literal_integer(self, empty_ctx: Context) -> None:
        """WITH 42 AS n RETURN n → single row n=42."""
        star = Star(context=empty_ctx)
        result = star.execute_query("WITH 42 AS n RETURN n")
        assert result["n"].iloc[0] == 42

    def test_with_arithmetic(self, empty_ctx: Context) -> None:
        """WITH 1 + 1 AS two RETURN two → 2."""
        star = Star(context=empty_ctx)
        result = star.execute_query("WITH 1 + 1 AS two RETURN two")
        assert result["two"].iloc[0] == 2

    def test_with_string(self, empty_ctx: Context) -> None:
        """WITH 'hello' AS s RETURN toUpper(s) AS u → 'HELLO'."""
        star = Star(context=empty_ctx)
        result = star.execute_query("WITH 'hello' AS s RETURN toUpper(s) AS u")
        assert result["u"].iloc[0] == "HELLO"

    def test_with_multiple_columns(self, empty_ctx: Context) -> None:
        """WITH 1 AS a, 2 AS b RETURN a + b AS r → 3."""
        star = Star(context=empty_ctx)
        result = star.execute_query("WITH 1 AS a, 2 AS b RETURN a + b AS r")
        assert result["r"].iloc[0] == 3

    def test_with_does_not_raise(self, empty_ctx: Context) -> None:
        """Regression: standalone WITH must not raise ValueError."""
        star = Star(context=empty_ctx)
        result = star.execute_query("WITH 1 AS n RETURN n")
        assert result is not None

    def test_with_then_where(self, empty_ctx: Context) -> None:
        """WITH 5 AS x RETURN CASE WHEN x > 3 THEN 'big' ELSE 'small' END AS r."""
        star = Star(context=empty_ctx)
        result = star.execute_query(
            "WITH 5 AS x RETURN CASE WHEN x > 3 THEN 'big' ELSE 'small' END AS r"
        )
        assert result["r"].iloc[0] == "big"

    def test_chained_with(self, empty_ctx: Context) -> None:
        """WITH 2 AS x WITH x * 3 AS y RETURN y → 6."""
        star = Star(context=empty_ctx)
        result = star.execute_query("WITH 2 AS x WITH x * 3 AS y RETURN y")
        assert result["y"].iloc[0] == 6
