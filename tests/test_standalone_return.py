"""Tests for standalone RETURN clauses (no preceding MATCH).

In Cypher, ``RETURN 1 + 1 AS two`` is a valid query that returns a
single row with the computed value.  Neo4j and openCypher both allow this.
Previously, ``execute_query`` raised ``ValueError: RETURN clause requires a
preceding clause`` when no MATCH/UNWIND/WITH preceded the RETURN.

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


class TestStandaloneReturn:
    """Standalone RETURN (no preceding MATCH) evaluates literal expressions."""

    def test_return_literal_integer(self, empty_ctx: Context) -> None:
        """RETURN 42 AS n → single row with n=42."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN 42 AS n")
        assert len(result) == 1
        assert result["n"].iloc[0] == 42

    def test_return_arithmetic_expression(self, empty_ctx: Context) -> None:
        """RETURN 1 + 1 AS two → 2."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN 1 + 1 AS two")
        assert result["two"].iloc[0] == 2

    def test_return_string_literal(self, empty_ctx: Context) -> None:
        """RETURN 'hello' AS s → 'hello'."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN 'hello' AS s")
        assert result["s"].iloc[0] == "hello"

    def test_return_boolean_literal(self, empty_ctx: Context) -> None:
        """RETURN true AS b → True."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN true AS b")
        assert result["b"].iloc[0] is True or result["b"].iloc[0] == True  # noqa: E712

    def test_return_scalar_function(self, empty_ctx: Context) -> None:
        """RETURN toUpper('hello') AS u → 'HELLO'."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN toUpper('hello') AS u")
        assert result["u"].iloc[0] == "HELLO"

    def test_return_list_literal(self, empty_ctx: Context) -> None:
        """RETURN [1, 2, 3] AS lst → [1, 2, 3]."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN [1, 2, 3] AS lst")
        assert result["lst"].iloc[0] == [1, 2, 3]

    def test_return_list_size(self, empty_ctx: Context) -> None:
        """RETURN size([1, 2, 3]) AS s → 3."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN size([1, 2, 3]) AS s")
        assert result["s"].iloc[0] == 3

    def test_return_multiple_columns(self, empty_ctx: Context) -> None:
        """RETURN 1 AS a, 2 AS b → a=1, b=2."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN 1 AS a, 2 AS b")
        assert result["a"].iloc[0] == 1
        assert result["b"].iloc[0] == 2

    def test_return_does_not_raise(self, empty_ctx: Context) -> None:
        """Regression: standalone RETURN must not raise ValueError."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN 1 AS n")
        assert result is not None
