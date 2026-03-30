"""TDD test for btrim() with single argument.

``btrim(str)`` (one-argument form, trim whitespace) previously raised
``ValueError: Function btrim requires at least 2 arguments``.
In Neo4j/openCypher semantics ``btrim(str)`` is equivalent to
``trim(str)`` — it trims whitespace from both ends.

TDD: tests written before the fix.
"""

from __future__ import annotations

import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture
def empty_ctx() -> Context:
    return Context(entity_mapping=EntityMapping(mapping={}))


class TestBtrimSingleArg:
    """btrim(str) → strip whitespace from both ends."""

    def test_btrim_trims_leading_whitespace(self, empty_ctx: Context) -> None:
        """btrim('  hello') → 'hello'."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN btrim('  hello') AS r")
        assert result["r"].iloc[0] == "hello"

    def test_btrim_trims_trailing_whitespace(self, empty_ctx: Context) -> None:
        """btrim('hello  ') → 'hello'."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN btrim('hello  ') AS r")
        assert result["r"].iloc[0] == "hello"

    def test_btrim_trims_both_ends(self, empty_ctx: Context) -> None:
        """btrim('  hello  ') → 'hello'."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN btrim('  hello  ') AS r")
        assert result["r"].iloc[0] == "hello"

    def test_btrim_no_whitespace_unchanged(self, empty_ctx: Context) -> None:
        """btrim('hello') → 'hello' (no change)."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN btrim('hello') AS r")
        assert result["r"].iloc[0] == "hello"

    def test_btrim_two_arg_still_works(self, empty_ctx: Context) -> None:
        """Regression: btrim('***Bob***', '*') → 'Bob' still works."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN btrim('***Bob***', '*') AS r")
        assert result["r"].iloc[0] == "Bob"

    def test_btrim_does_not_raise(self, empty_ctx: Context) -> None:
        """btrim(' x ') must not raise ValueError."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN btrim(' x ') AS r")
        assert result is not None
