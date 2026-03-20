"""TDD tests for map introspection scalar functions: keys(), values(), properties().

Previously, ``keys({a: 1, b: 2})`` raised ``ValueError: Unknown scalar
function: keys`` because the special-case handler in
``binding_evaluator.py`` only matched ``Variable`` arguments.  The scalar
registry had no entry for ``keys``, ``values``, or ``properties`` when
the input is a map (dict) value produced by a ``MapLiteral`` expression.

These functions are registered as scalar functions so that the
fall-through path (non-Variable argument) works correctly.

TDD: all tests written before the implementation.
"""

from __future__ import annotations

import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture()
def empty_ctx() -> Context:
    return Context(entity_mapping=EntityMapping(mapping={}))


class TestKeysFunction:
    """keys(map) → list of keys."""

    def test_keys_on_map_literal(self, empty_ctx: Context) -> None:
        """keys({a: 1, b: 2}) returns ['a', 'b'] (any order)."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN keys({a: 1, b: 2}) AS k")
        keys = result["k"].iloc[0]
        assert sorted(keys) == ["a", "b"]

    def test_keys_single_entry(self, empty_ctx: Context) -> None:
        """keys({x: 99}) returns ['x']."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN keys({x: 99}) AS k")
        assert result["k"].iloc[0] == ["x"]

    def test_keys_empty_map(self, empty_ctx: Context) -> None:
        """keys({}) returns []."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN keys({}) AS k")
        assert result["k"].iloc[0] == []

    def test_keys_null_returns_null(self, empty_ctx: Context) -> None:
        """keys(null) returns null."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN keys(null) AS k")
        val = result["k"].iloc[0]
        assert val is None or (isinstance(val, float) and val != val)

    def test_keys_does_not_raise(self, empty_ctx: Context) -> None:
        """keys({name: 'Alice'}) must not raise."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN keys({name: 'Alice'}) AS k")
        assert result is not None


class TestValuesFunction:
    """values(map) → list of values."""

    def test_values_on_map_literal(self, empty_ctx: Context) -> None:
        """values({a: 1, b: 2}) returns [1, 2] (any order)."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN values({a: 1, b: 2}) AS v")
        vals = result["v"].iloc[0]
        assert sorted(vals) == [1, 2]

    def test_values_single_entry(self, empty_ctx: Context) -> None:
        """values({x: 99}) returns [99]."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN values({x: 99}) AS v")
        assert result["v"].iloc[0] == [99]

    def test_values_empty_map(self, empty_ctx: Context) -> None:
        """values({}) returns []."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN values({}) AS v")
        assert result["v"].iloc[0] == []

    def test_values_null_returns_null(self, empty_ctx: Context) -> None:
        """values(null) returns null."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN values(null) AS v")
        val = result["v"].iloc[0]
        assert val is None or (isinstance(val, float) and val != val)

    def test_values_does_not_raise(self, empty_ctx: Context) -> None:
        """values({score: 42}) must not raise."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN values({score: 42}) AS v")
        assert result is not None


class TestPropertiesFunctionMapLiteral:
    """properties(map) → returns the map itself."""

    def test_properties_on_map_literal(self, empty_ctx: Context) -> None:
        """properties({a: 1}) returns {a: 1}."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN properties({a: 1}) AS p")
        assert result["p"].iloc[0] == {"a": 1}

    def test_properties_empty_map(self, empty_ctx: Context) -> None:
        """properties({}) returns {}."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN properties({}) AS p")
        assert result["p"].iloc[0] == {}

    def test_properties_null_returns_null(self, empty_ctx: Context) -> None:
        """properties(null) returns null."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN properties(null) AS p")
        val = result["p"].iloc[0]
        assert val is None or (isinstance(val, float) and val != val)

    def test_properties_does_not_raise(self, empty_ctx: Context) -> None:
        """properties({name: 'Bob'}) must not raise."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN properties({name: 'Bob'}) AS p")
        assert result is not None
