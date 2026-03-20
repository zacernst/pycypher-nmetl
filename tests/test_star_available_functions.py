"""Tests for Star.available_functions() discoverability method."""

import pytest
from pycypher.relational_models import (
    Context,
    EntityMapping,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture
def empty_context() -> Context:
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestAvailableFunctions:
    def test_returns_list(self, empty_context: Context) -> None:
        """available_functions() returns a list."""
        star = Star(context=empty_context)
        result = star.available_functions()
        assert isinstance(result, list)

    def test_returns_sorted_strings(self, empty_context: Context) -> None:
        """Returned list is sorted and contains strings."""
        star = Star(context=empty_context)
        result = star.available_functions()
        assert all(isinstance(name, str) for name in result)
        assert result == sorted(result)

    def test_non_empty(self, empty_context: Context) -> None:
        """Registry is non-empty — built-in functions are registered."""
        star = Star(context=empty_context)
        result = star.available_functions()
        assert len(result) > 0

    def test_string_functions_present(self, empty_context: Context) -> None:
        """Core string functions are registered."""
        star = Star(context=empty_context)
        funcs = star.available_functions()
        for name in ("toupper", "tolower", "trim", "substring"):
            assert name in funcs, f"Expected '{name}' in available_functions()"

    def test_math_functions_present(self, empty_context: Context) -> None:
        """Core math functions are registered."""
        star = Star(context=empty_context)
        funcs = star.available_functions()
        for name in ("abs", "ceil", "floor", "sqrt"):
            assert name in funcs, f"Expected '{name}' in available_functions()"

    def test_trig_functions_present(self, empty_context: Context) -> None:
        """Trigonometric functions are registered."""
        star = Star(context=empty_context)
        funcs = star.available_functions()
        for name in ("sin", "cos", "tan"):
            assert name in funcs, f"Expected '{name}' in available_functions()"

    def test_utility_functions_present(self, empty_context: Context) -> None:
        """Utility functions coalesce, toString, toInteger are registered."""
        star = Star(context=empty_context)
        funcs = star.available_functions()
        for name in ("coalesce", "tostring", "tointeger"):
            assert name in funcs, f"Expected '{name}' in available_functions()"

    def test_list_functions_present(self, empty_context: Context) -> None:
        """List functions head, last, tail, size are registered."""
        star = Star(context=empty_context)
        funcs = star.available_functions()
        for name in ("head", "last", "tail", "size"):
            assert name in funcs, f"Expected '{name}' in available_functions()"

    def test_no_duplicates(self, empty_context: Context) -> None:
        """Returned list has no duplicate entries."""
        star = Star(context=empty_context)
        result = star.available_functions()
        assert len(result) == len(set(result))
