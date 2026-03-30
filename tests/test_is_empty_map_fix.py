"""TDD test for isEmpty({}) false-positive bug.

``isEmpty({})`` previously returned ``False`` because the ``_check``
function in the extended-string registration only handled ``str`` and
``list``, missing ``dict`` (maps).  In openCypher/Neo4j semantics,
``isEmpty({})`` must return ``True`` for an empty map.

TDD: test written before the fix.
"""

from __future__ import annotations

import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture
def empty_ctx() -> Context:
    return Context(entity_mapping=EntityMapping(mapping={}))


class TestIsEmptyMap:
    """isEmpty() must return True for empty maps, False for non-empty maps."""

    def test_empty_map_is_empty(self, empty_ctx: Context) -> None:
        """isEmpty({}) → True."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN isEmpty({}) AS r")
        assert bool(result["r"].iloc[0]) is True

    def test_non_empty_map_is_not_empty(self, empty_ctx: Context) -> None:
        """isEmpty({a: 1}) → False."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN isEmpty({a: 1}) AS r")
        assert bool(result["r"].iloc[0]) is False

    def test_empty_string_still_works(self, empty_ctx: Context) -> None:
        """Regression: isEmpty('') → True still works after fix."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN isEmpty('') AS r")
        assert bool(result["r"].iloc[0]) is True

    def test_empty_list_still_works(self, empty_ctx: Context) -> None:
        """Regression: isEmpty([]) → True still works after fix."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN isEmpty([]) AS r")
        assert bool(result["r"].iloc[0]) is True

    def test_null_still_is_empty(self, empty_ctx: Context) -> None:
        """Regression: isEmpty(null) → True still works after fix."""
        star = Star(context=empty_ctx)
        result = star.execute_query("RETURN isEmpty(null) AS r")
        assert bool(result["r"].iloc[0]) is True
