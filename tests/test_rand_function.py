"""TDD tests for rand() scalar function.

In openCypher / Neo4j, ``rand()`` returns a random float in [0.0, 1.0).
It is commonly used in sampling queries:
    MATCH (n:Person) WHERE rand() < 0.1 RETURN n.name

All tests written before implementation (TDD step 1).
"""

from __future__ import annotations

import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture
def empty_star() -> Star:
    return Star(context=Context(entity_mapping=EntityMapping(mapping={})))


class TestRandFunction:
    """rand() must return a float in [0.0, 1.0)."""

    def test_rand_does_not_raise(self, empty_star: Star) -> None:
        """RETURN rand() AS r must not raise."""
        empty_star.execute_query("RETURN rand() AS r")

    def test_rand_returns_a_value(self, empty_star: Star) -> None:
        """rand() returns exactly one row."""
        result = empty_star.execute_query("RETURN rand() AS r")
        assert len(result) == 1

    def test_rand_returns_float(self, empty_star: Star) -> None:
        """rand() value is a Python float."""
        result = empty_star.execute_query("RETURN rand() AS r")
        assert isinstance(float(result["r"].iloc[0]), float)

    def test_rand_in_range(self, empty_star: Star) -> None:
        """rand() value is in [0.0, 1.0)."""
        result = empty_star.execute_query("RETURN rand() AS r")
        v = float(result["r"].iloc[0])
        assert 0.0 <= v < 1.0

    def test_rand_is_in_available_functions(self, empty_star: Star) -> None:
        """Rand must appear in Star.available_functions()."""
        assert "rand" in empty_star.available_functions()

    def test_rand_in_where_clause(self) -> None:
        """rand() in WHERE is evaluated per row and filters correctly in aggregate."""
        import pandas as pd
        from pycypher.ingestion import ContextBuilder

        ctx = ContextBuilder.from_dict(
            {
                "Item": pd.DataFrame(
                    {"__ID__": list(range(100)), "val": list(range(100))},
                ),
            },
        )
        star = Star(context=ctx)
        # With rand() always >= 0.0, no rows should be filtered out when using < 1.0
        result = star.execute_query(
            "MATCH (i:Item) WHERE rand() < 1.0 RETURN count(i) AS n",
        )
        assert int(result["n"].iloc[0]) == 100

    def test_rand_zero_arg_only(self, empty_star: Star) -> None:
        """rand() takes zero arguments — calling with an argument should raise."""
        with pytest.raises(Exception):
            empty_star.execute_query("RETURN rand(1) AS r")
