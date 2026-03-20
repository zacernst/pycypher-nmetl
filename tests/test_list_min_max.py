"""TDD tests for scalar min() and max() on lists.

Neo4j: min([1, 2, 3]) → 1, max([1, 2, 3]) → 3.
These are scalar functions operating on a list argument, distinct from
aggregation min/max used in RETURN/WITH GROUP BY contexts.

All tests written before the fix (TDD step 1).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture()
def star() -> Star:
    df = pd.DataFrame({ID_COLUMN: [1], "n": [1]})
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "n"],
        source_obj_attribute_map={"n": "n"},
        attribute_map={"n": "n"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


class TestScalarMin:
    def test_registered(self, star: Star) -> None:
        assert "min" in star.available_functions()

    def test_min_integers(self, star: Star) -> None:
        r = star.execute_query("RETURN min([3, 1, 2]) AS r")
        assert r["r"].iloc[0] == 1

    def test_min_floats(self, star: Star) -> None:
        r = star.execute_query("RETURN min([3.5, 1.1, 2.2]) AS r")
        assert abs(r["r"].iloc[0] - 1.1) < 1e-9

    def test_min_strings(self, star: Star) -> None:
        r = star.execute_query(
            "RETURN min(['banana', 'apple', 'cherry']) AS r"
        )
        assert r["r"].iloc[0] == "apple"

    def test_min_single_element(self, star: Star) -> None:
        r = star.execute_query("RETURN min([42]) AS r")
        assert r["r"].iloc[0] == 42

    def test_min_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN min(null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_min_empty_list_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN min([]) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_min_ignores_null_elements(self, star: Star) -> None:
        """null elements are ignored, not included in the min."""
        r = star.execute_query("RETURN min([3, null, 1]) AS r")
        assert r["r"].iloc[0] == 1

    def test_min_all_null_elements_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN min([null, null]) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_min_negative_numbers(self, star: Star) -> None:
        r = star.execute_query("RETURN min([-1, -5, -3]) AS r")
        assert r["r"].iloc[0] == -5


class TestScalarMax:
    def test_registered(self, star: Star) -> None:
        assert "max" in star.available_functions()

    def test_max_integers(self, star: Star) -> None:
        r = star.execute_query("RETURN max([3, 1, 2]) AS r")
        assert r["r"].iloc[0] == 3

    def test_max_floats(self, star: Star) -> None:
        r = star.execute_query("RETURN max([3.5, 1.1, 2.2]) AS r")
        assert abs(r["r"].iloc[0] - 3.5) < 1e-9

    def test_max_strings(self, star: Star) -> None:
        r = star.execute_query(
            "RETURN max(['banana', 'apple', 'cherry']) AS r"
        )
        assert r["r"].iloc[0] == "cherry"

    def test_max_single_element(self, star: Star) -> None:
        r = star.execute_query("RETURN max([42]) AS r")
        assert r["r"].iloc[0] == 42

    def test_max_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN max(null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_max_empty_list_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN max([]) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_max_ignores_null_elements(self, star: Star) -> None:
        r = star.execute_query("RETURN max([3, null, 1]) AS r")
        assert r["r"].iloc[0] == 3

    def test_max_all_null_elements_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN max([null, null]) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_max_negative_numbers(self, star: Star) -> None:
        r = star.execute_query("RETURN max([-1, -5, -3]) AS r")
        assert r["r"].iloc[0] == -1
