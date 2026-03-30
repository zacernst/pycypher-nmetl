"""TDD tests for `sort()` and `flatten()` list functions.

Neo4j/openCypher:
  - `sort(list)` — returns a new sorted list (ascending); null elements go last
  - `flatten(list)` — returns a flat list (single-level deep), null preserved

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


@pytest.fixture
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
        ),
    )


class TestSort:
    def test_registered(self, star: Star) -> None:
        assert "sort" in star.available_functions()

    def test_sort_integers(self, star: Star) -> None:
        r = star.execute_query("RETURN sort([3, 1, 2]) AS r")
        result = r["r"].iloc[0]
        assert list(result) == [1, 2, 3]

    def test_sort_strings(self, star: Star) -> None:
        r = star.execute_query(
            "RETURN sort(['banana', 'apple', 'cherry']) AS r",
        )
        result = r["r"].iloc[0]
        assert result == ["apple", "banana", "cherry"]

    def test_sort_empty_list(self, star: Star) -> None:
        r = star.execute_query("RETURN sort([]) AS r")
        result = r["r"].iloc[0]
        assert result == []

    def test_sort_single_element(self, star: Star) -> None:
        r = star.execute_query("RETURN sort([42]) AS r")
        result = r["r"].iloc[0]
        assert list(result) == [42]

    def test_sort_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN sort(null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_sort_already_sorted(self, star: Star) -> None:
        r = star.execute_query("RETURN sort([1, 2, 3]) AS r")
        result = r["r"].iloc[0]
        assert list(result) == [1, 2, 3]

    def test_sort_floats(self, star: Star) -> None:
        r = star.execute_query("RETURN sort([3.5, 1.1, 2.2]) AS r")
        result = r["r"].iloc[0]
        assert result[0] < result[1] < result[2]


class TestFlatten:
    def test_registered(self, star: Star) -> None:
        assert "flatten" in star.available_functions()

    def test_flatten_nested(self, star: Star) -> None:
        r = star.execute_query("RETURN flatten([[1, 2], [3, 4]]) AS r")
        result = r["r"].iloc[0]
        assert list(result) == [1, 2, 3, 4]

    def test_flatten_mixed(self, star: Star) -> None:
        r = star.execute_query("RETURN flatten([[1, 2], 3, [4, 5]]) AS r")
        result = r["r"].iloc[0]
        assert list(result) == [1, 2, 3, 4, 5]

    def test_flatten_already_flat(self, star: Star) -> None:
        r = star.execute_query("RETURN flatten([1, 2, 3]) AS r")
        result = r["r"].iloc[0]
        assert list(result) == [1, 2, 3]

    def test_flatten_empty(self, star: Star) -> None:
        r = star.execute_query("RETURN flatten([]) AS r")
        result = r["r"].iloc[0]
        assert result == []

    def test_flatten_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN flatten(null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_flatten_single_level_deep(self, star: Star) -> None:
        """Flatten only goes one level deep per openCypher spec."""
        r = star.execute_query("RETURN flatten([[1, [2, 3]], [4]]) AS r")
        result = r["r"].iloc[0]
        # [1, [2, 3], 4] — only first level flattened
        assert result[0] == 1
        assert result[1] == [2, 3]
        assert result[2] == 4
