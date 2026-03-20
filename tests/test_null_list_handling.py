"""Tests documenting null/non-list handling in list comprehension, quantifier, reduce.

These characterization tests ensure the null-as-empty-list semantics are
preserved after the _is_null_raw_list helper extraction refactor.

The helper should be extractable without any behavioral change.
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
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "lst": [[1, 2, 3], None, [4, 5]],
        }
    )
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "lst"],
        source_obj_attribute_map={"lst": "lst"},
        attribute_map={"lst": "lst"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


class TestListComprehensionNullList:
    """List comprehension: null list → empty result list."""

    def test_null_list_gives_empty(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL RETURN [x IN n.lst | x * 2] AS r"
        )
        assert r["r"].iloc[0] == []

    def test_non_null_list_maps_correctly(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NOT NULL RETURN [x IN n.lst | x + 1] AS r"
            " ORDER BY n.lst[0]"
        )
        results = r["r"].tolist()
        assert results[0] == [2, 3, 4]
        assert results[1] == [5, 6]

    def test_null_list_in_where_filter(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL RETURN [x IN n.lst WHERE x > 0 | x] AS r"
        )
        assert r["r"].iloc[0] == []

    def test_null_literal_list_comprehension(self, star: Star) -> None:
        r = star.execute_query("RETURN [x IN null | x] AS r")
        assert r["r"].iloc[0] == []


class TestQuantifierNullList:
    """Quantifier functions: null list → vacuously true (all/none) or false (any/single)."""

    def test_any_on_null_list_is_false(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL RETURN any(x IN n.lst WHERE x > 0) AS r"
        )
        assert r["r"].iloc[0] == False

    def test_all_on_null_list_is_true(self, star: Star) -> None:
        """all() on empty/null list is vacuously true."""
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL RETURN all(x IN n.lst WHERE x > 0) AS r"
        )
        assert r["r"].iloc[0] == True

    def test_none_on_null_list_is_true(self, star: Star) -> None:
        """none() on empty/null list is vacuously true."""
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL RETURN none(x IN n.lst WHERE x > 0) AS r"
        )
        assert r["r"].iloc[0] == True

    def test_single_on_null_list_is_false(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL RETURN single(x IN n.lst WHERE x > 0) AS r"
        )
        assert r["r"].iloc[0] == False


class TestReduceNullList:
    """reduce() on null list: returns the initial accumulator value."""

    def test_reduce_on_null_list_returns_init(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL "
            "RETURN reduce(acc = 0, x IN n.lst | acc + x) AS r"
        )
        assert r["r"].iloc[0] == 0

    def test_reduce_with_non_zero_init(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL "
            "RETURN reduce(acc = 99, x IN n.lst | acc + x) AS r"
        )
        assert r["r"].iloc[0] == 99

    def test_reduce_on_normal_list(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NOT NULL AND n.lst[0] = 1 "
            "RETURN reduce(acc = 0, x IN n.lst | acc + x) AS r"
        )
        assert r["r"].iloc[0] == 6  # 1+2+3


class TestIsNullRawListHelper:
    """Verify the helper is extractable by running all three cases together."""

    def test_all_three_work_on_mixed_data(self, star: Star) -> None:
        """End-to-end: list comp + quantifier + reduce all handle null list correctly."""
        r = star.execute_query(
            "MATCH (n:N) WHERE n.lst IS NULL "
            "RETURN "
            "  [x IN n.lst | x] AS lc, "
            "  any(x IN n.lst WHERE x > 0) AS q, "
            "  reduce(acc = 42, x IN n.lst | acc + x) AS red"
        )
        assert r["lc"].iloc[0] == []
        assert r["q"].iloc[0] == False
        assert r["red"].iloc[0] == 42
