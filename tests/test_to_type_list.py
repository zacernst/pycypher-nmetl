"""TDD tests for toStringList, toIntegerList, toFloatList, toBooleanList.

Neo4j list conversion functions apply the corresponding scalar conversion
to each element of a list, with null for unconvertible elements.

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


class TestToStringList:
    def test_registered(self, star: Star) -> None:
        assert "tostringlist" in star.available_functions()

    def test_converts_integers(self, star: Star) -> None:
        r = star.execute_query("RETURN toStringList([1, 2, 3]) AS r")
        assert r["r"].iloc[0] == ["1", "2", "3"]

    def test_converts_floats(self, star: Star) -> None:
        r = star.execute_query("RETURN toStringList([1.5, 2.5]) AS r")
        result = r["r"].iloc[0]
        assert result == ["1.5", "2.5"]

    def test_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN toStringList(null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_elements_preserved(self, star: Star) -> None:
        r = star.execute_query("RETURN toStringList([1, null, 3]) AS r")
        result = r["r"].iloc[0]
        assert result[0] == "1"
        assert result[1] is None
        assert result[2] == "3"

    def test_empty_list(self, star: Star) -> None:
        r = star.execute_query("RETURN toStringList([]) AS r")
        assert r["r"].iloc[0] == []


class TestToIntegerList:
    def test_registered(self, star: Star) -> None:
        assert "tointegerlist" in star.available_functions()

    def test_converts_strings(self, star: Star) -> None:
        r = star.execute_query("RETURN toIntegerList(['1', '2', '3']) AS r")
        result = r["r"].iloc[0]
        assert [int(x) for x in result] == [1, 2, 3]

    def test_converts_floats_by_truncating(self, star: Star) -> None:
        r = star.execute_query("RETURN toIntegerList([1.9, 2.1]) AS r")
        result = r["r"].iloc[0]
        assert [int(x) for x in result] == [1, 2]

    def test_invalid_string_becomes_null(self, star: Star) -> None:
        r = star.execute_query("RETURN toIntegerList(['1', 'abc', '3']) AS r")
        result = r["r"].iloc[0]
        assert result[0] == 1
        assert result[1] is None
        assert result[2] == 3

    def test_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN toIntegerList(null) AS r")
        assert pd.isna(r["r"].iloc[0])


class TestToFloatList:
    def test_registered(self, star: Star) -> None:
        assert "tofloatlist" in star.available_functions()

    def test_converts_strings(self, star: Star) -> None:
        r = star.execute_query("RETURN toFloatList(['1.1', '2.2']) AS r")
        result = r["r"].iloc[0]
        assert abs(result[0] - 1.1) < 1e-9
        assert abs(result[1] - 2.2) < 1e-9

    def test_converts_integers(self, star: Star) -> None:
        r = star.execute_query("RETURN toFloatList([1, 2, 3]) AS r")
        result = r["r"].iloc[0]
        assert result == [1.0, 2.0, 3.0]

    def test_invalid_string_becomes_null(self, star: Star) -> None:
        r = star.execute_query(
            "RETURN toFloatList(['1.1', 'abc', '3.3']) AS r"
        )
        result = r["r"].iloc[0]
        assert abs(result[0] - 1.1) < 1e-9
        assert result[1] is None
        assert abs(result[2] - 3.3) < 1e-9

    def test_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN toFloatList(null) AS r")
        assert pd.isna(r["r"].iloc[0])


class TestToBooleanList:
    def test_registered(self, star: Star) -> None:
        assert "tobooleanlist" in star.available_functions()

    def test_converts_strings(self, star: Star) -> None:
        r = star.execute_query("RETURN toBooleanList(['true', 'false']) AS r")
        result = r["r"].iloc[0]
        assert result[0] is True
        assert result[1] is False

    def test_invalid_string_becomes_null(self, star: Star) -> None:
        r = star.execute_query(
            "RETURN toBooleanList(['true', 'yes', 'false']) AS r"
        )
        result = r["r"].iloc[0]
        assert result[0] is True
        assert result[1] is None
        assert result[2] is False

    def test_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN toBooleanList(null) AS r")
        assert pd.isna(r["r"].iloc[0])
