"""TDD tests for charCodeAt(string, index) scalar function.

Neo4j: charCodeAt(str, index) returns the Unicode code point of the
character at zero-based index in the string.
Complement to char(codePoint).

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
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "word": ["ABC", "hello", "☺"],
        },
    )
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "word"],
        source_obj_attribute_map={"word": "word"},
        attribute_map={"word": "word"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


class TestCharCodeAt:
    def test_registered(self, star: Star) -> None:
        assert "charcodeat" in star.available_functions()

    def test_uppercase_a(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt('A', 0) AS r")
        assert r["r"].iloc[0] == 65

    def test_second_character(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt('ABC', 1) AS r")
        assert r["r"].iloc[0] == 66  # B

    def test_lowercase(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt('hello', 0) AS r")
        assert r["r"].iloc[0] == 104  # h

    def test_space(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt(' ', 0) AS r")
        assert r["r"].iloc[0] == 32

    def test_unicode_char(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt('☺', 0) AS r")
        assert r["r"].iloc[0] == 9786

    def test_null_string_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt(null, 0) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_index_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN charCodeAt('A', null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_out_of_range_index_returns_null(self, star: Star) -> None:
        """Index beyond string length returns null (Neo4j behaviour)."""
        r = star.execute_query("RETURN charCodeAt('A', 5) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_roundtrip_with_char(self, star: Star) -> None:
        """char(charCodeAt(s, i)) == charAt(s, i) for valid index."""
        r = star.execute_query("RETURN char(charCodeAt('Hello', 0)) AS r")
        assert r["r"].iloc[0] == "H"

    def test_column_input(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (n:N) WHERE n.word = 'ABC' RETURN charCodeAt(n.word, 0) AS r",
        )
        assert r["r"].iloc[0] == 65  # A
