"""TDD tests for char(codePoint) scalar function.

Neo4j: char(n) returns the string character corresponding to Unicode
code point n. Equivalent to Python's chr(n).

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
            "code": [65, 97, 48],  # A, a, 0
        },
    )
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "code"],
        source_obj_attribute_map={"code": "code"},
        attribute_map={"code": "code"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


class TestCharFunction:
    def test_registered(self, star: Star) -> None:
        assert "char" in star.available_functions()

    def test_uppercase_a(self, star: Star) -> None:
        r = star.execute_query("RETURN char(65) AS r")
        assert r["r"].iloc[0] == "A"

    def test_lowercase_a(self, star: Star) -> None:
        r = star.execute_query("RETURN char(97) AS r")
        assert r["r"].iloc[0] == "a"

    def test_digit_zero(self, star: Star) -> None:
        r = star.execute_query("RETURN char(48) AS r")
        assert r["r"].iloc[0] == "0"

    def test_space(self, star: Star) -> None:
        r = star.execute_query("RETURN char(32) AS r")
        assert r["r"].iloc[0] == " "

    def test_newline(self, star: Star) -> None:
        r = star.execute_query("RETURN char(10) AS r")
        assert r["r"].iloc[0] == "\n"

    def test_unicode_code_point(self, star: Star) -> None:
        """char(9786) → ☺ (smiley face code point)."""
        r = star.execute_query("RETURN char(9786) AS r")
        assert r["r"].iloc[0] == chr(9786)

    def test_null_input_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN char(null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_column_input(self, star: Star) -> None:
        """Char applied to a column of code points."""
        r = star.execute_query(
            "MATCH (n:N) RETURN char(n.code) AS r ORDER BY n.code",
        )
        result = r["r"].tolist()
        assert result == ["0", "A", "a"]

    def test_in_string_concatenation(self, star: Star) -> None:
        """char(65) + 'BC' → 'ABC'."""
        r = star.execute_query("RETURN char(65) + 'BC' AS r")
        assert r["r"].iloc[0] == "ABC"
