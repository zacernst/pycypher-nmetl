"""TDD tests for the charAt(string, index) scalar function.

Neo4j standard Cypher function:
    charAt(string, index) → single character at zero-based *index*

Edge cases:
    - out-of-range index → null (not an error per Neo4j semantics)
    - null string → null
    - null index → null
    - negative index → null
    - multi-char string → correct character at position

All tests written before implementation (TDD step 1).
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
def empty_star() -> Star:
    return Star(
        context=Context(
            entity_mapping=EntityMapping(),
            relationship_mapping=RelationshipMapping(),
        )
    )


@pytest.fixture()
def word_star() -> Star:
    df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "word": ["hello", "world", "abc"]}
    )
    t = EntityTable(
        entity_type="Word",
        identifier="Word",
        column_names=[ID_COLUMN, "word"],
        source_obj_attribute_map={"word": "word"},
        attribute_map={"word": "word"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Word": t}),
            relationship_mapping=RelationshipMapping(),
        )
    )


class TestCharAtFunction:
    """charAt(string, index) — zero-based character extraction."""

    def test_first_char(self, empty_star: Star) -> None:
        """charAt('hello', 0) == 'h'."""
        r = empty_star.execute_query("RETURN charAt('hello', 0) AS c")
        assert r["c"].iloc[0] == "h"

    def test_last_char(self, empty_star: Star) -> None:
        """charAt('hello', 4) == 'o'."""
        r = empty_star.execute_query("RETURN charAt('hello', 4) AS c")
        assert r["c"].iloc[0] == "o"

    def test_middle_char(self, empty_star: Star) -> None:
        """charAt('hello', 2) == 'l'."""
        r = empty_star.execute_query("RETURN charAt('hello', 2) AS c")
        assert r["c"].iloc[0] == "l"

    def test_out_of_range_returns_null(self, empty_star: Star) -> None:
        """charAt('hello', 10) returns null (not an error)."""
        r = empty_star.execute_query("RETURN charAt('hello', 10) AS c")
        assert r["c"].iloc[0] is None or pd.isna(r["c"].iloc[0])

    def test_negative_index_returns_null(self, empty_star: Star) -> None:
        """charAt('hello', -1) returns null."""
        r = empty_star.execute_query("RETURN charAt('hello', -1) AS c")
        assert r["c"].iloc[0] is None or pd.isna(r["c"].iloc[0])

    def test_null_string_returns_null(self, empty_star: Star) -> None:
        """charAt(null, 0) returns null."""
        r = empty_star.execute_query("RETURN charAt(null, 0) AS c")
        assert r["c"].iloc[0] is None or pd.isna(r["c"].iloc[0])

    def test_null_index_returns_null(self, empty_star: Star) -> None:
        """charAt('hello', null) returns null."""
        r = empty_star.execute_query("RETURN charAt('hello', null) AS c")
        assert r["c"].iloc[0] is None or pd.isna(r["c"].iloc[0])

    def test_does_not_raise(self, empty_star: Star) -> None:
        """charAt must not raise for valid inputs."""
        empty_star.execute_query("RETURN charAt('abc', 1) AS c")

    def test_in_available_functions(self, empty_star: Star) -> None:
        """charAt must appear in available_functions() (lowercased)."""
        fns = empty_star.available_functions()
        assert "charat" in fns or "charAt" in fns

    def test_per_row_on_entity(self, word_star: Star) -> None:
        """charAt(w.word, 0) extracts the first char for each row."""
        r = word_star.execute_query(
            "MATCH (w:Word) RETURN w.word AS word, charAt(w.word, 0) AS first "
            "ORDER BY w.word"
        )
        result = dict(zip(r["word"], r["first"]))
        assert result["abc"] == "a"
        assert result["hello"] == "h"
        assert result["world"] == "w"

    def test_in_where_clause(self, word_star: Star) -> None:
        """WHERE charAt(w.word, 0) = 'h' filters correctly."""
        r = word_star.execute_query(
            "MATCH (w:Word) WHERE charAt(w.word, 0) = 'h' RETURN w.word AS word"
        )
        assert list(r["word"]) == ["hello"]
