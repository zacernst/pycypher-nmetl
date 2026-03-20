"""TDD tests for `repeat()` function and 2-argument `lpad`/`rpad`.

Neo4j / openCypher:
  - `repeat(str, n)` — repeat *str* exactly *n* times; null inputs → null
  - `lpad(str, n [, fill=' '])` — 2-arg form pads with spaces (currently errors)
  - `rpad(str, n [, fill=' '])` — 2-arg form pads with spaces (currently errors)

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
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", None],
            "n": [3, 0, 2],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "n"],
        source_obj_attribute_map={"name": "name", "n": "n"},
        attribute_map={"name": "name", "n": "n"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


class TestRepeat:
    def test_registered(self, star: Star) -> None:
        assert "repeat" in star.available_functions()

    def test_basic(self, star: Star) -> None:
        r = star.execute_query("RETURN repeat('abc', 3) AS r")
        assert r["r"].iloc[0] == "abcabcabc"

    def test_zero_times(self, star: Star) -> None:
        r = star.execute_query("RETURN repeat('abc', 0) AS r")
        assert r["r"].iloc[0] == ""

    def test_one_time(self, star: Star) -> None:
        r = star.execute_query("RETURN repeat('xy', 1) AS r")
        assert r["r"].iloc[0] == "xy"

    def test_empty_string(self, star: Star) -> None:
        r = star.execute_query("RETURN repeat('', 5) AS r")
        assert r["r"].iloc[0] == ""

    def test_null_string_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN repeat(null, 3) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_count_returns_null(self, star: Star) -> None:
        r = star.execute_query("RETURN repeat('x', null) AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_column_input(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN repeat('*', 4) AS r"
        )
        assert r["r"].iloc[0] == "****"

    def test_in_where_clause(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) WHERE repeat('x', 2) = 'xx' RETURN p.name ORDER BY p.name"
        )
        # All non-null-name rows match (repeat is a constant expression here)
        names = [
            x
            for x in r["name"].tolist()
            if x is not None and not (isinstance(x, float) and pd.isna(x))
        ]
        assert "Alice" in names
        assert "Bob" in names


class TestLpadTwoArg:
    def test_two_arg_pads_with_spaces(self, star: Star) -> None:
        r = star.execute_query("RETURN lpad('hello', 8) AS r")
        assert r["r"].iloc[0] == "   hello"

    def test_two_arg_exact_length(self, star: Star) -> None:
        r = star.execute_query("RETURN lpad('hello', 5) AS r")
        assert r["r"].iloc[0] == "hello"

    def test_two_arg_truncates(self, star: Star) -> None:
        r = star.execute_query("RETURN lpad('hello', 3) AS r")
        assert r["r"].iloc[0] == "hel"

    def test_three_arg_still_works(self, star: Star) -> None:
        r = star.execute_query("RETURN lpad('Bob', 6, '*') AS r")
        assert r["r"].iloc[0] == "***Bob"

    def test_null_propagation(self, star: Star) -> None:
        r = star.execute_query("RETURN lpad(null, 5) AS r")
        assert pd.isna(r["r"].iloc[0])


class TestRpadTwoArg:
    def test_two_arg_pads_with_spaces(self, star: Star) -> None:
        r = star.execute_query("RETURN rpad('hello', 8) AS r")
        assert r["r"].iloc[0] == "hello   "

    def test_two_arg_exact_length(self, star: Star) -> None:
        r = star.execute_query("RETURN rpad('hello', 5) AS r")
        assert r["r"].iloc[0] == "hello"

    def test_two_arg_truncates(self, star: Star) -> None:
        r = star.execute_query("RETURN rpad('hello', 3) AS r")
        assert r["r"].iloc[0] == "hel"

    def test_three_arg_still_works(self, star: Star) -> None:
        r = star.execute_query("RETURN rpad('Bob', 6, '*') AS r")
        assert r["r"].iloc[0] == "Bob***"

    def test_null_propagation(self, star: Star) -> None:
        r = star.execute_query("RETURN rpad(null, 5) AS r")
        assert pd.isna(r["r"].iloc[0])
