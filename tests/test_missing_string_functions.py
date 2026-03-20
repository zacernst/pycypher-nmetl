"""Tests for missing string functions: lpad, rpad, btrim.

These are standard Neo4j Cypher string functions not yet present in
pycypher's scalar function registry.

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def name_context() -> Context:
    """Context with a single Person entity table."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "code": ["X  ", "  Y", " Z "],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "code"],
        source_obj_attribute_map={"name": "name", "code": "code"},
        attribute_map={"name": "name", "code": "code"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# lpad(str, size, fillchar) — left-pad to given width
# ---------------------------------------------------------------------------


class TestLpad:
    """lpad(original, size, fill) pads the string on the left."""

    def test_lpad_basic(self, name_context: Context) -> None:
        """lpad pads a short string to the specified width."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN lpad(p.name, 6, '*') AS padded"
        )
        assert result["padded"].iloc[0] == "***Bob"

    def test_lpad_already_at_width(self, name_context: Context) -> None:
        """lpad does nothing when the string is already long enough."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN lpad(p.name, 5, '-') AS padded"
        )
        assert result["padded"].iloc[0] == "Alice"

    def test_lpad_longer_than_width(self, name_context: Context) -> None:
        """lpad truncates to width when the string exceeds it."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Charlie' "
            "RETURN lpad(p.name, 4, '-') AS padded"
        )
        # Neo4j lpad truncates: 'Charlie'[:4] = 'Char'
        assert result["padded"].iloc[0] == "Char"

    def test_lpad_space_fill(self, name_context: Context) -> None:
        """lpad works with a space as the fill character."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN lpad(p.name, 5, ' ') AS padded"
        )
        assert result["padded"].iloc[0] == "  Bob"

    def test_lpad_does_not_raise(self, name_context: Context) -> None:
        """Regression: lpad must not raise NotImplementedError."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN lpad(p.name, 8, '.') AS padded"
        )
        assert result is not None
        assert len(result) == 3


# ---------------------------------------------------------------------------
# rpad(str, size, fillchar) — right-pad to given width
# ---------------------------------------------------------------------------


class TestRpad:
    """rpad(original, size, fill) pads the string on the right."""

    def test_rpad_basic(self, name_context: Context) -> None:
        """rpad pads a short string on the right."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN rpad(p.name, 6, '*') AS padded"
        )
        assert result["padded"].iloc[0] == "Bob***"

    def test_rpad_already_at_width(self, name_context: Context) -> None:
        """rpad does nothing when the string is already long enough."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN rpad(p.name, 5, '-') AS padded"
        )
        assert result["padded"].iloc[0] == "Alice"

    def test_rpad_longer_than_width(self, name_context: Context) -> None:
        """rpad truncates to width when the string exceeds it."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Charlie' "
            "RETURN rpad(p.name, 4, '-') AS padded"
        )
        assert result["padded"].iloc[0] == "Char"

    def test_rpad_space_fill(self, name_context: Context) -> None:
        """rpad works with a space as the fill character."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN rpad(p.name, 5, ' ') AS padded"
        )
        assert result["padded"].iloc[0] == "Bob  "

    def test_rpad_does_not_raise(self, name_context: Context) -> None:
        """Regression: rpad must not raise NotImplementedError."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN rpad(p.name, 8, '.') AS padded"
        )
        assert result is not None
        assert len(result) == 3


# ---------------------------------------------------------------------------
# btrim(str, trim_char) — trim trim_char from both ends
# ---------------------------------------------------------------------------


class TestBtrim:
    """btrim(original, trim_char) strips trim_char from both ends."""

    def test_btrim_both_ends(self, name_context: Context) -> None:
        """btrim removes the given character from both sides."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN btrim('***Bob***', '*') AS trimmed"
        )
        assert result["trimmed"].iloc[0] == "Bob"

    def test_btrim_only_leading(self, name_context: Context) -> None:
        """btrim on leading chars only removes the specified char."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN btrim('***Bob', '*') AS trimmed"
        )
        assert result["trimmed"].iloc[0] == "Bob"

    def test_btrim_only_trailing(self, name_context: Context) -> None:
        """btrim on trailing chars only removes the specified char."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN btrim('Bob***', '*') AS trimmed"
        )
        assert result["trimmed"].iloc[0] == "Bob"

    def test_btrim_no_match(self, name_context: Context) -> None:
        """btrim leaves the string unchanged when trim_char is absent."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN btrim('Bob', '*') AS trimmed"
        )
        assert result["trimmed"].iloc[0] == "Bob"

    def test_btrim_on_column(self, name_context: Context) -> None:
        """btrim works on a column expression."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN btrim(p.code, ' ') AS trimmed"
        )
        # p.code for Bob is '  Y'; btrim with ' ' → 'Y'
        assert result["trimmed"].iloc[0] == "Y"

    def test_btrim_does_not_raise(self, name_context: Context) -> None:
        """Regression: btrim must not raise NotImplementedError."""
        star = Star(context=name_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN btrim(p.code, ' ') AS trimmed"
        )
        assert result is not None
        assert len(result) == 3
