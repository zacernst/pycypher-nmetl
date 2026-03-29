"""Tests for extended string scalar functions (extended_string_functions.py).

Covers: left, right, ltrim, rtrim, replace, split, join, reverse, length,
isEmpty, lpad, rpad, repeat, btrim, indexOf, charAt, char, charCodeAt,
normalize, startsWith, endsWith, contains, byteSize.

999-line module with only 1 test reference identified by coverage survey.
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


@pytest.fixture()
def str_context() -> Context:
    """Context with diverse string data for testing."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", None, "Dave Smith"],
            "padded": ["  hello  ", "***Bob***", "  trim me  ", None],
            "csv": ["a,b,c", "x,,y", None, "single"],
            "code": [65, 72, None, 233],  # Unicode code points
        }
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=list(df.columns),
        source_obj_attribute_map={c: c for c in df.columns if c != ID_COLUMN},
        attribute_map={c: c for c in df.columns if c != ID_COLUMN},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


def _query(ctx: Context, cypher: str) -> pd.DataFrame:
    star = Star(context=ctx)
    return star.execute_query(cypher)


# ---------------------------------------------------------------------------
# left / right
# ---------------------------------------------------------------------------


class TestLeftRight:
    def test_left_extracts_prefix(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN left(x.name, 3) AS s",
        )
        assert result["s"].iloc[0] == "Ali"

    def test_right_extracts_suffix(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN right(x.name, 3) AS s",
        )
        assert result["s"].iloc[0] == "ice"


# ---------------------------------------------------------------------------
# ltrim / rtrim / btrim
# ---------------------------------------------------------------------------


class TestTrimFunctions:
    def test_ltrim_strips_leading(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN ltrim(x.padded) AS s",
        )
        assert result["s"].iloc[0] == "hello  "

    def test_rtrim_strips_trailing(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN rtrim(x.padded) AS s",
        )
        assert result["s"].iloc[0] == "  hello"

    def test_btrim_strips_both(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN btrim(x.padded) AS s",
        )
        assert result["s"].iloc[0] == "hello"

    def test_btrim_custom_char(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN btrim(x.padded, '*') AS s",
        )
        assert result["s"].iloc[0] == "Bob"


# ---------------------------------------------------------------------------
# replace
# ---------------------------------------------------------------------------


class TestReplace:
    def test_replace_substring(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN replace(x.name, 'li', 'LI') AS s",
        )
        assert result["s"].iloc[0] == "ALIce"

    def test_replace_no_match(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN replace(x.name, 'xyz', '!') AS s",
        )
        assert result["s"].iloc[0] == "Bob"


# ---------------------------------------------------------------------------
# split
# ---------------------------------------------------------------------------


class TestSplit:
    def test_split_comma(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN split(x.csv, ',') AS parts",
        )
        assert result["parts"].iloc[0] == ["a", "b", "c"]

    def test_split_no_delimiter(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Dave Smith' RETURN split(x.csv, ',') AS parts",
        )
        assert result["parts"].iloc[0] == ["single"]


# ---------------------------------------------------------------------------
# reverse
# ---------------------------------------------------------------------------


class TestReverse:
    def test_reverse_string(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN reverse(x.name) AS s",
        )
        assert result["s"].iloc[0] == "ecilA"


# ---------------------------------------------------------------------------
# length
# ---------------------------------------------------------------------------


class TestLength:
    def test_string_length(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN length(x.name) AS n",
        )
        assert result["n"].iloc[0] == 5

    def test_longer_string_length(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Dave Smith' RETURN length(x.name) AS n",
        )
        assert result["n"].iloc[0] == 10


# ---------------------------------------------------------------------------
# isEmpty
# ---------------------------------------------------------------------------


class TestIsEmpty:
    def test_non_empty_string(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN isEmpty(x.name) AS e",
        )
        assert result["e"].iloc[0] == False

    def test_empty_string_literal(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN isEmpty('') AS e",
        )
        assert result["e"].iloc[0] == True


# ---------------------------------------------------------------------------
# lpad / rpad
# ---------------------------------------------------------------------------


class TestPadFunctions:
    def test_lpad_default_space(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN lpad(x.name, 6) AS s",
        )
        assert result["s"].iloc[0] == "   Bob"

    def test_lpad_custom_fill(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN lpad(x.name, 6, '*') AS s",
        )
        assert result["s"].iloc[0] == "***Bob"

    def test_rpad_default_space(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN rpad(x.name, 6) AS s",
        )
        assert result["s"].iloc[0] == "Bob   "

    def test_rpad_custom_fill(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN rpad(x.name, 6, '*') AS s",
        )
        assert result["s"].iloc[0] == "Bob***"

    def test_lpad_truncates_long_string(self, str_context: Context) -> None:
        """If string is longer than size, truncate to size (Neo4j semantics)."""
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN lpad(x.name, 3) AS s",
        )
        assert result["s"].iloc[0] == "Ali"


# ---------------------------------------------------------------------------
# repeat
# ---------------------------------------------------------------------------


class TestRepeat:
    def test_repeat_string(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN repeat(x.name, 3) AS s",
        )
        assert result["s"].iloc[0] == "BobBobBob"

    def test_repeat_zero(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Bob' RETURN repeat(x.name, 0) AS s",
        )
        assert result["s"].iloc[0] == ""


# ---------------------------------------------------------------------------
# charAt / char / charCodeAt
# ---------------------------------------------------------------------------


class TestCharFunctions:
    def test_char_at_index(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN charAt(x.name, 1) AS c",
        )
        assert result["c"].iloc[0] == "l"

    def test_char_from_code_point(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN char(x.code) AS c",
        )
        assert result["c"].iloc[0] == "A"

    def test_char_code_at(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN charCodeAt(x.name, 0) AS c",
        )
        assert result["c"].iloc[0] == 65  # ord('A')


# ---------------------------------------------------------------------------
# startsWith / endsWith / contains
# ---------------------------------------------------------------------------


class TestStringPredicates:
    def test_starts_with_true(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE startsWith(x.name, 'Ali') RETURN x.name AS n",
        )
        assert len(result) == 1
        assert result["n"].iloc[0] == "Alice"

    def test_ends_with_true(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE endsWith(x.name, 'ice') RETURN x.name AS n",
        )
        assert len(result) == 1
        assert result["n"].iloc[0] == "Alice"

    def test_contains_true(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE contains(x.name, 'ob') RETURN x.name AS n",
        )
        assert len(result) == 1
        assert result["n"].iloc[0] == "Bob"

    def test_contains_no_match(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE contains(x.name, 'xyz') RETURN x.name AS n",
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# byteSize
# ---------------------------------------------------------------------------


class TestByteSize:
    def test_ascii_byte_size(self, str_context: Context) -> None:
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN byteSize(x.name) AS b",
        )
        assert result["b"].iloc[0] == 5

    def test_unicode_byte_size(self, str_context: Context) -> None:
        """Multi-byte UTF-8 chars should count bytes, not characters."""
        result = _query(
            str_context,
            "MATCH (x:Item) WHERE x.name = 'Alice' RETURN byteSize('café') AS b",
        )
        # 'café' = 4 chars but 5 bytes in UTF-8 (é = 2 bytes)
        assert result["b"].iloc[0] == 5


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_nfc_default(self, str_context: Context) -> None:
        """normalize() with no form argument uses NFC."""
        # Use the actual decomposed form: 'e' + combining acute accent
        decomposed = "cafe\u0301"  # e + combining accent (NFD form)
        composed = "caf\u00e9"     # single é character (NFC form)
        result = _query(
            str_context,
            f"MATCH (x:Item) WHERE x.name = 'Alice' RETURN normalize('{decomposed}') AS s",
        )
        assert result["s"].iloc[0] == composed
