"""Tests for Cypher list comprehension support.

Covers: [x IN list | expr], [x IN list WHERE cond], [x IN list WHERE cond | expr]

All execute through Star.execute_query() for full integration coverage.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "scores": [[85, 90, 78], [60, 70], [95, 100, 88, 72]],
            "tags": [
                ["python", "data"],
                ["java", "sql"],
                ["rust", "python", "c"],
            ],
        }
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


# ===========================================================================
# Basic list comprehension — no WHERE, no map expression
# ===========================================================================


class TestListComprehensionBasic:
    """[x IN list] — identity comprehension (no transform, no filter)."""

    def test_literal_list_no_transform(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [1, 2, 3] | x] AS items"
        )
        assert result["items"].iloc[0] == [1, 2, 3]

    def test_literal_list_length_preserved(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [10, 20, 30, 40] | x] AS items"
        )
        assert len(result["items"].iloc[0]) == 4


# ===========================================================================
# List comprehension with map expression (transform)
# ===========================================================================


class TestListComprehensionMap:
    """[x IN list | expr] — apply a transformation to each element."""

    def test_multiply_literal_list(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [1, 2, 3] | x * 2] AS doubled"
        )
        assert result["doubled"].iloc[0] == [2, 4, 6]

    def test_add_constant_to_each(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [10, 20, 30] | x + 5] AS shifted"
        )
        assert result["shifted"].iloc[0] == [15, 25, 35]

    def test_string_concat_each(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [s IN ['a', 'b', 'c'] | toUpper(s)] AS upped"
        )
        assert result["upped"].iloc[0] == ["A", "B", "C"]

    def test_transform_applied_per_row_property(self, star: Star) -> None:
        """Each Person row applies the transform to their own scores list."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, [x IN p.scores | x + 10] AS bumped "
            "ORDER BY p.name ASC"
        )
        alice_bumped = result[result["name"] == "Alice"]["bumped"].iloc[0]
        assert alice_bumped == [95, 100, 88]


# ===========================================================================
# List comprehension with WHERE filter
# ===========================================================================


class TestListComprehensionFilter:
    """[x IN list WHERE cond] — keep only elements satisfying the predicate."""

    def test_filter_keeps_matching_elements(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2] AS big"
        )
        assert result["big"].iloc[0] == [3, 4, 5]

    def test_filter_removes_all_elements(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [1, 2, 3] WHERE x > 10] AS empty_list"
        )
        assert result["empty_list"].iloc[0] == []

    def test_filter_keeps_all_elements(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [1, 2, 3] WHERE x > 0] AS all_items"
        )
        assert result["all_items"].iloc[0] == [1, 2, 3]

    def test_filter_property_list(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN [x IN p.scores WHERE x >= 85] AS high_scores"
        )
        assert set(result["high_scores"].iloc[0]) == {85, 90}


# ===========================================================================
# List comprehension with both WHERE and map expression
# ===========================================================================


class TestListComprehensionFilterAndMap:
    """[x IN list WHERE cond | expr] — filter then transform."""

    def test_filter_and_double(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ "
            "RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 10] AS result"
        )
        assert result["result"].iloc[0] == [30, 40, 50]

    def test_filter_and_transform_on_property(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' "
            "RETURN [s IN p.tags WHERE s STARTS WITH 'p' | toUpper(s)] AS ptags"
        )
        # Carol has tags: ["rust", "python", "c"] — only "python" starts with 'p'
        assert result["ptags"].iloc[0] == ["PYTHON"]

    def test_filter_and_map_empty_result(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [1, 2, 3] WHERE x > 100 | x * 2] AS nothing"
        )
        assert result["nothing"].iloc[0] == []

    def test_per_row_filter_and_map(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, [x IN p.scores WHERE x > 80 | x] AS good "
            "ORDER BY p.name ASC"
        )
        alice_good = result[result["name"] == "Alice"]["good"].iloc[0]
        assert set(alice_good) == {85, 90}
        bob_good = result[result["name"] == "Bob"]["good"].iloc[0]
        assert bob_good == []  # Bob has [60, 70], none > 80


# ===========================================================================
# List comprehension in WITH clause
# ===========================================================================


class TestListComprehensionInWith:
    """List comprehensions used inside a WITH clause."""

    def test_with_comprehension_then_return(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ "
            "WITH [x IN [1, 2, 3, 4] WHERE x % 2 = 0 | x * x] AS even_squares "
            "RETURN even_squares"
        )
        assert result["even_squares"].iloc[0] == [4, 16]

    def test_with_comprehension_size(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p, [x IN p.tags WHERE size(x) > 4] AS long_tags "
            "RETURN p.name AS name, size(long_tags) AS n_long "
            "ORDER BY p.name ASC"
        )
        # Alice: ["python" (6), "data" (4)] → 1 long tag
        alice = result[result["name"] == "Alice"]["n_long"].iloc[0]
        assert alice == 1
