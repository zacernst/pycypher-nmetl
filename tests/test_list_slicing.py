"""Tests for list/string slicing expressions: list[start..end].

Cypher slice notation ``list[from..to]`` returns the sub-list (or
sub-string) at indices ``[from, to)``.  Either bound may be omitted:
``list[..3]`` → first 3 elements, ``list[2..]`` → from index 2 to end,
``list[..]`` → full copy.  Same semantics apply to strings.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "nums": [[10, 20, 30, 40, 50], [1, 2, 3], [100, 200]],
            "word": ["hello", "world", "cypher"],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


class TestListSlicing:
    """list[from..to] slicing."""

    def test_slice_basic(self, star: Star) -> None:
        """list[1..3] returns elements at indices 1 and 2."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.nums[1..3] AS sl",
        )
        assert list(result["sl"]) == [[20, 30]]

    def test_slice_open_end(self, star: Star) -> None:
        """list[2..] returns all elements from index 2 to end."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.nums[2..] AS sl",
        )
        assert list(result["sl"]) == [[30, 40, 50]]

    def test_slice_open_start(self, star: Star) -> None:
        """list[..2] returns first 2 elements."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.nums[..2] AS sl",
        )
        assert list(result["sl"]) == [[10, 20]]

    def test_slice_full_copy(self, star: Star) -> None:
        """list[..] returns a full copy of the list."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.nums[..] AS sl",
        )
        assert list(result["sl"]) == [[10, 20, 30, 40, 50]]

    def test_slice_out_of_bounds_end(self, star: Star) -> None:
        """list[..100] with end > length returns all elements."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.nums[..100] AS sl",
        )
        assert list(result["sl"]) == [[10, 20, 30, 40, 50]]

    def test_slice_string(self, star: Star) -> None:
        """str[1..4] returns characters 1..3 (end exclusive)."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.word[1..4] AS sl",
        )
        assert list(result["sl"]) == ["ell"]

    def test_slice_string_open_end(self, star: Star) -> None:
        """str[2..] returns suffix from index 2."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.word[2..] AS sl",
        )
        assert list(result["sl"]) == ["llo"]

    def test_slice_in_with_clause(self, star: Star) -> None:
        """Slicing works as a WITH projection expression."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH p.nums[0..2] AS first "
            "RETURN first",
        )
        assert list(result["first"]) == [[10, 20]]

    def test_slice_all_rows(self, star: Star) -> None:
        """Slicing applied to all rows returns correct sub-lists per row."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.nums[0..1] AS head "
            "ORDER BY name ASC",
        )
        # ORDER BY name ASC: Alice → [10], Bob → [1], Carol → [100]
        assert list(result["head"]) == [[10], [1], [100]]
