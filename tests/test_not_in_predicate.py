"""Tests for NOT IN predicate.

Cypher supports ``value NOT IN list`` as shorthand for ``NOT (value IN list)``.

    MATCH (p:Person) WHERE p.age NOT IN [25, 30] RETURN p.name
    MATCH (p:Person) WHERE p.dept NOT IN ['eng', 'sales'] RETURN p.name
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
            "__ID__": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
            "dept": ["eng", "sales", "eng", "mktg"],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


class TestNotInPredicate:
    """WHERE x NOT IN list — negated membership predicate."""

    def test_not_in_integers(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age NOT IN [25, 30] "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # Ages 35 (Carol) and 28 (Dave) are NOT in [25, 30]
        assert list(result["name"]) == ["Carol", "Dave"]

    def test_not_in_strings(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept NOT IN ['eng', 'sales'] "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # Only Dave (mktg) is not in ['eng', 'sales']
        assert list(result["name"]) == ["Dave"]

    def test_not_in_single_element_list(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept NOT IN ['eng'] "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        assert list(result["name"]) == ["Bob", "Dave"]

    def test_not_in_empty_list_returns_all(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age NOT IN [] "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # Nothing is in an empty list, so NOT IN [] is always True
        assert list(result["name"]) == ["Alice", "Bob", "Carol", "Dave"]

    def test_not_in_combined_with_and(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age NOT IN [25, 30] AND p.age > 28 "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # age NOT IN [25,30] → Carol(35), Dave(28). age > 28 → Alice(30),Carol(35)
        # Intersection: Carol(35)
        assert list(result["name"]) == ["Carol"]

    def test_not_in_with_clause(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, p.dept AS dept "
            "WHERE dept NOT IN ['eng'] "
            "RETURN name ORDER BY name ASC",
        )
        assert list(result["name"]) == ["Bob", "Dave"]

    def test_in_and_not_in_complement(self, star: Star) -> None:
        """IN and NOT IN on the same list must yield complementary row sets."""
        in_result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept IN ['eng'] RETURN p.name AS name",
        )
        not_in_result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept NOT IN ['eng'] RETURN p.name AS name",
        )
        in_names = set(in_result["name"])
        not_in_names = set(not_in_result["name"])
        all_names = {"Alice", "Bob", "Carol", "Dave"}
        assert in_names | not_in_names == all_names
        assert in_names & not_in_names == set()
