"""Tests for UNION and UNION ALL query combinators.

UNION combines results from two queries, removing duplicates.
UNION ALL combines results without deduplication.

Both forms require matching column names in both sub-queries.
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
            "age": [30, 25, 35],
            "dept": ["eng", "eng", "mktg"],
        }
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    return Star(context=ContextBuilder.from_dict({"Person": people_df}))


# ===========================================================================
# UNION (deduplicating)
# ===========================================================================


class TestUnion:
    """UNION removes duplicate rows across both result sets."""

    def test_union_disjoint_results(self, star: Star) -> None:
        """Non-overlapping results — UNION returns all rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name "
            "UNION "
            "MATCH (p:Person) WHERE p.age < 27 RETURN p.name AS name"
        )
        names = set(result["name"])
        assert names == {"Alice", "Carol", "Bob"}
        assert len(result) == 3

    def test_union_overlapping_results_deduplicates(self, star: Star) -> None:
        """Rows common to both sides appear exactly once."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age >= 25 RETURN p.name AS name "
            "UNION "
            "MATCH (p:Person) WHERE p.age <= 30 RETURN p.name AS name"
        )
        # age>=25: Alice(30), Bob(25), Carol(35) — 3 rows
        # age<=30: Alice(30), Bob(25)             — 2 rows
        # Union deduplicates: {Alice, Bob, Carol}
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}
        assert len(result) == 3

    def test_union_preserves_all_columns(self, star: Star) -> None:
        """UNION with multiple columns aligns correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'eng' RETURN p.name AS name, p.age AS age "
            "UNION "
            "MATCH (p:Person) WHERE p.dept = 'mktg' RETURN p.name AS name, p.age AS age"
        )
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}
        assert len(result) == 3

    def test_union_empty_first_side(self, star: Star) -> None:
        """First query returns no rows — result equals second query."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 1000 RETURN p.name AS name "
            "UNION "
            "MATCH (p:Person) WHERE p.age < 27 RETURN p.name AS name"
        )
        assert list(result["name"]) == ["Bob"]

    def test_union_empty_second_side(self, star: Star) -> None:
        """Second query returns no rows — result equals first query."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age < 27 RETURN p.name AS name "
            "UNION "
            "MATCH (p:Person) WHERE p.age > 1000 RETURN p.name AS name"
        )
        assert list(result["name"]) == ["Bob"]

    def test_union_both_sides_identical_deduplicates(self, star: Star) -> None:
        """Identical queries: UNION returns the same rows as one query."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name "
            "UNION "
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert len(result) == 3


# ===========================================================================
# UNION ALL (non-deduplicating)
# ===========================================================================


class TestUnionAll:
    """UNION ALL preserves duplicate rows."""

    def test_union_all_disjoint_results(self, star: Star) -> None:
        """Non-overlapping: UNION ALL == UNION here."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name "
            "UNION ALL "
            "MATCH (p:Person) WHERE p.age < 27 RETURN p.name AS name"
        )
        assert len(result) == 3
        assert set(result["name"]) == {"Alice", "Carol", "Bob"}

    def test_union_all_overlapping_keeps_duplicates(self, star: Star) -> None:
        """Rows appearing in both sub-queries survive in UNION ALL."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age >= 25 RETURN p.name AS name "
            "UNION ALL "
            "MATCH (p:Person) WHERE p.age <= 30 RETURN p.name AS name"
        )
        # side1: Alice, Bob, Carol (3)
        # side2: Alice, Bob       (2)
        # total: 5 (no deduplication)
        assert len(result) == 5
        assert result["name"].value_counts()["Alice"] == 2
        assert result["name"].value_counts()["Bob"] == 2
        assert result["name"].value_counts()["Carol"] == 1

    def test_union_all_identical_queries_doubles(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name "
            "UNION ALL "
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert len(result) == 6
