"""Tests for multiple consecutive MATCH clauses.

In Cypher, two MATCH clauses without a WITH between them are semantically
equivalent to a single MATCH with a comma-separated pattern:

    MATCH (p:Person) MATCH (q:Person) WHERE p.age > q.age
    ≡
    MATCH (p:Person), (q:Person) WHERE p.age > q.age

The second MATCH's WHERE clause may reference variables from the first MATCH.
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
        },
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    """Alice→Bob, Alice→Carol."""
    return pd.DataFrame(
        {
            "__ID__": [10, 11],
            "__SOURCE__": [1, 1],
            "__TARGET__": [2, 3],
        },
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


@pytest.fixture
def star_with_edges(people_df: pd.DataFrame, knows_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict(
        {"Person": people_df, "KNOWS": knows_df},
    )
    return Star(context=context)


# ===========================================================================
# Cartesian product double MATCH (no shared variables)
# ===========================================================================


class TestDoubleMatchCartesian:
    """MATCH (p) MATCH (q) — Cartesian product with cross-MATCH WHERE."""

    def test_cross_match_where_age_comparison(self, star: Star) -> None:
        """WHERE referencing variables from both MATCH clauses."""
        result = star.execute_query(
            "MATCH (p:Person) MATCH (q:Person) "
            "WHERE p.age > q.age "
            "RETURN p.name AS p, q.name AS q "
            "ORDER BY p.name ASC, q.name ASC",
        )
        # Pairs where p.age > q.age:
        # Alice(30) > Bob(25): (Alice, Bob)
        # Carol(35) > Alice(30): (Carol, Alice)
        # Carol(35) > Bob(25): (Carol, Bob)
        assert len(result) == 3
        p_names = list(result["p"])
        assert p_names.count("Alice") == 1
        assert p_names.count("Carol") == 2

    def test_cross_match_equal_filter(self, star: Star) -> None:
        """Cross-MATCH WHERE equality excludes self-pairs."""
        result = star.execute_query(
            "MATCH (p:Person) MATCH (q:Person) "
            "WHERE p.dept = q.dept AND p.name <> q.name "
            "RETURN p.name AS p, q.name AS q "
            "ORDER BY p.name ASC, q.name ASC",
        )
        # Both Alice and Bob are in 'eng'; Carol is in 'mktg'.
        # Valid pairs: (Alice, Bob), (Bob, Alice)
        assert len(result) == 2
        assert set(result["p"]) == {"Alice", "Bob"}

    def test_cross_match_no_filter_is_cartesian(self, star: Star) -> None:
        """Without WHERE, double MATCH is a Cartesian product."""
        result = star.execute_query(
            "MATCH (p:Person) MATCH (q:Person) RETURN p.name AS p, q.name AS q",
        )
        # 3 × 3 = 9 rows
        assert len(result) == 9

    def test_cross_match_with_limit(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) MATCH (q:Person) "
            "WHERE p.age > q.age "
            "RETURN p.name AS p, q.name AS q "
            "LIMIT 2",
        )
        assert len(result) <= 2


# ===========================================================================
# Double MATCH with shared variable (join rather than Cartesian)
# ===========================================================================


class TestDoubleMatchWithSharedVariable:
    """MATCH (p) MATCH (p)-[:R]->(q) — join on shared 'p' variable."""

    def test_match_then_relationship_match(
        self,
        star_with_edges: Star,
    ) -> None:
        """Second MATCH re-uses p from first MATCH, joined on p."""
        result = star_with_edges.execute_query(
            "MATCH (p:Person) "
            "MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS src, q.name AS tgt "
            "ORDER BY q.name ASC",
        )
        # Alice knows Bob and Carol
        assert list(result["tgt"]) == ["Bob", "Carol"]
        assert all(s == "Alice" for s in result["src"])

    def test_match_then_filtered_relationship_match(
        self,
        star_with_edges: Star,
    ) -> None:
        result = star_with_edges.execute_query(
            "MATCH (p:Person) WHERE p.age >= 30 "
            "MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS src, q.name AS tgt "
            "ORDER BY q.name ASC",
        )
        # Only Alice (30) and Carol (35) pass age filter;
        # but only Alice has outgoing KNOWS edges.
        assert list(result["tgt"]) == ["Bob", "Carol"]

    def test_three_match_clauses(self, star: Star) -> None:
        """Three consecutive MATCHes produce correct results."""
        result = star.execute_query(
            "MATCH (a:Person) MATCH (b:Person) MATCH (c:Person) "
            "WHERE a.age < b.age AND b.age < c.age "
            "RETURN a.name AS a, b.name AS b, c.name AS c",
        )
        # Only one ordering: Bob(25) < Alice(30) < Carol(35)
        assert len(result) == 1
        assert result["a"].iloc[0] == "Bob"
        assert result["b"].iloc[0] == "Alice"
        assert result["c"].iloc[0] == "Carol"
