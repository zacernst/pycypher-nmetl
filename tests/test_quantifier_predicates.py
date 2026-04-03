"""Tests for Cypher quantifier predicate support.

Covers: any(x IN list WHERE cond), all(x IN list WHERE cond),
        none(x IN list WHERE cond), single(x IN list WHERE cond)

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
        },
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


# ===========================================================================
# any() — returns True when at least one element satisfies the predicate
# ===========================================================================


class TestAny:
    """any(x IN list WHERE cond) — at least one match."""

    def test_any_literal_list_true(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN any(x IN [1, 2, 3] WHERE x > 2) AS result",
        )
        assert (
            result["result"].iloc[0] is True
            or result["result"].iloc[0] == True
        )

    def test_any_literal_list_false(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN any(x IN [1, 2, 3] WHERE x > 10) AS result",
        )
        assert (
            result["result"].iloc[0] is False
            or result["result"].iloc[0] == False
        )

    def test_any_empty_list(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN any(x IN [] WHERE x > 0) AS result",
        )
        assert (
            result["result"].iloc[0] is False
            or result["result"].iloc[0] == False
        )

    def test_any_on_property_list(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN any(x IN p.scores WHERE x >= 90) AS has_high",
        )
        assert (
            result["has_high"].iloc[0] is True
            or result["has_high"].iloc[0] == True
        )

    def test_any_per_row(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, any(x IN p.scores WHERE x >= 90) AS has_high "
            "ORDER BY p.name ASC",
        )
        alice = result[result["name"] == "Alice"]["has_high"].iloc[0]
        bob = result[result["name"] == "Bob"]["has_high"].iloc[0]
        assert bool(alice) is True
        assert bool(bob) is False

    def test_any_all_match(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN any(x IN [1, 2, 3] WHERE x > 0) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_any_in_where_clause(self, star: Star) -> None:
        """any() as a WHERE predicate filters rows."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE any(x IN p.scores WHERE x >= 90) "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        names = list(result["name"])
        assert "Alice" in names
        assert "Bob" not in names


# ===========================================================================
# all() — returns True when every element satisfies the predicate
# ===========================================================================


class TestAll:
    """all(x IN list WHERE cond) — every element must match."""

    def test_all_literal_list_true(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN all(x IN [1, 2, 3] WHERE x > 0) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_all_literal_list_false(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN all(x IN [1, 2, 3] WHERE x > 1) AS result",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_all_empty_list_vacuously_true(self, star: Star) -> None:
        """all() on an empty list is vacuously true (standard semantics)."""
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN all(x IN [] WHERE x > 0) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_all_on_property_list(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN all(x IN p.scores WHERE x < 80) AS all_low",
        )
        assert bool(result["all_low"].iloc[0]) is True

    def test_all_per_row(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, all(x IN p.scores WHERE x > 50) AS all_pass "
            "ORDER BY p.name ASC",
        )
        alice = result[result["name"] == "Alice"]["all_pass"].iloc[0]
        bob = result[result["name"] == "Bob"]["all_pass"].iloc[0]
        assert bool(alice) is True
        assert bool(bob) is True  # Bob has [60, 70], both > 50

    def test_all_in_where_clause(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE all(x IN p.scores WHERE x > 50) "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        names = list(result["name"])
        # Alice [85,90,78], Bob [60,70], Carol [95,100,88,72] — all > 50 for everyone
        assert len(names) == 3


# ===========================================================================
# none() — returns True when no element satisfies the predicate
# ===========================================================================


class TestNone:
    """none(x IN list WHERE cond) — no element must match."""

    def test_none_literal_list_true(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN none(x IN [1, 2, 3] WHERE x > 10) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_none_literal_list_false(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN none(x IN [1, 2, 3] WHERE x > 2) AS result",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_none_empty_list(self, star: Star) -> None:
        """none() on an empty list is vacuously true."""
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN none(x IN [] WHERE x > 0) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_none_on_property_list(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN none(x IN p.scores WHERE x > 100) AS no_perfect",
        )
        assert bool(result["no_perfect"].iloc[0]) is True

    def test_none_per_row(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, none(x IN p.scores WHERE x > 90) AS no_high "
            "ORDER BY p.name ASC",
        )
        alice = result[result["name"] == "Alice"]["no_high"].iloc[0]
        carol = result[result["name"] == "Carol"]["no_high"].iloc[0]
        # Alice has 90 (not > 90), Bob [60,70]  — none > 90
        # Carol has [95, 100, 88, 72] → 95 and 100 are > 90 → not none
        assert bool(alice) is True
        assert bool(carol) is False


# ===========================================================================
# single() — returns True when exactly one element satisfies the predicate
# ===========================================================================


class TestSingle:
    """single(x IN list WHERE cond) — exactly one element must match."""

    def test_single_exactly_one(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN single(x IN [1, 2, 3] WHERE x = 2) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_single_zero_matches(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN single(x IN [1, 2, 3] WHERE x > 10) AS result",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_single_two_matches(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN single(x IN [1, 2, 3] WHERE x > 1) AS result",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_single_empty_list(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1] AS _ RETURN single(x IN [] WHERE x > 0) AS result",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_single_on_property_list(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN single(x IN p.scores WHERE x = 90) AS exactly_one_90",
        )
        assert bool(result["exactly_one_90"].iloc[0]) is True

    def test_single_in_where_clause(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE single(x IN p.scores WHERE x > 85) "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # Alice: [85, 90, 78] → only 90 > 85 → single=True
        # Bob: [60, 70] → none > 85 → single=False
        # Carol: [95, 100, 88, 72] → 95, 100, 88 > 85 → single=False
        names = list(result["name"])
        assert names == ["Alice"]


# ===========================================================================
# String tag predicates (quantifiers over string lists)
# ===========================================================================


class TestQuantifierOnStringList:
    """Quantifiers applied to string property lists."""

    def test_any_tag_starts_with(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, any(t IN p.tags WHERE t STARTS WITH 'p') AS has_py "
            "ORDER BY p.name ASC",
        )
        alice = result[result["name"] == "Alice"]["has_py"].iloc[0]
        bob = result[result["name"] == "Bob"]["has_py"].iloc[0]
        assert bool(alice) is True  # Alice has "python"
        assert bool(bob) is False  # Bob has "java", "sql"

    def test_all_tags_short(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, all(t IN p.tags WHERE size(t) <= 4) AS all_short "
            "ORDER BY p.name ASC",
        )
        bob = result[result["name"] == "Bob"]["all_short"].iloc[0]
        alice = result[result["name"] == "Alice"]["all_short"].iloc[0]
        assert bool(bob) is True  # Bob: "java"(4), "sql"(3) — both <= 4
        assert bool(alice) is False  # Alice: "python"(6) > 4
