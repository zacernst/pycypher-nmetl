"""Tests for UNWIND and OPTIONAL MATCH clauses.

Both features are implemented in star.py but were previously untested.
These tests cover all major use cases plus edge cases and serve as a
regression guard for future refactoring.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "tags": [
                ["python", "data"],
                ["java"],
                [],
            ],  # third person has no tags
        }
    )


@pytest.fixture
def products_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [10, 20, 30],
            "title": ["Widget", "Gadget", "Doohickey"],
        }
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    """Alice knows Bob; Bob knows Carol; Carol knows nobody."""
    return pd.DataFrame(
        {
            "__ID__": [100, 101],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
        }
    )


@pytest.fixture
def likes_df() -> pd.DataFrame:
    """Alice likes Widget; Alice likes Gadget; Bob likes Widget."""
    return pd.DataFrame(
        {
            "__ID__": [200, 201, 202],
            "__SOURCE__": [1, 1, 2],
            "__TARGET__": [10, 20, 10],
        }
    )


@pytest.fixture
def star_people(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


@pytest.fixture
def star_with_rels(
    people_df: pd.DataFrame,
    products_df: pd.DataFrame,
    knows_df: pd.DataFrame,
    likes_df: pd.DataFrame,
) -> Star:
    context = (
        ContextBuilder()
        .add_entity("Person", people_df)
        .add_entity("Product", products_df)
        .add_relationship(
            "KNOWS", knows_df, source_col="__SOURCE__", target_col="__TARGET__"
        )
        .add_relationship(
            "LIKES", likes_df, source_col="__SOURCE__", target_col="__TARGET__"
        )
        .build()
    )
    return Star(context=context)


# ===========================================================================
# UNWIND — standalone (no preceding MATCH)
# ===========================================================================


class TestUnwindStandalone:
    """UNWIND as the first clause — no preceding MATCH."""

    def test_unwind_literal_list_returns_rows(self, star_people: Star) -> None:
        result = star_people.execute_query("UNWIND [1, 2, 3] AS n RETURN n")
        assert list(result["n"]) == [1, 2, 3]

    def test_unwind_literal_string_list(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "UNWIND ['alpha', 'beta', 'gamma'] AS s RETURN s"
        )
        assert set(result["s"]) == {"alpha", "beta", "gamma"}
        assert len(result) == 3

    def test_unwind_empty_list_returns_no_rows(
        self, star_people: Star
    ) -> None:
        result = star_people.execute_query("UNWIND [] AS n RETURN n")
        assert len(result) == 0

    def test_unwind_single_element_list(self, star_people: Star) -> None:
        result = star_people.execute_query("UNWIND [42] AS n RETURN n")
        assert len(result) == 1
        assert result["n"].iloc[0] == 42

    def test_unwind_with_expression(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "UNWIND [1, 2, 3] AS n RETURN n * 2 AS doubled"
        )
        assert set(result["doubled"]) == {2, 4, 6}

    def test_unwind_with_where_filter(self, star_people: Star) -> None:
        # Cypher does not allow WHERE directly after UNWIND; use WITH as a bridge
        result = star_people.execute_query(
            "UNWIND [1, 2, 3, 4, 5] AS n WITH n WHERE n > 2 RETURN n"
        )
        assert set(result["n"]) == {3, 4, 5}

    def test_unwind_with_alias_in_return(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "UNWIND ['x', 'y'] AS item RETURN item AS letter"
        )
        assert "letter" in result.columns
        assert set(result["letter"]) == {"x", "y"}


# ===========================================================================
# UNWIND — after MATCH (property list expansion)
# ===========================================================================


class TestUnwindAfterMatch:
    """UNWIND after a MATCH clause — expand a property list into rows."""

    def test_unwind_property_list_expands_rows(
        self, star_people: Star
    ) -> None:
        # Alice has 2 tags, Bob has 1, Carol has 0 → expect 3 rows total
        result = star_people.execute_query(
            "MATCH (p:Person) UNWIND p.tags AS tag RETURN p.name AS person, tag"
        )
        assert len(result) == 3
        assert "person" in result.columns
        assert "tag" in result.columns

    def test_unwind_property_list_correct_values(
        self, star_people: Star
    ) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) UNWIND p.tags AS tag RETURN p.name AS person, tag"
        )
        alice_tags = set(result[result["person"] == "Alice"]["tag"])
        assert alice_tags == {"python", "data"}
        bob_tags = set(result[result["person"] == "Bob"]["tag"])
        assert bob_tags == {"java"}

    def test_unwind_property_empty_list_rows_excluded(
        self, star_people: Star
    ) -> None:
        # Carol has [] tags — her rows should be omitted
        result = star_people.execute_query(
            "MATCH (p:Person) UNWIND p.tags AS tag RETURN p.name AS person, tag"
        )
        assert "Carol" not in set(result["person"])

    def test_unwind_preserves_other_properties(
        self, star_people: Star
    ) -> None:
        """p.age must be accessible after UNWIND."""
        result = star_people.execute_query(
            "MATCH (p:Person) UNWIND p.tags AS tag "
            "RETURN p.name AS person, p.age AS age, tag"
        )
        # Alice (age=30) should appear twice (2 tags)
        alice_rows = result[result["person"] == "Alice"]
        assert len(alice_rows) == 2
        assert all(alice_rows["age"] == 30)

    def test_unwind_collect_then_unwind_roundtrip(
        self, star_people: Star
    ) -> None:
        """collect() then UNWIND should produce the same names, in some order."""
        result = star_people.execute_query(
            "MATCH (p:Person) "
            "WITH collect(p.name) AS names "
            "UNWIND names AS name "
            "RETURN name"
        )
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}
        assert len(result) == 3

    def test_unwind_after_match_range_function(
        self, star_people: Star
    ) -> None:
        """UNWIND range(1, 3) AS n should produce 3 rows."""
        result = star_people.execute_query("UNWIND range(1, 3) AS n RETURN n")
        assert set(result["n"]) == {1, 2, 3}


# ===========================================================================
# OPTIONAL MATCH — basic left-join semantics
# ===========================================================================


class TestOptionalMatchBasic:
    """OPTIONAL MATCH produces NULL for rows with no matching pattern."""

    def test_optional_match_includes_all_base_rows(
        self, star_with_rels: Star
    ) -> None:
        # Carol has no outgoing KNOWS edges
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS person, q.name AS friend"
        )
        # All 3 persons must appear
        persons = set(result["person"])
        assert "Alice" in persons
        assert "Bob" in persons
        assert "Carol" in persons

    def test_optional_match_null_for_missing_relationship(
        self, star_with_rels: Star
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS person, q.name AS friend"
        )
        carol_rows = result[result["person"] == "Carol"]
        assert len(carol_rows) == 1
        assert pd.isna(carol_rows["friend"].iloc[0])

    def test_optional_match_non_null_for_matched_rows(
        self, star_with_rels: Star
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS person, q.name AS friend"
        )
        alice_rows = result[result["person"] == "Alice"]
        assert len(alice_rows) == 1
        assert alice_rows["friend"].iloc[0] == "Bob"

    def test_optional_match_row_count_correct(
        self, star_with_rels: Star
    ) -> None:
        # Alice→Bob (1), Bob→Carol (1), Carol→None (1) = 3 rows
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS person, q.name AS friend"
        )
        assert len(result) == 3

    def test_optional_match_with_where_on_base(
        self, star_with_rels: Star
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS person, q.name AS friend"
        )
        assert len(result) == 1
        assert result["person"].iloc[0] == "Carol"
        assert pd.isna(result["friend"].iloc[0])

    def test_optional_match_multiple_matches_per_row(
        self, star_with_rels: Star
    ) -> None:
        # Alice LIKES Widget and Gadget → should have 2 rows for Alice
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:LIKES]->(pr:Product) "
            "RETURN p.name AS person, pr.title AS product "
            "ORDER BY p.name ASC, pr.title ASC"
        )
        alice_rows = result[result["person"] == "Alice"]
        assert len(alice_rows) == 2
        assert set(alice_rows["product"]) == {"Widget", "Gadget"}

    def test_optional_match_nonexistent_rel_type_returns_nulls(
        self, star_with_rels: Star
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:HATES]->(q:Person) "
            "RETURN p.name AS person, q.name AS enemy"
        )
        # No HATES relationship exists — all 3 rows with NULL enemy
        assert len(result) == 3
        assert result["enemy"].isna().all()

    def test_optional_match_as_first_clause_empty_entity(
        self, star_with_rels: Star
    ) -> None:
        # OPTIONAL MATCH on entity type that doesn't exist → empty result
        result = star_with_rels.execute_query(
            "OPTIONAL MATCH (x:Unicorn) RETURN x"
        )
        assert len(result) == 0


# ===========================================================================
# OPTIONAL MATCH — filtering on optionally-matched columns
# ===========================================================================


class TestOptionalMatchFiltering:
    """WHERE and aggregation on OPTIONAL MATCH results."""

    def test_optional_match_filter_non_null_friends(
        self, star_with_rels: Star
    ) -> None:
        # WHERE after OPTIONAL MATCH restricts what gets optionally matched,
        # NOT whether a row appears in the final result. Carol still appears
        # with a NULL friend.  To post-filter, use WITH ... WHERE.
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "WHERE q.name IS NOT NULL "
            "RETURN p.name AS person, q.name AS friend"
        )
        # All 3 persons appear; Carol has NULL friend
        assert set(result["person"]) == {"Alice", "Bob", "Carol"}
        carol_rows = result[result["person"] == "Carol"]
        assert pd.isna(carol_rows["friend"].iloc[0])

    def test_optional_match_count_friends(self, star_with_rels: Star) -> None:
        # count() should count non-null friend entries
        result = star_with_rels.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS person, count(q) AS friend_count "
            "ORDER BY p.name ASC"
        )
        counts = dict(zip(result["person"], result["friend_count"]))
        assert counts["Alice"] == 1
        assert counts["Bob"] == 1
        # Carol's count: 0 (null q doesn't count)
        assert counts["Carol"] == 0
