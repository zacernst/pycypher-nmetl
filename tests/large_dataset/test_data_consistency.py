"""Data consistency tests for large dataset operations.

Verifies that query results remain correct and consistent at scale.
Catches subtle bugs that only manifest with larger datasets (e.g.,
hash collisions, integer overflow, floating-point precision loss).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
    Star,
)

from .dataset_generator import SCALE_SMALL, SCALE_TINY, generate_social_graph

ID_COLUMN = "__ID__"


def _build_star(
    person_df: pd.DataFrame,
    knows_df: pd.DataFrame,
) -> Star:
    person_table = EntityTable.from_dataframe("Person", person_df)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            "__SOURCE__",
            "__TARGET__",
            "since",
            "weight",
        ],
        source_obj_attribute_map={"since": "since", "weight": "weight"},
        attribute_map={"since": "since", "weight": "weight"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table}
            ),
        )
    )


@pytest.fixture(scope="module")
def tiny_data() -> tuple[pd.DataFrame, pd.DataFrame, Star]:
    person_df, knows_df = generate_social_graph(SCALE_TINY)
    star = _build_star(person_df, knows_df)
    return person_df, knows_df, star


@pytest.fixture(scope="module")
def small_data() -> tuple[pd.DataFrame, pd.DataFrame, Star]:
    person_df, knows_df = generate_social_graph(SCALE_SMALL)
    star = _build_star(person_df, knows_df)
    return person_df, knows_df, star


# ---------------------------------------------------------------------------
# Row count consistency
# ---------------------------------------------------------------------------


class TestRowCountConsistency:
    """Verify that query results have correct row counts at scale."""

    def test_scan_returns_all_rows(
        self, tiny_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = tiny_data
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert len(result) == len(person_df)

    def test_filter_count_matches_pandas(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name AS name"
        )
        expected_count = len(person_df[person_df["age"] > 50])
        assert len(result) == expected_count

    def test_count_aggregation_correct(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = small_data
        result = star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert result["cnt"].iloc[0] == len(person_df)

    def test_join_count_matches_relationships(
        self, tiny_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, knows_df, star = tiny_data
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt"
        )
        assert len(result) == len(knows_df)


# ---------------------------------------------------------------------------
# Value correctness
# ---------------------------------------------------------------------------


class TestValueCorrectness:
    """Verify that returned values match the source data."""

    def test_names_match_source(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name ASC"
        )
        expected = sorted(person_df["name"].tolist())
        assert list(result["name"]) == expected

    def test_age_values_preserved(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age AS age ORDER BY p.age ASC"
        )
        expected = sorted(person_df["age"].tolist())
        assert list(result["age"]) == expected

    def test_sum_aggregation_correct(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) RETURN sum(p.age) AS total"
        )
        expected = person_df["age"].sum()
        assert result["total"].iloc[0] == pytest.approx(expected)

    def test_grouped_count_correct(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        person_df, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.city AS city, count(p) AS cnt ORDER BY city ASC"
        )
        expected = (
            person_df.groupby("city")
            .size()
            .reset_index(name="cnt")
            .sort_values("city")
        )
        assert list(result["city"]) == list(expected["city"])
        assert list(result["cnt"]) == list(expected["cnt"])


# ---------------------------------------------------------------------------
# Ordering consistency
# ---------------------------------------------------------------------------


class TestOrderingConsistency:
    """Verify ORDER BY produces correct results at scale."""

    def test_ascending_order_correct(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age AS age ORDER BY p.age ASC"
        )
        ages = list(result["age"])
        assert ages == sorted(ages)

    def test_descending_order_correct(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, _, star = small_data
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age AS age ORDER BY p.age DESC"
        )
        ages = list(result["age"])
        assert ages == sorted(ages, reverse=True)

    def test_limit_returns_correct_subset(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, _, star = small_data
        full = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY p.age DESC"
        )
        limited = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, p.age AS age "
            "ORDER BY p.age DESC LIMIT 10"
        )
        assert len(limited) == 10  # noqa: PLR2004
        assert list(limited["age"]) == list(full["age"].head(10))


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Verify queries produce identical results on repeated execution."""

    def test_scan_deterministic(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, _, star = small_data
        query = "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name ASC"
        r1 = star.execute_query(query)
        r2 = star.execute_query(query)
        pd.testing.assert_frame_equal(r1, r2)

    def test_aggregation_deterministic(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, _, star = small_data
        query = "MATCH (p:Person) RETURN p.city AS city, count(p) AS cnt ORDER BY city ASC"
        r1 = star.execute_query(query)
        r2 = star.execute_query(query)
        pd.testing.assert_frame_equal(r1, r2)

    def test_join_deterministic(
        self, small_data: tuple[pd.DataFrame, pd.DataFrame, Star]
    ) -> None:
        _, _, star = small_data
        query = (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt "
            "ORDER BY src ASC, tgt ASC"
        )
        r1 = star.execute_query(query)
        r2 = star.execute_query(query)
        pd.testing.assert_frame_equal(r1, r2)
