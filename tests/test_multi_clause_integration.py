"""Integration tests for multi-clause query combinations.

Tests realistic query patterns that exercise multiple Cypher clauses together,
focusing on clause interaction edge cases and data flow validation across
clause boundaries.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures: Social network graph
# ---------------------------------------------------------------------------


@pytest.fixture
def social_star() -> Star:
    """Star with Person entities, City entities, KNOWS and LIVES_IN rels."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 40],
            "dept": ["eng", "mktg", "eng", "sales", "eng"],
            "salary": [100_000, 80_000, 110_000, 90_000, 120_000],
        },
    )
    city_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12],
            "name": ["NYC", "SF", "LA"],
            "population": [8_000_000, 800_000, 4_000_000],
        },
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103, 104, 105],
            "__SOURCE__": [1, 2, 3, 1, 4],
            "__TARGET__": [2, 3, 1, 3, 5],
            "since": [2020, 2021, 2019, 2022, 2023],
        },
    )
    lives_in_df = pd.DataFrame(
        {
            ID_COLUMN: [201, 202, 203, 204, 205],
            "__SOURCE__": [1, 2, 3, 4, 5],
            "__TARGET__": [10, 11, 10, 12, 11],
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "dept", "salary"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
            "salary": "salary",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
            "salary": "salary",
        },
        source_obj=people_df,
    )
    city_table = EntityTable(
        entity_type="City",
        identifier="City",
        column_names=[ID_COLUMN, "name", "population"],
        source_obj_attribute_map={
            "name": "name",
            "population": "population",
        },
        attribute_map={
            "name": "name",
            "population": "population",
        },
        source_obj=city_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    lives_in_table = RelationshipTable(
        relationship_type="LIVES_IN",
        identifier="LIVES_IN",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=lives_in_df,
        source_entity_type="Person",
        target_entity_type="City",
    )
    ctx = Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "City": city_table},
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": knows_table,
                "LIVES_IN": lives_in_table,
            },
        ),
    )
    return Star(context=ctx)


# ===========================================================================
# MATCH + WHERE + RETURN combinations
# ===========================================================================


class TestMatchWhereReturn:
    """MATCH + WHERE + RETURN interaction tests."""

    def test_simple_where_filter(self, social_star: Star) -> None:
        """WHERE filters rows before RETURN."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
        )
        names = set(result.iloc[:, 0])
        assert names == {"Carol", "Eve"}

    def test_where_with_and(self, social_star: Star) -> None:
        """WHERE with AND combines conditions."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 25 AND n.dept = 'eng' RETURN n.name",
        )
        names = set(result.iloc[:, 0])
        assert names == {"Alice", "Carol", "Eve"}

    def test_where_with_or(self, social_star: Star) -> None:
        """WHERE with OR combines conditions."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.age < 26 OR n.age > 39 RETURN n.name",
        )
        names = set(result.iloc[:, 0])
        assert names == {"Bob", "Eve"}

    def test_where_on_relationship_property(self, social_star: Star) -> None:
        """WHERE can filter on relationship properties."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE r.since >= 2022 "
            "RETURN a.name, b.name",
        )
        assert len(result) == 2  # Alice->Carol(2022), Dave->Eve(2023)

    def test_where_string_predicate(self, social_star: Star) -> None:
        """WHERE with STARTS WITH string predicate."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n.name",
        )
        assert len(result) == 1
        assert result.iloc[0, 0] == "Alice"

    def test_where_null_check(self, social_star: Star) -> None:
        """WHERE with IS NOT NULL."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name",
        )
        assert len(result) == 5


# ===========================================================================
# MATCH + WITH + RETURN combinations
# ===========================================================================


class TestMatchWithReturn:
    """Tests for WITH clause as intermediate projection."""

    def test_with_alias_then_return(self, social_star: Star) -> None:
        """WITH creates alias for downstream use."""
        result = social_star.execute_query(
            "MATCH (n:Person) WITH n.name AS personName RETURN personName",
        )
        assert len(result) == 5
        assert "personName" in result.columns

    def test_with_where_filter(self, social_star: Star) -> None:
        """WITH + WHERE filters intermediate results."""
        result = social_star.execute_query(
            "MATCH (n:Person) WITH n, n.age AS age WHERE age > 30 RETURN n.name",
        )
        names = set(result.iloc[:, 0])
        assert names == {"Carol", "Eve"}

    def test_chained_with_clauses(self, social_star: Star) -> None:
        """Multiple WITH clauses chain transformations."""
        result = social_star.execute_query(
            "MATCH (n:Person) "
            "WITH n.name AS name, n.age AS age "
            "WITH name, age * 2 AS doubleAge "
            "RETURN name, doubleAge",
        )
        assert len(result) == 5
        # Alice: 30 * 2 = 60
        alice_row = result[result["name"] == "Alice"]
        assert alice_row["doubleAge"].iloc[0] == 60

    def test_with_aggregation(self, social_star: Star) -> None:
        """WITH clause with aggregation."""
        result = social_star.execute_query(
            "MATCH (n:Person) "
            "WITH n.dept AS dept, count(n) AS cnt "
            "RETURN dept, cnt "
            "ORDER BY cnt DESC",
        )
        # eng: 3, mktg: 1, sales: 1
        eng_row = result[result["dept"] == "eng"]
        assert eng_row["cnt"].iloc[0] == 3

    def test_with_order_by_limit(self, social_star: Star) -> None:
        """WITH clause with ORDER BY and LIMIT."""
        result = social_star.execute_query(
            "MATCH (n:Person) WITH n ORDER BY n.age DESC LIMIT 3 RETURN n.name, n.age",
        )
        assert len(result) == 3
        # Top 3 by age: Eve(40), Carol(35), Alice(30)
        ages = list(result.iloc[:, 1])
        assert ages == [40, 35, 30]


# ===========================================================================
# Relationship traversal + filtering
# ===========================================================================


class TestRelationshipTraversal:
    """Multi-hop relationship traversal with filtering."""

    def test_single_hop_with_filter(self, social_star: Star) -> None:
        """Single hop relationship with WHERE filter on target."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE b.age > 30 "
            "RETURN a.name, b.name",
        )
        # b must be Carol(35) or Eve(40)
        b_names = set(result.iloc[:, 1])
        assert b_names.issubset({"Carol", "Eve"})

    def test_two_hop_traversal(self, social_star: Star) -> None:
        """Two-hop relationship traversal."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, b.name, c.name",
        )
        assert len(result) > 0

    def test_variable_length_path(self, social_star: Star) -> None:
        """Variable-length path [*1..2]."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN DISTINCT a.name, b.name",
        )
        # Should find both direct and 2-hop connections
        assert len(result) > 5  # More than just direct edges

    def test_cross_type_relationship(self, social_star: Star) -> None:
        """Traverse Person->LIVES_IN->City."""
        result = social_star.execute_query(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name",
        )
        assert len(result) == 5
        # Alice lives in NYC
        alice_row = result[result.iloc[:, 0] == "Alice"]
        assert alice_row.iloc[0, 1] == "NYC"


# ===========================================================================
# Aggregation patterns
# ===========================================================================


class TestAggregation:
    """Aggregation across various clause combinations."""

    def test_count_with_group_by(self, social_star: Star) -> None:
        """GROUP BY via WITH aggregation."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.dept AS dept, count(n) AS cnt",
        )
        assert len(result) == 3  # eng, mktg, sales

    def test_sum_aggregation(self, social_star: Star) -> None:
        """SUM aggregation."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.dept = 'eng' RETURN sum(n.salary) AS total",
        )
        assert result["total"].iloc[0] == 330_000

    def test_avg_aggregation(self, social_star: Star) -> None:
        """AVG aggregation."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN avg(n.age) AS avgAge",
        )
        # (30 + 25 + 35 + 28 + 40) / 5 = 31.6
        assert abs(result["avgAge"].iloc[0] - 31.6) < 0.1

    def test_min_max_aggregation(self, social_star: Star) -> None:
        """MIN and MAX aggregation."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN min(n.age) AS youngest, max(n.age) AS oldest",
        )
        assert result["youngest"].iloc[0] == 25
        assert result["oldest"].iloc[0] == 40

    def test_collect_aggregation(self, social_star: Star) -> None:
        """COLLECT aggregation."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.dept = 'eng' RETURN collect(n.name) AS engineers",
        )
        engineers = result["engineers"].iloc[0]
        assert set(engineers) == {"Alice", "Carol", "Eve"}


# ===========================================================================
# OPTIONAL MATCH patterns
# ===========================================================================


class TestOptionalMatch:
    """OPTIONAL MATCH interaction with other clauses."""

    def test_optional_match_with_null(self, social_star: Star) -> None:
        """OPTIONAL MATCH produces null for unmatched patterns."""
        result = social_star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(friend:Person) "
            "RETURN p.name, friend.name",
        )
        # All 5 persons should appear, some with null friends
        assert len(result) >= 5


# ===========================================================================
# UNWIND patterns
# ===========================================================================


class TestUnwind:
    """UNWIND clause interaction tests."""

    def test_unwind_literal_list(self, social_star: Star) -> None:
        """UNWIND a literal list."""
        result = social_star.execute_query("UNWIND [1, 2, 3] AS x RETURN x")
        assert len(result) == 3
        assert set(result.iloc[:, 0]) == {1, 2, 3}

    def test_unwind_with_match(self, social_star: Star) -> None:
        """UNWIND combined with MATCH."""
        result = social_star.execute_query(
            "UNWIND ['Alice', 'Bob'] AS name "
            "MATCH (n:Person) WHERE n.name = name "
            "RETURN n.name, n.age",
        )
        assert len(result) == 2
        names = set(result.iloc[:, 0])
        assert names == {"Alice", "Bob"}


# ===========================================================================
# RETURN clause variants
# ===========================================================================


class TestReturnVariants:
    """Various RETURN clause features."""

    def test_return_distinct(self, social_star: Star) -> None:
        """RETURN DISTINCT removes duplicates."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN DISTINCT n.dept",
        )
        assert len(result) == 3

    def test_return_order_by(self, social_star: Star) -> None:
        """RETURN with ORDER BY."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name",
        )
        names = list(result.iloc[:, 0])
        assert names == sorted(names)

    def test_return_order_by_desc(self, social_star: Star) -> None:
        """RETURN with ORDER BY DESC."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.age ORDER BY n.age DESC",
        )
        ages = list(result.iloc[:, 0])
        assert ages == sorted(ages, reverse=True)

    def test_return_limit(self, social_star: Star) -> None:
        """RETURN with LIMIT."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.name LIMIT 2",
        )
        assert len(result) == 2

    def test_return_skip(self, social_star: Star) -> None:
        """RETURN with SKIP."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 2",
        )
        assert len(result) == 3  # 5 - 2 = 3

    def test_return_skip_limit(self, social_star: Star) -> None:
        """RETURN with SKIP + LIMIT for pagination."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 1 LIMIT 2",
        )
        assert len(result) == 2

    def test_return_expression(self, social_star: Star) -> None:
        """RETURN with computed expression."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age + 10 AS agePlus10",
        )
        assert result["agePlus10"].iloc[0] == 40

    def test_return_multiple_columns(self, social_star: Star) -> None:
        """RETURN multiple columns."""
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.name, n.age, n.dept",
        )
        assert len(result.columns) == 3
        assert len(result) == 5


# ===========================================================================
# Complex multi-clause combinations
# ===========================================================================


class TestComplexCombinations:
    """Complex multi-clause scenarios."""

    def test_match_with_aggregation_where_return(
        self,
        social_star: Star,
    ) -> None:
        """MATCH + WITH (aggregation) + WHERE + RETURN pipeline."""
        result = social_star.execute_query(
            "MATCH (n:Person) "
            "WITH n.dept AS dept, count(n) AS cnt "
            "WHERE cnt > 1 "
            "RETURN dept, cnt",
        )
        # Only eng(3) has count > 1
        assert len(result) == 1
        assert result["dept"].iloc[0] == "eng"
        assert result["cnt"].iloc[0] == 3

    def test_match_relationship_aggregation(self, social_star: Star) -> None:
        """Count outgoing relationships per person."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name, count(b) AS friends "
            "ORDER BY friends DESC",
        )
        # Alice has 2 outgoing KNOWS, Bob has 1, Carol has 1, Dave has 1
        alice_row = result[result.iloc[:, 0] == "Alice"]
        assert alice_row["friends"].iloc[0] == 2

    def test_multi_match_cross_type(self, social_star: Star) -> None:
        """Multi-pattern MATCH across entity types."""
        result = social_star.execute_query(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) WHERE c.name = 'NYC' RETURN p.name",
        )
        # Alice and Carol live in NYC
        names = set(result.iloc[:, 0])
        assert names == {"Alice", "Carol"}

    def test_function_in_where(self, social_star: Star) -> None:
        """Function call in WHERE clause."""
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE toUpper(n.name) = 'ALICE' RETURN n.name",
        )
        assert len(result) == 1
        assert result.iloc[0, 0] == "Alice"

    def test_case_in_return(self, social_star: Star) -> None:
        """CASE expression in RETURN."""
        result = social_star.execute_query(
            "MATCH (n:Person) "
            "RETURN n.name, "
            "CASE WHEN n.age >= 35 THEN 'senior' ELSE 'junior' END AS level "
            "ORDER BY n.name",
        )
        alice_row = result[result.iloc[:, 0] == "Alice"]
        assert alice_row["level"].iloc[0] == "junior"
        carol_row = result[result.iloc[:, 0] == "Carol"]
        assert carol_row["level"].iloc[0] == "senior"
