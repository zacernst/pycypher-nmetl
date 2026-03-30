"""Tests for ORDER BY, LIMIT, SKIP, and DISTINCT support in RETURN and WITH clauses.

TDD red phase — these tests verify that projection modifiers are actually applied
during query execution (not just parsed and silently ignored).
"""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture
def people_context() -> Context:
    """Five people with distinct names, ages, and departments."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Charlie", "Alice", "Eve", "Bob", "Dave"],
            "age": [30, 25, 35, 20, 28],
            "dept": ["Eng", "Sales", "Eng", "Eng", "Sales"],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "dept"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
        },
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ─────────────────────────────────────────────────────────────────────────────
# LIMIT
# ─────────────────────────────────────────────────────────────────────────────


class TestReturnLimit:
    def test_limit_returns_exactly_n_rows(
        self,
        people_context: Context,
    ) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name LIMIT 2")
        assert len(result) == 2

    def test_limit_zero_returns_empty(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name LIMIT 0")
        assert len(result) == 0

    def test_limit_larger_than_data_returns_all(
        self,
        people_context: Context,
    ) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name LIMIT 100")
        assert len(result) == 5

    def test_limit_preserves_column(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name LIMIT 3")
        assert "name" in result.columns


# ─────────────────────────────────────────────────────────────────────────────
# SKIP
# ─────────────────────────────────────────────────────────────────────────────


class TestReturnSkip:
    def test_skip_removes_first_n_rows(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name SKIP 2")
        assert len(result) == 3

    def test_skip_zero_returns_all(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name SKIP 0")
        assert len(result) == 5

    def test_skip_larger_than_data_returns_empty(
        self,
        people_context: Context,
    ) -> None:
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name SKIP 100")
        assert len(result) == 0


# ─────────────────────────────────────────────────────────────────────────────
# SKIP + LIMIT combined
# ─────────────────────────────────────────────────────────────────────────────


class TestReturnSkipLimit:
    def test_skip_and_limit_combined(self, people_context: Context) -> None:
        """ORDER BY age ASC → [20,25,28,30,35]; SKIP 1 LIMIT 2 → [25,28]."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age ORDER BY p.age ASC SKIP 1 LIMIT 2",
        )
        assert len(result) == 2
        ages = result["age"].tolist()
        assert ages == [25, 28]

    def test_skip_all_then_limit(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name SKIP 5 LIMIT 2",
        )
        assert len(result) == 0


# ─────────────────────────────────────────────────────────────────────────────
# ORDER BY
# ─────────────────────────────────────────────────────────────────────────────


class TestReturnOrderBy:
    def test_order_by_ascending(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age ASC",
        )
        ages = result["age"].tolist()
        assert ages == sorted(ages)

    def test_order_by_descending(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC",
        )
        ages = result["age"].tolist()
        assert ages == sorted(ages, reverse=True)

    def test_order_by_string_asc(self, people_context: Context) -> None:
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name ASC",
        )
        names = result["name"].tolist()
        assert names == sorted(names)

    def test_order_by_default_is_ascending(
        self,
        people_context: Context,
    ) -> None:
        """ORDER BY without ASC/DESC should default to ascending."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age",
        )
        ages = result["age"].tolist()
        assert ages == sorted(ages)

    def test_order_by_with_limit(self, people_context: Context) -> None:
        """ORDER BY age DESC LIMIT 1 → youngest person not in result, oldest is."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 1",
        )
        assert len(result) == 1
        assert result["age"].iloc[0] == 35  # Eve is oldest

    def test_order_by_non_returned_column(
        self,
        people_context: Context,
    ) -> None:
        """ORDER BY on a property that is NOT in RETURN items."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.age ASC",
        )
        # Result should be names sorted by age: Bob(20), Alice(25), Dave(28), Charlie(30), Eve(35)
        assert result["name"].tolist() == [
            "Bob",
            "Alice",
            "Dave",
            "Charlie",
            "Eve",
        ]


# ─────────────────────────────────────────────────────────────────────────────
# DISTINCT
# ─────────────────────────────────────────────────────────────────────────────


class TestReturnDistinct:
    def test_distinct_removes_duplicates(
        self,
        people_context: Context,
    ) -> None:
        """Three people in Eng, two in Sales — DISTINCT dept returns 2 rows."""
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN DISTINCT p.dept")
        assert len(result) == 2
        assert set(result["dept"].tolist()) == {"Eng", "Sales"}

    def test_distinct_no_duplicates_unchanged(
        self,
        people_context: Context,
    ) -> None:
        """When all values are unique, DISTINCT makes no difference."""
        star = Star(context=people_context)
        result = star.execute_query("MATCH (p:Person) RETURN DISTINCT p.name")
        assert len(result) == 5

    def test_distinct_with_limit(self, people_context: Context) -> None:
        """DISTINCT then LIMIT applies LIMIT after dedup."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN DISTINCT p.dept LIMIT 1",
        )
        assert len(result) == 1


# ─────────────────────────────────────────────────────────────────────────────
# WITH clause modifiers
# ─────────────────────────────────────────────────────────────────────────────


class TestWithModifiers:
    def test_with_limit_restricts_subsequent_return(
        self,
        people_context: Context,
    ) -> None:
        """WITH LIMIT 2 should pass only 2 rows to subsequent RETURN."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name LIMIT 2 RETURN name",
        )
        assert len(result) == 2

    def test_with_order_by_then_limit(self, people_context: Context) -> None:
        """WITH ORDER BY age ASC LIMIT 1 → youngest person (Bob, age 20)."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, p.age AS age ORDER BY age ASC LIMIT 1 "
            "RETURN name, age",
        )
        assert len(result) == 1
        assert result["age"].iloc[0] == 20
        assert result["name"].iloc[0] == "Bob"

    def test_with_distinct_deduplicates(self, people_context: Context) -> None:
        """WITH DISTINCT dept should pass only unique depts to RETURN."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH DISTINCT p.dept AS dept RETURN dept",
        )
        assert len(result) == 2
        assert set(result["dept"].tolist()) == {"Eng", "Sales"}

    def test_with_skip(self, people_context: Context) -> None:
        """WITH SKIP 3 should pass last 2 rows to RETURN."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name SKIP 3 RETURN name",
        )
        assert len(result) == 2


# ─────────────────────────────────────────────────────────────────────────────
# ORDER BY with aggregated aliases
# ─────────────────────────────────────────────────────────────────────────────


class TestOrderByAggregatedAliases:
    """ORDER BY applied to aggregated aliases computed in RETURN or WITH."""

    def test_return_order_by_count_star_desc(
        self,
        people_context: Context,
    ) -> None:
        """RETURN dept, count(*) AS n ORDER BY n DESC should rank larger group first."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS n ORDER BY n DESC",
        )
        # Eng has 3 people, Sales has 2
        assert result["dept"].iloc[0] == "Eng"
        assert result["n"].iloc[0] == 3
        assert result["dept"].iloc[1] == "Sales"
        assert result["n"].iloc[1] == 2

    def test_return_order_by_sum_desc(self, people_context: Context) -> None:
        """RETURN dept, sum(age) AS total ORDER BY total DESC — Eng > Sales."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.dept AS dept, sum(p.age) AS total ORDER BY total DESC",
        )
        # Eng: 30+35+20=85  Sales: 25+28=53
        assert result["dept"].iloc[0] == "Eng"
        assert result["total"].iloc[0] == 85
        assert result["dept"].iloc[1] == "Sales"
        assert result["total"].iloc[1] == 53

    def test_return_order_by_avg_asc(self, people_context: Context) -> None:
        """RETURN dept, avg(age) AS avg_age ORDER BY avg_age ASC — Sales first."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.dept AS dept, avg(p.age) AS avg_age ORDER BY avg_age ASC",
        )
        # Sales avg: (25+28)/2 = 26.5   Eng avg: (30+35+20)/3 ≈ 28.33
        assert result["dept"].iloc[0] == "Sales"
        assert result["dept"].iloc[1] == "Eng"

    def test_return_order_by_sum_asc_limit(
        self,
        people_context: Context,
    ) -> None:
        """ORDER BY sum ASC LIMIT 1 should return the dept with the smallest sum."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.dept AS dept, sum(p.age) AS total ORDER BY total ASC LIMIT 1",
        )
        assert len(result) == 1
        assert result["dept"].iloc[0] == "Sales"
        assert result["total"].iloc[0] == 53

    def test_with_order_by_aggregated_alias_feeds_return(
        self,
        people_context: Context,
    ) -> None:
        """WITH dept, count(*) AS n ORDER BY n DESC LIMIT 1 → only top group returned."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.dept AS dept, count(*) AS n ORDER BY n DESC LIMIT 1 "
            "RETURN dept, n",
        )
        assert len(result) == 1
        assert result["dept"].iloc[0] == "Eng"
        assert result["n"].iloc[0] == 3

    def test_with_order_by_avg_alias_feeds_return_all_rows(
        self,
        people_context: Context,
    ) -> None:
        """WITH dept, avg(age) AS avg_age ORDER BY avg_age DESC feeds RETURN correctly."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.dept AS dept, avg(p.age) AS avg_age ORDER BY avg_age DESC "
            "RETURN dept, avg_age",
        )
        # Two groups; Eng avg ≈ 28.33 > Sales avg 26.5 → Eng first
        assert len(result) == 2
        assert result["dept"].iloc[0] == "Eng"
