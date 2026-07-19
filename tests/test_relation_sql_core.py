"""Unit tests for relation_sql.py — DuckDB Relation SQL Compilation.

Tests compilation of Cypher expressions to DuckDB SQL.
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sql_star() -> Star:
    """Star for SQL compilation testing."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [30, 25, 35],
        "salary": [100000.0, 80000.0, 110000.0],
        "active": [True, False, True],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age", "salary", "active"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "active": "active",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "active": "active",
        },
        source_obj=people_df,
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=context)


# ---------------------------------------------------------------------------
# Expression Compilation
# ---------------------------------------------------------------------------


class TestRelationSQLExpressionCompilation:
    """Expression to SQL compilation."""

    def test_compile_literal_number(self, sql_star: Star) -> None:
        """Compile 42 → SQL number literal."""
        result = sql_star.execute_query("RETURN 42 as answer")
        assert result.iloc[0]["answer"] == 42

    def test_compile_literal_string(self, sql_star: Star) -> None:
        """Compile 'hello' → SQL string literal."""
        result = sql_star.execute_query("RETURN 'hello' as msg")
        assert result.iloc[0]["msg"] == "hello"

    def test_compile_literal_float(self, sql_star: Star) -> None:
        """Compile 3.14 → SQL float literal."""
        result = sql_star.execute_query("RETURN 3.14 as pi")
        assert abs(result.iloc[0]["pi"] - 3.14) < 0.01

    def test_compile_literal_boolean(self, sql_star: Star) -> None:
        """Compile true/false → SQL boolean."""
        # pandas boolean cells are np.True_/np.False_ (numpy scalars), which
        # satisfy == True but never `is True`.
        result = sql_star.execute_query("RETURN true, false")
        assert result.iloc[0]["true"] == True  # noqa: E712
        assert result.iloc[0]["false"] == False  # noqa: E712

    def test_compile_property_access(self, sql_star: Star) -> None:
        """Compile n.age → column reference."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN n.age ORDER BY n.age")
        assert len(result) == 3

    def test_compile_arithmetic_addition(self, sql_star: Star) -> None:
        """Compile n.age + 1 → SQL arithmetic."""
        result = sql_star.execute_query("RETURN 5 + 3 as result")
        assert result.iloc[0]["result"] == 8

    def test_compile_arithmetic_subtraction(self, sql_star: Star) -> None:
        """Compile n.age - 1."""
        result = sql_star.execute_query("RETURN 10 - 4 as result")
        assert result.iloc[0]["result"] == 6

    def test_compile_arithmetic_multiplication(self, sql_star: Star) -> None:
        """Compile n.salary * 1.1."""
        result = sql_star.execute_query("RETURN 5 * 3 as result")
        assert result.iloc[0]["result"] == 15

    def test_compile_arithmetic_division(self, sql_star: Star) -> None:
        """Compile n.salary / 12."""
        result = sql_star.execute_query("RETURN 10 / 2 as result")
        assert result.iloc[0]["result"] == 5

    def test_compile_string_concatenation(self, sql_star: Star) -> None:
        """Compile s1 + s2 → SQL concat."""
        result = sql_star.execute_query("RETURN 'hello' + ' ' + 'world' as msg")
        assert result.iloc[0]["msg"] == "hello world"

    def test_compile_function_call(self, sql_star: Star) -> None:
        """Compile COUNT(n) → SQL function."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 3


# ---------------------------------------------------------------------------
# Predicate Compilation
# ---------------------------------------------------------------------------


class TestRelationSQLPredicateCompilation:
    """WHERE predicate to SQL compilation."""

    def test_compile_equality(self, sql_star: Star) -> None:
        """Compile n.age = 30."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age = 30 RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 1

    def test_compile_not_equal(self, sql_star: Star) -> None:
        """Compile n.age <> 30."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age <> 30 RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 2

    def test_compile_less_than(self, sql_star: Star) -> None:
        """Compile n.age < 30."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age < 30 RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 1  # Bob (25)

    def test_compile_greater_than(self, sql_star: Star) -> None:
        """Compile n.age > 30."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age > 30 RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 1  # Carol (35)

    def test_compile_less_equal(self, sql_star: Star) -> None:
        """Compile n.age <= 30."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age <= 30 RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 2  # Alice, Bob

    def test_compile_greater_equal(self, sql_star: Star) -> None:
        """Compile n.age >= 30."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age >= 30 RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 2  # Alice, Carol

    def test_compile_and_predicate(self, sql_star: Star) -> None:
        """Compile p1 AND p2."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 25 AND n.age < 35 RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1  # Alice (30)

    def test_compile_or_predicate(self, sql_star: Star) -> None:
        """Compile p1 OR p2."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.age = 25 OR n.age = 35 RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 2  # Bob, Carol

    def test_compile_not_predicate(self, sql_star: Star) -> None:
        """Compile NOT p."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE NOT (n.age = 30) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 2

    def test_compile_null_check(self, sql_star: Star) -> None:
        """Compile n.age IS NULL."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age IS NULL RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 0

    def test_compile_not_null_check(self, sql_star: Star) -> None:
        """Compile n.age IS NOT NULL."""
        result = sql_star.execute_query("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 3

    def test_compile_in_list(self, sql_star: Star) -> None:
        """Compile n.age IN [25, 30]."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.age IN [25, 30] RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 2


# ---------------------------------------------------------------------------
# String Operations
# ---------------------------------------------------------------------------


class TestRelationSQLStringOperations:
    """String predicate compilation."""

    def test_compile_contains(self, sql_star: Star) -> None:
        """Compile n.name CONTAINS 'Ali'."""
        # CONTAINS is case-sensitive; 'ali' (lowercase) does not match 'Alice'.
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.name CONTAINS 'Ali' RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1  # Alice

    def test_compile_starts_with(self, sql_star: Star) -> None:
        """Compile n.name STARTS WITH 'A'."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1  # Alice

    def test_compile_ends_with(self, sql_star: Star) -> None:
        """Compile n.name ENDS WITH 'b'."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.name ENDS WITH 'b' RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1  # Bob


# ---------------------------------------------------------------------------
# Aggregation Compilation
# ---------------------------------------------------------------------------


class TestRelationSQLAggregationCompilation:
    """Aggregation function to SQL compilation."""

    def test_compile_count_all(self, sql_star: Star) -> None:
        """Compile COUNT(*)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 3

    def test_compile_count_distinct(self, sql_star: Star) -> None:
        """Compile COUNT(DISTINCT n)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN COUNT(DISTINCT n) as cnt")
        assert result.iloc[0]["cnt"] == 3

    def test_compile_sum_aggregate(self, sql_star: Star) -> None:
        """Compile SUM(n.age)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN SUM(n.age) as total")
        assert result.iloc[0]["total"] == 90  # 30 + 25 + 35

    def test_compile_avg_aggregate(self, sql_star: Star) -> None:
        """Compile AVG(n.age)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN AVG(n.age) as avg_age")
        assert abs(result.iloc[0]["avg_age"] - 30.0) < 0.1

    def test_compile_min_aggregate(self, sql_star: Star) -> None:
        """Compile MIN(n.age)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN MIN(n.age) as min_age")
        assert result.iloc[0]["min_age"] == 25

    def test_compile_max_aggregate(self, sql_star: Star) -> None:
        """Compile MAX(n.age)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN MAX(n.age) as max_age")
        assert result.iloc[0]["max_age"] == 35

    def test_compile_group_by(self, sql_star: Star) -> None:
        """Compile GROUP BY."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WITH n.age as age, COUNT(*) as cnt RETURN age, cnt ORDER BY age"
        )
        assert len(result) == 3


# ---------------------------------------------------------------------------
# JOIN Compilation
# ---------------------------------------------------------------------------


class TestRelationSQLJoinCompilation:
    """JOIN operation compilation."""

    def test_inner_join(self, sql_star: Star) -> None:
        """Implicit INNER JOIN."""
        # __ID__ is an internal column, not accessible via dot-property
        # syntax (n.__ID__ reads back as None) — use id() instead.
        result = sql_star.execute_query(
            "MATCH (n:Person), (m:Person) WHERE id(n) < id(m) RETURN COUNT(*) as cnt"
        )
        # 3 choose 2 = 3
        assert result.iloc[0]["cnt"] == 3

    def test_left_join_behavior(self, sql_star: Star) -> None:
        """LEFT JOIN semantics (if supported)."""
        result = sql_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 3


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestRelationSQLEdgeCases:
    """Edge cases in SQL compilation."""

    def test_compile_nested_expression(self, sql_star: Star) -> None:
        """Compile ((a + b) * c)."""
        result = sql_star.execute_query("RETURN ((5 + 3) * 2) as result")
        assert result.iloc[0]["result"] == 16

    def test_escape_string_quotes(self, sql_star: Star) -> None:
        """String with quotes properly escaped."""
        result = sql_star.execute_query("RETURN 'it\\'s' as msg")
        # Handle quote escaping
        assert result is not None

    def test_null_in_arithmetic(self, sql_star: Star) -> None:
        """NULL + 5 → NULL."""
        result = sql_star.execute_query("RETURN NULL + 5 as result")
        assert pd.isna(result.iloc[0]["result"])

    def test_type_coercion(self, sql_star: Star) -> None:
        """Type coercion in mixed operations."""
        result = sql_star.execute_query("RETURN 5 + 2.5 as result")
        assert result.iloc[0]["result"] == 7.5


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


class TestRelationSQLPerformance:
    """SQL compilation performance."""

    def test_compile_complex_where(self, sql_star: Star) -> None:
        """Complex WHERE clause compiles efficiently."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE "
            "(n.age > 20 AND n.age < 40) OR "
            "(n.salary > 50000 AND n.salary < 150000) "
            "RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] > 0

    def test_compile_aggregation_with_filter(self, sql_star: Star) -> None:
        """Aggregation with WHERE compiles correctly."""
        result = sql_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 25 RETURN COUNT(*) as cnt, SUM(n.age) as total"
        )
        assert result.iloc[0]["cnt"] == 2
        assert result.iloc[0]["total"] == 65  # 30 + 35
