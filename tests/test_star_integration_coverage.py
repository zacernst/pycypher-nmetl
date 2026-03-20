"""Integration tests for star.py uncovered execution paths.

Targets the 72 uncovered lines identified in coverage analysis:
- Input validation (empty query, bad parameters)
- shortestPath error branches
- Variable-length path edge cases
- UNION query handling
- FOREACH/MERGE special cases
"""

from __future__ import annotations

import pytest
from pycypher.star import Star


class TestExecuteQueryValidation:
    """Cover lines 265-271: input validation in execute_query."""

    def test_empty_query_string_raises(self, social_star: Star) -> None:
        with pytest.raises(ValueError, match="empty or whitespace"):
            social_star.execute_query("   ")

    def test_parameters_must_be_dict(self, social_star: Star) -> None:
        with pytest.raises(TypeError, match="parameters must be a dict"):
            social_star.execute_query(
                "MATCH (n:Person) RETURN n.name",
                parameters=[1, 2],  # type: ignore[arg-type]
            )

    def test_valid_parameters_dict(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name",
            parameters={"min_age": 30},
        )
        assert len(result) >= 1


class TestShortestPathErrors:
    """Cover lines 608-644: shortestPath validation errors."""

    def test_shortest_path_basic(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH p = shortestPath((a:Person)-[:KNOWS]->(b:Person)) "
            "WHERE a.name = 'Alice' AND b.name = 'Bob' "
            "RETURN a.name, b.name"
        )
        assert len(result) >= 1

    def test_shortest_path_with_path_variable(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH p = shortestPath((a:Person)-[:KNOWS]->(b:Person)) "
            "WHERE a.name = 'Alice' "
            "RETURN a.name, b.name"
        )
        assert len(result) >= 1


class TestVariableLengthPaths:
    """Cover lines 687-691, 742, 747-750, 808-809, 848-854."""

    def test_variable_length_path(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN a.name, b.name"
        )
        assert len(result) >= 1

    def test_variable_length_with_anonymous_rel(
        self, social_star: Star
    ) -> None:
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        assert len(result) >= 1


class TestUnionQueries:
    """Cover lines 1029-1030, 1158, 1187-1188."""

    def test_union_all(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name AS name "
            "UNION ALL "
            "MATCH (n:Person) WHERE n.age < 26 RETURN n.name AS name"
        )
        assert len(result) >= 2

    def test_union_distinct(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.dept = 'eng' RETURN n.dept AS d "
            "UNION "
            "MATCH (n:Person) WHERE n.dept = 'eng' RETURN n.dept AS d"
        )
        # UNION deduplicates
        assert len(result) >= 1


class TestCreateDeleteMerge:
    """Cover MERGE/CREATE/DELETE paths in star.py."""

    def test_create_node(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "CREATE (n:Person {name: 'Eve', age: 22}) RETURN n.name"
        )
        assert len(result) == 1
        assert result.iloc[0, 0] == "Eve"

    def test_merge_existing_node(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MERGE (n:Person {name: 'Alice'}) RETURN n.name"
        )
        assert len(result) >= 1

    def test_merge_new_node(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MERGE (n:Person {name: 'Zara'}) RETURN n.name"
        )
        assert len(result) == 1
        assert result.iloc[0, 0] == "Zara"

    def test_delete_node(self, social_star: Star) -> None:
        social_star.execute_query(
            "MATCH (n:Person) WHERE n.name = 'Dave' DELETE n"
        )
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.name = 'Dave' RETURN n.name"
        )
        assert len(result) == 0

    def test_set_property(self, social_star: Star) -> None:
        social_star.execute_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31"
        )
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age"
        )
        assert result.iloc[0, 0] == 31


class TestForeachClause:
    """Cover lines 1498-1500, 1588, 1609: FOREACH handling."""

    def test_foreach_set(self, social_star: Star) -> None:
        social_star.execute_query(
            "MATCH (n:Person) WHERE n.dept = 'eng' "
            "FOREACH (x IN [1] | SET n.reviewed = true)"
        )
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.reviewed = true RETURN n.name"
        )
        # At least the eng people should be marked
        assert len(result) >= 1


class TestWithReturnEdgeCases:
    """Cover WITH/RETURN edge case branches."""

    def test_with_distinct(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) WITH DISTINCT n.dept AS dept RETURN dept"
        )
        # 3 distinct departments: eng, mktg, sales
        assert len(result) == 3

    def test_with_order_by_skip_limit(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) "
            "WITH n.name AS name ORDER BY name SKIP 1 LIMIT 2 "
            "RETURN name"
        )
        assert len(result) == 2

    def test_with_aggregation(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) WITH n.dept AS dept, count(n) AS cnt RETURN dept, cnt"
        )
        assert len(result) >= 1

    def test_with_multi_key_aggregation(self, social_star: Star) -> None:
        """Cover multi-group-key tuple path (lines 1498-1500)."""
        result = social_star.execute_query(
            "MATCH (n:Person) "
            "WITH n.dept AS dept, n.age > 29 AS senior, count(n) AS cnt "
            "RETURN dept, senior, cnt"
        )
        assert len(result) >= 2

    def test_return_star(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN *"
        )
        assert len(result) == 1

    def test_unwind_with_return(self, social_star: Star) -> None:
        result = social_star.execute_query("UNWIND [1, 2, 3] AS x RETURN x")
        assert len(result) == 3

    def test_standalone_return_expression(self, social_star: Star) -> None:
        result = social_star.execute_query("RETURN 1 + 2 AS val")
        assert result.iloc[0, 0] == 3
