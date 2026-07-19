"""Unit tests for star.py — Core Query Executor Facade.

Tests the main Star class that orchestrates pattern matching, clause execution,
and mutation operations. Focus on public API, error handling, and resource
management.
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
def basic_star() -> Star:
    """Star with Person and KNOWS relationship data."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carol", "Dave"],
        "age": [30, 25, 35, 28],
    })
    knows_df = pd.DataFrame({
        "__ID__": [101, 102, 103],
        "__SOURCE__": [1, 2, 3],
        "__TARGET__": [2, 3, 1],
        "since": [2020, 2021, 2019],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=["__ID__", "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table}),
    )
    return Star(context=context)


@pytest.fixture
def empty_star() -> Star:
    """Star with empty context."""
    context = Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=context)


# ---------------------------------------------------------------------------
# Query Execution: Happy Path
# ---------------------------------------------------------------------------


class TestStarExecuteQueryHappyPath:
    """Basic query execution paths with valid queries."""

    def test_execute_simple_match(self, basic_star: Star) -> None:
        """MATCH single node type."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        assert len(result) == 4
        assert set(result.columns) == {"name"}
        assert list(result["name"]) == ["Alice", "Bob", "Carol", "Dave"]

    def test_execute_match_with_where(self, basic_star: Star) -> None:
        """MATCH with WHERE predicate."""
        result = basic_star.execute_query("MATCH (p:Person) WHERE p.age > 28 RETURN p.name ORDER BY p.name")
        assert len(result) == 2
        names = set(result["name"])
        assert names == {"Alice", "Carol"}

    def test_execute_match_with_return_subset(self, basic_star: Star) -> None:
        """MATCH with specific return columns."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.name")
        assert set(result.columns) == {"name", "age"}
        assert len(result) == 4

    def test_execute_match_relationship(self, basic_star: Star) -> None:
        """MATCH with relationship pattern."""
        result = basic_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name, b.name"
        )
        assert len(result) == 3
        assert set(result.columns) == {"a.name", "b.name"}

    def test_execute_create_node(self, basic_star: Star) -> None:
        """CREATE single node."""
        result = basic_star.execute_query("CREATE (n:Person {name: 'Eve', age: 32}) RETURN n.name")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Eve"

    def test_execute_set_property(self, basic_star: Star) -> None:
        """SET node property."""
        basic_star.execute_query("MATCH (p:Person {name: 'Alice'}) SET p.age = 31")
        result = basic_star.execute_query("MATCH (p:Person {name: 'Alice'}) RETURN p.age")
        assert result.iloc[0]["age"] == 31

    def test_execute_delete_node(self, basic_star: Star) -> None:
        """DELETE node."""
        basic_star.execute_query("MATCH (p:Person {name: 'Dave'}) DELETE p")
        result = basic_star.execute_query("MATCH (p:Person) RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 3

    def test_execute_with_parameters(self, basic_star: Star) -> None:
        """Query with parameterized values."""
        result = basic_star.execute_query(
            "MATCH (p:Person) WHERE p.name = $name RETURN p.age",
            parameters={"name": "Bob"},
        )
        assert len(result) == 1
        assert result.iloc[0]["age"] == 25

    def test_execute_multi_clause_match_with_return(self, basic_star: Star) -> None:
        """MATCH → RETURN sequence."""
        result = basic_star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 2"
        )
        assert len(result) == 2
        assert result.iloc[0]["name"] == "Carol"


# ---------------------------------------------------------------------------
# Query Execution: Edge Cases
# ---------------------------------------------------------------------------


class TestStarExecuteQueryEdgeCases:
    """Edge cases in query execution."""

    def test_execute_empty_result(self, basic_star: Star) -> None:
        """Query returns 0 rows."""
        result = basic_star.execute_query("MATCH (p:Person) WHERE p.age > 100 RETURN p.name")
        assert len(result) == 0

    def test_execute_null_values(self, basic_star: Star) -> None:
        """Properties with NULL values."""
        result = basic_star.execute_query("MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p.name")
        assert all(val is not None for val in result["name"])

    def test_execute_large_result_set(self, basic_star: Star) -> None:
        """Query returns many rows (creates Cartesian product)."""
        result = basic_star.execute_query(
            "MATCH (a:Person), (b:Person) RETURN a.name, b.name"
        )
        assert len(result) == 16  # 4 * 4 Cartesian product

    def test_execute_unicode_properties(self, basic_star: Star) -> None:
        """Properties with Unicode characters."""
        basic_star.execute_query("CREATE (n:Person {name: 'Aliçe', age: 30})")
        result = basic_star.execute_query("MATCH (p:Person {name: 'Aliçe'}) RETURN p.name")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Aliçe"

    def test_execute_limit_skip(self, basic_star: Star) -> None:
        """LIMIT and SKIP clauses."""
        # Grammar order is SKIP before LIMIT.
        result = basic_star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 1 LIMIT 2"
        )
        assert len(result) == 2
        assert list(result["name"]) == ["Bob", "Carol"]

    def test_execute_order_by_multiple_columns(self, basic_star: Star) -> None:
        """ORDER BY with multiple columns."""
        result = basic_star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC, p.name"
        )
        assert len(result) == 4


# ---------------------------------------------------------------------------
# Query Execution: Error Handling
# ---------------------------------------------------------------------------


class TestStarExecuteQueryErrorHandling:
    """Error paths and exception handling."""

    def test_execute_syntax_error(self, basic_star: Star) -> None:
        """Invalid Cypher syntax."""
        with pytest.raises(Exception):  # SyntaxError or similar
            basic_star.execute_query("MATCH (p:Person RETURN p")  # Missing )

    def test_execute_missing_property(self, basic_star: Star) -> None:
        """Reference to non-existent property."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.nonexistent")
        # Property not in schema defaults to None
        assert result is not None

    def test_execute_type_error(self, basic_star: Star) -> None:
        """Type mismatch in operation raises rather than silently coercing."""
        with pytest.raises(TypeError):
            basic_star.execute_query("MATCH (p:Person) WHERE p.name > 10 RETURN p.name")

    def test_execute_missing_parameter(self, basic_star: Star) -> None:
        """Parameter not provided."""
        with pytest.raises(Exception):
            basic_star.execute_query(
                "MATCH (p:Person) WHERE p.name = $missing RETURN p.name"
            )  # No parameters dict

    def test_execute_nonexistent_label(self, empty_star: Star) -> None:
        """Query for entity type that doesn't exist raises GraphTypeNotFoundError."""
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            empty_star.execute_query("MATCH (p:Unknown) RETURN p")


# ---------------------------------------------------------------------------
# Explain Functionality
# ---------------------------------------------------------------------------


class TestStarExplain:
    """Query explanation and plan generation."""

    def test_explain_simple_query(self, basic_star: Star) -> None:
        """EXPLAIN output for simple query."""
        explanation = basic_star.explain_query("MATCH (p:Person) RETURN p.name")
        assert explanation is not None
        assert isinstance(explanation, str)
        assert len(explanation) > 0

    def test_explain_complex_query(self, basic_star: Star) -> None:
        """EXPLAIN output for multi-clause query."""
        explanation = basic_star.explain_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 25 RETURN a.name, b.name"
        )
        assert explanation is not None
        assert "MATCH" in explanation or "Scan" in explanation or "Filter" in explanation

    def test_explain_returns_readable_text(self, basic_star: Star) -> None:
        """EXPLAIN output is human-readable."""
        explanation = basic_star.explain_query("MATCH (p:Person) RETURN p.name")
        assert isinstance(explanation, str)
        # Should not be empty or gibberish
        assert len(explanation) > 10


# ---------------------------------------------------------------------------
# Resource Management
# ---------------------------------------------------------------------------


class TestStarResourceManagement:
    """Proper cleanup and resource handling."""

    def test_star_initialization(self, basic_star: Star) -> None:
        """Star initializes with valid context."""
        assert basic_star.context is not None
        assert basic_star.context.entity_mapping is not None

    def test_star_context_accessibility(self, basic_star: Star) -> None:
        """Context is accessible after initialization."""
        assert "Person" in basic_star.context.entity_mapping.mapping
        assert "KNOWS" in basic_star.context.relationship_mapping.mapping

    def test_multiple_queries_on_same_star(self, basic_star: Star) -> None:
        """Multiple queries can execute on same Star instance."""
        result1 = basic_star.execute_query("MATCH (p:Person) RETURN COUNT(*) as cnt")
        result2 = basic_star.execute_query("MATCH (p:Person) WHERE p.age > 25 RETURN COUNT(*) as cnt")
        result3 = basic_star.execute_query("MATCH (p:Person) RETURN p.name ORDER BY p.name")

        assert result1.iloc[0]["cnt"] == 4
        assert result2.iloc[0]["cnt"] == 3
        assert len(result3) == 4


# ---------------------------------------------------------------------------
# Query Result Format
# ---------------------------------------------------------------------------


class TestStarResultFormat:
    """Verify result DataFrame format."""

    def test_result_is_dataframe(self, basic_star: Star) -> None:
        """Result is a pandas DataFrame."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.name")
        assert isinstance(result, pd.DataFrame)

    def test_result_column_names(self, basic_star: Star) -> None:
        """Result column names match RETURN items."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.name, p.age")
        assert set(result.columns) == {"name", "age"}

    def test_result_row_count(self, basic_star: Star) -> None:
        """Result row count matches expected count."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 4

    def test_result_data_types(self, basic_star: Star) -> None:
        """Result columns have appropriate data types."""
        result = basic_star.execute_query("MATCH (p:Person) RETURN p.name, p.age")
        # Result columns carry object dtype (mixed-type-safe); values
        # themselves are still the correct Python types.
        assert result["name"].dtype == object
        assert all(isinstance(val, (int, float)) for val in result["age"])
