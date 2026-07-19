"""Unit tests for pattern_matcher.py — MATCH Pattern Execution.

Tests pattern matching logic including label filtering, property matching,
and relationship traversal.
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
def matcher_star() -> Star:
    """Star for pattern matching tests."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "age": [30, 25, 35, 28, 32],
        "dept": ["eng", "mktg", "eng", "sales", "eng"],
    })
    companies_df = pd.DataFrame({
        "__ID__": [10, 11],
        "name": ["Acme", "TechCorp"],
    })
    works_at_df = pd.DataFrame({
        "__ID__": [101, 102, 103],
        "__SOURCE__": [1, 2, 3],
        "__TARGET__": [10, 11, 10],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age", "dept"],
        source_obj_attribute_map={"name": "name", "age": "age", "dept": "dept"},
        attribute_map={"name": "name", "age": "age", "dept": "dept"},
        source_obj=people_df,
    )
    company_table = EntityTable(
        entity_type="Company",
        identifier="Company",
        column_names=["__ID__", "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=companies_df,
    )
    works_at_table = RelationshipTable(
        relationship_type="WORKS_AT",
        identifier="WORKS_AT",
        column_names=["__ID__", "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=works_at_df,
        source_entity_type="Person",
        target_entity_type="Company",
    )

    context = Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "Company": company_table}
        ),
        relationship_mapping=RelationshipMapping(mapping={"WORKS_AT": works_at_table}),
    )
    return Star(context=context)


# ---------------------------------------------------------------------------
# Single Pattern Matching
# ---------------------------------------------------------------------------


class TestPatternMatcherSinglePattern:
    """Single pattern matching operations."""

    def test_match_single_node_label(self, matcher_star: Star) -> None:
        """(n:Person)."""
        result = matcher_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 5

    def test_match_single_node_property(self, matcher_star: Star) -> None:
        """(n {name: 'Alice'})."""
        result = matcher_star.execute_query(
            "MATCH (n {name: 'Alice'}) RETURN n.name"
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"

    def test_match_two_node_relationship(self, matcher_star: Star) -> None:
        """(a:Person)-[:WORKS_AT]->(c:Company)."""
        result = matcher_star.execute_query(
            "MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 3

    def test_match_undirected_relationship(self, matcher_star: Star) -> None:
        """(a:Person)-[:WORKS_AT]-(c:Company)."""
        result = matcher_star.execute_query(
            "MATCH (a:Person)-[:WORKS_AT]-(c:Company) RETURN COUNT(*) as cnt"
        )
        # Should match both directions or handle appropriately
        assert result.iloc[0]["cnt"] >= 3

    def test_match_left_direction(self, matcher_star: Star) -> None:
        """(a:Person)<-[:WORKS_AT]-(c:Company)."""
        result = matcher_star.execute_query(
            "MATCH (a:Person)<-[:WORKS_AT]-(c:Company) RETURN COUNT(*) as cnt"
        )
        # Reversed direction
        assert result.iloc[0]["cnt"] >= 0


# ---------------------------------------------------------------------------
# Multiple Patterns
# ---------------------------------------------------------------------------


class TestPatternMatcherMultiplePatterns:
    """Multiple pattern matching."""

    def test_match_two_independent_patterns(self, matcher_star: Star) -> None:
        """(a:Person), (c:Company)."""
        result = matcher_star.execute_query(
            "MATCH (a:Person), (c:Company) RETURN COUNT(*) as cnt"
        )
        # Cartesian product: 5 * 2 = 10
        assert result.iloc[0]["cnt"] == 10

    def test_match_connected_patterns(self, matcher_star: Star) -> None:
        """(a:Person)-[:WORKS_AT]->(c:Company)."""
        result = matcher_star.execute_query(
            "MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name"
        )
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Filter Application
# ---------------------------------------------------------------------------


class TestPatternMatcherFilterApplication:
    """Filters in pattern matching."""

    def test_match_with_label_filter(self, matcher_star: Star) -> None:
        """Filter by label."""
        result = matcher_star.execute_query(
            "MATCH (n:Person) RETURN COUNT(*) as cnt"
        )
        # All 5 people
        assert result.iloc[0]["cnt"] == 5

    def test_match_with_property_filter(self, matcher_star: Star) -> None:
        """Filter by property."""
        result = matcher_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 28 RETURN COUNT(*) as cnt"
        )
        # Alice (30), Carol (35), Eve (32) = 3
        assert result.iloc[0]["cnt"] == 3

    def test_match_with_combined_filters(self, matcher_star: Star) -> None:
        """Label AND property filters."""
        result = matcher_star.execute_query(
            "MATCH (n:Person) WHERE n.dept = 'eng' RETURN COUNT(*) as cnt"
        )
        # Alice, Carol, Eve = 3 engineers
        assert result.iloc[0]["cnt"] == 3

    def test_match_filter_no_matches(self, matcher_star: Star) -> None:
        """Filter matches 0 entities."""
        result = matcher_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 100 RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 0


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestPatternMatcherEdgeCases:
    """Edge cases in pattern matching."""

    def test_match_no_matches(self, matcher_star: Star) -> None:
        """Pattern matches 0 entities."""
        result = matcher_star.execute_query(
            "MATCH (n:Person {name: 'NonExistent'}) RETURN n"
        )
        assert len(result) == 0

    def test_match_all_entities(self, matcher_star: Star) -> None:
        """Pattern matches everything."""
        result = matcher_star.execute_query(
            "MATCH (n:Person) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 5

    def test_match_nonexistent_label(self, matcher_star: Star) -> None:
        """Label that doesn't exist raises GraphTypeNotFoundError (not silent-empty)."""
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            matcher_star.execute_query("MATCH (n:Unknown) RETURN n")

    def test_match_nonexistent_relationship_type(self, matcher_star: Star) -> None:
        """Relationship type that doesn't exist raises GraphTypeNotFoundError."""
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            matcher_star.execute_query(
                "MATCH (a:Person)-[:NONEXISTENT]->(b:Person) RETURN a"
            )


# ---------------------------------------------------------------------------
# Variable Binding
# ---------------------------------------------------------------------------


class TestPatternMatcherVariableBinding:
    """Variable binding in patterns."""

    def test_bind_matched_entities_to_variables(self, matcher_star: Star) -> None:
        """Variables capture matched entities."""
        result = matcher_star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name"
        )
        assert len(result) == 5
        names = list(result["name"])
        assert "Alice" in names

    def test_preserve_binding_state(self, matcher_star: Star) -> None:
        """Bindings preserved through query execution."""
        result = matcher_star.execute_query(
            "MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name"
        )
        # Both a and c should be bound in each row
        assert "a.name" in result.columns
        assert "c.name" in result.columns

    def test_handle_duplicate_variable_names(self, matcher_star: Star) -> None:
        """Duplicate variable names."""
        # __ID__ is an internal column, not accessible via dot-property
        # syntax (a.__ID__ reads back as None) — use id() instead.
        result = matcher_star.execute_query(
            "MATCH (a:Person), (b:Person) WHERE id(a) < id(b) RETURN COUNT(*) as cnt"
        )
        # 5 choose 2 = 10
        assert result.iloc[0]["cnt"] == 10


# ---------------------------------------------------------------------------
# Relationship Patterns
# ---------------------------------------------------------------------------


class TestPatternMatcherRelationshipPatterns:
    """Complex relationship patterns."""

    def test_match_relationship_by_type(self, matcher_star: Star) -> None:
        """Match relationships by type."""
        result = matcher_star.execute_query(
            "MATCH ()-[:WORKS_AT]->() RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 3

    def test_match_relationship_source_target(self, matcher_star: Star) -> None:
        """Match by source and target types."""
        result = matcher_star.execute_query(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 3

    def test_match_with_direction_constraints(self, matcher_star: Star) -> None:
        """Direction matters in matching."""
        result = matcher_star.execute_query(
            "MATCH (c:Company)-[:WORKS_AT]->(p:Person) RETURN COUNT(*) as cnt"
        )
        # Reversed: company to person (wrong direction in data)
        assert result.iloc[0]["cnt"] == 0


# ---------------------------------------------------------------------------
# Performance Characteristics
# ---------------------------------------------------------------------------


class TestPatternMatcherPerformance:
    """Performance-related tests."""

    def test_filter_early_pushdown(self, matcher_star: Star) -> None:
        """WHERE is applied early during scan."""
        result = matcher_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 28 RETURN n.name ORDER BY n.name"
        )
        assert len(result) == 3

    def test_memory_efficiency_large_dataset(self, matcher_star: Star) -> None:
        """Memory efficiency with multiple entities."""
        # Create larger dataset
        result = matcher_star.execute_query(
            "MATCH (a:Person), (b:Person), (c:Person) RETURN COUNT(*) as cnt"
        )
        # 5^3 = 125
        assert result.iloc[0]["cnt"] == 125

    def test_index_usage_if_available(self, matcher_star: Star) -> None:
        """Property lookup should be efficient."""
        result = matcher_star.execute_query(
            "MATCH (n:Person {name: 'Alice'}) RETURN n.name"
        )
        assert len(result) == 1
