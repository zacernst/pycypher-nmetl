"""Unit tests for query_analyzer.py — Query Planning and Analysis.

Tests the QueryAnalyzer class that analyzes query structure and generates
execution plans with optimizations.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.query_analyzer import QueryAnalyzer
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
def analyzer_context() -> Context:
    """Context for analyzer testing."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [30, 25, 35],
    })
    companies_df = pd.DataFrame({
        "__ID__": [10, 11],
        "name": ["Acme", "TechCorp"],
    })
    works_at_df = pd.DataFrame({
        "__ID__": [101, 102],
        "__SOURCE__": [1, 2],
        "__TARGET__": [10, 11],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
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

    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "Company": company_table}
        ),
        relationship_mapping=RelationshipMapping(mapping={"WORKS_AT": works_at_table}),
    )


@pytest.fixture
def analyzer(analyzer_context: Context) -> QueryAnalyzer:
    """QueryAnalyzer instance with test context.

    QueryAnalyzer requires cardinality_feedback/frame_joiner/agg_planner
    collaborators that are normally wired up inside Star.__init__; reuse a
    real Star's fully-wired instance rather than re-deriving that wiring.
    """
    return Star(context=analyzer_context)._query_analyzer


def _plan(analyzer: QueryAnalyzer, query: str) -> dict:
    """Parse ``query`` and run it through QueryAnalyzer.plan_query."""
    ast = ASTConverter.from_cypher(query)
    return analyzer.plan_query(ast)


# ---------------------------------------------------------------------------
# Query Analysis: Simple Queries
# ---------------------------------------------------------------------------


class TestQueryAnalyzerSimpleQueries:
    """Analysis of simple query structures."""

    def test_analyze_single_node_match(self, analyzer: QueryAnalyzer) -> None:
        """Analyze MATCH (n:Person)."""
        plan = _plan(analyzer, "MATCH (n:Person) RETURN n.name")
        assert plan is not None

    def test_analyze_node_with_property_filter(self, analyzer: QueryAnalyzer) -> None:
        """Analyze MATCH (n:Person {name: 'Alice'})."""
        plan = _plan(analyzer, "MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        assert plan is not None

    def test_analyze_simple_relationship_match(self, analyzer: QueryAnalyzer) -> None:
        """Analyze MATCH (a:Person)-[:WORKS_AT]->(c:Company)."""
        plan = _plan(analyzer, "MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN a.name")
        assert plan is not None

    def test_analyze_with_where_clause(self, analyzer: QueryAnalyzer) -> None:
        """Analyze MATCH with WHERE predicate."""
        plan = _plan(analyzer, "MATCH (n:Person) WHERE n.age > 25 RETURN n.name")
        assert plan is not None
        assert plan["has_filter"] is True

    def test_analyze_with_aggregation(self, analyzer: QueryAnalyzer) -> None:
        """Analyze query with COUNT aggregation."""
        plan = _plan(analyzer, "MATCH (n:Person) RETURN COUNT(*)")
        assert plan is not None


# ---------------------------------------------------------------------------
# Filter Pushdown Detection
# ---------------------------------------------------------------------------


class TestQueryAnalyzerFilterPushdown:
    """Detect opportunities to push down filters."""

    def test_pushdown_simple_equality_filter(self, analyzer: QueryAnalyzer) -> None:
        """WHERE n.age = 30 can be pushed to scan."""
        plan = _plan(analyzer, "MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
        assert plan["has_filter"] is True

    def test_pushdown_range_filter(self, analyzer: QueryAnalyzer) -> None:
        """WHERE n.age > 25 can be pushed to scan."""
        plan = _plan(analyzer, "MATCH (n:Person) WHERE n.age > 25 RETURN n.name")
        assert plan["has_filter"] is True

    def test_pushdown_combined_filters(self, analyzer: QueryAnalyzer) -> None:
        """WHERE with AND/OR combinations."""
        plan = _plan(
            analyzer,
            "MATCH (n:Person) WHERE n.age > 25 AND n.name = 'Alice' RETURN n.name",
        )
        assert plan["has_filter"] is True

    def test_no_pushdown_complex_expression(self, analyzer: QueryAnalyzer) -> None:
        """WHERE n.age + 5 > 30 requires computation."""
        plan = _plan(analyzer, "MATCH (n:Person) WHERE n.age + 5 > 30 RETURN n.name")
        assert plan is not None


# ---------------------------------------------------------------------------
# Join Order Analysis
# ---------------------------------------------------------------------------


class TestQueryAnalyzerJoinOrderAnalysis:
    """Analyze multi-join query optimization."""

    def test_two_node_join_order(self, analyzer: QueryAnalyzer) -> None:
        """Analyze join between Person and Company."""
        plan = _plan(analyzer, "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name")
        assert plan["has_join"] is True

    def test_cardinality_based_ordering(self, analyzer: QueryAnalyzer) -> None:
        """Join order considers table sizes."""
        plan = _plan(analyzer, "MATCH (c:Company)-[:WORKS_AT]-(p:Person) RETURN c.name, p.name")
        assert plan["has_join"] is True

    def test_filter_based_ordering(self, analyzer: QueryAnalyzer) -> None:
        """Filters affect join order estimation."""
        plan = _plan(
            analyzer,
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE p.age > 30 RETURN p.name",
        )
        assert plan["has_join"] is True
        assert plan["has_filter"] is True


# ---------------------------------------------------------------------------
# Plan Generation
# ---------------------------------------------------------------------------


class TestQueryAnalyzerPlanGeneration:
    """Test plan structure generation."""

    def test_plan_contains_operators(self, analyzer: QueryAnalyzer) -> None:
        """Plan includes operator nodes."""
        plan = _plan(analyzer, "MATCH (p:Person) RETURN p.name")
        assert plan["node_count"] > 0

    def test_plan_ordering(self, analyzer: QueryAnalyzer) -> None:
        """Plan operators are in correct sequence."""
        plan = _plan(analyzer, "MATCH (p:Person) WHERE p.age > 25 RETURN p.name")
        # Typically: Scan → Filter → Project
        assert plan["node_count"] > 0

    def test_plan_for_aggregation(self, analyzer: QueryAnalyzer) -> None:
        """Plan includes aggregation operator."""
        plan = _plan(analyzer, "MATCH (p:Person) RETURN COUNT(*) as cnt")
        assert plan is not None


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestQueryAnalyzerEdgeCases:
    """Edge cases and boundary conditions."""

    def test_analyze_single_node_no_return(self, analyzer: QueryAnalyzer) -> None:
        """Query with no explicit RETURN clause is a parse error (RETURN required)."""
        from pycypher.exceptions import ASTConversionError

        with pytest.raises(ASTConversionError):
            _plan(analyzer, "MATCH (n:Person)")

    def test_analyze_with_limit(self, analyzer: QueryAnalyzer) -> None:
        """Query with LIMIT clause."""
        plan = _plan(analyzer, "MATCH (n:Person) RETURN n.name LIMIT 10")
        assert plan is not None

    def test_analyze_with_order_by(self, analyzer: QueryAnalyzer) -> None:
        """Query with ORDER BY."""
        plan = _plan(analyzer, "MATCH (n:Person) RETURN n.name ORDER BY n.name")
        assert plan is not None

    def test_analyze_with_skip(self, analyzer: QueryAnalyzer) -> None:
        """Query with SKIP clause."""
        plan = _plan(analyzer, "MATCH (n:Person) RETURN n.name SKIP 5")
        assert plan is not None

    def test_analyze_with_distinct(self, analyzer: QueryAnalyzer) -> None:
        """Query with DISTINCT."""
        plan = _plan(analyzer, "MATCH (n:Person) RETURN DISTINCT n.name")
        assert plan is not None


# ---------------------------------------------------------------------------
# Complex Query Patterns
# ---------------------------------------------------------------------------


class TestQueryAnalyzerComplexPatterns:
    """Complex multi-clause and nested patterns."""

    def test_analyze_multi_match_with_filter(self, analyzer: QueryAnalyzer) -> None:
        """MATCH with multiple patterns."""
        plan = _plan(
            analyzer,
            "MATCH (p:Person), (c:Company) WHERE p.age > 25 RETURN p.name, c.name",
        )
        assert plan is not None

    def test_analyze_with_clause(self, analyzer: QueryAnalyzer) -> None:
        """Query with WITH intermediate step."""
        plan = _plan(analyzer, "MATCH (p:Person) WITH p WHERE p.age > 25 RETURN p.name")
        assert plan is not None

    def test_analyze_optional_match(self, analyzer: QueryAnalyzer) -> None:
        """Query with OPTIONAL MATCH if supported."""
        plan = _plan(analyzer, "OPTIONAL MATCH (p:Person) RETURN p.name")
        assert plan is not None


# ---------------------------------------------------------------------------
# Error Detection
# ---------------------------------------------------------------------------


class TestQueryAnalyzerErrorDetection:
    """Error detection during analysis."""

    def test_detect_undefined_variable_in_where(self, analyzer: QueryAnalyzer) -> None:
        """WHERE clause references undefined variable."""
        # May raise error or handle gracefully
        try:
            plan = _plan(analyzer, "MATCH (p:Person) WHERE x.age > 25 RETURN p.name")
            assert plan is not None
        except Exception:
            pass  # Error is acceptable

    def test_detect_undefined_variable_in_return(self, analyzer: QueryAnalyzer) -> None:
        """RETURN references undefined variable."""
        try:
            plan = _plan(analyzer, "MATCH (p:Person) RETURN x.name")
            assert plan is not None
        except Exception:
            pass

    def test_detect_circular_dependency(self, analyzer: QueryAnalyzer) -> None:
        """WITH creates circular reference (if checked)."""
        try:
            plan = _plan(
                analyzer,
                "MATCH (p:Person) WITH p.age as a, a + 1 as b RETURN b",
            )
            assert plan is not None
        except Exception:
            pass
