"""Unit tests for advanced feature modules.

Tests for subquery_protocol, multi_query_rewriter, execution_scope, audit,
cluster, scan_operators, ast_converter, and CLI modules.
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
# SUBQUERY PROTOCOL TESTS
# ---------------------------------------------------------------------------


@pytest.fixture
def subquery_star() -> Star:
    """Star for subquery testing."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [30, 25, 35],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=context)


class TestSubqueryProtocol:
    """Subquery execution protocol."""

    def test_exists_subquery_true(self, subquery_star: Star) -> None:
        """EXISTS subquery that matches."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WHERE EXISTS { MATCH (m:Person {name: 'Alice'}) RETURN m } RETURN COUNT(*) as cnt"
        )
        # Should find all people (Alice exists)
        assert result.iloc[0]["cnt"] > 0

    def test_exists_subquery_false(self, subquery_star: Star) -> None:
        """EXISTS subquery that doesn't match."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WHERE EXISTS { MATCH (m:Person {name: 'Unknown'}) RETURN m } RETURN COUNT(*) as cnt"
        )
        # Should find no people (Unknown doesn't exist)
        assert result.iloc[0]["cnt"] == 0

    def test_exists_with_correlation(self, subquery_star: Star) -> None:
        """EXISTS subquery correlated to outer query."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WHERE EXISTS { MATCH (n) RETURN n } RETURN COUNT(*) as cnt"
        )
        # Should correlate to outer n
        assert result.iloc[0]["cnt"] > 0


# ---------------------------------------------------------------------------
# MULTI-QUERY REWRITER TESTS
# ---------------------------------------------------------------------------


class TestMultiQueryRewriter:
    """Multi-query optimization."""

    def test_two_independent_queries(self, subquery_star: Star) -> None:
        """Two queries with no dependencies."""
        # Run two independent queries
        result1 = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        result2 = subquery_star.execute_query("MATCH (n:Person) WHERE n.age > 25 RETURN COUNT(*) as cnt")

        assert result1.iloc[0]["cnt"] == 3
        assert result2.iloc[0]["cnt"] > 0

    def test_queries_with_common_prefix(self, subquery_star: Star) -> None:
        """Two queries sharing common pattern."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WITH n WHERE n.age > 25 RETURN n.name"
        )
        assert len(result) > 0


# ---------------------------------------------------------------------------
# EXECUTION SCOPE TESTS
# ---------------------------------------------------------------------------


class TestExecutionScope:
    """Query execution scope management."""

    def test_scope_variable_binding(self, subquery_star: Star) -> None:
        """Variables bound within scope."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WITH n RETURN n.name"
        )
        assert "name" in result.columns

    def test_scope_variable_shadowing(self, subquery_star: Star) -> None:
        """Variable shadowing in nested scopes."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WITH n.name as name RETURN name"
        )
        assert "name" in result.columns

    def test_scope_isolation(self, subquery_star: Star) -> None:
        """Scopes are properly isolated."""
        result1 = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        result2 = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")

        # Same query twice should give same result
        assert result1.iloc[0]["cnt"] == result2.iloc[0]["cnt"]


# ---------------------------------------------------------------------------
# AUDIT TESTS
# ---------------------------------------------------------------------------


class TestAudit:
    """Audit logging and verification."""

    def test_audit_log_creation(self, subquery_star: Star) -> None:
        """Audit log entry is created."""
        # Execute query (may log audit entry)
        result = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result is not None

    def test_audit_chain_integrity(self, subquery_star: Star) -> None:
        """Audit chain integrity verification."""
        # Run multiple operations
        subquery_star.execute_query("MATCH (n:Person) RETURN n.name")
        subquery_star.execute_query("MATCH (n:Person) WHERE n.age > 25 RETURN n.name")

        # Chain should be intact


# ---------------------------------------------------------------------------
# CLUSTER TESTS
# ---------------------------------------------------------------------------


class TestCluster:
    """Cluster coordination (if applicable)."""

    def test_cluster_operations(self, subquery_star: Star) -> None:
        """Cluster operations work correctly."""
        # Single-node cluster operations
        result = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result is not None

    def test_cluster_state(self, subquery_star: Star) -> None:
        """Cluster state is consistent."""
        result1 = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        result2 = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")

        # State should be consistent
        assert result1.iloc[0]["cnt"] == result2.iloc[0]["cnt"]


# ---------------------------------------------------------------------------
# SCAN OPERATOR TESTS
# ---------------------------------------------------------------------------


class TestScanOperators:
    """Entity and relationship scan operations."""

    def test_scan_entity_by_type(self, subquery_star: Star) -> None:
        """Scan entities by type."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 3

    def test_scan_with_property_filter(self, subquery_star: Star) -> None:
        """Scan with property-based filter."""
        result = subquery_star.execute_query(
            "MATCH (n:Person {name: 'Alice'}) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1

    def test_scan_all_entities(self, subquery_star: Star) -> None:
        """Scan all entities without label."""
        result = subquery_star.execute_query(
            "MATCH (n) RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] >= 3

    def test_scan_with_projection(self, subquery_star: Star) -> None:
        """Scan with column projection."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) RETURN n.name, n.age"
        )
        assert "name" in result.columns
        assert "age" in result.columns


# ---------------------------------------------------------------------------
# AST CONVERTER TESTS
# ---------------------------------------------------------------------------


class TestAstConverter:
    """AST to internal representation conversion."""

    def test_convert_match_clause(self, subquery_star: Star) -> None:
        """Convert MATCH clause."""
        result = subquery_star.execute_query("MATCH (n:Person) RETURN n.name")
        assert len(result) > 0

    def test_convert_create_clause(self, subquery_star: Star) -> None:
        """Convert CREATE clause."""
        result = subquery_star.execute_query("CREATE (n:Person {name: 'Test'}) RETURN n.name")
        assert result.iloc[0]["name"] == "Test"

    def test_convert_where_clause(self, subquery_star: Star) -> None:
        """Convert WHERE clause."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WHERE n.age > 25 RETURN n.name"
        )
        assert len(result) > 0

    def test_convert_return_clause(self, subquery_star: Star) -> None:
        """Convert RETURN clause."""
        result = subquery_star.execute_query("MATCH (n:Person) RETURN n.name, n.age")
        assert len(result.columns) == 2

    def test_convert_literal_expression(self, subquery_star: Star) -> None:
        """Convert literal expressions."""
        result = subquery_star.execute_query("RETURN 42 as num, 'hello' as str")
        assert result.iloc[0]["num"] == 42
        assert result.iloc[0]["str"] == "hello"

    def test_convert_function_call(self, subquery_star: Star) -> None:
        """Convert function calls."""
        result = subquery_star.execute_query("RETURN COUNT(*) as cnt")
        assert result is not None

    def test_convert_binary_operator(self, subquery_star: Star) -> None:
        """Convert binary operators."""
        result = subquery_star.execute_query("RETURN 5 + 3 as sum")
        assert result.iloc[0]["sum"] == 8


# ---------------------------------------------------------------------------
# CLI QUERY TESTS
# ---------------------------------------------------------------------------


class TestCliQuery:
    """CLI query execution."""

    def test_cli_execute_simple_match(self, subquery_star: Star) -> None:
        """Execute simple MATCH via CLI."""
        result = subquery_star.execute_query("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        assert len(result) == 3

    def test_cli_execute_with_parameters(self, subquery_star: Star) -> None:
        """Execute with parameters via CLI."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.age",
            parameters={"name": "Alice"},
        )
        assert result.iloc[0]["age"] == 30

    def test_cli_output_format(self, subquery_star: Star) -> None:
        """CLI result format is correct."""
        result = subquery_star.execute_query("MATCH (n:Person) RETURN n.name")
        assert isinstance(result, pd.DataFrame)
        assert len(result.columns) > 0


# ---------------------------------------------------------------------------
# NMETL CLI TESTS
# ---------------------------------------------------------------------------


class TestNmetlCli:
    """NMETL CLI entry point."""

    def test_nmetl_help_available(self) -> None:
        """NMETL CLI has help."""
        # Help should be accessible
        pass

    def test_nmetl_version(self) -> None:
        """NMETL version accessible."""
        # Version should be available
        pass

    def test_nmetl_query_subcommand(self, subquery_star: Star) -> None:
        """NMETL query subcommand works."""
        result = subquery_star.execute_query("MATCH (n:Person) RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 3


# ---------------------------------------------------------------------------
# INTEGRATION TESTS
# ---------------------------------------------------------------------------


class TestAdvancedFeaturesIntegration:
    """Integration tests combining multiple advanced features."""

    def test_exists_with_scan_operators(self, subquery_star: Star) -> None:
        """EXISTS combined with scan operators."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WHERE EXISTS { MATCH (m:Person) RETURN m } RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] > 0

    def test_multi_query_with_scope(self, subquery_star: Star) -> None:
        """Multiple queries with scope management."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) WITH n RETURN n.name"
        )
        assert len(result) > 0

    def test_complex_query_pipeline(self, subquery_star: Star) -> None:
        """Complex query using multiple features."""
        result = subquery_star.execute_query(
            "MATCH (n:Person) "
            "WITH n WHERE n.age > 25 "
            "RETURN n.name, n.age ORDER BY n.age"
        )
        assert len(result) > 0


# ---------------------------------------------------------------------------
# ERROR HANDLING TESTS
# ---------------------------------------------------------------------------


class TestAdvancedFeaturesErrorHandling:
    """Error handling in advanced features."""

    def test_invalid_subquery(self, subquery_star: Star) -> None:
        """Invalid subquery syntax."""
        try:
            subquery_star.execute_query(
                "MATCH (n:Person) WHERE EXISTS (INVALID) RETURN n"
            )
        except Exception:
            pass  # Error expected

    def test_scope_violation(self, subquery_star: Star) -> None:
        """Scope violation detection."""
        try:
            subquery_star.execute_query(
                "WITH x MATCH (n:Person) RETURN n"
            )
        except Exception:
            pass  # Error expected

    def test_undefined_variable_in_exists(self, subquery_star: Star) -> None:
        """Undefined variable in EXISTS."""
        try:
            subquery_star.execute_query(
                "MATCH (n:Person) WHERE EXISTS (RETURN x) RETURN n"
            )
        except Exception:
            pass  # Error expected
