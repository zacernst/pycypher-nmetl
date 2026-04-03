"""Integration test suite for cross-epic TDD coordination.

Verifies that all feature epics work together seamlessly:
- Backend engine + query execution
- CLI infrastructure + query pipeline
- ML optimization + query planning
- Performance monitoring + execution metrics
- API surface stability across changes

Run with::

    uv run pytest tests/test_tdd_integration_coordinator.py -v

"""

from __future__ import annotations

import importlib
import time
from typing import Any

import pandas as pd
import pytest
from pycypher.star import Star

from tdd_helpers import (
    QueryTestCase,
    assert_no_api_breakage,
    assert_performance_within,
    assert_query_result,
    build_multi_entity_star,
    build_scaled_star,
)


# ---------------------------------------------------------------------------
# Fixture: standard social graph for integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def social_star() -> Star:
    """Standard social graph for integration testing."""
    return build_multi_entity_star(
        entity_specs={
            "Person": {
                "__ID__": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
                "age": [30, 25, 35, 28, 32],
                "dept": ["eng", "mktg", "eng", "sales", "eng"],
                "salary": [100_000, 80_000, 110_000, 90_000, 105_000],
            },
            "Company": {
                "__ID__": [101, 102],
                "name": ["Acme", "Globex"],
                "industry": ["tech", "finance"],
            },
        },
        relationship_specs={
            "KNOWS": {
                "__ID__": [201, 202, 203, 204],
                "__SOURCE__": [1, 2, 3, 1],
                "__TARGET__": [2, 3, 1, 4],
                "since": [2020, 2021, 2019, 2022],
            },
            "WORKS_AT": {
                "__ID__": [301, 302, 303, 304, 305],
                "__SOURCE__": [1, 2, 3, 4, 5],
                "__TARGET__": [101, 102, 101, 102, 101],
            },
        },
    )


# ---------------------------------------------------------------------------
# 1. Core query execution integration
# ---------------------------------------------------------------------------


class TestCoreQueryIntegration:
    """Verify core query execution works across all supported patterns."""

    def test_simple_match_return(self, social_star: Star) -> None:
        """Basic MATCH-RETURN pipeline works."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name"
        )
        assert len(result) == 5
        assert "name" in result.columns

    def test_where_filter(self, social_star: Star) -> None:
        """WHERE clause filters correctly."""
        result = social_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name ORDER BY p.name"
        )
        assert sorted(result["name"].tolist()) == ["Carol", "Eve"]

    def test_aggregation(self, social_star: Star) -> None:
        """Aggregation functions work correctly."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN p.dept, COUNT(p) AS cnt ORDER BY p.dept"
        )
        assert "dept" in result.columns
        assert "cnt" in result.columns
        eng_row = result[result["dept"] == "eng"]
        assert eng_row["cnt"].iloc[0] == 3

    def test_relationship_traversal(self, social_star: Star) -> None:
        """Relationship pattern matching works."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name, b.name"
        )
        assert len(result) == 4  # 4 KNOWS edges

    def test_multi_hop_traversal(self, social_star: Star) -> None:
        """Multi-hop relationship patterns work."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, c.name"
        )
        assert len(result) > 0


class TestQueryTestCaseBuilder:
    """Verify the TDD QueryTestCase helper works correctly."""

    def test_basic_query_test_case(self) -> None:
        """QueryTestCase runs and validates correctly."""
        case = QueryTestCase(
            name="basic_match",
            entities={
                "Person": {
                    "__ID__": [1, 2],
                    "name": ["Alice", "Bob"],
                    "age": [30, 25],
                },
            },
            query="MATCH (p:Person) RETURN p.name ORDER BY p.name",
            expected_columns=["name"],
            expected_rows=[["Alice"], ["Bob"]],
        )
        result = case.run()
        assert len(result) == 2

    def test_empty_result_case(self) -> None:
        """QueryTestCase correctly validates empty results."""
        case = QueryTestCase(
            name="no_match",
            entities={
                "Person": {
                    "__ID__": [1],
                    "name": ["Alice"],
                    "age": [30],
                },
            },
            query="MATCH (p:Person) WHERE p.age > 100 RETURN p.name",
            expected_empty=True,
        )
        result = case.run()
        assert result.empty

    def test_row_count_validation(self) -> None:
        """QueryTestCase validates row count."""
        case = QueryTestCase(
            name="count_check",
            entities={
                "Person": {
                    "__ID__": [1, 2, 3],
                    "name": ["A", "B", "C"],
                    "age": [20, 30, 40],
                },
            },
            query="MATCH (p:Person) RETURN p.name",
            expected_row_count=3,
        )
        case.run()

    def test_performance_constraint(self) -> None:
        """QueryTestCase enforces performance constraint."""
        case = QueryTestCase(
            name="perf_check",
            entities={
                "Person": {
                    "__ID__": [1, 2],
                    "name": ["Alice", "Bob"],
                    "age": [30, 25],
                },
            },
            query="MATCH (p:Person) RETURN p.name",
            max_duration_seconds=5.0,  # generous for CI
        )
        case.run()


# ---------------------------------------------------------------------------
# 2. Performance regression guards
# ---------------------------------------------------------------------------


class TestPerformanceBaseline:
    """Ensure query execution performance doesn't regress."""

    @pytest.fixture
    def scaled_star(self) -> Star:
        """1K node graph for performance testing."""
        return build_scaled_star(n_persons=1000, avg_degree=5)

    def test_simple_match_performance(self, scaled_star: Star) -> None:
        """Simple MATCH on 1K nodes completes within budget."""
        assert_performance_within(
            scaled_star,
            "MATCH (p:Person) RETURN p.name",
            max_seconds=2.0,
            msg="1K_simple_match",
        )

    def test_filter_performance(self, scaled_star: Star) -> None:
        """WHERE filter on 1K nodes completes within budget."""
        assert_performance_within(
            scaled_star,
            "MATCH (p:Person) WHERE p.age > 40 RETURN p.name",
            max_seconds=2.0,
            msg="1K_filter",
        )

    def test_aggregation_performance(self, scaled_star: Star) -> None:
        """Aggregation on 1K nodes completes within budget."""
        assert_performance_within(
            scaled_star,
            "MATCH (p:Person) RETURN p.dept, COUNT(p) AS cnt",
            max_seconds=2.0,
            msg="1K_aggregation",
        )

    def test_relationship_join_performance(self, scaled_star: Star) -> None:
        """Relationship traversal on 1K nodes completes within budget."""
        assert_performance_within(
            scaled_star,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 100",
            max_seconds=5.0,
            msg="1K_relationship",
        )


# ---------------------------------------------------------------------------
# 3. API surface stability
# ---------------------------------------------------------------------------


class TestAPISurface:
    """Ensure public API remains stable across changes."""

    def test_no_api_removals(self) -> None:
        """No public API names have been removed."""
        added, removed = assert_no_api_breakage()
        assert not removed

    def test_core_exports_present(self) -> None:
        """Critical public API exports are present."""
        import pycypher

        critical_names = ["Star", "Context", "EntityTable", "RelationshipTable"]
        for name in critical_names:
            assert hasattr(pycypher, name), f"Missing critical export: {name}"

    def test_star_execute_query_interface(self) -> None:
        """Star.execute_query maintains its interface contract."""
        star = build_multi_entity_star(
            {"Person": {"__ID__": [1], "name": ["Test"], "age": [25]}},
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert isinstance(result, pd.DataFrame)
        assert "name" in result.columns


# ---------------------------------------------------------------------------
# 4. Module import health checks
# ---------------------------------------------------------------------------


class TestModuleHealth:
    """Verify all core modules import cleanly without side effects."""

    @pytest.mark.parametrize(
        "module_path",
        [
            "pycypher.star",
            "pycypher.relational_models",
            "pycypher.grammar_parser",
            "pycypher.query_planner",
            "pycypher.backend_engine",
            "pycypher.scalar_function_evaluator",
            "pycypher.aggregation_evaluator",
            "pycypher.binding_evaluator",
            "pycypher.exceptions",
            "pycypher.semantic_validator",
            "shared.logger",
            "shared.metrics",
            "shared.helpers",
        ],
    )
    def test_module_imports(self, module_path: str) -> None:
        """Module imports without error."""
        mod = importlib.import_module(module_path)
        assert mod is not None


# ---------------------------------------------------------------------------
# 5. Cross-feature integration scenarios
# ---------------------------------------------------------------------------


class TestCrossFeatureIntegration:
    """Integration tests verifying multiple features work together."""

    def test_scalar_functions_in_queries(self, social_star: Star) -> None:
        """Scalar functions work within query execution."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN toUpper(p.name) AS upper_name ORDER BY upper_name"
        )
        assert "upper_name" in result.columns
        assert len(result) == 5
        assert result["upper_name"].iloc[0] == "ALICE"

    def test_multiple_aggregations(self, social_star: Star) -> None:
        """Multiple aggregation functions in single query."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN COUNT(p) AS total, AVG(p.age) AS avg_age"
        )
        assert "total" in result.columns
        assert "avg_age" in result.columns
        assert len(result) == 1
        assert result["total"].iloc[0] == 5

    def test_query_with_limit(self, social_star: Star) -> None:
        """LIMIT clause works with other features."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.salary DESC LIMIT 3"
        )
        assert len(result) == 3

    def test_distinct_values(self, social_star: Star) -> None:
        """DISTINCT works correctly."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN DISTINCT p.dept ORDER BY p.dept"
        )
        assert len(result) == 3  # eng, mktg, sales
