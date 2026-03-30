"""TDD tests for the multi_query_executor module.

Sprint 6, Phase 4.2: MultiQueryExecutor — the user-facing API that
orchestrates all multi-query components into a single entry point.

RED phase: API contracts, input validation integration, component
orchestration, and error handling.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# API contract tests
# ---------------------------------------------------------------------------


class TestMultiQueryExecutorAPIContract:
    """MultiQueryExecutor must expose the main API."""

    def test_executor_has_execute_multi_query_method(self) -> None:
        """MultiQueryExecutor exposes execute_multi_query()."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        assert callable(executor.execute_multi_query)

    def test_executor_has_validate_method(self) -> None:
        """MultiQueryExecutor exposes validate()."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        assert callable(executor.validate)

    def test_executor_has_analyze_method(self) -> None:
        """MultiQueryExecutor exposes analyze()."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        assert callable(executor.analyze)

    def test_executor_has_combine_method(self) -> None:
        """MultiQueryExecutor exposes combine()."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        assert callable(executor.combine)


# ---------------------------------------------------------------------------
# Input validation integration tests
# ---------------------------------------------------------------------------


class TestInputValidationIntegration:
    """Executor must validate inputs before processing."""

    def test_reject_empty_query_string(self) -> None:
        """Empty Cypher string raises ValueError."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        with pytest.raises(ValueError, match=r"[Vv]alid"):
            executor.execute_multi_query([("q1", "")], star)

    def test_reject_duplicate_ids(self) -> None:
        """Duplicate query IDs raise ValueError."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        with pytest.raises(ValueError, match=r"[Dd]uplicate"):
            executor.execute_multi_query(
                [
                    ("q1", "CREATE (n:Person {name: 'Alice'})"),
                    ("q1", "MATCH (n:Person) RETURN n.name"),
                ],
                star,
            )


# ---------------------------------------------------------------------------
# Validation-only mode tests
# ---------------------------------------------------------------------------


class TestValidationOnlyMode:
    """Executor.validate() checks inputs without executing."""

    def test_validate_valid_queries(self) -> None:
        """Valid queries pass validation."""
        from pycypher.input_validator import InputValidationResult
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        result = executor.validate(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        assert isinstance(result, InputValidationResult)
        assert result.is_valid

    def test_validate_invalid_queries(self) -> None:
        """Invalid queries fail validation."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        result = executor.validate([("q1", "")])
        assert not result.is_valid


# ---------------------------------------------------------------------------
# Analysis-only mode tests
# ---------------------------------------------------------------------------


class TestAnalysisOnlyMode:
    """Executor.analyze() returns dependency graph without executing."""

    def test_analyze_returns_dependency_graph(self) -> None:
        """analyze() returns a DependencyGraph."""
        from pycypher.multi_query_analyzer import DependencyGraph
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        graph = executor.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        assert isinstance(graph, DependencyGraph)
        assert len(graph.nodes) == 2

    def test_analyze_detects_dependencies(self) -> None:
        """analyze() correctly identifies dependencies."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        graph = executor.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        q2 = next(n for n in graph.nodes if n.query_id == "q2")
        assert "q1" in q2.dependencies


# ---------------------------------------------------------------------------
# Combination-only mode tests
# ---------------------------------------------------------------------------


class TestCombinationOnlyMode:
    """Executor.combine() returns combined Cypher without executing."""

    def test_combine_returns_string(self) -> None:
        """combine() returns a Cypher string."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        combined = executor.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        assert isinstance(combined, str)
        assert "CREATE" in combined
        assert "MATCH" in combined

    def test_combine_inserts_with_star(self) -> None:
        """combine() inserts WITH * between queries."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        combined = executor.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        assert "WITH *" in combined


# ---------------------------------------------------------------------------
# Execution tests (independent CREATEs — no shadow layer issue)
# ---------------------------------------------------------------------------


class TestExecution:
    """Executor must produce correct results for supported patterns."""

    def test_execute_independent_creates(self) -> None:
        """Independent CREATEs execute correctly."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        executor.execute_multi_query(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
            ],
            star,
        )
        # Verify both entities were created
        persons = star.execute_query(
            "MATCH (p:Person) RETURN p.name",
        )
        companies = star.execute_query(
            "MATCH (c:Company) RETURN c.name",
        )
        assert len(persons) == 1
        assert len(companies) == 1

    def test_execute_returns_dataframe(self) -> None:
        """execute_multi_query() returns a DataFrame."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        result = executor.execute_multi_query(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ],
            star,
        )
        assert isinstance(result, pd.DataFrame)

    def test_execute_empty_list_returns_empty_dataframe(self) -> None:
        """Empty query list returns empty DataFrame."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        result = executor.execute_multi_query([], star)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
