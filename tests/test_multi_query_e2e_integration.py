"""End-to-end integration tests for the multi-query composition system.

Sprint 7: Validates the complete pipeline from user input through
dependency analysis, query combination, and execution. Tests cover
realistic ETL workflows, error propagation, and component integration.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Full pipeline E2E tests
# ---------------------------------------------------------------------------


class TestFullPipelineE2E:
    """End-to-end tests through MultiQueryExecutor."""

    def test_independent_creates_e2e(self) -> None:
        """Independent CREATEs produce correct final state."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        executor.execute_multi_query(
            [
                ("create_person", "CREATE (p:Person {name: 'Alice'})"),
                ("create_company", "CREATE (c:Company {name: 'Acme'})"),
            ],
            star,
        )

        persons = star.execute_query(
            "MATCH (p:Person) RETURN p.name",
        )
        companies = star.execute_query(
            "MATCH (c:Company) RETURN c.name",
        )
        assert len(persons) == 1
        assert persons.iloc[0, 0] == "Alice"
        assert len(companies) == 1
        assert companies.iloc[0, 0] == "Acme"

    def test_single_create_e2e(self) -> None:
        """Single CREATE through executor works correctly."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        executor.execute_multi_query(
            [("q1", "CREATE (n:Widget {id: 42})")],
            star,
        )

        result = star.execute_query(
            "MATCH (w:Widget) RETURN w.id",
        )
        assert len(result) == 1
        assert result.iloc[0, 0] == 42

    def test_empty_query_list_e2e(self) -> None:
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


# ---------------------------------------------------------------------------
# Component integration validation
# ---------------------------------------------------------------------------


class TestComponentIntegration:
    """Validate that all components integrate correctly."""

    def test_validate_then_analyze(self) -> None:
        """Validation and analysis produce consistent results."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        queries = [
            ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ("q2", "MATCH (n:Person) RETURN n.name"),
        ]

        # Both should succeed
        validation = executor.validate(queries)
        assert validation.is_valid

        graph = executor.analyze(queries)
        assert len(graph.nodes) == 2

    def test_analyze_then_combine(self) -> None:
        """Analysis and combination produce consistent results."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        queries = [
            ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ("q2", "MATCH (n:Person) RETURN n.name"),
        ]

        graph = executor.analyze(queries)
        # Q2 depends on Q1
        q2 = next(n for n in graph.nodes if n.query_id == "q2")
        assert "q1" in q2.dependencies

        combined = executor.combine(queries)
        assert "CREATE" in combined
        assert "WITH *" in combined
        assert "MATCH" in combined

    def test_combined_query_is_parseable(self) -> None:
        """Combined Cypher string is parseable by ASTConverter."""
        from pycypher.ast_models import ASTConverter
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        combined = executor.combine(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
            ]
        )

        ast = ASTConverter.from_cypher(combined)
        assert ast is not None
        assert len(ast.clauses) > 0

    def test_ast_rewriter_roundtrip(self) -> None:
        """ASTRewriter can serialize and re-parse combined queries."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        merged = rewriter.merge_queries([q1, q2])
        cypher = rewriter.to_cypher(merged)

        # Must be re-parseable
        reparsed = ASTConverter.from_cypher(cypher)
        assert reparsed is not None
        assert len(reparsed.clauses) > 0

    def test_validator_catches_bad_combined_query(self) -> None:
        """CombinedQueryValidator catches malformed ASTs."""
        from pycypher.ast_models import (
            Query,
            Return,
            ReturnItem,
            Variable,
        )
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        # Two RETURN clauses — invalid
        bad = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="n")),
                    ]
                ),
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="m")),
                    ]
                ),
            ],
        )
        result = validator.validate(bad)
        assert not result.is_valid


# ---------------------------------------------------------------------------
# Error propagation tests
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    """Errors must propagate cleanly through the pipeline."""

    def test_invalid_input_raises_valueerror(self) -> None:
        """Invalid inputs raise ValueError before execution."""
        from pycypher.multi_query_executor import MultiQueryExecutor
        from pycypher.relational_models import Context
        from pycypher.star import Star

        executor = MultiQueryExecutor()
        ctx = Context()
        star = Star(context=ctx)

        with pytest.raises(ValueError, match=r"[Vv]alid"):
            executor.execute_multi_query([("q1", "")], star)

    def test_duplicate_ids_raise_valueerror(self) -> None:
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

    def test_circular_dependency_raises_valueerror(self) -> None:
        """Circular dependencies propagate as ValueError."""
        from pycypher.ast_models import Query
        from pycypher.multi_query_analyzer import (
            DependencyGraph,
            QueryNode,
        )
        from pycypher.query_combiner import QueryCombiner

        node_a = QueryNode(
            query_id="a",
            cypher_query="",
            ast=Query(clauses=[]),
            produces={"X"},
            consumes={"Y"},
            dependencies={"b"},
        )
        node_b = QueryNode(
            query_id="b",
            cypher_query="",
            ast=Query(clauses=[]),
            produces={"Y"},
            consumes={"X"},
            dependencies={"a"},
        )
        graph = DependencyGraph(nodes=[node_a, node_b])
        combiner = QueryCombiner()

        with pytest.raises(ValueError, match=r"[Cc]ircular"):
            combiner.combine_from_graph(graph)


# ---------------------------------------------------------------------------
# Dependency graph correctness tests
# ---------------------------------------------------------------------------


class TestDependencyGraphCorrectness:
    """Verify dependency analysis integrates correctly end-to-end."""

    def test_diamond_dependency_ordering(self) -> None:
        """Diamond: Q3 depends on both Q1 and Q2."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        graph = executor.analyze(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
                ("q3", "MATCH (p:Person), (c:Company) RETURN p, c"),
            ]
        )

        q3 = next(n for n in graph.nodes if n.query_id == "q3")
        assert q3.dependencies == {"q1", "q2"}

    def test_chain_dependency_ordering(self) -> None:
        """Chain Q1→Q2→Q3 ordering is preserved."""
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        combined = executor.combine(
            [
                ("q3", "MATCH (e:Employee) RETURN e.n"),
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {n: p.name})"),
            ]
        )

        # CREATE Person must appear before CREATE Employee
        person_pos = combined.find("CREATE (p:Person")
        employee_pos = combined.find("CREATE (e:Employee")
        assert person_pos < employee_pos


# ---------------------------------------------------------------------------
# Multi-query module cross-integration tests
# ---------------------------------------------------------------------------


class TestCrossModuleIntegration:
    """Validate that all sprint deliverables work together."""

    def test_all_modules_importable(self) -> None:
        """All multi-query modules import cleanly."""
        from pycypher.ast_rewriter import ASTRewriter  # noqa: F401
        from pycypher.input_validator import (  # noqa: F401
            InputValidationResult,
            InputValidator,
        )
        from pycypher.multi_query_analyzer import (  # noqa: F401
            DependencyGraph,
            QueryDependencyAnalyzer,
            QueryNode,
        )
        from pycypher.multi_query_executor import (  # noqa: F401
            MultiQueryExecutor,
        )
        from pycypher.query_combiner import QueryCombiner  # noqa: F401
        from pycypher.query_validator import (  # noqa: F401
            CombinedQueryValidator,
            ValidationResult,
        )
        from pycypher.variable_manager import (  # noqa: F401
            VariableManager,
        )

    def test_variable_manager_with_dependency_analysis(self) -> None:
        """Variable manager and dependency analyzer work on same ASTs."""
        from pycypher.ast_models import ASTConverter
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer
        from pycypher.variable_manager import VariableManager

        analyzer = QueryDependencyAnalyzer()
        vm = VariableManager()

        graph = analyzer.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ]
        )

        # Both queries use variable 'n'
        q1_vars = vm.collect_variables(graph.nodes[0].ast)
        q2_vars = vm.collect_variables(graph.nodes[1].ast)
        assert "n" in q1_vars
        assert "n" in q2_vars

    def test_full_roundtrip_validate_analyze_combine(self) -> None:
        """Full roundtrip: validate → analyze → combine → parse."""
        from pycypher.ast_models import ASTConverter
        from pycypher.multi_query_executor import MultiQueryExecutor

        executor = MultiQueryExecutor()
        queries = [
            ("q1", "CREATE (p:Person {name: 'Alice'})"),
            ("q2", "CREATE (c:Company {name: 'Acme'})"),
        ]

        # Validate
        assert executor.validate(queries).is_valid

        # Analyze
        graph = executor.analyze(queries)
        assert len(graph.nodes) == 2

        # Combine
        combined = executor.combine(queries)
        assert isinstance(combined, str)

        # Parse combined output
        ast = ASTConverter.from_cypher(combined)
        assert ast is not None
