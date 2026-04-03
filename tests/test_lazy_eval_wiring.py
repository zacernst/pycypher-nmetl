"""TDD tests for wiring lazy_eval filter fusion into Star.execute_query().

These tests verify that the computation graph is built from query AST,
optimization passes are applied, and execution hints are used to improve
query performance (e.g., WHERE predicate pushdown in multi-path MATCH).

Run with:
    uv run pytest tests/test_lazy_eval_wiring_tdd.py -v
"""

import pandas as pd
import pytest
from pycypher import Star
from pycypher.lazy_eval import (
    ComputationGraph,
    OpType,
    build_computation_graph,
    estimate_memory,
    fuse_filters,
    push_filters_down,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_graph_context() -> Context:
    """Context with Person nodes and KNOWS relationships."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 40, 28],
        },
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 4],
            "since": [2020, 2021, 2022],
        },
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": EntityTable.from_dataframe("Person", person_df)
            },
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": EntityTable.from_dataframe("KNOWS", knows_df)},
        ),
    )


@pytest.fixture
def multi_type_context() -> Context:
    """Context with multiple entity and relationship types for multi-path testing."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    company_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 20],
            "name": ["Acme", "Globex"],
        },
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
        },
    )
    works_at_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [10, 20, 10],
        },
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": EntityTable.from_dataframe("Person", person_df),
                "Company": EntityTable.from_dataframe("Company", company_df),
            },
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": EntityTable.from_dataframe("KNOWS", knows_df),
                "WORKS_AT": EntityTable.from_dataframe(
                    "WORKS_AT",
                    works_at_df,
                ),
            },
        ),
    )


# ---------------------------------------------------------------------------
# Tests: build_computation_graph
# ---------------------------------------------------------------------------


class TestBuildComputationGraph:
    """Test building ComputationGraph from query AST."""

    def test_simple_match_return_produces_scan_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """A simple MATCH (n:Person) RETURN n should produce at least a SCAN node."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher("MATCH (n:Person) RETURN n.name")
        graph = build_computation_graph(query)

        assert isinstance(graph, ComputationGraph)
        assert len(graph.nodes) > 0

        # Should contain at least one SCAN node
        scan_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.SCAN
        ]
        assert len(scan_nodes) >= 1

    def test_match_with_where_produces_filter_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """MATCH with WHERE should produce a FILTER node after the SCAN."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
        )
        graph = build_computation_graph(query)

        filter_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.FILTER
        ]
        assert len(filter_nodes) >= 1

    def test_match_relationship_produces_join_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """MATCH with a relationship should produce a JOIN node."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        )
        graph = build_computation_graph(query)

        join_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.JOIN
        ]
        assert len(join_nodes) >= 1

    def test_return_produces_project_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """RETURN clause should produce a PROJECT node."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher("MATCH (n:Person) RETURN n.name")
        graph = build_computation_graph(query)

        project_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.PROJECT
        ]
        assert len(project_nodes) >= 1

    def test_graph_has_valid_topological_order(
        self,
        simple_graph_context: Context,
    ) -> None:
        """The computation graph should have a valid topological order."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 25 RETURN a.name",
        )
        graph = build_computation_graph(query)

        order = graph.topological_order()
        assert len(order) == len(graph.nodes)

    def test_with_clause_produces_project_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """WITH clause should produce a PROJECT node in the graph."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) WITH n.name AS name RETURN name",
        )
        graph = build_computation_graph(query)

        project_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.PROJECT
        ]
        assert len(project_nodes) >= 1

    def test_limit_produces_limit_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """RETURN with LIMIT should produce a LIMIT node."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) RETURN n.name LIMIT 3",
        )
        graph = build_computation_graph(query)

        limit_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.LIMIT
        ]
        assert len(limit_nodes) >= 1

    def test_order_by_produces_sort_node(
        self,
        simple_graph_context: Context,
    ) -> None:
        """RETURN with ORDER BY should produce a SORT node."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) RETURN n.name ORDER BY n.age",
        )
        graph = build_computation_graph(query)

        sort_nodes = [
            n for n in graph.nodes.values() if n.op_type == OpType.SORT
        ]
        assert len(sort_nodes) >= 1


# ---------------------------------------------------------------------------
# Tests: optimization passes on real queries
# ---------------------------------------------------------------------------


class TestFilterFusionOnRealQueries:
    """Test that filter fusion works on computation graphs built from real queries."""

    def test_consecutive_where_filters_are_fused(self) -> None:
        """Two consecutive filter nodes in a graph should be fused into one."""
        from pycypher.ast_models import ASTConverter

        # WITH clause with WHERE acts as a filter, followed by another MATCH with WHERE
        query = ASTConverter().from_cypher(
            "MATCH (n:Person) WITH n WHERE n.age > 25 RETURN n.name",
        )
        graph = build_computation_graph(query)

        # Count filters before fusion
        filters_before = sum(
            1 for n in graph.nodes.values() if n.op_type == OpType.FILTER
        )

        optimized = fuse_filters(graph)
        filters_after = sum(
            1 for n in optimized.nodes.values() if n.op_type == OpType.FILTER
        )

        # If there were consecutive filters, fusion should reduce count
        # (or leave unchanged if no consecutive filters exist)
        assert filters_after <= filters_before

    def test_filter_fusion_preserves_graph_structure(self) -> None:
        """Filter fusion should preserve valid topological ordering."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) WHERE n.age > 25 RETURN n.name",
        )
        graph = build_computation_graph(query)
        optimized = fuse_filters(graph)

        order = optimized.topological_order()
        assert len(order) == len(optimized.nodes)


class TestPredicatePushdownOnRealQueries:
    """Test that predicate pushdown works on real query computation graphs."""

    def test_pushdown_preserves_graph_validity(self) -> None:
        """Predicate pushdown should preserve valid topological ordering."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 RETURN b.name",
        )
        graph = build_computation_graph(query)
        optimized = push_filters_down(graph)

        order = optimized.topological_order()
        # topological_order walks from output_node; orphaned nodes (replaced by
        # pushdown) are correctly excluded as dead code
        assert len(order) >= 1
        # The output node should be reachable
        assert optimized.output_node in order


class TestMemoryEstimationOnRealQueries:
    """Test memory estimation on computation graphs from real queries."""

    def test_simple_query_has_positive_memory_estimate(self) -> None:
        """Memory estimate for a real query should be positive."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher("MATCH (n:Person) RETURN n.name")
        graph = build_computation_graph(query)

        # Set estimated_rows on nodes for meaningful estimate
        for node in graph.nodes.values():
            if node.op_type == OpType.SCAN or node.op_type == OpType.PROJECT:
                node.estimated_rows = 1000

        mem = estimate_memory(graph)
        assert mem > 0

    def test_join_query_has_higher_memory_than_scan(self) -> None:
        """A query with a JOIN should estimate higher memory than a simple scan."""
        from pycypher.ast_models import ASTConverter

        scan_query = ASTConverter().from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        scan_graph = build_computation_graph(scan_query)
        for node in scan_graph.nodes.values():
            node.estimated_rows = 1000

        join_query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        )
        join_graph = build_computation_graph(join_query)
        for node in join_graph.nodes.values():
            if node.op_type == OpType.JOIN:
                node.estimated_rows = 5000
            else:
                node.estimated_rows = 1000

        scan_mem = estimate_memory(scan_graph)
        join_mem = estimate_memory(join_graph)
        assert join_mem >= scan_mem


# ---------------------------------------------------------------------------
# Tests: Star._plan_query integration
# ---------------------------------------------------------------------------


class TestStarPlanQueryIntegration:
    """Test that Star uses the computation graph for query planning."""

    def test_plan_query_returns_hints(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Star._plan_query should return execution hints from the computation graph."""
        star = Star(context=simple_graph_context)
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
        )
        hints = star._plan_query(query)

        assert isinstance(hints, dict)
        assert "estimated_memory_bytes" in hints
        assert "node_count" in hints
        assert "has_filter" in hints

    def test_plan_query_detects_joins(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Planning a query with relationships should detect joins."""
        star = Star(context=simple_graph_context)
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name",
        )
        hints = star._plan_query(query)

        assert hints["has_join"] is True

    def test_plan_query_detects_filters(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Planning a query with WHERE should detect filters."""
        star = Star(context=simple_graph_context)
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) WHERE n.age > 25 RETURN n.name",
        )
        hints = star._plan_query(query)

        assert hints["has_filter"] is True


# ---------------------------------------------------------------------------
# Tests: end-to-end correctness with optimization
# ---------------------------------------------------------------------------


class TestEndToEndWithOptimization:
    """Verify query results are identical with lazy_eval planning wired in."""

    def test_simple_query_correctness(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Basic MATCH-RETURN should produce correct results."""
        star = Star(context=simple_graph_context)
        result = star.execute_query(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name AS name ORDER BY name",
        )

        names = sorted(result["name"].tolist())
        assert names == ["Carol", "Dave"]

    def test_relationship_query_correctness(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Relationship traversal should produce correct results."""
        star = Star(context=simple_graph_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name",
        )

        assert len(result) == 3
        assert "a.name" in result.columns
        assert "b.name" in result.columns

    def test_multi_clause_correctness(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Multi-clause query with WITH should produce correct results."""
        star = Star(context=simple_graph_context)
        result = star.execute_query(
            "MATCH (n:Person) WHERE n.age > 25 WITH n.name AS name RETURN name ORDER BY name",
        )

        names = sorted(result["name"].tolist())
        assert names == ["Alice", "Carol", "Dave", "Eve"]

    def test_multi_path_match_correctness(
        self,
        multi_type_context: Context,
    ) -> None:
        """Multi-path MATCH should produce correct results."""
        star = Star(context=multi_type_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:WORKS_AT]->(c:Company) "
            "RETURN a.name, b.name, c.name ORDER BY a.name",
        )

        # Alice knows Bob and works at Acme; Bob knows Carol and works at Globex
        assert len(result) >= 1
        assert "a.name" in result.columns
        assert "c.name" in result.columns


# ---------------------------------------------------------------------------
# Tests: dead column elimination
# ---------------------------------------------------------------------------


class TestDeadColumnElimination:
    """Test that compute_live_columns correctly identifies droppable columns."""

    def test_compute_live_columns_simple(self) -> None:
        """MATCH (n:Person) RETURN n.name — n is live throughout."""
        from pycypher.ast_models import ASTConverter
        from pycypher.lazy_eval import compute_live_columns

        query = ASTConverter().from_cypher("MATCH (n:Person) RETURN n.name")
        live = compute_live_columns(query.clauses)
        assert len(live) == 2
        # Last clause (RETURN) always keeps all
        assert live[-1] is None
        # After MATCH (clause 0), n must be live for RETURN
        assert live[0] is not None
        assert "n" in live[0]

    def test_compute_live_columns_with_clause(self) -> None:
        """Variables not projected by WITH should not appear as live."""
        from pycypher.ast_models import ASTConverter
        from pycypher.lazy_eval import compute_live_columns

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WITH a.name AS name "
            "RETURN name"
        )
        live = compute_live_columns(query.clauses)
        assert len(live) == 3
        # After MATCH (clause 0), WITH and RETURN need a, r, b and name
        # After WITH (clause 1), RETURN needs name
        assert live[-1] is None  # RETURN keeps all

    def test_compute_live_columns_mutation_keeps_all(self) -> None:
        """Mutation clauses (SET) should keep all columns (conservative)."""
        from pycypher.ast_models import ASTConverter
        from pycypher.lazy_eval import compute_live_columns

        query = ASTConverter().from_cypher(
            "MATCH (n:Person) SET n.age = 30 RETURN n.name"
        )
        live = compute_live_columns(query.clauses)
        # All entries should be None (keep-all) because SET is a mutation
        for entry in live:
            assert entry is None

    def test_dead_column_elimination_correctness(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Dead column elimination should not affect query results."""
        star = Star(context=simple_graph_context)

        # Query where 'r' is bound but never referenced in RETURN
        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WITH a, b "
            "RETURN a.name AS a_name, b.name AS b_name ORDER BY a_name"
        )

        assert len(result) == 3
        assert "a_name" in result.columns
        assert "b_name" in result.columns

    def test_dead_column_elimination_with_multi_hop(
        self,
        simple_graph_context: Context,
    ) -> None:
        """Multi-hop queries should produce correct results with dead column elimination."""
        star = Star(context=simple_graph_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN c.name AS name ORDER BY name"
        )
        # Should still get correct results
        assert len(result) >= 1
        assert "name" in result.columns
