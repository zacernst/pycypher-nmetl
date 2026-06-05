"""TDD tests for the multi_query_analyzer module.

Sprint 2: Dependency Analysis Foundation — tests for QueryNode,
DependencyGraph, and QueryDependencyAnalyzer.

RED phase: interface contracts and behavioral expectations.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Interface contract tests
# ---------------------------------------------------------------------------


class TestQueryNodeInterfaceContract:
    """QueryNode dataclass must have produces/consumes/dependencies metadata."""

    def test_query_node_has_required_fields(self) -> None:
        """QueryNode must expose all required fields."""
        from pycypher.multi_query_analyzer import QueryNode

        # Verify the class has the expected field names via __dataclass_fields__
        fields = set(QueryNode.__dataclass_fields__)
        assert "query_id" in fields
        assert "cypher_query" in fields
        assert "ast" in fields
        assert "produces" in fields
        assert "consumes" in fields
        assert "dependencies" in fields

    def test_query_node_defaults(self) -> None:
        """produces, consumes, dependencies default to empty sets."""
        from pycypher.ast_models import Query
        from pycypher.multi_query_analyzer import QueryNode

        node = QueryNode(
            query_id="q1",
            cypher_query="MATCH (n) RETURN n",
            ast=Query(clauses=[]),
        )
        assert node.produces == set()
        assert node.consumes == set()
        assert node.dependencies == set()


class TestDependencyGraphInterfaceContract:
    """DependencyGraph must expose nodes and topological_sort."""

    def test_dependency_graph_has_nodes(self) -> None:
        """DependencyGraph must have a nodes attribute."""
        from pycypher.multi_query_analyzer import DependencyGraph

        graph = DependencyGraph(nodes=[])
        assert graph.nodes == []

    def test_dependency_graph_has_topological_sort(self) -> None:
        """DependencyGraph must expose a topological_sort method."""
        from pycypher.multi_query_analyzer import DependencyGraph

        graph = DependencyGraph(nodes=[])
        assert callable(graph.topological_sort)

    def test_topological_sort_empty_graph(self) -> None:
        """Empty graph returns empty list."""
        from pycypher.multi_query_analyzer import DependencyGraph

        graph = DependencyGraph(nodes=[])
        assert graph.topological_sort() == []


class TestQueryDependencyAnalyzerInterfaceContract:
    """QueryDependencyAnalyzer must accept (query_id, cypher) pairs and return DependencyGraph."""

    def test_analyzer_has_analyze_method(self) -> None:
        """Analyzer must expose an analyze() method."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        assert callable(analyzer.analyze)

    def test_analyze_returns_dependency_graph(self) -> None:
        """analyze() must return a DependencyGraph instance."""
        from pycypher.multi_query_analyzer import (
            DependencyGraph,
            QueryDependencyAnalyzer,
        )

        analyzer = QueryDependencyAnalyzer()
        result = analyzer.analyze([])
        assert isinstance(result, DependencyGraph)


# ---------------------------------------------------------------------------
# Produced/consumed type extraction tests
# ---------------------------------------------------------------------------


class TestExtractProducedTypes:
    """Analyzer must extract entity/relationship types from CREATE clauses."""

    def test_extract_produced_types_from_create_node(self) -> None:
        """CREATE (n:Person) should produce {'Person'}."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ],
        )
        assert graph.nodes[0].produces == {"Person"}

    def test_extract_produced_types_from_create_relationship(self) -> None:
        """CREATE ()-[:KNOWS]->() should produce {'KNOWS'}."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (a:Person)-[:KNOWS]->(b:Person)"),
            ],
        )
        assert "KNOWS" in graph.nodes[0].produces
        assert "Person" in graph.nodes[0].produces

    def test_extract_produced_types_from_create_multiple_types(self) -> None:
        """CREATE with multiple node types extracts all."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (p:Person), (c:Company)"),
            ],
        )
        assert graph.nodes[0].produces == {"Person", "Company"}

    def test_no_produced_types_from_match_only_query(self) -> None:
        """A pure MATCH query produces nothing."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "MATCH (n:Person) RETURN n"),
            ],
        )
        assert graph.nodes[0].produces == set()


class TestExtractConsumedTypes:
    """Analyzer must extract entity/relationship types from MATCH clauses."""

    def test_extract_consumed_types_from_match_node(self) -> None:
        """MATCH (n:Person) should consume {'Person'}."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "MATCH (n:Person) RETURN n"),
            ],
        )
        assert graph.nodes[0].consumes == {"Person"}

    def test_extract_consumed_types_from_match_relationship(self) -> None:
        """MATCH ()-[:KNOWS]->() should consume {'KNOWS'}."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"),
            ],
        )
        assert "KNOWS" in graph.nodes[0].consumes
        assert "Person" in graph.nodes[0].consumes

    def test_no_consumed_types_from_create_only_query(self) -> None:
        """A pure CREATE query consumes nothing."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ],
        )
        assert graph.nodes[0].consumes == set()


# ---------------------------------------------------------------------------
# Dependency graph construction tests
# ---------------------------------------------------------------------------


class TestBuildDependencyGraph:
    """Analyzer must infer dependencies from produces/consumes overlap."""

    def test_build_simple_linear_dependency(self) -> None:
        """Q2 consumes what Q1 produces → Q2 depends on Q1."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        q1 = next(n for n in graph.nodes if n.query_id == "q1")
        q2 = next(n for n in graph.nodes if n.query_id == "q2")
        assert q1.dependencies == set()
        assert q2.dependencies == {"q1"}

    def test_no_dependency_when_no_overlap(self) -> None:
        """Independent queries have no dependencies."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
            ],
        )
        q1 = next(n for n in graph.nodes if n.query_id == "q1")
        q2 = next(n for n in graph.nodes if n.query_id == "q2")
        assert q1.dependencies == set()
        assert q2.dependencies == set()

    def test_diamond_dependency(self) -> None:
        """Diamond: Q3 depends on both Q1 and Q2."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
                ("q3", "MATCH (p:Person), (c:Company) RETURN p, c"),
            ],
        )
        q3 = next(n for n in graph.nodes if n.query_id == "q3")
        assert q3.dependencies == {"q1", "q2"}

    def test_chain_dependency(self) -> None:
        """Chain: Q1 → Q2 → Q3 (transitive)."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {name: p.name})"),
                ("q3", "MATCH (e:Employee) RETURN e.name"),
            ],
        )
        q1 = next(n for n in graph.nodes if n.query_id == "q1")
        q2 = next(n for n in graph.nodes if n.query_id == "q2")
        q3 = next(n for n in graph.nodes if n.query_id == "q3")
        assert q1.dependencies == set()
        assert "q1" in q2.dependencies
        assert "q2" in q3.dependencies


# ---------------------------------------------------------------------------
# Topological sort tests
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    """DependencyGraph.topological_sort returns dependency-respecting order."""

    def test_topological_sort_linear_chain(self) -> None:
        """Linear chain Q1→Q2→Q3 yields [Q1, Q2, Q3]."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {name: p.name})"),
                ("q3", "MATCH (e:Employee) RETURN e.name"),
            ],
        )
        order = graph.topological_sort()
        ids = [n.query_id for n in order]
        # Q1 must come before Q2, Q2 must come before Q3
        assert ids.index("q1") < ids.index("q2")
        assert ids.index("q2") < ids.index("q3")

    def test_topological_sort_independent_queries(self) -> None:
        """Independent queries can appear in any order (all returned)."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
            ],
        )
        order = graph.topological_sort()
        assert len(order) == 2
        assert {n.query_id for n in order} == {"q1", "q2"}

    def test_topological_sort_diamond(self) -> None:
        """Diamond: Q1 and Q2 before Q3."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
                ("q3", "MATCH (p:Person), (c:Company) RETURN p, c"),
            ],
        )
        order = graph.topological_sort()
        ids = [n.query_id for n in order]
        assert ids.index("q1") < ids.index("q3")
        assert ids.index("q2") < ids.index("q3")


# ---------------------------------------------------------------------------
# Circular dependency detection tests
# ---------------------------------------------------------------------------


class TestCircularDependencyDetection:
    """DependencyGraph.topological_sort must raise on circular dependencies."""

    def test_circular_dependency_detection_raises_error(self) -> None:
        """Manually constructed circular graph raises ValueError."""
        from pycypher.ast_models import Query
        from pycypher.multi_query_analyzer import (
            DependencyGraph,
            QueryNode,
        )

        node_a = QueryNode(
            query_id="a",
            cypher_query="",
            ast=Query(clauses=[]),
            dependencies={"b"},
        )
        node_b = QueryNode(
            query_id="b",
            cypher_query="",
            ast=Query(clauses=[]),
            dependencies={"a"},
        )
        graph = DependencyGraph(nodes=[node_a, node_b])
        with pytest.raises(ValueError, match=r"[Cc]ircular"):
            graph.topological_sort()

    def test_self_referential_dependency_raises_error(self) -> None:
        """A node that depends on itself is circular."""
        from pycypher.ast_models import Query
        from pycypher.multi_query_analyzer import (
            DependencyGraph,
            QueryNode,
        )

        node = QueryNode(
            query_id="self",
            cypher_query="",
            ast=Query(clauses=[]),
            dependencies={"self"},
        )
        graph = DependencyGraph(nodes=[node])
        with pytest.raises(ValueError, match=r"[Cc]ircular"):
            graph.topological_sort()


# ---------------------------------------------------------------------------
# MERGE clause handling tests
# ---------------------------------------------------------------------------


class TestMergeClauseHandling:
    """MERGE both produces and consumes — must be handled correctly."""

    def test_merge_produces_and_consumes(self) -> None:
        """MERGE (n:Person) both produces and consumes Person."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "MERGE (n:Person {name: 'Alice'})"),
            ],
        )
        node = graph.nodes[0]
        # MERGE is an upsert — consumes (match) AND produces (create)
        assert "Person" in node.produces
        assert "Person" in node.consumes


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases for analyzer robustness."""

    def test_single_query_no_dependencies(self) -> None:
        """Single query has no dependencies."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "MATCH (n:Person) RETURN n"),
            ],
        )
        assert len(graph.nodes) == 1
        assert graph.nodes[0].dependencies == set()

    def test_empty_query_list(self) -> None:
        """Empty input produces empty graph."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze([])
        assert len(graph.nodes) == 0

    def test_query_node_preserves_cypher_string(self) -> None:
        """QueryNode stores the original Cypher string."""
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        cypher = "MATCH (n:Person) RETURN n.name"
        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze([("q1", cypher)])
        assert graph.nodes[0].cypher_query == cypher

    def test_query_node_preserves_ast(self) -> None:
        """QueryNode stores the parsed AST."""
        from pycypher.ast_models import Query
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("q1", "MATCH (n:Person) RETURN n"),
            ],
        )
        assert isinstance(graph.nodes[0].ast, Query)


# ---------------------------------------------------------------------------
# Property-level dependency tracking
# ---------------------------------------------------------------------------


class TestPropertyLevelDependencies:
    """SET writes and PropertyLookup reads must form dependency edges.

    The analyzer namespaces property entries as ``Label.prop`` and unions them
    into the existing ``produces`` / ``consumes`` sets. This makes the dependency
    graph aware of property-level data flow without changing the intersection
    rule.
    """

    def test_set_property_appears_in_produces(self) -> None:
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "writer",
                    "MATCH (s:State) SET s.num_counties = 5",
                ),
            ],
        )
        node = graph.nodes[0]
        assert "State.num_counties" in node.produces
        # Bare label still consumed by the MATCH.
        assert "State" in node.consumes

    def test_property_read_appears_in_consumes(self) -> None:
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "reader",
                    "MATCH (s:State) RETURN s.num_counties",
                ),
            ],
        )
        node = graph.nodes[0]
        assert "State.num_counties" in node.consumes
        assert "State.num_counties" not in node.produces

    def test_writer_then_reader_dependency_edge(self) -> None:
        # The classic case: writer SETs s.num_counties, reader RETURNs it.
        # Reader must depend on writer.
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "reader",
                    "MATCH (s:State) RETURN s.num_counties",
                ),
                (
                    "writer",
                    "MATCH (s:State) SET s.num_counties = 5",
                ),
            ],
        )
        nodes = {n.query_id: n for n in graph.nodes}
        assert "writer" in nodes["reader"].dependencies
        assert "reader" not in nodes["writer"].dependencies
        # Topological sort puts writer before reader.
        order = [n.query_id for n in graph.topological_sort()]
        assert order.index("writer") < order.index("reader")

    def test_no_self_dependency_for_read_then_write(self) -> None:
        # A query that reads x and writes x in the same query must not
        # depend on itself (analyzer skips the self-edge).
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "rw",
                    "MATCH (s:State) WITH s, s.x AS x SET s.x = x + 1",
                ),
            ],
        )
        node = graph.nodes[0]
        assert "rw" not in node.dependencies
        assert "State.x" in node.produces
        assert "State.x" in node.consumes

    def test_unrelated_properties_create_no_edge(self) -> None:
        # writer SETs s.a, reader RETURNs s.b — different properties, no edge.
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "writer",
                    "MATCH (s:State) SET s.a = 1",
                ),
                (
                    "reader",
                    "MATCH (s:State) RETURN s.b",
                ),
            ],
        )
        nodes = {n.query_id: n for n in graph.nodes}
        # Both touch :State (via MATCH), but no producer for s.b and no
        # consumer for s.a, so neither query depends on the other.
        assert nodes["reader"].dependencies == set()
        assert nodes["writer"].dependencies == set()

    def test_multi_label_node_records_under_each_label(self) -> None:
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "q",
                    "MATCH (n:A:B) SET n.x = 1 RETURN n.y",
                ),
            ],
        )
        node = graph.nodes[0]
        assert "A.x" in node.produces
        assert "B.x" in node.produces
        assert "A.y" in node.consumes
        assert "B.y" in node.consumes

    def test_unresolved_variable_skipped(self) -> None:
        # `x` is a WITH-introduced variable with no label binding;
        # property tracking on it must not raise and must not add entries.
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "q",
                    "MATCH (s:State) WITH s.num AS x RETURN x",
                ),
            ],
        )
        node = graph.nodes[0]
        # We do see s.num via the WITH-RHS PropertyLookup.
        assert "State.num" in node.consumes
        # But there's no spurious "x.<anything>" entry.
        assert not any(
            entry.startswith("x.") for entry in node.consumes | node.produces
        )

    def test_set_map_form_not_tracked(self) -> None:
        # SET v = {map} is intentionally not tracked — known limitation.
        # We verify it doesn't crash and doesn't produce a spurious entry.
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                (
                    "q",
                    "MATCH (s:State) SET s = {a: 1, b: 2}",
                ),
            ],
        )
        node = graph.nodes[0]
        # No property-level produces (only the bare label may appear).
        assert all("." not in entry for entry in node.produces)

    def test_type_level_dependencies_still_work(self) -> None:
        # Pre-existing behavior: CREATE→MATCH still creates an edge.
        from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

        analyzer = QueryDependencyAnalyzer()
        graph = analyzer.analyze(
            [
                ("reader", "MATCH (n:Foo) RETURN n"),
                ("writer", "CREATE (n:Foo {x: 1})"),
            ],
        )
        nodes = {n.query_id: n for n in graph.nodes}
        assert "writer" in nodes["reader"].dependencies
