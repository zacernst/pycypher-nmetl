"""TDD tests for the query_combiner module.

Sprint 4: Query Combination Engine — transforms multiple Cypher queries
into a single combined query using dependency-ordered clause sequencing
with WITH * variable passing.

RED phase: interface contracts, combination logic, semantic equivalence.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Interface contract tests
# ---------------------------------------------------------------------------


class TestQueryCombinerInterfaceContract:
    """QueryCombiner must define a combine() method."""

    def test_combiner_has_combine_method(self) -> None:
        """QueryCombiner exposes a combine() method."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        assert callable(combiner.combine)

    def test_combine_accepts_query_id_cypher_pairs(self) -> None:
        """combine() accepts list of (query_id, cypher) tuples."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ],
        )
        assert isinstance(result, str)

    def test_combine_empty_list_returns_empty(self) -> None:
        """Empty input returns empty string."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        assert combiner.combine([]) == ""

    def test_combine_single_query_returns_unchanged(self) -> None:
        """Single query is returned as-is (stripped)."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        cypher = "CREATE (n:Person {name: 'Alice'})"
        result = combiner.combine([("q1", cypher)])
        assert cypher in result


# ---------------------------------------------------------------------------
# WITH clause insertion tests
# ---------------------------------------------------------------------------


class TestWithClauseInsertion:
    """Combiner must insert WITH * between dependent queries."""

    def test_insert_with_between_two_queries(self) -> None:
        """Two dependent queries get WITH * between them."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        assert "WITH *" in result

    def test_no_with_for_single_query(self) -> None:
        """Single query has no WITH * inserted."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ],
        )
        assert "WITH *" not in result

    def test_with_inserted_between_each_query_pair(self) -> None:
        """Three queries get WITH * between each pair."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {n: p.name})"),
                ("q3", "MATCH (e:Employee) RETURN e.n"),
            ],
        )
        # Should have exactly 2 WITH * separators
        assert result.count("WITH *") == 2


# ---------------------------------------------------------------------------
# Ordering preservation tests
# ---------------------------------------------------------------------------


class TestOrderingPreservation:
    """Combiner must respect dependency order from topological sort."""

    def test_preserve_create_before_match(self) -> None:
        """CREATE query appears before MATCH that depends on it."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        # Provide queries in reverse order — combiner must reorder
        result = combiner.combine(
            [
                ("q2", "MATCH (n:Person) RETURN n.name"),
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ],
        )
        create_pos = result.find("CREATE")
        match_pos = result.find("MATCH")
        assert create_pos < match_pos

    def test_independent_queries_both_present(self) -> None:
        """Independent queries are both included."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
            ],
        )
        assert "Person" in result
        assert "Company" in result

    def test_chain_ordering_preserved(self) -> None:
        """Chain Q1→Q2→Q3 maintains strict ordering."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q3", "MATCH (e:Employee) RETURN e.n"),
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {n: p.name})"),
            ],
        )
        # Find positions of the type-specific keywords
        person_create = result.find("CREATE (p:Person")
        employee_create = result.find("CREATE (e:Employee")
        employee_match = result.find("MATCH (e:Employee)")
        assert person_create < employee_create
        assert employee_create < employee_match


# ---------------------------------------------------------------------------
# RETURN clause stripping tests
# ---------------------------------------------------------------------------


class TestReturnClauseHandling:
    """Intermediate RETURN clauses must be stripped from non-final queries."""

    def test_strip_intermediate_return(self) -> None:
        """RETURN in non-final query is stripped."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'}) RETURN n"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        # Only one RETURN in the combined output
        assert result.count("RETURN") == 1
        # The final RETURN should be for n.name
        assert "RETURN n.name" in result

    def test_preserve_final_return(self) -> None:
        """RETURN in the final query is preserved."""
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        assert "RETURN n.name" in result


# ---------------------------------------------------------------------------
# Cypher syntax validity tests
# ---------------------------------------------------------------------------


class TestCypherSyntaxValidity:
    """Combined query must be parseable Cypher."""

    def test_combined_query_is_parseable(self) -> None:
        """Combined output can be parsed by ASTConverter."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        # Should not raise
        ast = ASTConverter.from_cypher(result)
        assert ast is not None
        assert len(ast.clauses) > 0

    def test_chain_combined_is_parseable(self) -> None:
        """Three-query chain produces parseable Cypher."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_combiner import QueryCombiner

        combiner = QueryCombiner()
        result = combiner.combine(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {n: p.name})"),
                ("q3", "MATCH (e:Employee) RETURN e.n"),
            ],
        )
        ast = ASTConverter.from_cypher(result)
        assert ast is not None


# ---------------------------------------------------------------------------
# Semantic equivalence validation tests
# ---------------------------------------------------------------------------


class TestSemanticEquivalence:
    """Combined query must produce identical results to sequential exec."""

    @pytest.mark.xfail(
        reason=(
            "CREATE→MATCH within a single query is not semantically equivalent "
            "to sequential execution: CREATE stages in shadow layer, MATCH "
            "requires committed entity types. This is a known limitation of "
            "single-query combination for cross-query CREATE→MATCH patterns."
        ),
        strict=True,
    )
    def test_create_then_match_equivalence(self) -> None:
        """Combined CREATE+MATCH gives same result as sequential."""
        from pycypher.query_combiner import QueryCombiner
        from pycypher.relational_models import Context
        from pycypher.star import Star

        ctx = Context()
        star = Star(context=ctx)

        # Sequential execution (commits after each query)
        star.execute_query("CREATE (n:Person {name: 'Alice'})")
        sequential_result = star.execute_query(
            "MATCH (n:Person) RETURN n.name",
        )

        # Combined execution (single transaction — shadow layer
        # not visible to subsequent MATCH)
        ctx2 = Context()
        star2 = Star(context=ctx2)
        combiner = QueryCombiner()
        combined = combiner.combine(
            [
                ("q1", "CREATE (n:Person {name: 'Alice'})"),
                ("q2", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        combined_result = star2.execute_query(combined)

        pd.testing.assert_frame_equal(
            sequential_result.reset_index(drop=True),
            combined_result.reset_index(drop=True),
        )

    @pytest.mark.xfail(
        reason=(
            "CREATE→MATCH chain within single query: shadow layer not visible "
            "to subsequent MATCH. Requires multi-statement transaction support."
        ),
        strict=True,
    )
    def test_create_chain_equivalence(self) -> None:
        """Chain: CREATE Person → CREATE Employee from Person → MATCH."""
        from pycypher.query_combiner import QueryCombiner
        from pycypher.relational_models import Context
        from pycypher.star import Star

        # Sequential (commits between queries)
        ctx1 = Context()
        star1 = Star(context=ctx1)
        star1.execute_query("CREATE (p:Person {name: 'Bob'})")
        star1.execute_query(
            "MATCH (p:Person) CREATE (e:Employee {n: p.name})",
        )
        seq_result = star1.execute_query(
            "MATCH (e:Employee) RETURN e.n",
        )

        # Combined (single transaction)
        ctx2 = Context()
        star2 = Star(context=ctx2)
        combiner = QueryCombiner()
        combined = combiner.combine(
            [
                ("q1", "CREATE (p:Person {name: 'Bob'})"),
                ("q2", "MATCH (p:Person) CREATE (e:Employee {n: p.name})"),
                ("q3", "MATCH (e:Employee) RETURN e.n"),
            ],
        )
        comb_result = star2.execute_query(combined)

        pd.testing.assert_frame_equal(
            seq_result.reset_index(drop=True),
            comb_result.reset_index(drop=True),
        )

    def test_independent_creates_equivalence(self) -> None:
        """Independent CREATEs produce same final state."""
        from pycypher.query_combiner import QueryCombiner
        from pycypher.relational_models import Context
        from pycypher.star import Star

        # Sequential
        ctx1 = Context()
        star1 = Star(context=ctx1)
        star1.execute_query("CREATE (p:Person {name: 'Alice'})")
        star1.execute_query("CREATE (c:Company {name: 'Acme'})")
        seq_persons = star1.execute_query(
            "MATCH (p:Person) RETURN p.name",
        )
        seq_companies = star1.execute_query(
            "MATCH (c:Company) RETURN c.name",
        )

        # Combined
        ctx2 = Context()
        star2 = Star(context=ctx2)
        combiner = QueryCombiner()
        combined = combiner.combine(
            [
                ("q1", "CREATE (p:Person {name: 'Alice'})"),
                ("q2", "CREATE (c:Company {name: 'Acme'})"),
            ],
        )
        star2.execute_query(combined)
        comb_persons = star2.execute_query(
            "MATCH (p:Person) RETURN p.name",
        )
        comb_companies = star2.execute_query(
            "MATCH (c:Company) RETURN c.name",
        )

        pd.testing.assert_frame_equal(
            seq_persons.reset_index(drop=True),
            comb_persons.reset_index(drop=True),
        )
        pd.testing.assert_frame_equal(
            seq_companies.reset_index(drop=True),
            comb_companies.reset_index(drop=True),
        )


# ---------------------------------------------------------------------------
# Circular dependency error propagation
# ---------------------------------------------------------------------------


class TestCircularDependencyPropagation:
    """Combiner must raise on circular dependencies."""

    def test_circular_dependency_raises(self) -> None:
        """Circular dependency propagates as ValueError."""
        from pycypher.ast_models import Query
        from pycypher.multi_query_analyzer import (
            DependencyGraph,
            QueryNode,
        )
        from pycypher.query_combiner import QueryCombiner

        # Manually construct a circular graph
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
