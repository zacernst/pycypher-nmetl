"""TDD tests for the ast_rewriter module.

Sprint 5, Phase 3.1: AST Rewriting Engine — transforms, clones, and
serializes Cypher ASTs for multi-query composition.

RED phase: interface contracts, node creation, cloning, variable
substitution, Cypher generation, and structural validation.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Interface contract tests
# ---------------------------------------------------------------------------


class TestASTRewriterInterfaceContract:
    """ASTRewriter must expose a well-defined public API."""

    def test_rewriter_has_create_with_clause_method(self) -> None:
        """ASTRewriter can create a WITH * clause node."""
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        assert callable(rewriter.create_with_star)

    def test_rewriter_has_strip_return_method(self) -> None:
        """ASTRewriter can strip RETURN from a Query AST."""
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        assert callable(rewriter.strip_return)

    def test_rewriter_has_merge_queries_method(self) -> None:
        """ASTRewriter can merge multiple Query ASTs into one."""
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        assert callable(rewriter.merge_queries)

    def test_rewriter_has_to_cypher_method(self) -> None:
        """ASTRewriter can serialize a Query AST back to Cypher."""
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        assert callable(rewriter.to_cypher)


# ---------------------------------------------------------------------------
# WITH * clause creation tests
# ---------------------------------------------------------------------------


class TestWithStarCreation:
    """ASTRewriter must create valid WITH * clause nodes."""

    def test_create_with_star_returns_with_clause(self) -> None:
        """create_with_star() returns a With AST node."""
        from pycypher.ast_models import With
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        node = rewriter.create_with_star()
        assert isinstance(node, With)

    def test_with_star_has_empty_items(self) -> None:
        """WITH * is represented as With(items=[]) — empty means wildcard."""
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        node = rewriter.create_with_star()
        # Parser represents WITH * as empty items list
        assert node.items == []

    def test_with_star_is_not_distinct(self) -> None:
        """WITH * is not DISTINCT by default."""
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        node = rewriter.create_with_star()
        assert node.distinct is False


# ---------------------------------------------------------------------------
# RETURN stripping tests
# ---------------------------------------------------------------------------


class TestStripReturn:
    """ASTRewriter must remove RETURN clauses from Query ASTs."""

    def test_strip_return_removes_return_clause(self) -> None:
        """strip_return() removes RETURN from a Query."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'}) RETURN n",
        )
        stripped = rewriter.strip_return(ast)
        # No RETURN clause should remain
        from pycypher.ast_models import Return

        assert not any(isinstance(c, Return) for c in stripped.clauses)

    def test_strip_return_preserves_other_clauses(self) -> None:
        """strip_return() keeps non-RETURN clauses intact."""
        from pycypher.ast_models import ASTConverter, Create
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'}) RETURN n",
        )
        stripped = rewriter.strip_return(ast)
        assert any(isinstance(c, Create) for c in stripped.clauses)

    def test_strip_return_on_no_return_is_idempotent(self) -> None:
        """strip_return() on query without RETURN returns equivalent AST."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        stripped = rewriter.strip_return(ast)
        assert len(stripped.clauses) == len(ast.clauses)

    def test_strip_return_is_immutable(self) -> None:
        """strip_return() does not mutate the original AST."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'}) RETURN n",
        )
        original_count = len(ast.clauses)
        rewriter.strip_return(ast)
        assert len(ast.clauses) == original_count


# ---------------------------------------------------------------------------
# Query merging tests
# ---------------------------------------------------------------------------


class TestMergeQueries:
    """ASTRewriter must merge multiple Query ASTs into one."""

    def test_merge_single_query_returns_clone(self) -> None:
        """Merging a single query returns an equivalent AST."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        merged = rewriter.merge_queries([ast])
        assert len(merged.clauses) == len(ast.clauses)

    def test_merge_two_queries_inserts_with_star(self) -> None:
        """Merging two queries inserts WITH * between them."""
        from pycypher.ast_models import ASTConverter, With
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        merged = rewriter.merge_queries([q1, q2])
        with_clauses = [c for c in merged.clauses if isinstance(c, With)]
        assert len(with_clauses) == 1

    def test_merge_strips_intermediate_returns(self) -> None:
        """Merging strips RETURN from non-final queries."""
        from pycypher.ast_models import ASTConverter, Return
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'}) RETURN n",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        merged = rewriter.merge_queries([q1, q2])
        returns = [c for c in merged.clauses if isinstance(c, Return)]
        # Only one RETURN (from final query)
        assert len(returns) == 1

    def test_merge_preserves_final_return(self) -> None:
        """Merging keeps RETURN from the last query."""
        from pycypher.ast_models import ASTConverter, Return
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        merged = rewriter.merge_queries([q1, q2])
        returns = [c for c in merged.clauses if isinstance(c, Return)]
        assert len(returns) == 1

    def test_merge_three_queries_inserts_two_with_stars(self) -> None:
        """Three queries produce two WITH * separators."""
        from pycypher.ast_models import ASTConverter, With
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (p:Person {name: 'Alice'})",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (p:Person) CREATE (e:Employee {n: p.name})",
        )
        q3 = ASTConverter.from_cypher(
            "MATCH (e:Employee) RETURN e.n",
        )
        merged = rewriter.merge_queries([q1, q2, q3])
        with_clauses = [c for c in merged.clauses if isinstance(c, With)]
        assert len(with_clauses) == 2

    def test_merge_empty_list_returns_empty_query(self) -> None:
        """Merging empty list returns Query with no clauses."""
        from pycypher.ast_models import Query
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        merged = rewriter.merge_queries([])
        assert isinstance(merged, Query)
        assert len(merged.clauses) == 0


# ---------------------------------------------------------------------------
# Cypher generation tests (AST → string)
# ---------------------------------------------------------------------------


class TestToCypher:
    """ASTRewriter must serialize Query ASTs back to valid Cypher strings."""

    def test_to_cypher_simple_create(self) -> None:
        """Serialize a simple CREATE query."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        cypher = rewriter.to_cypher(ast)
        assert isinstance(cypher, str)
        assert "CREATE" in cypher
        assert "Person" in cypher

    def test_to_cypher_match_return(self) -> None:
        """Serialize a MATCH...RETURN query."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        cypher = rewriter.to_cypher(ast)
        assert "MATCH" in cypher
        assert "RETURN" in cypher

    def test_to_cypher_with_star(self) -> None:
        """Serialize a WITH * clause."""
        from pycypher.ast_models import Query
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        with_node = rewriter.create_with_star()
        # Wrap in a Query for serialization
        q = Query(clauses=[with_node])
        cypher = rewriter.to_cypher(q)
        assert "WITH" in cypher
        assert "*" in cypher

    def test_to_cypher_roundtrip_parseable(self) -> None:
        """Serialized Cypher can be re-parsed by ASTConverter."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        original = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        cypher = rewriter.to_cypher(original)
        reparsed = ASTConverter.from_cypher(cypher)
        assert reparsed is not None
        assert len(reparsed.clauses) > 0

    def test_to_cypher_merged_query_parseable(self) -> None:
        """Merged query serializes to parseable Cypher."""
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
        reparsed = ASTConverter.from_cypher(cypher)
        assert reparsed is not None

    def test_to_cypher_match_with_where(self) -> None:
        """Serialize a MATCH with WHERE clause."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age > 25 RETURN n.name",
        )
        cypher = rewriter.to_cypher(ast)
        assert "MATCH" in cypher
        assert "WHERE" in cypher
        assert "RETURN" in cypher


# ---------------------------------------------------------------------------
# Structural validation tests
# ---------------------------------------------------------------------------


class TestASTStructuralValidation:
    """ASTRewriter must validate AST integrity after modifications."""

    def test_merged_query_has_correct_clause_count(self) -> None:
        """Two 1-clause queries + WITH * = 3 clauses total."""
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
        # CREATE + WITH * + MATCH + RETURN = 4
        assert len(merged.clauses) == 4

    def test_merged_query_clause_ordering(self) -> None:
        """Clauses appear in correct order: CREATE, WITH *, MATCH, RETURN."""
        from pycypher.ast_models import (
            ASTConverter,
            Create,
            Match,
            Return,
            With,
        )
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        merged = rewriter.merge_queries([q1, q2])
        types = [type(c) for c in merged.clauses]
        assert types == [Create, With, Match, Return]

    def test_merge_does_not_mutate_inputs(self) -> None:
        """merge_queries() does not mutate the input Query ASTs."""
        from pycypher.ast_models import ASTConverter
        from pycypher.ast_rewriter import ASTRewriter

        rewriter = ASTRewriter()
        q1 = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        q1_count = len(q1.clauses)
        q2_count = len(q2.clauses)
        rewriter.merge_queries([q1, q2])
        assert len(q1.clauses) == q1_count
        assert len(q2.clauses) == q2_count
