"""Tests for uncovered lines in query_complexity.py.

Targets specific code paths: UnionQuery handling, WHERE clause scoring,
MERGE/DELETE clauses, null expression guard, EXISTS subquery, list and
pattern comprehension, CASE expression, and recursive sub-expression lists.
"""

from __future__ import annotations

from typing import Any

from pycypher.ast_models import ASTConverter
from pycypher.query_complexity import (
    DEFAULT_WEIGHTS,
    ComplexityScore,
    _score_expression,
    score_query,
)


def _parse(cypher: str) -> Any:
    """Parse a Cypher string to an AST node."""
    return ASTConverter().from_cypher(cypher)


class TestUnionQueryHandling:
    """Lines 176-179: UnionQuery adds union weight and scores each statement."""

    def test_union_query_adds_union_weight(self) -> None:
        ast = _parse(
            "MATCH (a:Person) RETURN a.name UNION MATCH (b:Dog) RETURN b.name",
        )
        result = score_query(ast)
        assert "union" in result.breakdown
        # Two statements, each worth DEFAULT_WEIGHTS["union"]
        assert result.breakdown["union"] == DEFAULT_WEIGHTS["union"] * 2

    def test_union_query_scores_child_statements(self) -> None:
        ast = _parse(
            "MATCH (a:Person) RETURN a.name UNION MATCH (b:Dog) RETURN b.name",
        )
        result = score_query(ast)
        # Each statement contributes clauses (MATCH + RETURN = 2 each, 4 total)
        assert result.breakdown.get("clauses", 0) == 4
        # Each statement has one MATCH pattern
        assert result.breakdown.get("match_pattern", 0) == 4

    def test_union_all_also_scored(self) -> None:
        ast = _parse(
            "MATCH (a:Person) RETURN a.name UNION ALL MATCH (b:Dog) RETURN b.name",
        )
        result = score_query(ast)
        assert "union" in result.breakdown


class TestWhereClauseScoring:
    """Line 222: WHERE predicate triggers _score_expression."""

    def test_where_with_exists_scores_subquery(self) -> None:
        ast = _parse(
            "MATCH (a:Person) WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) } RETURN a.name",
        )
        result = score_query(ast)
        assert "exists_subquery" in result.breakdown
        assert (
            result.breakdown["exists_subquery"]
            == DEFAULT_WEIGHTS["exists_subquery"]
        )

    def test_where_predicate_path_exercised(self) -> None:
        """A simple WHERE comparison still passes through _score_expression."""
        no_where = score_query(_parse("MATCH (a:Person) RETURN a.name"))
        with_where = score_query(
            _parse("MATCH (a:Person) WHERE a.age > 21 RETURN a.name"),
        )
        # The WHERE path is exercised; basic comparisons add no extra weight
        # but the code path on line 222 is still reached.
        assert with_where.total >= no_where.total


class TestMergeClause:
    """Lines 248-250: Merge clause adds merge weight."""

    def test_merge_adds_merge_weight(self) -> None:
        ast = _parse('MERGE (a:Person {name: "Alice"}) RETURN a.name')
        result = score_query(ast)
        assert "merge" in result.breakdown
        assert result.breakdown["merge"] == DEFAULT_WEIGHTS["merge"]


class TestDeleteClause:
    """Lines 252-254: Delete clause adds delete weight."""

    def test_delete_adds_delete_weight(self) -> None:
        ast = _parse("MATCH (a:Person) DELETE a")
        result = score_query(ast)
        assert "delete" in result.breakdown
        assert result.breakdown["delete"] == DEFAULT_WEIGHTS["delete"]


class TestNullExpressionGuard:
    """Line 276: _score_expression returns early when expression is None."""

    def test_none_expression_is_noop(self) -> None:
        result = ComplexityScore()
        w = {**DEFAULT_WEIGHTS}
        _score_expression(None, w, result, depth=0)
        assert result.total == 0
        assert result.breakdown == {}


class TestExistsSubquery:
    """Lines 299-301: EXISTS expression adds exists_subquery weight."""

    def test_exists_adds_weight(self) -> None:
        ast = _parse(
            "MATCH (a:Person) WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) } RETURN a.name",
        )
        result = score_query(ast)
        assert (
            result.breakdown.get("exists_subquery", 0)
            == DEFAULT_WEIGHTS["exists_subquery"]
        )

    def test_exists_scores_higher_than_plain_where(self) -> None:
        plain = score_query(
            _parse("MATCH (a:Person) WHERE a.age > 21 RETURN a.name"),
        )
        with_exists = score_query(
            _parse(
                "MATCH (a:Person) "
                "WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) } "
                "RETURN a.name",
            ),
        )
        assert with_exists.total > plain.total


class TestListComprehension:
    """Line 304: List comprehension scoring."""

    def test_list_comprehension_adds_weight(self) -> None:
        ast = _parse(
            "MATCH (a:Person) RETURN [x IN a.tags WHERE x > 1 | x * 2]",
        )
        result = score_query(ast)
        assert "list_comprehension" in result.breakdown
        assert (
            result.breakdown["list_comprehension"]
            == DEFAULT_WEIGHTS["list_comprehension"]
        )


class TestPatternComprehension:
    """Line 307: Pattern comprehension scoring."""

    def test_pattern_comprehension_adds_weight(self) -> None:
        ast = _parse("MATCH (a:Person) RETURN [(a)-[:KNOWS]->(b) | b.name]")
        result = score_query(ast)
        assert "list_comprehension" in result.breakdown
        assert (
            result.breakdown["list_comprehension"]
            == DEFAULT_WEIGHTS["list_comprehension"]
        )


class TestCaseExpression:
    """Line 310: CASE expression scoring."""

    def test_case_expression_adds_weight(self) -> None:
        ast = _parse(
            'MATCH (a:Person) RETURN CASE WHEN a.age > 21 THEN "adult" ELSE "minor" END',
        )
        result = score_query(ast)
        assert "case_expression" in result.breakdown
        assert (
            result.breakdown["case_expression"]
            == DEFAULT_WEIGHTS["case_expression"]
        )


class TestRecursiveSubExpressionLists:
    """Lines 325-326: Nested expression lists are scored recursively."""

    def test_nested_function_arguments_scored(self) -> None:
        """count() inside a RETURN triggers aggregation via argument recursion."""
        ast = _parse("MATCH (a:Person) RETURN count(a)")
        result = score_query(ast)
        assert "aggregation" in result.breakdown

    def test_deeply_nested_aggregation(self) -> None:
        """Aggregation nested inside another expression is still detected."""
        ast = _parse("MATCH (a:Person) RETURN count(a) + count(a)")
        result = score_query(ast)
        assert (
            result.breakdown.get("aggregation", 0)
            >= 2 * DEFAULT_WEIGHTS["aggregation"]
        )
