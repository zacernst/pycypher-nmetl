"""Tests for query complexity scoring and limits.

Verifies that ``score_query()`` computes correct complexity scores for
various query patterns, and that ``execute_query(max_complexity_score=...)``
rejects queries that exceed the configured limit.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star
from pycypher.ast_models import ASTConverter
from pycypher.query_complexity import QueryComplexityError, score_query


def _parse(cypher: str) -> object:
    """Parse a Cypher string to an AST node."""
    return ASTConverter().from_cypher(cypher)


class TestComplexityScoring:
    """Verify complexity scores for various query patterns."""

    def test_simple_match_return(self) -> None:
        ast = _parse("MATCH (p:Person) RETURN p.name")
        score = score_query(ast)
        assert score.total > 0
        assert "clauses" in score.breakdown

    def test_optional_match_scores_higher(self) -> None:
        simple = score_query(_parse("MATCH (p:Person) RETURN p.name"))
        optional = score_query(
            _parse(
                "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q) RETURN p.name"
            )
        )
        assert optional.total > simple.total

    def test_variable_length_path_scores_high(self) -> None:
        fixed = score_query(
            _parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name")
        )
        varlen = score_query(
            _parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name")
        )
        assert varlen.total > fixed.total

    def test_unbounded_path_scores_highest(self) -> None:
        bounded = score_query(
            _parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name")
        )
        unbounded = score_query(
            _parse("MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name")
        )
        assert unbounded.total > bounded.total
        assert any(
            "unbounded" in w.lower() or "Unbounded" in w
            for w in unbounded.warnings
        )

    def test_aggregation_adds_complexity(self) -> None:
        no_agg = score_query(_parse("MATCH (p:Person) RETURN p.name"))
        with_agg = score_query(_parse("MATCH (p:Person) RETURN count(p)"))
        assert with_agg.total > no_agg.total

    def test_foreach_adds_complexity(self) -> None:
        no_foreach = score_query(_parse("MATCH (p:Person) RETURN p.name"))
        with_foreach = score_query(
            _parse(
                "MATCH (p:Person) FOREACH (x IN [1,2,3] | CREATE (n:Temp {v: x})) RETURN p.name"
            )
        )
        assert with_foreach.total > no_foreach.total

    def test_multiple_match_warns_cross_product(self) -> None:
        score = score_query(
            _parse("MATCH (a:Person) MATCH (b:Company) RETURN a.name, b.name")
        )
        assert any(
            "cross-product" in w.lower() or "cross_product" in w.lower()
            for w in score.warnings
        )

    def test_distinct_adds_complexity(self) -> None:
        no_dist = score_query(_parse("MATCH (p:Person) RETURN p.name"))
        with_dist = score_query(
            _parse("MATCH (p:Person) RETURN DISTINCT p.name")
        )
        assert with_dist.total > no_dist.total


class TestComplexityLimit:
    """Verify max_score enforcement."""

    def test_under_limit_succeeds(self) -> None:
        ast = _parse("MATCH (p:Person) RETURN p.name")
        score = score_query(ast, max_score=1000)
        assert score.total < 1000

    def test_over_limit_raises(self) -> None:
        ast = _parse("MATCH (p:Person) RETURN p.name")
        with pytest.raises(QueryComplexityError) as exc_info:
            score_query(ast, max_score=1)
        err = exc_info.value
        assert err.score > 1
        assert err.limit == 1
        assert isinstance(err, ValueError)


class TestComplexityErrorAttributes:
    """Verify QueryComplexityError diagnostic info."""

    def test_error_message(self) -> None:
        err = QueryComplexityError(
            score=50,
            limit=30,
            breakdown={"clauses": 20, "joins": 15, "agg": 15},
        )
        assert "50" in str(err)
        assert "30" in str(err)
        assert "clauses=20" in str(err)

    def test_error_inherits_value_error(self) -> None:
        err = QueryComplexityError(score=10, limit=5)
        assert isinstance(err, ValueError)


class TestExecuteQueryIntegration:
    """Verify execute_query with max_complexity_score parameter."""

    @pytest.fixture
    def star(self) -> Star:
        df = pd.DataFrame(
            {"__ID__": [1, 2], "name": ["Alice", "Bob"]},
        )
        return Star(context=ContextBuilder().add_entity("Person", df).build())

    def test_no_limit_runs(self, star: Star) -> None:
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 2

    def test_generous_limit_runs(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name",
            max_complexity_score=1000,
        )
        assert len(result) == 2

    def test_tiny_limit_raises(self, star: Star) -> None:
        with pytest.raises(QueryComplexityError):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                max_complexity_score=1,
            )

    def test_subsequent_query_works_after_rejection(self, star: Star) -> None:
        with pytest.raises(QueryComplexityError):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name", max_complexity_score=1
            )
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 2
