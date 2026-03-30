"""Tests for pycypher.multi_query_rewriter module.

Covers QueryRewriter, MultiQueryRewriter, and execute_combined — targeting 0% → 90%+ coverage.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star
from pycypher.multi_query_rewriter import (
    MultiQueryRewriter,
    QueryRewriter,
)


@pytest.fixture
def star() -> Star:
    """Star with Person entities."""
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    ctx = ContextBuilder().add_entity("Person", people).build()
    return Star(context=ctx)


class TestQueryRewriter:
    """Tests for QueryRewriter class."""

    def test_init(self) -> None:
        rewriter = QueryRewriter()
        assert rewriter._combiner is not None

    def test_combine_queries(self) -> None:
        rewriter = QueryRewriter()
        analyzer = MultiQueryRewriter().dependency_analyzer
        graph = analyzer.analyze(
            [
                ("q1", "MATCH (n:Person) RETURN n.name"),
            ],
        )
        result = rewriter.combine_queries(graph)
        assert isinstance(result, str)
        assert len(result) > 0


class TestMultiQueryRewriter:
    """Tests for MultiQueryRewriter class."""

    def test_init(self) -> None:
        mqr = MultiQueryRewriter()
        assert mqr.dependency_analyzer is not None
        assert mqr.query_rewriter is not None

    def test_analyze_dependencies(self) -> None:
        mqr = MultiQueryRewriter()
        graph = mqr.analyze_dependencies(
            [
                ("q1", "MATCH (n:Person) RETURN n.name AS name"),
                ("q2", "MATCH (n:Person) RETURN n.age AS age"),
            ],
        )
        assert len(graph.nodes) == 2

    def test_execute_combined_single_query(self, star: Star) -> None:
        mqr = MultiQueryRewriter()
        result = mqr.execute_combined(
            [("q1", "MATCH (n:Person) RETURN n.name AS name")],
            star,
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_execute_combined_multiple_queries(self, star: Star) -> None:
        mqr = MultiQueryRewriter()
        result = mqr.execute_combined(
            [
                ("q1", "MATCH (n:Person) RETURN n.name AS name"),
                ("q2", "MATCH (n:Person) RETURN n.name AS name"),
            ],
            star,
        )
        assert isinstance(result, pd.DataFrame)
