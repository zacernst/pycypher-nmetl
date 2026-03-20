"""Multi-query AST rewriting for single combined query execution.

This module implements the "Single Combined Query" approach for multi-query
composition. Instead of executing multiple queries sequentially with
intermediate state management, it rewrites multiple Cypher queries into a
single optimized AST and executes once via the existing Star.execute_query()
path.

Key components:

- :class:`~pycypher.multi_query_analyzer.QueryDependencyAnalyzer` — dependency analysis
- :class:`QueryRewriter` — combines multiple ASTs into single optimized query
- :class:`MultiQueryRewriter` — high-level interface for rewrite + execute
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from shared.logger import LOGGER

# Re-export analyzer components for backward compatibility.
from pycypher.multi_query_analyzer import (
    DependencyGraph,
    QueryDependencyAnalyzer,
)
from pycypher.query_combiner import QueryCombiner

if TYPE_CHECKING:
    import pandas as pd

    from pycypher.star import Star


class QueryRewriter:
    """Rewrites multiple queries into a single combined AST.

    Delegates to :class:`~pycypher.query_combiner.QueryCombiner` for the
    actual combination logic.  This class exists as a thin wrapper that
    can be extended with more sophisticated optimisation strategies
    (join merging, predicate pushdown) in the future.
    """

    def __init__(self) -> None:
        """Initialise the rewriter with an internal :class:`QueryCombiner`."""
        self._combiner = QueryCombiner()

    def combine_queries(self, dependency_graph: DependencyGraph) -> str:
        """Combine multiple queries into a single optimised Cypher query.

        Args:
            dependency_graph: Analysed dependencies between queries.

        Returns:
            Combined Cypher query string.

        """
        _t0 = time.perf_counter()
        LOGGER.debug(
            "QueryRewriter.combine_queries: nodes=%d",
            len(dependency_graph.nodes),
        )
        result = self._combiner.combine_from_graph(dependency_graph)
        LOGGER.debug(
            "QueryRewriter.combine_queries: combined_len=%d  elapsed=%.3fms",
            len(result),
            (time.perf_counter() - _t0) * 1000,
        )
        return result


class MultiQueryRewriter:
    """High-level interface for multi-query rewriting and execution.

    Orchestrates dependency analysis, query combination, and execution
    through the existing :meth:`Star.execute_query` path.

    Example::

        rewriter = MultiQueryRewriter()
        result = rewriter.execute_combined([
            ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ("q2", "MATCH (n:Person) RETURN n.name"),
        ], star)

    """

    def __init__(self) -> None:
        """Initialise the rewriter with dependency analysis and query rewriting components.

        Creates a :class:`QueryDependencyAnalyzer` for extracting
        produces/consumes metadata and a :class:`QueryRewriter` for
        combining ordered ASTs into a single Cypher string.
        """
        self.dependency_analyzer = QueryDependencyAnalyzer()
        self.query_rewriter = QueryRewriter()

    def execute_combined(
        self,
        queries: list[tuple[str, str]],
        star: Star,
    ) -> pd.DataFrame:
        """Execute multiple queries as a single combined query.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.
            star: :class:`~pycypher.star.Star` instance for execution.

        Returns:
            Final result DataFrame from combined query execution.

        """
        _t0 = time.perf_counter()
        LOGGER.debug("execute_combined: queries=%d", len(queries))

        _t_analyze = time.perf_counter()
        dependency_graph = self.dependency_analyzer.analyze(queries)
        _analyze_ms = (time.perf_counter() - _t_analyze) * 1000

        _t_rewrite = time.perf_counter()
        combined_cypher = self.query_rewriter.combine_queries(dependency_graph)
        _rewrite_ms = (time.perf_counter() - _t_rewrite) * 1000

        _t_exec = time.perf_counter()
        result = star.execute_query(combined_cypher)
        _exec_ms = (time.perf_counter() - _t_exec) * 1000

        LOGGER.debug(
            "execute_combined: queries=%d  rows=%d  "
            "analyze=%.3fms  rewrite=%.3fms  execute=%.3fms  total=%.3fms",
            len(queries),
            len(result),
            _analyze_ms,
            _rewrite_ms,
            _exec_ms,
            (time.perf_counter() - _t0) * 1000,
        )
        return result

    def analyze_dependencies(
        self,
        queries: list[tuple[str, str]],
    ) -> DependencyGraph:
        """Analyse query dependencies without executing.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.

        Returns:
            :class:`~pycypher.multi_query_analyzer.DependencyGraph`
            with produces/consumes/dependencies populated.

        """
        return self.dependency_analyzer.analyze(queries)
