"""QueryCombiner — transforms multiple Cypher queries into a single combined query.

Uses dependency analysis from :mod:`pycypher.multi_query_analyzer` to determine
execution order, then sequences the queries' clauses with ``WITH *`` separators
so that variables from earlier queries are visible to later ones.

Key design decisions:

- **RETURN stripping**: Intermediate queries' RETURN clauses are removed since
  they would terminate execution.  Only the final query's RETURN is preserved.
- **WITH * insertion**: Between each pair of queries, a ``WITH *`` clause passes
  all bound variables forward.
- **Topological ordering**: Dependency analysis ensures CREATE queries execute
  before MATCH queries that consume their types.
"""

from __future__ import annotations

import time

from shared.logger import LOGGER

from pycypher.ast_models import Return
from pycypher.multi_query_analyzer import (
    DependencyGraph,
    QueryDependencyAnalyzer,
    QueryNode,
)


class QueryCombiner:
    """Combines multiple Cypher queries into a single executable query.

    Uses :class:`~pycypher.multi_query_analyzer.QueryDependencyAnalyzer` to
    determine execution order, strips intermediate RETURN clauses, and inserts
    ``WITH *`` between query stages.

    Example::

        combiner = QueryCombiner()
        combined = combiner.combine([
            ("q1", "CREATE (n:Person {name: 'Alice'})"),
            ("q2", "MATCH (n:Person) RETURN n.name"),
        ])
        # combined == "CREATE (n:Person {name: 'Alice'})\\nWITH *\\nMATCH (n:Person) RETURN n.name"

    """

    def __init__(self) -> None:
        """Initialise with an internal :class:`QueryDependencyAnalyzer`.

        The analyzer is used by :meth:`combine` to infer topological
        execution order from entity/relationship type dependencies.
        """
        self._analyzer = QueryDependencyAnalyzer()

    def combine(self, queries: list[tuple[str, str]]) -> str:
        """Combine multiple queries into a single Cypher query string.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.

        Returns:
            A single combined Cypher query string.  Returns ``""`` for
            empty input.

        Raises:
            ValueError: If the dependency graph contains circular dependencies.

        """
        if not queries:
            LOGGER.debug("combine: empty input — returning empty string")
            return ""

        _t0 = time.perf_counter()
        LOGGER.debug("combine: queries=%d", len(queries))
        graph = self._analyzer.analyze(queries)
        result = self.combine_from_graph(graph)
        LOGGER.debug(
            "combine: queries=%d  combined_len=%d  elapsed=%.3fms",
            len(queries),
            len(result),
            (time.perf_counter() - _t0) * 1000,
        )
        return result

    def combine_from_graph(self, graph: DependencyGraph) -> str:
        """Combine queries from a pre-built dependency graph.

        Args:
            graph: :class:`~pycypher.multi_query_analyzer.DependencyGraph`
                with nodes populated.

        Returns:
            A single combined Cypher query string.

        Raises:
            ValueError: If the graph contains circular dependencies.
            SecurityError: If the combined query exceeds the size limit.

        """
        _t0 = time.perf_counter()
        execution_order = graph.topological_sort()

        if not execution_order:
            LOGGER.debug("combine_from_graph: empty execution order")
            return ""

        parts: list[str] = []
        returns_stripped = 0
        for i, node in enumerate(execution_order):
            # Strip RETURN from all but the last query — RETURN would
            # terminate execution and prevent subsequent clauses from
            # seeing the bound variables.  Uses AST-level removal to
            # avoid regex corruption when RETURN appears inside string
            # literals.
            if i < len(execution_order) - 1:
                cypher = self._strip_return_clause_via_ast(node)
                if cypher != node.cypher_query.strip():
                    returns_stripped += 1
            else:
                cypher = node.cypher_query.strip()

            if i > 0:
                parts.append("WITH *")

            parts.append(cypher)

        combined = "\n".join(parts)

        # Enforce combined query size limit — N queries each under the
        # per-query limit could combine to exceed safe bounds.
        from pycypher.grammar_parser import _enforce_query_size_limit

        _enforce_query_size_limit(combined)

        LOGGER.debug(
            "combine_from_graph: stages=%d  returns_stripped=%d  "
            "with_star_inserted=%d  combined_len=%d  elapsed=%.3fms",
            len(execution_order),
            returns_stripped,
            max(0, len(execution_order) - 1),
            len(combined),
            (time.perf_counter() - _t0) * 1000,
        )
        return combined

    @staticmethod
    def _strip_return_clause_via_ast(node: QueryNode) -> str:
        """Remove the trailing RETURN clause using AST analysis.

        Checks whether the last clause in the parsed AST is a
        :class:`~pycypher.ast_models.Return`.  If so, reconstructs the
        query string from non-RETURN clauses.  This is safe against
        ``RETURN`` appearing inside string literals, unlike the previous
        regex approach.

        Args:
            node: A :class:`~pycypher.multi_query_analyzer.QueryNode`
                with a parsed AST.

        Returns:
            The query string with the trailing RETURN clause removed,
            or the original stripped string if no RETURN is found.

        """
        ast = node.ast
        if not ast.clauses:
            return node.cypher_query.strip()

        # Only strip if the last clause is a RETURN.
        if not isinstance(ast.clauses[-1], Return):
            return node.cypher_query.strip()

        # Rebuild the query from clauses, excluding the final RETURN.
        from pycypher.ast_models import Query
        from pycypher.ast_rewriter import ASTRewriter

        stripped_ast = Query(clauses=list(ast.clauses[:-1]))
        return ASTRewriter().to_cypher(stripped_ast)
