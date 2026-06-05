"""Multi-query dependency analysis — extracts produces/consumes from ASTs.

Provides the core dependency analysis components for multi-query composition:

- :class:`QueryNode` — represents a single query with produces/consumes metadata
- :class:`DependencyGraph` — container with topological sort capability
- :class:`QueryDependencyAnalyzer` — extracts entity/relationship types from ASTs

These components are used by :mod:`pycypher.multi_query_rewriter` to determine
execution order when combining multiple Cypher queries.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

from shared.logger import LOGGER

from pycypher.ast_models import (
    ASTConverter,
    Create,
    Match,
    Merge,
    NodePattern,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipPattern,
    Set,
    Variable,
)
from pycypher.exceptions import CyclicDependencyError

# Query ID must be alphanumeric, underscores, hyphens, or dots.
# Max 256 characters to prevent memory abuse in error messages.
_QUERY_ID_PATTERN: re.Pattern[str] = re.compile(r"^[\w.\-]{1,256}$")
_MAX_QUERIES: int = 1_000


@dataclass
class QueryNode:
    """Represents one query in the dependency analysis.

    Attributes:
        query_id: Unique identifier for this query.
        cypher_query: Original Cypher query string.
        ast: Parsed :class:`~pycypher.ast_models.Query` AST.
        produces: Entity/relationship types created by this query.
        consumes: Entity/relationship types matched by this query.
        dependencies: IDs of other QueryNodes this query depends on.

    """

    query_id: str
    cypher_query: str
    ast: Query
    produces: set[str] = field(default_factory=set)
    consumes: set[str] = field(default_factory=set)
    dependencies: set[str] = field(default_factory=set)


@dataclass
class DependencyGraph:
    """Dependency graph for multi-query execution planning.

    Attributes:
        nodes: List of :class:`QueryNode` instances in the graph.

    """

    nodes: list[QueryNode]

    def topological_sort(self) -> list[QueryNode]:
        """Return nodes in dependency-respecting execution order.

        Uses Kahn's algorithm: repeatedly selects nodes whose dependencies
        have all been processed.

        Returns:
            List of :class:`QueryNode` in valid execution order.

        Raises:
            ValueError: If a circular dependency is detected.

        """
        _t0 = time.perf_counter()
        LOGGER.debug(
            "topological_sort: nodes=%d",
            len(self.nodes),
        )
        result: list[QueryNode] = []
        remaining = {node.query_id for node in self.nodes}
        node_map = {node.query_id: node for node in self.nodes}

        while remaining:
            ready = [
                node_map[nid]
                for nid in remaining
                if not (node_map[nid].dependencies & remaining)
            ]

            if not ready:
                LOGGER.error(
                    "topological_sort: circular dependency  remaining=%s",
                    remaining,
                )
                raise CyclicDependencyError(remaining)

            result.extend(ready)
            for node in ready:
                remaining.remove(node.query_id)

        LOGGER.debug(
            "topological_sort: order=[%s]  elapsed=%.3fms",
            ", ".join(n.query_id for n in result),
            (time.perf_counter() - _t0) * 1000,
        )
        return result


class QueryDependencyAnalyzer:
    """Analyzes query ASTs to build a dependency graph.

    Parses each Cypher query, extracts the entity/relationship types it
    creates (produces) and matches (consumes), then infers inter-query
    dependencies from the produces/consumes overlap.
    """

    def analyze(self, queries: list[tuple[str, str]]) -> DependencyGraph:
        """Analyze multiple queries to build a dependency graph.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.

        Returns:
            :class:`DependencyGraph` with produces/consumes/dependencies populated.

        Raises:
            SecurityError: If too many queries are submitted or a query ID
                is invalid.
            ValueError: If a query ID is invalid.

        """
        _t0 = time.perf_counter()
        LOGGER.debug("analyze: queries=%d", len(queries))

        if len(queries) > _MAX_QUERIES:
            from pycypher.exceptions import SecurityError

            msg = (
                f"Too many queries ({len(queries)}) submitted for "
                f"dependency analysis. Maximum is {_MAX_QUERIES}."
            )
            raise SecurityError(msg)

        nodes: list[QueryNode] = []

        # Step 1: Parse each query and extract produces/consumes.
        for query_id, cypher in queries:
            if not _QUERY_ID_PATTERN.match(query_id):
                msg = (
                    f"Invalid query_id: must match [\\w.\\-]{{1,256}}. "
                    f"Got: {query_id!r:.80}"
                )
                raise ValueError(msg)
            ast = ASTConverter.from_cypher(cypher)
            var_to_labels = self._build_var_label_map(ast)
            produces = self._extract_produced_types(ast)
            produces.update(
                self._extract_produced_properties(ast, var_to_labels),
            )
            consumes = self._extract_consumed_types(ast)
            consumes.update(
                self._extract_consumed_properties(ast, var_to_labels),
            )

            node = QueryNode(
                query_id=query_id,
                cypher_query=cypher,
                ast=ast,
                produces=produces,
                consumes=consumes,
            )
            nodes.append(node)
            LOGGER.debug(
                "analyze: query=%s  produces=%s  consumes=%s",
                query_id,
                produces,
                consumes,
            )

        # Step 2: Infer dependency relationships from type overlap.
        dep_count = 0
        for node in nodes:
            for other in nodes:
                if node is other:
                    continue
                # node depends on other if node consumes what other produces.
                if node.consumes & other.produces:
                    node.dependencies.add(other.query_id)
                    dep_count += 1

        LOGGER.debug(
            "analyze: nodes=%d  dependency_edges=%d  elapsed=%.3fms",
            len(nodes),
            dep_count,
            (time.perf_counter() - _t0) * 1000,
        )
        return DependencyGraph(nodes=nodes)

    def _extract_produced_types(self, ast: Query) -> set[str]:
        """Extract entity/relationship types created by this query.

        Scans CREATE and MERGE clauses for node labels and relationship types.
        """
        produced: set[str] = set()

        for clause in ast.clauses:
            if (isinstance(clause, Create) and clause.pattern) or (
                isinstance(clause, Merge) and clause.pattern
            ):
                produced.update(
                    self._extract_types_from_pattern(clause.pattern),
                )

        return produced

    def _extract_consumed_types(self, ast: Query) -> set[str]:
        """Extract entity/relationship types matched by this query.

        Scans MATCH and MERGE clauses for node labels and relationship types.
        MERGE consumes because it attempts to match before creating.
        """
        consumed: set[str] = set()

        for clause in ast.clauses:
            if (isinstance(clause, Match) and clause.pattern) or (
                isinstance(clause, Merge) and clause.pattern
            ):
                consumed.update(
                    self._extract_types_from_pattern(clause.pattern),
                )

        return consumed

    def _extract_types_from_pattern(self, pattern: Pattern) -> set[str]:
        """Extract all entity/relationship type labels from a pattern."""
        types: set[str] = set()

        for path in pattern.paths:
            if isinstance(path, PatternPath):
                for element in path.elements:
                    if isinstance(element, (NodePattern, RelationshipPattern)):
                        types.update(element.labels)

        return types

    def _build_var_label_map(self, ast: Query) -> dict[str, set[str]]:
        """Map each pattern variable to the set of labels it carries.

        Walks every MATCH/MERGE/CREATE pattern and records the labels declared
        on each node variable.  When a variable is re-bound across clauses
        (e.g. `MATCH (n:A) ... MATCH (n:B)` — atypical but legal), the union
        of labels is recorded.

        Variables introduced by WITH/UNWIND that don't carry a label cannot
        be resolved and are simply absent from the map; downstream property
        tracking skips them.
        """
        var_to_labels: dict[str, set[str]] = {}
        pattern_clauses: list[Pattern] = []
        for clause in ast.clauses:
            if (
                isinstance(clause, (Match, Merge, Create))
                and clause.pattern is not None
            ):
                pattern_clauses.append(clause.pattern)

        for pattern in pattern_clauses:
            for path in pattern.paths:
                if not isinstance(path, PatternPath):
                    continue
                for element in path.elements:
                    if not isinstance(element, NodePattern):
                        continue
                    name = self._node_pattern_var_name(element)
                    if name is None or not element.labels:
                        continue
                    var_to_labels.setdefault(name, set()).update(
                        element.labels,
                    )
        return var_to_labels

    @staticmethod
    def _node_pattern_var_name(node: NodePattern) -> str | None:
        """Return the variable name on *node*, or None for anonymous patterns.

        ``NodePattern.variable`` may be a :class:`Variable`, a bare string, or
        ``None`` depending on how the AST was built.  Normalize to ``str | None``.
        """
        var = getattr(node, "variable", None)
        if var is None:
            return None
        if isinstance(var, Variable):
            return var.name
        if isinstance(var, str):
            return var
        return None

    def _extract_produced_properties(
        self,
        ast: Query,
        var_to_labels: dict[str, set[str]],
    ) -> set[str]:
        """Return ``{Label.prop}`` entries for every ``SET v.prop = ...`` write.

        Only ``SetPropertyItem`` writes are tracked; ``SET v = {map}`` and
        ``SET v += {map}`` cannot be resolved statically and are silently
        skipped (a conservative choice — under-reporting beats false edges).

        Variables that don't appear in *var_to_labels* (e.g. WITH-introduced
        names without a known label) skip property tracking.
        """
        produced: set[str] = set()
        for set_node in ast.find_all(Set):
            for item in set_node.items:
                # The AST converter emits plain SetItem nodes:
                #   SET v.prop = expr  → property="prop"
                #   SET v:Label        → property=None  (label-set)
                #   SET v = {map}      → property="*"   (full-replace)
                #   SET v += {map}     → property="*"   (merge)
                # We track only the explicit-property form.
                if item.property is None or item.property == "*":
                    continue
                var_name = (
                    item.variable.name
                    if isinstance(item.variable, Variable)
                    else None
                )
                if var_name is None:
                    continue
                labels = var_to_labels.get(var_name)
                if not labels:
                    continue
                for label in labels:
                    produced.add(f"{label}.{item.property}")
        return produced

    def _extract_consumed_properties(
        self,
        ast: Query,
        var_to_labels: dict[str, set[str]],
    ) -> set[str]:
        """Return ``{Label.prop}`` entries for every property *read* in the query.

        Walks every :class:`PropertyLookup` reachable from the AST — covers
        WHERE, RETURN, WITH projections, ORDER BY, and SET right-hand-sides.
        A lookup whose receiver is something other than a :class:`Variable`
        (e.g. the result of a function call or map indexing) is skipped, as
        is a variable with no resolvable label.
        """
        consumed: set[str] = set()
        for lookup in ast.find_all(PropertyLookup):
            receiver = lookup.expression
            # Tolerate the legacy ``variable`` field on PropertyLookup.
            if receiver is None:
                receiver = lookup.variable
            if not isinstance(receiver, Variable):
                continue
            if lookup.property is None:
                continue
            labels = var_to_labels.get(receiver.name)
            if not labels:
                continue
            for label in labels:
                consumed.add(f"{label}.{lookup.property}")
        return consumed
