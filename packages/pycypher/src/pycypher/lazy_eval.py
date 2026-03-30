"""Lazy evaluation engine for deferred query execution.

Builds a computation graph of operations that are only executed when
results are materialised.  This enables:

1. **Operation fusion** — combine compatible sequential operations
   (e.g. filter→filter becomes a single filter with combined predicate).
2. **Predicate pushdown** — move filters as early as possible in the graph.
3. **Dead column elimination** — drop columns that are never referenced
   downstream.
4. **Memory estimation** — estimate peak memory before execution to select
   optimal backend/strategy.

The computation graph is a DAG where nodes are operations and edges
represent data flow.  Think of it as the DHF stack's neural mapping —
consciousness (data) flows through optimised pathways before being
materialised in the target sleeve (output DataFrame).

Architecture
------------

::

    LazyFrame  (user-facing handle)
      └── ComputationGraph (internal DAG)
            ├── ScanNode (leaf: entity/relationship scan)
            ├── FilterNode (predicate application)
            ├── JoinNode (two-input merge)
            ├── ProjectNode (column selection)
            ├── AggNode (aggregation)
            ├── SortNode (ordering)
            └── LimitNode (row truncation)

Usage::

    lf = LazyFrame.from_scan("Person", context)
    lf = lf.filter(lambda ids: ids > 100)
    lf = lf.join(other_lf, on="id")
    result = lf.collect()  # triggers execution
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

from shared.logger import LOGGER

# ---------------------------------------------------------------------------
# Operation types
# ---------------------------------------------------------------------------


class OpType(enum.Enum):
    """Types of operations in the computation graph."""

    SCAN = "scan"
    FILTER = "filter"
    JOIN = "join"
    PROJECT = "project"
    AGGREGATE = "aggregate"
    SORT = "sort"
    LIMIT = "limit"
    UNION = "union"


# ---------------------------------------------------------------------------
# Computation graph nodes
# ---------------------------------------------------------------------------


@dataclass
class OpNode:
    """A single operation in the computation graph.

    Attributes:
        op_type: The type of operation.
        params: Operation-specific parameters.
        inputs: List of input node IDs (empty for scan nodes).
        node_id: Unique identifier for this node.
        estimated_rows: Estimated output row count (for planning).
        estimated_memory_bytes: Estimated peak memory during execution.

    """

    op_type: OpType
    params: dict[str, Any] = field(default_factory=dict)
    inputs: list[int] = field(default_factory=list)
    node_id: int = 0
    estimated_rows: int = 0
    estimated_memory_bytes: int = 0


@dataclass
class ComputationGraph:
    """Directed acyclic graph of operations.

    The graph tracks operation dependencies and enables optimisation
    passes before execution.

    Attributes:
        nodes: Mapping from node ID to OpNode.
        output_node: The ID of the terminal (output) node.

    """

    nodes: dict[int, OpNode] = field(default_factory=dict)
    output_node: int = 0
    _next_id: int = field(default=0, repr=False)

    def add_node(self, op: OpNode) -> int:
        """Add an operation node and return its ID.

        Args:
            op: The operation node to add.

        Returns:
            The assigned node ID.

        """
        node_id = self._next_id
        self._next_id += 1
        op.node_id = node_id
        self.nodes[node_id] = op
        self.output_node = node_id
        return node_id

    def get_inputs(self, node_id: int) -> list[OpNode]:
        """Return the input nodes for the given node.

        Args:
            node_id: The node whose inputs to retrieve.

        Returns:
            List of input OpNode instances.

        """
        node = self.nodes[node_id]
        return [self.nodes[i] for i in node.inputs]

    def topological_order(self) -> list[int]:
        """Return node IDs in topological (execution) order.

        Returns:
            List of node IDs where each node appears after all its inputs.
            Empty list if the graph has no nodes.

        """
        if not self.nodes:
            return []

        visited: set[int] = set()
        order: list[int] = []

        def visit(nid: int) -> None:
            """DFS visit node and its dependencies, appending to order."""
            if nid in visited or nid not in self.nodes:
                return
            visited.add(nid)
            for input_id in self.nodes[nid].inputs:
                visit(input_id)
            order.append(nid)

        visit(self.output_node)
        return order


# ---------------------------------------------------------------------------
# Optimisation passes
# ---------------------------------------------------------------------------


def fuse_filters(graph: ComputationGraph) -> ComputationGraph:
    """Fuse consecutive filter nodes into a single filter.

    When two filters are chained (F2 ← F1 ← source), they can be
    combined into a single filter with an AND-combined predicate.
    This reduces DataFrame materialisation from 2× to 1×.

    Args:
        graph: The computation graph to optimise.

    Returns:
        A new graph with consecutive filters fused.

    """
    # Find filter→filter chains.  Track which nodes are consumed by a
    # fusion so they are not emitted independently.
    LOGGER.debug(
        "fuse_filters: scanning %d nodes for filter chains",
        len(graph.nodes),
    )
    fused = ComputationGraph()
    fused._next_id = graph._next_id
    id_map: dict[int, int] = {}
    consumed: set[int] = set()  # parent filter IDs absorbed by fusion

    # First pass: identify which parent filters will be fused
    for nid in graph.topological_order():
        node = graph.nodes[nid]
        if (
            node.op_type == OpType.FILTER
            and len(node.inputs) == 1
            and graph.nodes[node.inputs[0]].op_type == OpType.FILTER
        ):
            consumed.add(node.inputs[0])

    for nid in graph.topological_order():
        node = graph.nodes[nid]

        # Skip nodes that have been consumed by a fusion
        if nid in consumed:
            continue

        if (
            node.op_type == OpType.FILTER
            and len(node.inputs) == 1
            and node.inputs[0] in consumed
        ):
            # Fuse: combine predicates from both filters
            parent = graph.nodes[node.inputs[0]]
            fused_params = {
                "predicates": (
                    parent.params.get(
                        "predicates",
                        [parent.params.get("predicate")],
                    )
                    + [node.params.get("predicate")]
                ),
                "fused": True,
            }
            fused_node = OpNode(
                op_type=OpType.FILTER,
                params=fused_params,
                inputs=[id_map.get(i, i) for i in parent.inputs],
                estimated_rows=node.estimated_rows,
            )
            new_id = fused.add_node(fused_node)
            id_map[nid] = new_id
            id_map[parent.node_id] = new_id
        else:
            # Copy node with remapped inputs
            new_node = OpNode(
                op_type=node.op_type,
                params=dict(node.params),
                inputs=[id_map.get(i, i) for i in node.inputs],
                estimated_rows=node.estimated_rows,
                estimated_memory_bytes=node.estimated_memory_bytes,
            )
            new_id = fused.add_node(new_node)
            id_map[nid] = new_id

    fused.output_node = id_map.get(graph.output_node, graph.output_node)
    fused_count = len(consumed)
    if fused_count:
        LOGGER.debug(
            "fuse_filters: fused %d filter pairs into single filters",
            fused_count,
        )
    return fused


def push_filters_down(graph: ComputationGraph) -> ComputationGraph:
    """Push filter operations below joins where possible.

    If a filter only references columns from one side of a join,
    it can be applied before the join — reducing the join's input size.

    Args:
        graph: The computation graph to optimise.

    Returns:
        A new graph with filters pushed down past joins.

    """
    LOGGER.debug(
        "push_filters_down: scanning %d nodes for pushdown opportunities",
        len(graph.nodes),
    )
    optimised = ComputationGraph()
    optimised._next_id = graph._next_id
    id_map: dict[int, int] = {}
    _pushdowns = 0

    for nid in graph.topological_order():
        node = graph.nodes[nid]

        if (
            node.op_type == OpType.FILTER
            and len(node.inputs) == 1
            and graph.nodes[node.inputs[0]].op_type == OpType.JOIN
        ):
            join_node = graph.nodes[node.inputs[0]]
            filter_cols = set(node.params.get("columns_referenced", []))
            left_cols = set(join_node.params.get("left_columns", []))
            right_cols = set(join_node.params.get("right_columns", []))

            if filter_cols and filter_cols.issubset(left_cols):
                # Push filter to left input of join
                left_input = join_node.inputs[0] if join_node.inputs else None
                if left_input is not None:
                    mapped_left = id_map.get(left_input, left_input)
                    filter_node = OpNode(
                        op_type=OpType.FILTER,
                        params=dict(node.params),
                        inputs=[mapped_left],
                        estimated_rows=node.estimated_rows,
                    )
                    filter_id = optimised.add_node(filter_node)
                    # Create new join with filtered left input
                    new_join = OpNode(
                        op_type=OpType.JOIN,
                        params=dict(join_node.params),
                        inputs=[
                            filter_id,
                            *(id_map.get(i, i) for i in join_node.inputs[1:]),
                        ],
                        estimated_rows=join_node.estimated_rows,
                    )
                    new_id = optimised.add_node(new_join)
                    id_map[nid] = new_id
                    id_map[join_node.node_id] = new_id
                    _pushdowns += 1
                    continue
            elif filter_cols and filter_cols.issubset(right_cols):
                # Push filter to right input of join
                right_input = (
                    join_node.inputs[1] if len(join_node.inputs) > 1 else None
                )
                if right_input is not None:
                    mapped_right = id_map.get(right_input, right_input)
                    filter_node = OpNode(
                        op_type=OpType.FILTER,
                        params=dict(node.params),
                        inputs=[mapped_right],
                        estimated_rows=node.estimated_rows,
                    )
                    filter_id = optimised.add_node(filter_node)
                    new_join = OpNode(
                        op_type=OpType.JOIN,
                        params=dict(join_node.params),
                        inputs=[
                            id_map.get(
                                join_node.inputs[0],
                                join_node.inputs[0],
                            ),
                            filter_id,
                        ],
                        estimated_rows=join_node.estimated_rows,
                    )
                    new_id = optimised.add_node(new_join)
                    id_map[nid] = new_id
                    id_map[join_node.node_id] = new_id
                    _pushdowns += 1
                    continue

        # Default: copy node with remapped inputs
        new_node = OpNode(
            op_type=node.op_type,
            params=dict(node.params),
            inputs=[id_map.get(i, i) for i in node.inputs],
            estimated_rows=node.estimated_rows,
            estimated_memory_bytes=node.estimated_memory_bytes,
        )
        new_id = optimised.add_node(new_node)
        id_map[nid] = new_id

    optimised.output_node = id_map.get(graph.output_node, graph.output_node)
    if _pushdowns:
        LOGGER.debug(
            "push_filters_down: pushed %d filters below joins",
            _pushdowns,
        )
    return optimised


def estimate_memory(graph: ComputationGraph, avg_row_bytes: int = 100) -> int:
    """Estimate peak memory usage for executing the computation graph.

    Walks the graph in topological order and estimates the peak
    concurrent memory — like calculating the maximum neural bandwidth
    required for a multi-sleeve consciousness transfer.

    Args:
        graph: The computation graph to analyse.
        avg_row_bytes: Average bytes per row for estimation.

    Returns:
        Estimated peak memory in bytes.

    """
    LOGGER.debug(
        "estimate_memory: analysing %d nodes (avg_row_bytes=%d)",
        len(graph.nodes),
        avg_row_bytes,
    )
    peak = 0
    active: dict[int, int] = {}  # node_id → estimated bytes

    # Pre-build consumer counts: how many downstream nodes consume each input.
    # This replaces an O(N³) nested loop with O(V+E) construction + O(1) lookups.
    consumer_count: dict[int, int] = {}
    for node in graph.nodes.values():
        for input_id in node.inputs:
            consumer_count[input_id] = consumer_count.get(input_id, 0) + 1

    for nid in graph.topological_order():
        node = graph.nodes[nid]
        node_mem = node.estimated_rows * avg_row_bytes

        if node.op_type == OpType.JOIN and len(node.inputs) == 2:
            # Join needs both inputs + output in memory simultaneously
            input_mem = sum(active.get(i, 0) for i in node.inputs)
            concurrent = input_mem + node_mem
        elif node.op_type == OpType.SORT:
            # Sort needs input + temp sort buffer
            input_mem = sum(active.get(i, 0) for i in node.inputs)
            concurrent = input_mem + node_mem
        else:
            concurrent = node_mem

        peak = max(peak, concurrent)

        # Release input memory when no longer needed
        for input_id in node.inputs:
            consumer_count[input_id] -= 1
            if consumer_count[input_id] == 0 and input_id in active:
                del active[input_id]

        active[nid] = node_mem

    LOGGER.debug(
        "estimate_memory: peak estimated at %d bytes (%.1f MB)",
        peak,
        peak / (1024 * 1024),
    )
    return peak


# ---------------------------------------------------------------------------
# AST → ComputationGraph translation
# ---------------------------------------------------------------------------


def _extract_variables_from_predicate(predicate: Any) -> set[str]:
    """Extract variable names referenced by a WHERE predicate.

    Delegates to :func:`~pycypher.ast_models.extract_referenced_variables`,
    the canonical variable extraction function.  The underlying traversal
    is depth-limited and handles all AST node types automatically.

    Args:
        predicate: An AST predicate expression (must be an ASTNode).

    Returns:
        Set of variable name strings.

    Raises:
        SecurityError: If the AST nesting depth exceeds the configured limit.

    """
    from pycypher.ast_models import ASTNode, extract_referenced_variables

    if isinstance(predicate, ASTNode):
        return extract_referenced_variables(predicate)
    return set()

    return refs


def build_computation_graph(query: Any) -> ComputationGraph:
    """Translate a parsed Cypher Query AST into a ComputationGraph.

    This bridges the gap between the AST (produced by the grammar parser)
    and the lazy evaluation engine.  The resulting graph can be optimised
    with :func:`fuse_filters`, :func:`push_filters_down`, and analysed
    with :func:`estimate_memory` before execution.

    The translation is *structural* — it mirrors the clause sequence in the
    AST rather than simulating full execution semantics.  This is sufficient
    for the optimisation passes to detect filter fusion and pushdown
    opportunities.

    Args:
        query: A parsed :class:`~pycypher.ast_models.Query` AST node.

    Returns:
        A :class:`ComputationGraph` representing the query's operations.

    """
    from pycypher.ast_models import (
        Match,
        NodePattern,
        RelationshipPattern,
        Return,
        Unwind,
        With,
    )

    graph = ComputationGraph()
    current_node_id: int | None = None
    clause_count = len(query.clauses) if hasattr(query, "clauses") else 0
    LOGGER.debug(
        "build_computation_graph: translating %d clauses to computation DAG",
        clause_count,
    )

    for clause in query.clauses:
        if isinstance(clause, Match):
            # Each pattern path starts with node scans and potentially joins
            pattern = clause.pattern
            path_node_ids: list[int] = []

            for path in pattern.paths:
                elements = path.elements
                # First element is a node scan
                if elements:
                    first_el = elements[0]
                    var_name = ""
                    label = ""
                    if isinstance(first_el, NodePattern):
                        var_name = (
                            first_el.variable.name if first_el.variable else ""
                        )
                        label = first_el.labels[0] if first_el.labels else ""

                    scan_node = OpNode(
                        op_type=OpType.SCAN,
                        params={"entity_type": label, "variable": var_name},
                        inputs=[current_node_id]
                        if current_node_id is not None
                        else [],
                        estimated_rows=100,  # default estimate
                    )
                    scan_id = graph.add_node(scan_node)
                    prev_id = scan_id

                    # Walk relationship-node pairs
                    i = 1
                    while i + 1 <= len(elements) - 1:
                        rel_el = elements[i]
                        node_el = elements[i + 1]
                        i += 2

                        rel_label = ""
                        if (
                            isinstance(rel_el, RelationshipPattern)
                            and rel_el.labels
                        ):
                            rel_label = rel_el.labels[0]

                        next_var = ""
                        next_label = ""
                        if isinstance(node_el, NodePattern):
                            next_var = (
                                node_el.variable.name
                                if node_el.variable
                                else ""
                            )
                            next_label = (
                                node_el.labels[0] if node_el.labels else ""
                            )

                        # Scan the next node
                        next_scan = OpNode(
                            op_type=OpType.SCAN,
                            params={
                                "entity_type": next_label,
                                "variable": next_var,
                            },
                            estimated_rows=100,
                        )
                        next_scan_id = graph.add_node(next_scan)

                        # Join via relationship
                        left_cols = {var_name} if var_name else set()
                        right_cols = {next_var} if next_var else set()
                        join_node = OpNode(
                            op_type=OpType.JOIN,
                            params={
                                "rel_type": rel_label,
                                "left_columns": list(left_cols),
                                "right_columns": list(right_cols),
                            },
                            inputs=[prev_id, next_scan_id],
                            estimated_rows=200,
                        )
                        prev_id = graph.add_node(join_node)
                        # Accumulate all variables seen so far
                        if next_var:
                            left_cols.add(next_var)
                        var_name = next_var  # track for next hop

                    path_node_ids.append(prev_id)

            # If multiple paths, join them
            if len(path_node_ids) > 1:
                result_id = path_node_ids[0]
                for pid in path_node_ids[1:]:
                    cross_join = OpNode(
                        op_type=OpType.JOIN,
                        params={"cross_join": True},
                        inputs=[result_id, pid],
                        estimated_rows=500,
                    )
                    result_id = graph.add_node(cross_join)
                current_node_id = result_id
            elif path_node_ids:
                current_node_id = path_node_ids[0]

            # WHERE clause becomes a FILTER node
            if clause.where is not None:
                cols_referenced = _extract_variables_from_predicate(
                    clause.where,
                )
                filter_node = OpNode(
                    op_type=OpType.FILTER,
                    params={
                        "predicate": clause.where,
                        "columns_referenced": list(cols_referenced),
                    },
                    inputs=[current_node_id]
                    if current_node_id is not None
                    else [],
                    estimated_rows=50,
                )
                current_node_id = graph.add_node(filter_node)

        elif isinstance(clause, With):
            # WITH is a projection (possibly with aggregation)
            project_node = OpNode(
                op_type=OpType.PROJECT,
                params={"clause": "WITH"},
                inputs=[current_node_id]
                if current_node_id is not None
                else [],
                estimated_rows=100,
            )
            current_node_id = graph.add_node(project_node)

            # WITH WHERE becomes a filter after projection
            if clause.where is not None:
                cols_referenced = _extract_variables_from_predicate(
                    clause.where,
                )
                filter_node = OpNode(
                    op_type=OpType.FILTER,
                    params={
                        "predicate": clause.where,
                        "columns_referenced": list(cols_referenced),
                    },
                    inputs=[current_node_id],
                    estimated_rows=50,
                )
                current_node_id = graph.add_node(filter_node)

        elif isinstance(clause, Unwind):
            # UNWIND is like a flatMap — model as a special scan
            unwind_node = OpNode(
                op_type=OpType.SCAN,
                params={"clause": "UNWIND"},
                inputs=[current_node_id]
                if current_node_id is not None
                else [],
                estimated_rows=200,
            )
            current_node_id = graph.add_node(unwind_node)

        elif isinstance(clause, Return):
            # ORDER BY → SORT node
            if clause.order_by:
                sort_node = OpNode(
                    op_type=OpType.SORT,
                    params={"order_by": True},
                    inputs=[current_node_id]
                    if current_node_id is not None
                    else [],
                    estimated_rows=100,
                )
                current_node_id = graph.add_node(sort_node)

            # LIMIT → LIMIT node
            if clause.limit is not None:
                limit_node = OpNode(
                    op_type=OpType.LIMIT,
                    params={"limit": clause.limit},
                    inputs=[current_node_id]
                    if current_node_id is not None
                    else [],
                    estimated_rows=min(
                        100,
                        clause.limit if isinstance(clause.limit, int) else 100,
                    ),
                )
                current_node_id = graph.add_node(limit_node)

            # RETURN projection
            project_node = OpNode(
                op_type=OpType.PROJECT,
                params={"clause": "RETURN"},
                inputs=[current_node_id]
                if current_node_id is not None
                else [],
                estimated_rows=100,
            )
            current_node_id = graph.add_node(project_node)

    LOGGER.debug(
        "build_computation_graph: built DAG with %d nodes",
        len(graph.nodes),
    )
    return graph


# ---------------------------------------------------------------------------
# Dead column elimination
# ---------------------------------------------------------------------------


def _extract_clause_variables(clause: Any) -> set[str]:
    """Extract all variable names referenced by a single AST clause.

    Walks the clause's AST subtree to find every variable name that the
    clause reads or writes.  This includes pattern variables, WHERE
    predicates, RETURN/WITH projection items, SET/REMOVE targets, and
    DELETE expressions.

    Args:
        clause: An AST clause node (Match, With, Return, etc.).

    Returns:
        Set of variable name strings referenced by the clause.

    """
    from pycypher.ast_models import ASTNode, extract_referenced_variables

    refs: set[str] = set()

    # Generic: walk the clause itself if it's an ASTNode
    if isinstance(clause, ASTNode):
        refs.update(extract_referenced_variables(clause))

    return refs


def compute_live_columns(clauses: list[Any]) -> list[set[str] | None]:
    """Compute the set of live (needed) columns after each clause.

    For each clause index ``i``, returns the set of variable names that are
    referenced by *any* clause at index ``i+1`` or later.  A ``None`` entry
    means "keep all columns" (conservative fallback for mutation clauses
    where dropping columns could lose data).

    The result list has the same length as *clauses*.  Entry ``i`` is the
    set of columns that must be **retained** in the frame produced by clause
    ``i`` — any column not in this set can be safely dropped.

    Args:
        clauses: The ordered list of AST clause nodes from a parsed query.

    Returns:
        A list parallel to *clauses* where each element is either a
        ``set[str]`` of live variable names or ``None`` (keep-all).

    """
    from pycypher.ast_models import (
        Create,
        Delete,
        Foreach,
        Merge,
        Return,
        Set,
        With,
    )

    n = len(clauses)
    result: list[set[str] | None] = [None] * n

    if n == 0:
        return result

    # Last clause always keeps all columns (it's the output).
    result[-1] = None

    # Walk backwards: accumulate variables needed by all subsequent clauses.
    needed_after: set[str] = set()
    _keep_all = False  # True when a downstream clause needs all columns

    for i in range(n - 1, -1, -1):
        clause = clauses[i]

        # Mutation clauses (SET, DELETE, CREATE, MERGE, FOREACH) may read
        # any column's entity data — conservatively keep everything.
        if isinstance(clause, (Set, Delete, Create, Merge, Foreach)):
            result[i] = None
            # All preceding clauses must also keep everything since we
            # can't know what the mutation needs.
            for j in range(i):
                result[j] = None
            return result

        # RETURN * / WITH * (empty items list) reference all in-scope
        # columns — preceding clauses must keep everything.
        if isinstance(clause, (Return, With)) and not getattr(
            clause, "items", None
        ):
            _keep_all = True

        if _keep_all:
            result[i] = None
        else:
            clause_vars = _extract_clause_variables(clause)
            needed_after = needed_after | clause_vars

            if i < n - 1:
                result[i] = frozenset(needed_after) if needed_after else None

    return result
