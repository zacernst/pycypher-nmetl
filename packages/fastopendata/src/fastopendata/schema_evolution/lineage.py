"""Lightweight DAG-based data lineage tracking with impact analysis.

:class:`LineageGraph` models how data flows through transformations
(sources → processing steps → outputs).  It supports:

* Forward impact analysis: "if this source schema changes, which
  downstream datasets are affected?"
* Backward provenance: "where did this dataset's data come from?"
* Topological ordering for correct refresh sequencing.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto


class NodeType(Enum):
    """Classification of lineage graph nodes."""

    SOURCE = auto()  # External data source
    TRANSFORM = auto()  # Processing / transformation step
    SINK = auto()  # Output / materialized result
    VIEW = auto()  # Derived / materialized view


@dataclass(frozen=True, slots=True)
class LineageNode:
    """A vertex in the lineage graph."""

    node_id: str
    node_type: NodeType
    name: str
    schema_name: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LineageEdge:
    """A directed edge representing data flow from source to target."""

    source_id: str
    target_id: str
    transformation: str = ""
    metadata: dict[str, str] = field(default_factory=dict)


class LineageGraph:
    """DAG of data lineage relationships.

    Provides forward impact analysis, backward provenance tracing,
    and topological ordering for dependency-aware refresh.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, LineageNode] = {}
        self._edges: list[LineageEdge] = []
        # Adjacency lists
        self._forward: dict[str, list[str]] = {}  # parent → children
        self._backward: dict[str, list[str]] = {}  # child → parents

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        return len(self._edges)

    def add_node(self, node: LineageNode) -> None:
        self._nodes[node.node_id] = node
        self._forward.setdefault(node.node_id, [])
        self._backward.setdefault(node.node_id, [])

    def add_edge(self, edge: LineageEdge) -> None:
        if edge.source_id not in self._nodes:
            msg = f"Source node '{edge.source_id}' not in graph"
            raise ValueError(msg)
        if edge.target_id not in self._nodes:
            msg = f"Target node '{edge.target_id}' not in graph"
            raise ValueError(msg)
        self._edges.append(edge)
        self._forward[edge.source_id].append(edge.target_id)
        self._backward[edge.target_id].append(edge.source_id)

    def get_node(self, node_id: str) -> LineageNode | None:
        return self._nodes.get(node_id)

    def get_children(self, node_id: str) -> list[LineageNode]:
        """Direct downstream dependents."""
        return [self._nodes[cid] for cid in self._forward.get(node_id, [])]

    def get_parents(self, node_id: str) -> list[LineageNode]:
        """Direct upstream sources."""
        return [self._nodes[pid] for pid in self._backward.get(node_id, [])]

    def impact_analysis(self, node_id: str) -> list[LineageNode]:
        """Return all transitively downstream nodes (forward BFS).

        Answers: "If this node's schema or data changes, what is affected?"
        """
        visited: set[str] = set()
        queue: deque[str] = deque([node_id])
        result: list[LineageNode] = []

        while queue:
            current = queue.popleft()
            for child_id in self._forward.get(current, []):
                if child_id not in visited:
                    visited.add(child_id)
                    queue.append(child_id)
                    result.append(self._nodes[child_id])
        return result

    def provenance(self, node_id: str) -> list[LineageNode]:
        """Return all transitively upstream nodes (backward BFS).

        Answers: "Where did this data come from?"
        """
        visited: set[str] = set()
        queue: deque[str] = deque([node_id])
        result: list[LineageNode] = []

        while queue:
            current = queue.popleft()
            for parent_id in self._backward.get(current, []):
                if parent_id not in visited:
                    visited.add(parent_id)
                    queue.append(parent_id)
                    result.append(self._nodes[parent_id])
        return result

    def topological_order(self) -> list[LineageNode]:
        """Return nodes in dependency-safe processing order (Kahn's algorithm).

        Raises :class:`ValueError` if the graph contains a cycle.
        """
        in_degree: dict[str, int] = dict.fromkeys(self._nodes, 0)
        for edge in self._edges:
            in_degree[edge.target_id] += 1

        queue: deque[str] = deque(
            nid for nid, deg in in_degree.items() if deg == 0
        )
        result: list[LineageNode] = []

        while queue:
            nid = queue.popleft()
            result.append(self._nodes[nid])
            for child_id in self._forward.get(nid, []):
                in_degree[child_id] -= 1
                if in_degree[child_id] == 0:
                    queue.append(child_id)

        if len(result) != len(self._nodes):
            msg = "Lineage graph contains a cycle — topological order is undefined"
            raise ValueError(msg)

        return result

    def sources(self) -> list[LineageNode]:
        """Return all root source nodes (no incoming edges)."""
        return [
            self._nodes[nid]
            for nid, parents in self._backward.items()
            if not parents
        ]

    def sinks(self) -> list[LineageNode]:
        """Return all terminal sink nodes (no outgoing edges)."""
        return [
            self._nodes[nid]
            for nid, children in self._forward.items()
            if not children
        ]
