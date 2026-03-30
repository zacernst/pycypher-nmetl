"""Core data structures for swarm intelligence query processing.

This module defines the foundational abstractions shared across all
swarm algorithms:

- :class:`SwarmNode` — an individual processing node in the distributed
  cluster, with capabilities, load metrics, and neighbor connections.
- :class:`SwarmTopology` — the communication graph connecting nodes,
  supporting both direct neighbor messaging and local broadcast.
- :class:`SwarmConfig` — unified configuration for all swarm algorithms.
- :class:`EmergentMetrics` — tracks emergent collective properties that
  arise from local interactions.

Design Philosophy
-----------------

Swarm intelligence achieves global optimization through local interactions.
Each node knows only its own state and its immediate neighbors. There is
no master coordinator, no global state, and no centralized decision-making.

The core data structures enforce this locality constraint: a
:class:`SwarmNode` can only observe its own metrics and communicate with
its direct neighbors in the :class:`SwarmTopology`. Global properties
like load balance and route convergence are *emergent* — they are
measured by :class:`EmergentMetrics` but never used as inputs to
individual node decisions.

.. versionadded:: 0.0.31
"""

from __future__ import annotations

import enum
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

_logger = logging.getLogger(__name__)


class NodeCapability(enum.Enum):
    """Processing capabilities that a swarm node can advertise.

    Nodes are heterogeneous — some excel at scans, others at joins,
    others at aggregation. The swarm algorithms exploit this diversity
    to route work to the most suitable nodes.
    """

    SCAN = "scan"
    JOIN = "join"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    SORT = "sort"
    HASH = "hash"
    STREAM = "stream"


@dataclass
class SwarmNode:
    """An individual processing node in the distributed swarm.

    Each node maintains local state: its current load, processing
    capabilities, and connections to neighbor nodes. Nodes make
    autonomous decisions based only on local information — the
    emergent global behavior arises from these local interactions.

    Attributes
    ----------
    node_id : str
        Globally unique identifier.
    capabilities : set[NodeCapability]
        Operations this node can perform efficiently.
    load : float
        Current load as a fraction of capacity (0.0 = idle, 1.0 = saturated).
    throughput : float
        Recent observed throughput (queries/sec).
    latency_ms : float
        Recent observed processing latency (milliseconds).
    neighbor_ids : set[str]
        IDs of directly connected nodes in the topology.
    memory_mb : float
        Available memory in megabytes.
    active_queries : int
        Number of queries currently being processed.
    max_concurrent : int
        Maximum concurrent queries before saturation.
    metadata : dict[str, Any]
        Node-specific metadata for instrumentation.
    created_at : float
        Epoch timestamp when the node joined the swarm.

    """

    node_id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])
    capabilities: set[NodeCapability] = field(
        default_factory=lambda: set(NodeCapability),
    )
    load: float = 0.0
    throughput: float = 0.0
    latency_ms: float = 0.0
    neighbor_ids: set[str] = field(default_factory=set)
    memory_mb: float = 1024.0
    active_queries: int = 0
    max_concurrent: int = 8
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    @property
    def utilization(self) -> float:
        """Fraction of concurrent capacity in use."""
        if self.max_concurrent <= 0:
            return 1.0
        return min(1.0, self.active_queries / self.max_concurrent)

    @property
    def is_saturated(self) -> bool:
        """Whether the node is at or above capacity."""
        return self.utilization >= 1.0

    def accept_query(self) -> bool:
        """Attempt to accept a new query.

        Returns
        -------
        bool
            ``True`` if the node had capacity and accepted the query.

        """
        if self.is_saturated:
            return False
        self.active_queries += 1
        self.load = self.utilization
        return True

    def release_query(self, latency_ms: float) -> None:
        """Release a completed query and update metrics.

        Parameters
        ----------
        latency_ms:
            Observed latency for the completed query.

        """
        self.active_queries = max(0, self.active_queries - 1)
        self.load = self.utilization
        # Exponential moving average for latency
        alpha = 0.3
        self.latency_ms = alpha * latency_ms + (1.0 - alpha) * self.latency_ms


@dataclass
class SwarmTopology:
    """Communication graph connecting swarm nodes.

    The topology enforces locality: nodes can only send messages to
    their direct neighbors. This constraint is fundamental to swarm
    intelligence — global coordination emerges from local communication,
    not from a centralized controller.

    Supports multiple topologies:
    - **Ring**: Each node connects to its two neighbors.
    - **Mesh**: Each node connects to k nearest neighbors.
    - **Random**: Erdos-Renyi random graph with connection probability p.

    Attributes
    ----------
    nodes : dict[str, SwarmNode]
        All nodes in the swarm keyed by node_id.
    edges : set[tuple[str, str]]
        Undirected edges (node_id, node_id) in the topology.

    """

    nodes: dict[str, SwarmNode] = field(default_factory=dict)
    edges: set[tuple[str, str]] = field(default_factory=set)

    @property
    def size(self) -> int:
        """Number of nodes in the swarm."""
        return len(self.nodes)

    @property
    def connectivity(self) -> float:
        """Average degree / (size - 1), measuring connectedness."""
        if self.size <= 1:
            return 1.0
        total_degree = sum(len(n.neighbor_ids) for n in self.nodes.values())
        avg_degree = total_degree / self.size
        return avg_degree / (self.size - 1)

    def add_node(self, node: SwarmNode) -> None:
        """Add a node to the swarm.

        Parameters
        ----------
        node:
            The node to add.

        """
        self.nodes[node.node_id] = node
        _logger.debug("Node %s joined swarm (size=%d)", node.node_id, self.size)

    def connect(self, node_a_id: str, node_b_id: str) -> None:
        """Create an undirected edge between two nodes.

        Parameters
        ----------
        node_a_id:
            First node.
        node_b_id:
            Second node.

        """
        if node_a_id == node_b_id:
            return
        if node_a_id not in self.nodes or node_b_id not in self.nodes:
            return
        edge = (min(node_a_id, node_b_id), max(node_a_id, node_b_id))
        self.edges.add(edge)
        self.nodes[node_a_id].neighbor_ids.add(node_b_id)
        self.nodes[node_b_id].neighbor_ids.add(node_a_id)

    def neighbors_of(self, node_id: str) -> list[SwarmNode]:
        """Return the direct neighbors of a node.

        Parameters
        ----------
        node_id:
            The node whose neighbors to retrieve.

        Returns
        -------
        list[SwarmNode]
            Neighbor nodes in arbitrary order.

        """
        if node_id not in self.nodes:
            return []
        return [
            self.nodes[nid]
            for nid in self.nodes[node_id].neighbor_ids
            if nid in self.nodes
        ]

    def build_ring(self, nodes: list[SwarmNode]) -> None:
        """Connect nodes in a ring topology.

        Each node connects to its predecessor and successor,
        forming a cycle. This is the minimal connected topology.

        Parameters
        ----------
        nodes:
            Nodes to arrange in a ring.

        """
        for node in nodes:
            self.add_node(node)
        for i in range(len(nodes)):
            self.connect(nodes[i].node_id, nodes[(i + 1) % len(nodes)].node_id)

    def build_mesh(self, nodes: list[SwarmNode], k: int = 3) -> None:
        """Connect each node to its k nearest neighbors (by index).

        Parameters
        ----------
        nodes:
            Nodes to connect.
        k:
            Number of neighbors per node (capped at len(nodes) - 1).

        """
        for node in nodes:
            self.add_node(node)
        k = min(k, len(nodes) - 1)
        for i, node in enumerate(nodes):
            for offset in range(1, k + 1):
                j = (i + offset) % len(nodes)
                self.connect(node.node_id, nodes[j].node_id)


@dataclass
class SwarmConfig:
    """Unified configuration for all swarm optimization algorithms.

    Attributes
    ----------
    ant_colony_size : int
        Number of ants per iteration in ACO.
    ant_iterations : int
        Number of ACO iterations before convergence check.
    evaporation_rate : float
        Pheromone evaporation rate per iteration (0-1).
    pheromone_deposit : float
        Base pheromone quantity deposited by successful ants.
    alpha : float
        Pheromone influence weight in ACO probability (higher = more
        exploitation of known-good routes).
    beta : float
        Heuristic influence weight in ACO probability (higher = more
        greedy local decisions).
    bee_employed_count : int
        Number of employed bees in ABC.
    bee_onlooker_count : int
        Number of onlooker bees in ABC.
    bee_scout_limit : int
        Abandonment threshold for food sources in ABC.
    particle_count : int
        Number of particles in PSO.
    pso_iterations : int
        Number of PSO iterations.
    inertia_weight : float
        PSO inertia (momentum from previous velocity).
    cognitive_weight : float
        PSO cognitive coefficient (attraction to personal best).
    social_weight : float
        PSO social coefficient (attraction to global best).
    convergence_threshold : float
        Stop early if improvement drops below this fraction.

    """

    ant_colony_size: int = 20
    ant_iterations: int = 50
    evaporation_rate: float = 0.1
    pheromone_deposit: float = 1.0
    alpha: float = 1.0
    beta: float = 2.0
    bee_employed_count: int = 15
    bee_onlooker_count: int = 15
    bee_scout_limit: int = 10
    particle_count: int = 30
    pso_iterations: int = 100
    inertia_weight: float = 0.7
    inertia_weight_min: float = 0.4
    adaptive_inertia: bool = True
    cognitive_weight: float = 1.5
    social_weight: float = 1.5
    convergence_threshold: float = 0.001

    def inertia_at(self, iteration: int, max_iterations: int) -> float:
        """Return the inertia weight for the given iteration.

        When ``adaptive_inertia`` is enabled, linearly decreases
        from ``inertia_weight`` to ``inertia_weight_min`` over the
        course of optimization.  This encourages broad exploration
        in early iterations and fine-grained exploitation later.

        Parameters
        ----------
        iteration:
            Current iteration (0-indexed).
        max_iterations:
            Total number of iterations planned.

        Returns
        -------
        float
            Inertia weight for this iteration.

        """
        if not self.adaptive_inertia or max_iterations <= 1:
            return self.inertia_weight
        progress = iteration / (max_iterations - 1)
        return self.inertia_weight - progress * (
            self.inertia_weight - self.inertia_weight_min
        )


@dataclass
class EmergentMetrics:
    """Tracks emergent collective properties of the swarm.

    These metrics are *observed* (measured externally) rather than
    *computed* (used as inputs to local decisions). They quantify
    the degree to which simple local interactions produce globally
    beneficial patterns.

    Attributes
    ----------
    load_balance_entropy : float
        Shannon entropy of the load distribution across nodes.
        Higher entropy = more balanced load. Maximum is log2(N)
        for N equally loaded nodes.
    route_convergence : float
        Fraction of pheromone concentrated on the top route.
        Values near 1.0 indicate strong convergence; near 1/R
        (for R routes) indicates exploration.
    swarm_coherence : float
        Fraction of particles within epsilon of the global best
        in PSO. High coherence means the swarm has converged.
    collective_throughput : float
        Total queries/sec across all nodes.
    adaptation_rate : float
        Rate at which the swarm adapts to load changes,
        measured as inverse of convergence time (iterations).
    iteration : int
        Current optimization iteration.
    history : list[dict[str, float]]
        Per-iteration snapshot of key metrics for convergence analysis.

    """

    load_balance_entropy: float = 0.0
    route_convergence: float = 0.0
    swarm_coherence: float = 0.0
    collective_throughput: float = 0.0
    adaptation_rate: float = 0.0
    iteration: int = 0
    history: list[dict[str, float]] = field(default_factory=list)

    def compute_load_entropy(self, loads: list[float]) -> float:
        """Compute Shannon entropy of the load distribution.

        Parameters
        ----------
        loads:
            Load values for each node (0.0 to 1.0).

        Returns
        -------
        float
            Shannon entropy in bits. Maximum is log2(N) for uniform.

        """
        if not loads:
            return 0.0
        total = sum(loads) or 1.0
        probs = [l / total for l in loads if l > 0]
        if not probs:
            return 0.0
        entropy = -sum(p * math.log2(p) for p in probs)
        self.load_balance_entropy = entropy
        return entropy

    def compute_route_convergence(self, pheromone_levels: list[float]) -> float:
        """Compute route convergence from pheromone distribution.

        Parameters
        ----------
        pheromone_levels:
            Pheromone intensities on each candidate route.

        Returns
        -------
        float
            Fraction of total pheromone on the strongest route (0-1).

        """
        if not pheromone_levels:
            return 0.0
        total = sum(pheromone_levels)
        if total <= 0:
            return 0.0
        convergence = max(pheromone_levels) / total
        self.route_convergence = convergence
        return convergence

    def snapshot(self) -> dict[str, float]:
        """Capture current metrics as a dictionary and append to history.

        Returns
        -------
        dict[str, float]
            Current metric values.

        """
        snap = {
            "load_balance_entropy": self.load_balance_entropy,
            "route_convergence": self.route_convergence,
            "swarm_coherence": self.swarm_coherence,
            "collective_throughput": self.collective_throughput,
            "adaptation_rate": self.adaptation_rate,
            "iteration": float(self.iteration),
        }
        self.history.append(snap)
        return snap
