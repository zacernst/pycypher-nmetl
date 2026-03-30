r"""Ant Colony Optimization for dynamic query routing.

This module implements ACO-based query routing where virtual ants
traverse the node topology, depositing pheromones on edges that
lead to successful query execution. Over iterations, pheromone
accumulation creates positive feedback loops that converge on
optimal data-flow paths.

Biological Analogy
------------------

In nature, ants find shortest paths to food sources through indirect
communication via pheromone trails. An ant returning from a food
source deposits pheromone proportional to the food quality and
inversely proportional to the path length. Other ants probabilistically
follow stronger trails, reinforcing them further. Evaporation prevents
stale trails from dominating, allowing adaptation to changing conditions.

Query Routing Mapping
---------------------

- **Ant** = a query probe exploring a route through the node topology.
- **Pheromone** = accumulated success signal on an edge (node_a -> node_b).
- **Food quality** = inverse query latency on the route.
- **Evaporation** = decay of stale routing information.
- **Heuristic** = local node fitness (inverse load, capability match).

The probability of ant *k* choosing edge (i, j) is:

.. math::

    p_{ij}^k = \\frac
        {[\\tau_{ij}]^\\alpha \\cdot [\\eta_{ij}]^\\beta}
        {\\sum_{l \\in N_i} [\\tau_{il}]^\\alpha \\cdot [\\eta_{il}]^\\beta}

where :math:`\\tau_{ij}` is pheromone intensity, :math:`\\eta_{ij}` is
the heuristic desirability, and :math:`\\alpha, \\beta` control the
exploitation-exploration balance.

.. versionadded:: 0.0.31
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastopendata.swarm.core import (
    EmergentMetrics,
    NodeCapability,
    SwarmConfig,
    SwarmNode,
    SwarmTopology,
)

_logger = logging.getLogger(__name__)


@dataclass
class Pheromone:
    """Pheromone deposit on a directed edge in the topology.

    Pheromone encodes the collective experience of the swarm about
    the quality of routing queries through a particular edge.

    Attributes
    ----------
    source_id : str
        Origin node of the directed edge.
    target_id : str
        Destination node.
    intensity : float
        Current pheromone intensity (always >= min_intensity).
    deposit_count : int
        Number of times ants have deposited on this edge.
    last_deposit : float
        Epoch timestamp of the most recent deposit.
    min_intensity : float
        Floor to prevent complete evaporation (ensures exploration).
    max_intensity : float
        Ceiling to prevent runaway accumulation.

    """

    source_id: str = ""
    target_id: str = ""
    intensity: float = 1.0
    deposit_count: int = 0
    last_deposit: float = 0.0
    min_intensity: float = 0.1
    max_intensity: float = 10.0

    @property
    def edge_key(self) -> tuple[str, str]:
        """Canonical edge identifier (directed)."""
        return (self.source_id, self.target_id)

    def deposit(self, amount: float) -> None:
        """Add pheromone to this edge.

        Parameters
        ----------
        amount:
            Quantity to deposit (will be clamped to max_intensity).

        """
        self.intensity = min(self.max_intensity, self.intensity + amount)
        self.deposit_count += 1
        self.last_deposit = time.time()

    def evaporate(self, rate: float) -> None:
        """Apply evaporation to this edge.

        Parameters
        ----------
        rate:
            Evaporation rate (0-1). intensity *= (1 - rate).

        """
        self.intensity = max(
            self.min_intensity,
            self.intensity * (1.0 - rate),
        )


@dataclass
class QueryAnt:
    """A virtual ant that explores query routes through the topology.

    Each ant starts at a source node and traverses the topology,
    choosing next hops probabilistically based on pheromone intensity
    and local heuristic information. After reaching the destination
    (or exhausting its step budget), the ant deposits pheromone on
    its path proportional to the route quality.

    Attributes
    ----------
    ant_id : str
        Unique identifier.
    source_node_id : str
        Starting node.
    target_capability : NodeCapability
        The capability the ant is seeking (e.g., JOIN node).
    path : list[str]
        Ordered sequence of node IDs visited.
    path_cost : float
        Accumulated cost (latency) along the path.
    found_target : bool
        Whether the ant reached a node with the target capability.
    max_steps : int
        Maximum number of hops before giving up.
    metadata : dict[str, Any]
        Ant-specific metadata.

    """

    ant_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    source_node_id: str = ""
    target_capability: NodeCapability = NodeCapability.JOIN
    path: list[str] = field(default_factory=list)
    path_cost: float = 0.0
    found_target: bool = False
    max_steps: int = 10
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def path_length(self) -> int:
        """Number of hops taken."""
        return max(0, len(self.path) - 1)

    @property
    def fitness(self) -> float:
        """Ant's route fitness (higher is better).

        Fitness is the inverse of path cost if the target was found,
        zero otherwise. This drives pheromone deposits — shorter,
        successful routes deposit more pheromone.
        """
        if not self.found_target or self.path_cost <= 0:
            return 0.0
        return 1.0 / self.path_cost


class AntColonyOptimizer:
    """Ant Colony Optimization engine for query routing.

    Manages the pheromone matrix, dispatches ants, and iteratively
    refines routing decisions. The optimizer runs for a configured
    number of iterations, each consisting of:

    1. **Construction** — Each ant builds a route from source to target.
    2. **Evaluation** — Route quality (inverse latency) is measured.
    3. **Pheromone update** — Successful ants deposit pheromone;
       all edges evaporate.
    4. **Convergence check** — Stop early if the best route stabilizes.

    Parameters
    ----------
    topology : SwarmTopology
        The node communication graph.
    config : SwarmConfig
        Algorithm parameters.

    """

    def __init__(
        self,
        topology: SwarmTopology,
        config: SwarmConfig | None = None,
    ) -> None:
        self.topology = topology
        self.config = config or SwarmConfig()
        self.pheromone_matrix: dict[tuple[str, str], Pheromone] = {}
        self.metrics = EmergentMetrics()
        self._initialize_pheromones()

    def _initialize_pheromones(self) -> None:
        """Seed uniform pheromone on all edges."""
        for edge in self.topology.edges:
            a, b = edge
            # Directed edges in both directions
            for src, tgt in [(a, b), (b, a)]:
                key = (src, tgt)
                if key not in self.pheromone_matrix:
                    self.pheromone_matrix[key] = Pheromone(
                        source_id=src,
                        target_id=tgt,
                        intensity=1.0,
                    )

    def _heuristic(self, node: SwarmNode, target_cap: NodeCapability) -> float:
        """Compute local heuristic desirability for a candidate node.

        The heuristic combines capability match, inverse load, and
        available capacity — all locally observable properties.

        Parameters
        ----------
        node:
            Candidate next-hop node.
        target_cap:
            The capability being sought.

        Returns
        -------
        float
            Heuristic value (higher = more desirable).

        """
        # Capability match bonus
        cap_bonus = 3.0 if target_cap in node.capabilities else 1.0
        # Inverse load (prefer less loaded nodes)
        load_factor = 1.0 / (1.0 + node.load * 5.0)
        # Capacity factor
        cap_factor = 1.0 - node.utilization
        return cap_bonus * load_factor * (0.5 + cap_factor)

    def _select_next_hop(
        self,
        current_id: str,
        visited: set[str],
        target_cap: NodeCapability,
    ) -> str | None:
        """Probabilistically select the next node using ACO transition rule.

        Parameters
        ----------
        current_id:
            Current node.
        visited:
            Already-visited nodes (excluded from selection).
        target_cap:
            Capability being sought.

        Returns
        -------
        str | None
            Selected neighbor node_id, or None if no unvisited neighbors.

        """
        neighbors = [
            n
            for n in self.topology.neighbors_of(current_id)
            if n.node_id not in visited
        ]
        if not neighbors:
            return None

        alpha = self.config.alpha
        beta = self.config.beta

        # Compute selection probabilities
        scores: list[float] = []
        for neighbor in neighbors:
            edge_key = (current_id, neighbor.node_id)
            pheromone = self.pheromone_matrix.get(edge_key)
            tau = pheromone.intensity if pheromone else 0.1
            eta = self._heuristic(neighbor, target_cap)
            scores.append((tau**alpha) * (eta**beta))

        total = sum(scores)
        if total <= 0:
            return random.choice(neighbors).node_id

        # Roulette wheel selection
        r = random.random() * total
        cumulative = 0.0
        for neighbor, score in zip(neighbors, scores, strict=True):
            cumulative += score
            if cumulative >= r:
                return neighbor.node_id
        return neighbors[-1].node_id

    def _construct_route(
        self,
        source_id: str,
        target_cap: NodeCapability,
    ) -> QueryAnt:
        """Dispatch a single ant to construct a route.

        Parameters
        ----------
        source_id:
            Starting node.
        target_cap:
            Capability being sought.

        Returns
        -------
        QueryAnt
            The ant with its completed path and cost.

        """
        ant = QueryAnt(
            source_node_id=source_id,
            target_capability=target_cap,
            path=[source_id],
        )
        visited = {source_id}

        for _ in range(ant.max_steps):
            current_id = ant.path[-1]
            current_node = self.topology.nodes.get(current_id)
            if not current_node:
                break

            # Check if current node has the target capability
            if target_cap in current_node.capabilities and len(ant.path) > 1:
                ant.found_target = True
                break

            next_id = self._select_next_hop(current_id, visited, target_cap)
            if next_id is None:
                break

            visited.add(next_id)
            ant.path.append(next_id)

            # Accumulate cost (latency of the next node)
            next_node = self.topology.nodes.get(next_id)
            if next_node:
                ant.path_cost += max(1.0, next_node.latency_ms) + next_node.load * 10.0

        return ant

    def _deposit_pheromones(self, ants: list[QueryAnt]) -> None:
        """Deposit pheromone on edges traversed by successful ants.

        Only ants that found their target deposit pheromone.
        The deposit amount is proportional to route fitness
        (inversely proportional to path cost).

        Parameters
        ----------
        ants:
            All ants from the current iteration.

        """
        for ant in ants:
            if not ant.found_target:
                continue
            deposit = self.config.pheromone_deposit * ant.fitness
            for i in range(len(ant.path) - 1):
                edge_key = (ant.path[i], ant.path[i + 1])
                if edge_key in self.pheromone_matrix:
                    self.pheromone_matrix[edge_key].deposit(deposit)
                else:
                    p = Pheromone(
                        source_id=ant.path[i],
                        target_id=ant.path[i + 1],
                    )
                    p.deposit(deposit)
                    self.pheromone_matrix[edge_key] = p

    def _evaporate_all(self) -> None:
        """Apply evaporation to all pheromone edges."""
        for pheromone in self.pheromone_matrix.values():
            pheromone.evaporate(self.config.evaporation_rate)

    def optimize(
        self,
        source_id: str,
        target_capability: NodeCapability,
    ) -> AntColonyResult:
        """Run the full ACO optimization loop.

        Parameters
        ----------
        source_id:
            Starting node for all ants.
        target_capability:
            The capability to route toward.

        Returns
        -------
        AntColonyResult
            Best route found, convergence metrics, and iteration history.

        """
        start_time = time.time()
        best_ant: QueryAnt | None = None
        best_fitness = 0.0
        stagnation_count = 0

        for iteration in range(self.config.ant_iterations):
            self.metrics.iteration = iteration

            # Construct routes for all ants
            ants: list[QueryAnt] = []
            for _ in range(self.config.ant_colony_size):
                ant = self._construct_route(source_id, target_capability)
                ants.append(ant)

            # Find iteration best
            successful = [a for a in ants if a.found_target]
            if successful:
                iter_best = max(successful, key=lambda a: a.fitness)
                if iter_best.fitness > best_fitness:
                    best_fitness = iter_best.fitness
                    best_ant = iter_best
                    stagnation_count = 0
                else:
                    stagnation_count += 1

            # Pheromone update
            self._evaporate_all()
            self._deposit_pheromones(ants)

            # Compute emergent metrics
            pheromone_levels = [p.intensity for p in self.pheromone_matrix.values()]
            self.metrics.compute_route_convergence(pheromone_levels)
            loads = [n.load for n in self.topology.nodes.values()]
            self.metrics.compute_load_entropy(loads)
            self.metrics.snapshot()

            # Early convergence check
            if stagnation_count >= self.config.ant_iterations // 4:
                _logger.info(
                    "ACO converged at iteration %d (stagnation=%d)",
                    iteration,
                    stagnation_count,
                )
                break

        elapsed_ms = (time.time() - start_time) * 1000.0

        return AntColonyResult(
            best_route=best_ant.path if best_ant else [source_id],
            best_cost=best_ant.path_cost if best_ant else float("inf"),
            best_fitness=best_fitness,
            found_target=best_ant.found_target if best_ant else False,
            iterations_run=self.metrics.iteration + 1,
            successful_ants=sum(
                1 for p in self.pheromone_matrix.values() if p.deposit_count > 0
            ),
            route_convergence=self.metrics.route_convergence,
            elapsed_ms=elapsed_ms,
            pheromone_snapshot={
                f"{p.source_id}->{p.target_id}": p.intensity
                for p in self.pheromone_matrix.values()
                if p.deposit_count > 0
            },
        )


@dataclass
class AntColonyResult:
    """Result of an ACO routing optimization run.

    Attributes
    ----------
    best_route : list[str]
        Ordered node IDs of the optimal route found.
    best_cost : float
        Total cost (latency) of the best route.
    best_fitness : float
        Fitness (1/cost) of the best route.
    found_target : bool
        Whether any ant reached the target capability.
    iterations_run : int
        Number of ACO iterations executed.
    successful_ants : int
        Number of edges that received pheromone deposits.
    route_convergence : float
        Final pheromone convergence metric.
    elapsed_ms : float
        Wall-clock time for the optimization.
    pheromone_snapshot : dict[str, float]
        Final pheromone intensities on deposited edges.

    """

    best_route: list[str] = field(default_factory=list)
    best_cost: float = float("inf")
    best_fitness: float = 0.0
    found_target: bool = False
    iterations_run: int = 0
    successful_ants: int = 0
    route_convergence: float = 0.0
    elapsed_ms: float = 0.0
    pheromone_snapshot: dict[str, float] = field(default_factory=dict)
