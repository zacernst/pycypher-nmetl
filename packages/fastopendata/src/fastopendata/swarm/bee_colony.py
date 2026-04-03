"""Artificial Bee Colony algorithm for load balancing and resource allocation.

This module implements the ABC algorithm where three bee roles cooperate
to discover optimal resource allocation across heterogeneous nodes:

- **Employed bees** exploit known food sources (node allocations),
  evaluating their fitness and performing local search.
- **Onlooker bees** observe waggle dances and probabilistically select
  food sources proportional to their fitness, amplifying good solutions.
- **Scout bees** abandon exhausted food sources and randomly discover
  new allocations, preventing premature convergence.

Biological Analogy
------------------

In a real bee colony, foragers communicate food source quality through
waggle dances — the duration and vigor of the dance encodes the food
source's distance and quality. Other bees preferentially recruit to
better sources. When a source is depleted, the bee becomes a scout
and explores randomly. This three-phase cycle (exploit, recruit, explore)
achieves near-optimal resource allocation with no central coordinator.

Load Balancing Mapping
----------------------

- **Food source** = a candidate allocation of query partitions to nodes.
- **Nectar quality** = inverse of max-node-latency (load-balanced fitness).
- **Waggle dance** = sharing fitness information with onlooker bees.
- **Abandonment** = resetting a stagnant allocation (scout phase).
- **Local search** = perturbation of one partition assignment.

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
    SwarmConfig,
    SwarmTopology,
)

_logger = logging.getLogger(__name__)


@dataclass
class FoodSource:
    """A candidate resource allocation (food source) in the ABC algorithm.

    Each food source represents a complete assignment of query partitions
    to processing nodes. The colony evaluates and refines these allocations
    through the employed-onlooker-scout cycle.

    Attributes
    ----------
    source_id : str
        Unique identifier.
    allocation : dict[str, str]
        Mapping of partition_id -> node_id.
    fitness : float
        Current fitness (higher = better load balance).
    trial_count : int
        Number of iterations since last improvement. When this exceeds
        the scout limit, the source is abandoned.
    best_fitness : float
        Best fitness ever observed for this source.
    node_loads : dict[str, float]
        Per-node load implied by this allocation.
    metadata : dict[str, Any]
        Source-specific metadata.

    """

    source_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    allocation: dict[str, str] = field(default_factory=dict)
    fitness: float = 0.0
    trial_count: int = 0
    best_fitness: float = 0.0
    node_loads: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_stagnant(self) -> bool:
        """Whether this source hasn't improved recently."""
        return self.trial_count > 0 and self.fitness <= self.best_fitness


@dataclass
class WorkerBee:
    """A bee in the ABC colony.

    Bees cycle through roles: employed -> onlooker -> scout.
    Each role has a distinct behavior in the optimization loop.

    Attributes
    ----------
    bee_id : str
        Unique identifier.
    role : str
        Current role: ``"employed"``, ``"onlooker"``, or ``"scout"``.
    assigned_source_id : str
        The food source this bee is currently exploiting.
    discoveries : int
        Number of improved solutions found.

    """

    bee_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    role: str = "employed"
    assigned_source_id: str = ""
    discoveries: int = 0


class BeeColonyOptimizer:
    """Artificial Bee Colony optimizer for query load balancing.

    Distributes query partitions across heterogeneous nodes to minimize
    the maximum node latency (makespan), achieving balanced utilization.
    The three-phase cycle (employed, onlooker, scout) balances
    exploitation of good allocations with exploration of new ones.

    Parameters
    ----------
    topology : SwarmTopology
        Available processing nodes.
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
        self.food_sources: list[FoodSource] = []
        self.bees: list[WorkerBee] = []
        self.metrics = EmergentMetrics()

    def _evaluate_fitness(
        self,
        allocation: dict[str, str],
        partition_costs: dict[str, float],
    ) -> tuple[float, dict[str, float]]:
        """Evaluate the fitness of an allocation.

        Fitness is the inverse of the makespan (maximum node load).
        This drives the swarm toward balanced allocations where no
        single node is a bottleneck.

        Parameters
        ----------
        allocation:
            Mapping of partition_id -> node_id.
        partition_costs:
            Processing cost of each partition.

        Returns
        -------
        tuple[float, dict[str, float]]
            (fitness, per-node-load dict).

        """
        node_loads: dict[str, float] = dict.fromkeys(self.topology.nodes, 0.0)

        for part_id, node_id in allocation.items():
            cost = partition_costs.get(part_id, 1.0)
            if node_id in node_loads:
                node = self.topology.nodes[node_id]
                # Adjust cost by node capability (faster nodes handle more)
                speed_factor = 1.0 / (1.0 + node.latency_ms * 0.01)
                node_loads[node_id] += cost / max(0.1, speed_factor)

        makespan = max(node_loads.values()) if node_loads else float("inf")
        if makespan <= 0:
            return 0.0, node_loads
        fitness = 1.0 / makespan
        return fitness, node_loads

    def _random_allocation(
        self,
        partition_ids: list[str],
    ) -> dict[str, str]:
        """Generate a random allocation of partitions to nodes.

        Parameters
        ----------
        partition_ids:
            Partitions to assign.

        Returns
        -------
        dict[str, str]
            Random allocation mapping.

        """
        node_ids = list(self.topology.nodes.keys())
        if not node_ids:
            return {}
        return {pid: random.choice(node_ids) for pid in partition_ids}

    def _local_search(
        self,
        source: FoodSource,
        partition_ids: list[str],
        partition_costs: dict[str, float],
    ) -> FoodSource:
        """Perform local search by perturbing one partition assignment.

        Selects a random partition and reassigns it to a random
        different node. If the new allocation improves fitness,
        the source is updated.

        Parameters
        ----------
        source:
            Food source to perturb.
        partition_ids:
            All partition IDs.
        partition_costs:
            Cost per partition.

        Returns
        -------
        FoodSource
            Updated food source (may be unchanged if no improvement).

        """
        if not partition_ids or not self.topology.nodes:
            return source

        node_ids = list(self.topology.nodes.keys())

        # Perturb: reassign one random partition
        candidate = dict(source.allocation)
        part = random.choice(partition_ids)
        old_node = candidate.get(part, "")
        new_node = random.choice(
            [n for n in node_ids if n != old_node] or node_ids
        )
        candidate[part] = new_node

        new_fitness, new_loads = self._evaluate_fitness(
            candidate, partition_costs
        )

        if new_fitness > source.fitness:
            source.allocation = candidate
            source.fitness = new_fitness
            source.node_loads = new_loads
            source.best_fitness = max(source.best_fitness, new_fitness)
            source.trial_count = 0
            return source

        source.trial_count += 1
        return source

    def _initialize_colony(
        self,
        partition_ids: list[str],
        partition_costs: dict[str, float],
    ) -> None:
        """Initialize food sources and bees.

        Parameters
        ----------
        partition_ids:
            Partitions to distribute.
        partition_costs:
            Cost per partition.

        """
        num_sources = self.config.bee_employed_count
        self.food_sources = []
        self.bees = []

        for _ in range(num_sources):
            alloc = self._random_allocation(partition_ids)
            fitness, loads = self._evaluate_fitness(alloc, partition_costs)
            source = FoodSource(
                allocation=alloc,
                fitness=fitness,
                best_fitness=fitness,
                node_loads=loads,
            )
            self.food_sources.append(source)

            # One employed bee per food source
            bee = WorkerBee(
                role="employed",
                assigned_source_id=source.source_id,
            )
            self.bees.append(bee)

        # Add onlooker bees (not assigned to specific sources yet)
        for _ in range(self.config.bee_onlooker_count):
            self.bees.append(WorkerBee(role="onlooker"))

    def _employed_phase(
        self,
        partition_ids: list[str],
        partition_costs: dict[str, float],
    ) -> None:
        """Employed bee phase: exploit assigned food sources.

        Each employed bee performs local search on its assigned
        food source, attempting to improve the allocation.
        """
        for bee in self.bees:
            if bee.role != "employed":
                continue
            for source in self.food_sources:
                if source.source_id == bee.assigned_source_id:
                    old_fitness = source.fitness
                    self._local_search(source, partition_ids, partition_costs)
                    if source.fitness > old_fitness:
                        bee.discoveries += 1
                    break

    def _onlooker_phase(
        self,
        partition_ids: list[str],
        partition_costs: dict[str, float],
    ) -> None:
        """Onlooker bee phase: recruit to promising food sources.

        Onlooker bees observe the waggle dances (fitness values) of
        employed bees and probabilistically select food sources to
        exploit. Better sources attract more onlookers — this is the
        positive feedback mechanism.
        """
        if not self.food_sources:
            return

        total_fitness = sum(max(0.01, s.fitness) for s in self.food_sources)

        for bee in self.bees:
            if bee.role != "onlooker":
                continue

            # Roulette wheel selection based on waggle dance (fitness)
            r = random.random() * total_fitness
            cumulative = 0.0
            selected = self.food_sources[0]
            for source in self.food_sources:
                cumulative += max(0.01, source.fitness)
                if cumulative >= r:
                    selected = source
                    break

            # Perform local search on selected source
            old_fitness = selected.fitness
            self._local_search(selected, partition_ids, partition_costs)
            if selected.fitness > old_fitness:
                bee.discoveries += 1

    def _scout_phase(
        self,
        partition_ids: list[str],
        partition_costs: dict[str, float],
    ) -> None:
        """Scout bee phase: abandon exhausted sources and explore.

        Food sources that haven't improved for ``scout_limit`` iterations
        are abandoned. The assigned bee becomes a scout and discovers
        a new random allocation.
        """
        for i, source in enumerate(self.food_sources):
            if source.trial_count >= self.config.bee_scout_limit:
                _logger.debug(
                    "Scout replacing source %s (stagnant for %d trials)",
                    source.source_id,
                    source.trial_count,
                )
                alloc = self._random_allocation(partition_ids)
                fitness, loads = self._evaluate_fitness(alloc, partition_costs)
                self.food_sources[i] = FoodSource(
                    allocation=alloc,
                    fitness=fitness,
                    best_fitness=fitness,
                    node_loads=loads,
                )

    def optimize(
        self,
        partition_ids: list[str],
        partition_costs: dict[str, float],
        iterations: int | None = None,
    ) -> BeeColonyResult:
        """Run the full ABC optimization loop.

        Parameters
        ----------
        partition_ids:
            Query partitions to distribute across nodes.
        partition_costs:
            Processing cost per partition.
        iterations:
            Override number of iterations (defaults to config).

        Returns
        -------
        BeeColonyResult
            Best allocation found with convergence diagnostics.

        """
        start_time = time.time()
        max_iter = iterations or self.config.ant_iterations
        self._initialize_colony(partition_ids, partition_costs)

        best_source: FoodSource | None = None
        best_fitness = 0.0

        for iteration in range(max_iter):
            self.metrics.iteration = iteration

            # Three-phase cycle
            self._employed_phase(partition_ids, partition_costs)
            self._onlooker_phase(partition_ids, partition_costs)
            self._scout_phase(partition_ids, partition_costs)

            # Track global best
            for source in self.food_sources:
                if source.fitness > best_fitness:
                    best_fitness = source.fitness
                    best_source = FoodSource(
                        source_id=source.source_id,
                        allocation=dict(source.allocation),
                        fitness=source.fitness,
                        best_fitness=source.best_fitness,
                        node_loads=dict(source.node_loads),
                    )

            # Emergent metrics
            if self.food_sources:
                loads = [
                    max(s.node_loads.values()) if s.node_loads else 0.0
                    for s in self.food_sources
                ]
                self.metrics.compute_load_entropy(loads)
            self.metrics.snapshot()

        elapsed_ms = (time.time() - start_time) * 1000.0

        # Compute load balance quality
        load_imbalance = 0.0
        if best_source and best_source.node_loads:
            loads = list(best_source.node_loads.values())
            avg_load = sum(loads) / len(loads) if loads else 0.0
            if avg_load > 0:
                load_imbalance = max(loads) / avg_load - 1.0

        return BeeColonyResult(
            best_allocation=best_source.allocation if best_source else {},
            best_fitness=best_fitness,
            node_loads=best_source.node_loads if best_source else {},
            load_imbalance=load_imbalance,
            iterations_run=max_iter,
            sources_explored=sum(1 for b in self.bees if b.discoveries > 0),
            scouts_deployed=sum(
                1
                for s in self.food_sources
                if s.trial_count == 0 and s.fitness > 0
            ),
            elapsed_ms=elapsed_ms,
        )


@dataclass
class BeeColonyResult:
    """Result of an ABC load balancing optimization run.

    Attributes
    ----------
    best_allocation : dict[str, str]
        Optimal partition-to-node mapping found.
    best_fitness : float
        Fitness of the best allocation (inverse makespan).
    node_loads : dict[str, float]
        Per-node load under the best allocation.
    load_imbalance : float
        Ratio of max load to average load, minus 1.
        0.0 = perfectly balanced, higher = more imbalanced.
    iterations_run : int
        Number of ABC iterations executed.
    sources_explored : int
        Number of bees that found improvements.
    scouts_deployed : int
        Number of scout replacements that occurred.
    elapsed_ms : float
        Wall-clock time for the optimization.

    """

    best_allocation: dict[str, str] = field(default_factory=dict)
    best_fitness: float = 0.0
    node_loads: dict[str, float] = field(default_factory=dict)
    load_imbalance: float = 0.0
    iterations_run: int = 0
    sources_explored: int = 0
    scouts_deployed: int = 0
    elapsed_ms: float = 0.0
