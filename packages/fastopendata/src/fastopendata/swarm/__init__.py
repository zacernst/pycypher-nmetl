"""Collective intelligence swarm query processing with emergent optimization.

This package implements distributed query processing inspired by swarm
intelligence, where individual query execution nodes exhibit emergent
collective behavior for global optimization without centralized control.

Three bio-inspired algorithms cooperate to optimize different aspects
of query execution:

- **Ant Colony Optimization** (:mod:`~fastopendata.swarm.ant_colony`) —
  Dynamic query routing via pheromone-guided path selection. Ants
  deposit pheromones on successful query routes, creating positive
  feedback loops that converge on optimal data-flow topologies.

- **Artificial Bee Colony** (:mod:`~fastopendata.swarm.bee_colony`) —
  Load balancing and resource allocation using employed, onlooker,
  and scout bee roles. Waggle-dance communication propagates fitness
  information, achieving balanced utilization across heterogeneous
  nodes.

- **Particle Swarm Optimization** (:mod:`~fastopendata.swarm.particle_swarm`) —
  Distributed join ordering via velocity-driven exploration of the
  permutation space. Particles balance personal-best memory against
  swarm-best social influence, converging on near-optimal join
  sequences.

The key insight is that **global optimization emerges from simple local
interactions** — no node needs a global view of the system. Each agent
follows local rules (deposit pheromone, dance proportionally, update
velocity), yet the collective converges on solutions that approximate
or match centralized optimizers.

Emergent Properties
-------------------

1. **Self-Organization**: Query routes, load distribution, and join
   orders emerge without central coordination.
2. **Adaptivity**: Pheromone evaporation and particle momentum allow
   the swarm to track changing workloads and data distributions.
3. **Robustness**: No single point of failure — the swarm degrades
   gracefully when nodes fail.
4. **Scalability**: Communication is local (neighbor-to-neighbor),
   so overhead grows sub-linearly with cluster size.

.. versionadded:: 0.0.31
"""

from fastopendata.swarm.ant_colony import (
    AntColonyOptimizer,
    AntColonyResult,
    Pheromone,
    QueryAnt,
)
from fastopendata.swarm.bee_colony import (
    BeeColonyOptimizer,
    BeeColonyResult,
    FoodSource,
    WorkerBee,
)
from fastopendata.swarm.core import (
    EmergentMetrics,
    NodeCapability,
    SwarmConfig,
    SwarmNode,
    SwarmTopology,
)
from fastopendata.swarm.particle_swarm import (
    JoinParticle,
    ParticleSwarmOptimizer,
    ParticleSwarmResult,
    SwarmVelocity,
)

__all__: list[str] = [
    # Core
    "EmergentMetrics",
    "NodeCapability",
    "SwarmConfig",
    "SwarmNode",
    "SwarmTopology",
    # Ant Colony
    "AntColonyOptimizer",
    "AntColonyResult",
    "Pheromone",
    "QueryAnt",
    # Bee Colony
    "BeeColonyOptimizer",
    "BeeColonyResult",
    "FoodSource",
    "WorkerBee",
    # Particle Swarm
    "JoinParticle",
    "ParticleSwarmOptimizer",
    "ParticleSwarmResult",
    "SwarmVelocity",
]
