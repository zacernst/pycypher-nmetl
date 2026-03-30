"""Particle Swarm Optimization for distributed join ordering.

This module implements PSO-based optimization of join orderings in
query execution plans. Each particle represents a candidate join
permutation and moves through the permutation space guided by
personal-best memory and swarm-best social influence.

Biological Analogy
------------------

In nature, bird flocks and fish schools exhibit coordinated movement
without a leader. Each individual adjusts its velocity based on three
influences:

1. **Inertia** — tendency to continue in the current direction.
2. **Cognitive** — attraction toward the individual's own best-known
   position (personal experience).
3. **Social** — attraction toward the best position found by any
   member of the swarm (collective intelligence).

This three-way balance between momentum, personal experience, and
social learning enables efficient exploration of complex search
spaces.

Join Ordering Mapping
---------------------

- **Particle position** = a permutation of join operations.
- **Velocity** = a sequence of transpositions (swap operations) that
  transform one permutation into another.
- **Fitness** = inverse estimated cost of executing joins in this order.
- **Personal best** = best join order this particle has found.
- **Global best** = best join order found by any particle.

The key insight is that permutation spaces are discrete, so we use
the **swap-sequence** velocity representation: the velocity is a
list of (i, j) index pairs. Applying the velocity to a permutation
performs the swaps in order, producing a new permutation.

.. versionadded:: 0.0.31
"""

from __future__ import annotations

import logging
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastopendata.swarm.core import EmergentMetrics, SwarmConfig

_logger = logging.getLogger(__name__)


@dataclass
class SwarmVelocity:
    """Velocity in permutation space, represented as a swap sequence.

    A velocity is a list of transpositions (index pairs) that, when
    applied sequentially to a permutation, produce a new permutation.
    This representation allows arithmetic-like operations (scaling,
    addition) in the discrete permutation domain.

    Attributes
    ----------
    swaps : list[tuple[int, int]]
        Ordered sequence of (i, j) index swaps.

    """

    swaps: list[tuple[int, int]] = field(default_factory=list)

    @property
    def magnitude(self) -> int:
        """Number of swap operations in this velocity."""
        return len(self.swaps)

    def apply_to(self, permutation: list[str]) -> list[str]:
        """Apply this velocity (swap sequence) to a permutation.

        Parameters
        ----------
        permutation:
            Input permutation to transform.

        Returns
        -------
        list[str]
            New permutation after applying all swaps.

        """
        result = list(permutation)
        for i, j in self.swaps:
            if 0 <= i < len(result) and 0 <= j < len(result):
                result[i], result[j] = result[j], result[i]
        return result

    def scale(self, factor: float) -> SwarmVelocity:
        """Probabilistically retain swaps based on a scaling factor.

        Each swap is kept with probability ``factor``. This implements
        the analog of scalar multiplication for discrete velocities.

        Parameters
        ----------
        factor:
            Retention probability (0 = drop all, 1 = keep all).

        Returns
        -------
        SwarmVelocity
            Scaled velocity.

        """
        kept = [s for s in self.swaps if random.random() < min(1.0, abs(factor))]
        return SwarmVelocity(swaps=kept)

    @staticmethod
    def difference(perm_a: list[str], perm_b: list[str]) -> SwarmVelocity:
        """Compute the velocity that transforms perm_b into perm_a.

        Finds the minimum swap sequence that transforms perm_b into
        perm_a using the Kendall tau distance (number of pairwise
        disagreements).

        Parameters
        ----------
        perm_a:
            Target permutation.
        perm_b:
            Source permutation.

        Returns
        -------
        SwarmVelocity
            Swap sequence transforming perm_b -> perm_a.

        """
        if len(perm_a) != len(perm_b):
            return SwarmVelocity()

        # Build position index for O(1) lookup instead of O(n) scan
        working = list(perm_b)
        pos_of: dict[str, int] = {v: i for i, v in enumerate(working)}
        swaps: list[tuple[int, int]] = []

        for i in range(len(working)):
            target_val = perm_a[i]
            if working[i] == target_val:
                continue
            # O(1) lookup via position index instead of O(n) linear scan
            j = pos_of[target_val]
            swaps.append((i, j))
            # Update both the working array and the position index
            displaced = working[i]
            working[i], working[j] = working[j], working[i]
            pos_of[target_val] = i
            pos_of[displaced] = j

        return SwarmVelocity(swaps=swaps)

    def combine(self, other: SwarmVelocity) -> SwarmVelocity:
        """Concatenate two velocities (addition analog).

        Parameters
        ----------
        other:
            Velocity to append.

        Returns
        -------
        SwarmVelocity
            Combined velocity.

        """
        return SwarmVelocity(swaps=self.swaps + other.swaps)


@dataclass
class JoinParticle:
    """A particle exploring the join order permutation space.

    Each particle maintains its current position (join order),
    velocity (pending swap operations), personal-best position,
    and fitness history.

    Attributes
    ----------
    particle_id : str
        Unique identifier.
    position : list[str]
        Current join order (permutation of join keys).
    velocity : SwarmVelocity
        Current velocity (swap sequence to apply next).
    personal_best : list[str]
        Best join order this particle has ever found.
    personal_best_fitness : float
        Fitness of the personal best.
    current_fitness : float
        Fitness of the current position.
    stagnation_count : int
        Iterations since last personal improvement.
    metadata : dict[str, Any]
        Particle-specific metadata.

    """

    particle_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    position: list[str] = field(default_factory=list)
    velocity: SwarmVelocity = field(default_factory=SwarmVelocity)
    personal_best: list[str] = field(default_factory=list)
    personal_best_fitness: float = 0.0
    current_fitness: float = 0.0
    stagnation_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_improved(self) -> bool:
        """Whether the particle improved its personal best this iteration."""
        return self.stagnation_count == 0


class ParticleSwarmOptimizer:
    """PSO engine for distributed join order optimization.

    Manages a swarm of particles exploring the space of join order
    permutations. Each iteration:

    1. **Evaluate** — compute fitness of each particle's current position.
    2. **Update personal/global bests** — record improvements.
    3. **Update velocity** — combine inertia, cognitive, and social
       components.
    4. **Move** — apply velocity to get new positions.
    5. **Check convergence** — stop if the swarm has converged.

    Parameters
    ----------
    config : SwarmConfig
        Algorithm parameters (inertia, cognitive, social weights).
    cost_fn : callable
        Function mapping a join order (list[str]) to an estimated cost
        (float, lower = better). If not provided, a simple positional
        cost model is used.

    """

    def __init__(
        self,
        config: SwarmConfig | None = None,
        cost_fn: Any | None = None,
    ) -> None:
        self.config = config or SwarmConfig()
        self.cost_fn = cost_fn
        self.particles: list[JoinParticle] = []
        self.global_best: list[str] = []
        self.global_best_fitness: float = 0.0
        self.metrics = EmergentMetrics()

    def _default_cost(
        self,
        join_order: list[str],
        cardinalities: dict[str, float],
    ) -> float:
        """Default cost model: intermediate result size growth.

        Estimates cost as the cumulative product of cardinalities
        in join order. Joining small tables first reduces intermediate
        sizes.

        Parameters
        ----------
        join_order:
            Sequence of join keys.
        cardinalities:
            Estimated cardinality per join key.

        Returns
        -------
        float
            Estimated total cost.

        """
        total = 0.0
        running_size = 1.0
        for key in join_order:
            card = cardinalities.get(key, 100.0)
            running_size *= math.sqrt(card)  # sub-linear growth assumption
            total += running_size
        return total

    def _evaluate_fitness(
        self,
        join_order: list[str],
        cardinalities: dict[str, float],
    ) -> float:
        """Evaluate fitness of a join order.

        Parameters
        ----------
        join_order:
            Candidate join permutation.
        cardinalities:
            Estimated cardinality per join key.

        Returns
        -------
        float
            Fitness (inverse cost, higher = better).

        """
        if self.cost_fn is not None:
            cost = self.cost_fn(join_order)
        else:
            cost = self._default_cost(join_order, cardinalities)
        if cost <= 0:
            return 0.0
        return 1.0 / cost

    def _initialize_swarm(
        self,
        join_keys: list[str],
        cardinalities: dict[str, float],
    ) -> None:
        """Create initial particle swarm with random permutations.

        Parameters
        ----------
        join_keys:
            Join keys to permute.
        cardinalities:
            Cardinality estimates.

        """
        self.particles = []
        self.global_best = []
        self.global_best_fitness = 0.0

        for _ in range(self.config.particle_count):
            perm = list(join_keys)
            random.shuffle(perm)
            fitness = self._evaluate_fitness(perm, cardinalities)

            particle = JoinParticle(
                position=perm,
                personal_best=list(perm),
                personal_best_fitness=fitness,
                current_fitness=fitness,
            )
            self.particles.append(particle)

            if fitness > self.global_best_fitness:
                self.global_best_fitness = fitness
                self.global_best = list(perm)

    def _update_velocity(
        self,
        particle: JoinParticle,
        iteration: int = 0,
        max_iterations: int = 1,
    ) -> None:
        """Update a particle's velocity using the PSO equation.

        velocity = inertia * v_old
                 + cognitive * rand * (personal_best - position)
                 + social * rand * (global_best - position)

        In permutation space, subtraction yields swap sequences,
        and scalar multiplication probabilistically retains swaps.

        When adaptive inertia is enabled, the inertia weight decreases
        linearly from ``inertia_weight`` to ``inertia_weight_min``,
        encouraging exploration early and exploitation later.

        Parameters
        ----------
        particle:
            The particle to update.
        iteration:
            Current iteration index (for adaptive inertia).
        max_iterations:
            Total iterations planned (for adaptive inertia).

        """
        w = self.config.inertia_at(iteration, max_iterations)
        c1 = self.config.cognitive_weight
        c2 = self.config.social_weight

        # Inertia component
        inertia = particle.velocity.scale(w)

        # Cognitive component: personal_best - position
        cognitive_diff = SwarmVelocity.difference(
            particle.personal_best,
            particle.position,
        )
        cognitive = cognitive_diff.scale(c1 * random.random())

        # Social component: global_best - position
        social_diff = SwarmVelocity.difference(
            self.global_best,
            particle.position,
        )
        social = social_diff.scale(c2 * random.random())

        # Combine all components
        particle.velocity = inertia.combine(cognitive).combine(social)

    def _move_particle(self, particle: JoinParticle) -> None:
        """Apply velocity to move a particle to a new position.

        Parameters
        ----------
        particle:
            The particle to move.

        """
        particle.position = particle.velocity.apply_to(particle.position)

    def optimize(
        self,
        join_keys: list[str],
        cardinalities: dict[str, float] | None = None,
        iterations: int | None = None,
    ) -> ParticleSwarmResult:
        """Run the full PSO optimization loop.

        Parameters
        ----------
        join_keys:
            Join keys to optimize the ordering of.
        cardinalities:
            Estimated cardinality per join key. Defaults to 100 each.
        iterations:
            Override number of iterations.

        Returns
        -------
        ParticleSwarmResult
            Best join order found with convergence diagnostics.

        """
        start_time = time.time()
        cardinalities = cardinalities or dict.fromkeys(join_keys, 100.0)
        max_iter = iterations or self.config.pso_iterations

        if len(join_keys) <= 1:
            return ParticleSwarmResult(
                best_join_order=list(join_keys),
                best_fitness=1.0,
                best_cost=self._default_cost(join_keys, cardinalities),
                iterations_run=0,
                elapsed_ms=0.0,
            )

        self._initialize_swarm(join_keys, cardinalities)
        stagnation = 0

        for iteration in range(max_iter):
            self.metrics.iteration = iteration
            improved_global = False

            for particle in self.particles:
                # Update velocity and move
                self._update_velocity(particle, iteration, max_iter)
                self._move_particle(particle)

                # Evaluate new position
                fitness = self._evaluate_fitness(particle.position, cardinalities)
                particle.current_fitness = fitness

                # Update personal best
                if fitness > particle.personal_best_fitness:
                    particle.personal_best = list(particle.position)
                    particle.personal_best_fitness = fitness
                    particle.stagnation_count = 0
                else:
                    particle.stagnation_count += 1

                # Update global best
                if fitness > self.global_best_fitness:
                    self.global_best_fitness = fitness
                    self.global_best = list(particle.position)
                    improved_global = True

            if improved_global:
                stagnation = 0
            else:
                stagnation += 1

            # Swarm coherence: fraction of particles near global best
            near_best = sum(
                1
                for p in self.particles
                if abs(p.current_fitness - self.global_best_fitness)
                / max(0.001, self.global_best_fitness)
                < 0.1
            )
            self.metrics.swarm_coherence = near_best / max(1, len(self.particles))
            self.metrics.snapshot()

            # Convergence check
            if stagnation >= max_iter // 5:
                _logger.info(
                    "PSO converged at iteration %d (stagnation=%d)",
                    iteration,
                    stagnation,
                )
                break

        elapsed_ms = (time.time() - start_time) * 1000.0
        best_cost = self._default_cost(self.global_best, cardinalities)

        return ParticleSwarmResult(
            best_join_order=self.global_best,
            best_fitness=self.global_best_fitness,
            best_cost=best_cost,
            iterations_run=self.metrics.iteration + 1,
            particles_converged=sum(
                1 for p in self.particles if p.stagnation_count > 5
            ),
            swarm_coherence=self.metrics.swarm_coherence,
            elapsed_ms=elapsed_ms,
            fitness_history=[
                snap.get("swarm_coherence", 0.0) for snap in self.metrics.history
            ],
        )


@dataclass
class ParticleSwarmResult:
    """Result of a PSO join order optimization run.

    Attributes
    ----------
    best_join_order : list[str]
        Optimal join permutation found.
    best_fitness : float
        Fitness of the best order (inverse cost).
    best_cost : float
        Estimated cost of executing joins in the best order.
    iterations_run : int
        Number of PSO iterations executed.
    particles_converged : int
        Number of particles that stopped improving.
    swarm_coherence : float
        Final fraction of particles near the global best.
    elapsed_ms : float
        Wall-clock time for the optimization.
    fitness_history : list[float]
        Per-iteration swarm coherence for convergence visualization.

    """

    best_join_order: list[str] = field(default_factory=list)
    best_fitness: float = 0.0
    best_cost: float = float("inf")
    iterations_run: int = 0
    particles_converged: int = 0
    swarm_coherence: float = 0.0
    elapsed_ms: float = 0.0
    fitness_history: list[float] = field(default_factory=list)
