"""Core data structures for the multiverse execution model.

This module defines the fundamental abstractions:

- :class:`Universe` — a single execution branch exploring a specific
  query strategy (join order, scan method, predicate placement).
- :class:`BranchPoint` — records where a universe forked from its
  parent, tracking the decision that created the divergence.
- :class:`MultiverseState` — the superposition of all active universes,
  with coherence tracking and decoherence detection.
- :class:`CollapseResult` — the outcome of measuring (collapsing) the
  multiverse to select the optimal execution result.

Many-Worlds Mapping
-------------------

In Everett's many-worlds interpretation, every quantum measurement
causes the universe to branch into orthogonal worlds — one per
possible outcome. No world is discarded; all coexist in superposition.

For query execution, the analogous "measurement" is a **planning
decision** — choosing a join order, selecting an index, or placing a
predicate. Each choice creates a branch. Rather than committing to one
plan via a cost model (the "Copenhagen" approach of traditional
optimizers), the multiverse executor explores all branches in parallel
and collapses to the best observed result.

The key insight is that **coherence** — shared sub-computations across
branches — dramatically reduces the total work. If two universes share
the same entity scan but differ in join order, the scan is computed
once and shared, analogous to quantum entanglement preserving
correlations across branches.

.. versionadded:: 0.0.30
"""

from __future__ import annotations

import enum
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

_logger = logging.getLogger(__name__)


class UniverseStatus(enum.Enum):
    """Lifecycle status of an execution universe.

    States progress: NASCENT -> SUPERPOSED -> EXECUTING -> COLLAPSED / DECOHERED

    NASCENT:     Universe created but not yet scheduled.
    SUPERPOSED:  Universe is part of the active superposition and may
                 share coherent sub-computations with other universes.
    EXECUTING:   Universe is actively running its execution plan.
    COLLAPSED:   Universe was selected as the observed outcome.
    DECOHERED:   Universe diverged beyond recovery (e.g., exceeded
                 resource limits) and was pruned from the superposition.
    """

    NASCENT = "nascent"
    SUPERPOSED = "superposed"
    EXECUTING = "executing"
    COLLAPSED = "collapsed"
    DECOHERED = "decohered"


@dataclass
class BranchPoint:
    """Records a decision point where one universe forked from another.

    Attributes
    ----------
    parent_universe_id : str
        The universe that branched.
    decision_type : str
        Category of the planning decision (e.g., ``"join_order"``,
        ``"scan_method"``, ``"predicate_placement"``).
    decision_value : str
        The specific choice made at this branch point.
    timestamp : float
        When the branch occurred (epoch seconds).
    metadata : dict[str, Any]
        Additional context (estimated cost, cardinality, etc.).

    """

    parent_universe_id: str
    decision_type: str
    decision_value: str
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Universe:
    """A single execution branch in the multiverse.

    Each universe represents a complete query execution strategy —
    a specific combination of join orders, scan methods, predicate
    placements, and other optimizer decisions. Universes execute
    independently but may share coherent sub-computations via the
    coherence graph.

    Attributes
    ----------
    universe_id : str
        Globally unique identifier for this branch.
    status : UniverseStatus
        Current lifecycle state.
    plan_fingerprint : str
        Hash of the execution plan, used for deduplication.
    branch_history : list[BranchPoint]
        Ordered sequence of decisions that created this branch.
    coherent_fragments : set[str]
        Fragment fingerprints shared with other universes (entangled).
    cost_estimate : float
        A priori cost estimate from the planner.
    actual_cost : float | None
        Observed cost after execution (``None`` until measured).
    result : Any
        The execution result (``None`` until execution completes).
    elapsed_ms : float
        Execution wall-clock time in milliseconds.
    created_at : float
        Epoch timestamp when the universe was instantiated.
    metadata : dict[str, Any]
        Arbitrary metadata for instrumentation and debugging.

    """

    universe_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    status: UniverseStatus = UniverseStatus.NASCENT
    plan_fingerprint: str = ""
    branch_history: list[BranchPoint] = field(default_factory=list)
    coherent_fragments: set[str] = field(default_factory=set)
    cost_estimate: float = float("inf")
    actual_cost: float | None = None
    result: Any = None
    elapsed_ms: float = 0.0
    created_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def depth(self) -> int:
        """Number of branching decisions from the root universe."""
        return len(self.branch_history)

    @property
    def is_active(self) -> bool:
        """Whether the universe is still participating in the superposition."""
        return self.status in (
            UniverseStatus.NASCENT,
            UniverseStatus.SUPERPOSED,
            UniverseStatus.EXECUTING,
        )

    def fingerprint_plan(self, plan_description: str) -> str:
        """Compute a stable fingerprint for this universe's execution plan.

        Parameters
        ----------
        plan_description:
            Canonical string representation of the execution plan.

        Returns
        -------
        str
            SHA-256 hex digest (first 16 characters).

        """
        digest = hashlib.sha256(plan_description.encode()).hexdigest()[:16]
        self.plan_fingerprint = digest
        return digest

    def decohere(self, reason: str) -> None:
        """Mark this universe as decohered (pruned from superposition).

        Parameters
        ----------
        reason:
            Human-readable explanation of why the universe was pruned.

        """
        self.status = UniverseStatus.DECOHERED
        self.metadata["decoherence_reason"] = reason
        self.metadata["decohered_at"] = time.time()
        _logger.debug(
            "Universe %s decohered: %s",
            self.universe_id,
            reason,
        )


@dataclass
class CollapseResult:
    """Outcome of collapsing the multiverse to a single observed result.

    When the multiverse "collapses," the executor selects the best
    universe based on observed execution metrics (latency, cost,
    result quality) and returns its result as the canonical output.

    Attributes
    ----------
    selected_universe_id : str
        The universe whose result was chosen.
    result : Any
        The execution result from the selected universe.
    total_universes : int
        Number of universes that were explored.
    collapsed_universes : int
        Number that completed execution before collapse.
    decohered_universes : int
        Number that were pruned before completion.
    coherence_savings_pct : float
        Percentage of computation saved via shared fragments.
    collapse_reason : str
        Why this particular universe was selected.
    elapsed_ms : float
        Total wall-clock time for the multiverse exploration.
    universe_costs : dict[str, float]
        Mapping of universe_id -> actual_cost for completed universes.

    """

    selected_universe_id: str
    result: Any
    total_universes: int
    collapsed_universes: int
    decohered_universes: int
    coherence_savings_pct: float
    collapse_reason: str
    elapsed_ms: float
    universe_costs: dict[str, float] = field(default_factory=dict)


@dataclass
class MultiverseState:
    """Superposition of all active execution universes.

    Tracks the full set of parallel execution branches, manages
    coherence relationships between them, and detects when universes
    should be pruned (decohered) due to resource exhaustion or
    dominance by cheaper alternatives.

    Attributes
    ----------
    universes : dict[str, Universe]
        All universes keyed by ``universe_id``.
    coherence_map : dict[str, set[str]]
        Maps fragment fingerprints to sets of universe IDs that share
        that fragment (entanglement tracking).
    max_universes : int
        Upper bound on simultaneously active universes.
    decoherence_threshold : float
        Cost ratio beyond which a universe is pruned. A universe is
        decohered if its estimated cost exceeds the best estimate by
        this factor (e.g., 3.0 means 3x worse than the best).

    """

    universes: dict[str, Universe] = field(default_factory=dict)
    coherence_map: dict[str, set[str]] = field(default_factory=dict)
    max_universes: int = 16
    decoherence_threshold: float = 3.0

    @property
    def active_universes(self) -> list[Universe]:
        """Return all universes still in the superposition."""
        return [u for u in self.universes.values() if u.is_active]

    @property
    def superposition_size(self) -> int:
        """Number of currently active universes."""
        return len(self.active_universes)

    def spawn_universe(
        self,
        *,
        parent_id: str | None = None,
        decision_type: str = "",
        decision_value: str = "",
        cost_estimate: float = float("inf"),
        metadata: dict[str, Any] | None = None,
    ) -> Universe:
        """Create a new universe, optionally branching from a parent.

        If the maximum number of active universes is reached, the
        highest-cost universe is decohered to make room.

        Parameters
        ----------
        parent_id:
            Universe to branch from (``None`` for root universe).
        decision_type:
            Category of the planning decision that caused this branch.
        decision_value:
            The specific choice made.
        cost_estimate:
            A priori cost estimate for the new branch.
        metadata:
            Additional context.

        Returns
        -------
        Universe
            The newly created universe.

        """
        # Enforce capacity — decohere the most expensive active universe
        while self.superposition_size >= self.max_universes:
            worst = max(self.active_universes, key=lambda u: u.cost_estimate)
            worst.decohere(
                f"capacity limit ({self.max_universes}): "
                f"cost {worst.cost_estimate:.2f} exceeded",
            )

        universe = Universe(
            cost_estimate=cost_estimate,
            metadata=metadata or {},
        )

        # Inherit branch history from parent
        if parent_id and parent_id in self.universes:
            parent = self.universes[parent_id]
            universe.branch_history = list(parent.branch_history)
            universe.coherent_fragments = set(parent.coherent_fragments)

        # Record the branch point
        if decision_type:
            universe.branch_history.append(
                BranchPoint(
                    parent_universe_id=parent_id or "root",
                    decision_type=decision_type,
                    decision_value=decision_value,
                ),
            )

        universe.status = UniverseStatus.SUPERPOSED
        self.universes[universe.universe_id] = universe
        _logger.debug(
            "Spawned universe %s (cost=%.2f, depth=%d, active=%d)",
            universe.universe_id,
            cost_estimate,
            universe.depth,
            self.superposition_size,
        )
        return universe

    def register_coherence(
        self,
        fragment_fingerprint: str,
        universe_ids: set[str],
    ) -> None:
        """Register that multiple universes share a sub-computation.

        Parameters
        ----------
        fragment_fingerprint:
            Fingerprint of the shared plan fragment.
        universe_ids:
            Set of universe IDs that share this fragment.

        """
        if fragment_fingerprint not in self.coherence_map:
            self.coherence_map[fragment_fingerprint] = set()
        self.coherence_map[fragment_fingerprint].update(universe_ids)

        # Update each universe's coherent fragment set
        for uid in universe_ids:
            if uid in self.universes:
                self.universes[uid].coherent_fragments.add(fragment_fingerprint)

    def apply_decoherence(self) -> list[str]:
        """Prune universes whose cost exceeds the decoherence threshold.

        Compares each active universe's estimated cost against the
        cheapest active universe. Those exceeding the threshold ratio
        are decohered.

        Returns
        -------
        list[str]
            IDs of universes that were decohered.

        """
        active = self.active_universes
        if len(active) <= 1:
            return []

        best_cost = min(u.cost_estimate for u in active)
        if best_cost <= 0:
            return []

        decohered: list[str] = []
        for universe in active:
            ratio = universe.cost_estimate / best_cost
            if ratio > self.decoherence_threshold:
                universe.decohere(
                    f"cost ratio {ratio:.2f} exceeds threshold "
                    f"{self.decoherence_threshold:.1f}",
                )
                decohered.append(universe.universe_id)

        return decohered

    def coherence_savings(self) -> float:
        """Estimate the percentage of computation saved via coherence.

        Returns
        -------
        float
            Percentage (0-100) of fragment computations saved by
            sharing across universes.

        """
        if not self.coherence_map:
            return 0.0

        total_fragment_refs = sum(len(uids) for uids in self.coherence_map.values())
        unique_fragments = len(self.coherence_map)

        if total_fragment_refs <= unique_fragments:
            return 0.0

        # Savings = (shared refs - unique computations) / total refs
        saved = total_fragment_refs - unique_fragments
        return (saved / total_fragment_refs) * 100.0
