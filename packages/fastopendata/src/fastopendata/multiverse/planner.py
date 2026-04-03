"""Many-worlds plan enumeration with coherence-based deduplication.

This module implements the "branching" phase of the multiverse executor:
given a query's execution plan, it generates all plausible alternative
plans — each representing a distinct "universe" — and identifies shared
sub-computations (coherent fragments) that can be computed once and
reused across multiple universes.

Architecture
------------

::

    PlanVariant
    ├── plan_description      — canonical representation of the strategy
    ├── join_order             — permutation of join operations
    ├── scan_strategies        — index/scan choices per entity
    ├── predicate_placements   — where predicates are evaluated
    └── estimated_cost         — a priori cost from the optimizer

    CoherenceGraph
    ├── nodes                  — unique plan fragments (sub-computations)
    ├── edges                  — shared-by relationships to plan variants
    └── compute_entanglement() — identify maximally shared fragments

    MultiversePlanner
    ├── enumerate_universes()  — generate all plan variants
    ├── build_coherence()      — construct the coherence graph
    └── prune_dominated()      — remove strictly dominated variants

Many-Worlds Analogy
-------------------

In quantum mechanics, a system in superposition explores all possible
states simultaneously. Here, the planner places the execution plan into
superposition by enumerating alternatives along three axes:

1. **Join order permutations** — different orderings of multi-way joins,
   analogous to different measurement orderings in quantum circuits.
2. **Scan strategy choices** — full scan vs. index scan vs. hash probe,
   analogous to choosing measurement bases.
3. **Predicate placement** — pushing predicates closer to scans vs.
   evaluating them later, analogous to early vs. deferred measurement.

The coherence graph captures "entanglement" between universes: if two
join orderings share the same initial scan, that scan result is
entangled (shared) across both branches.

.. versionadded:: 0.0.30
"""

from __future__ import annotations

import hashlib
import itertools
import logging
import math
from dataclasses import dataclass, field
from typing import Any

from fastopendata.multiverse.core import MultiverseState

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Plan variant: a single candidate execution strategy
# ---------------------------------------------------------------------------


@dataclass
class PlanVariant:
    """A candidate execution strategy representing one 'universe'.

    Attributes
    ----------
    variant_id : str
        Unique identifier derived from the plan fingerprint.
    plan_description : str
        Canonical string describing the full strategy.
    join_order : tuple[str, ...]
        Ordered sequence of join keys (e.g., entity/relationship names).
    scan_strategies : dict[str, str]
        Mapping of entity/relationship -> scan method
        (``"full_scan"``, ``"index_lookup"``, ``"hash_probe"``).
    predicate_placements : dict[str, str]
        Mapping of predicate expression -> placement
        (``"pushdown"`` near scan or ``"post_join"`` after join).
    estimated_cost : float
        A priori cost estimate.
    fragment_fingerprints : list[str]
        Fingerprints of sub-computations in execution order.
    metadata : dict[str, Any]
        Additional planner-generated context.

    """

    variant_id: str = ""
    plan_description: str = ""
    join_order: tuple[str, ...] = ()
    scan_strategies: dict[str, str] = field(default_factory=dict)
    predicate_placements: dict[str, str] = field(default_factory=dict)
    estimated_cost: float = float("inf")
    fragment_fingerprints: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Compute variant_id from plan_description if not set."""
        if not self.variant_id and self.plan_description:
            self.variant_id = hashlib.sha256(
                self.plan_description.encode(),
            ).hexdigest()[:12]

    def fingerprint_fragment(self, fragment_desc: str) -> str:
        """Compute a stable fingerprint for a plan sub-computation.

        Parameters
        ----------
        fragment_desc:
            Canonical description of the fragment (e.g., ``"scan:Person"``).

        Returns
        -------
        str
            SHA-256 hex prefix (12 chars).

        """
        fp = hashlib.sha256(fragment_desc.encode()).hexdigest()[:12]
        if fp not in self.fragment_fingerprints:
            self.fragment_fingerprints.append(fp)
        return fp


# ---------------------------------------------------------------------------
# Coherence graph: tracks shared sub-computations across universes
# ---------------------------------------------------------------------------


@dataclass
class CoherenceGraph:
    """Graph of shared sub-computations across plan variants.

    Nodes are unique fragment fingerprints; edges connect fragments
    to the plan variants that include them. High-degree nodes represent
    sub-computations shared by many universes -- prime candidates for
    memoization.

    Attributes
    ----------
    fragment_to_variants : dict[str, set[str]]
        Maps fragment fingerprint -> set of variant IDs that include it.
    fragment_descriptions : dict[str, str]
        Maps fragment fingerprint -> human-readable description.

    """

    fragment_to_variants: dict[str, set[str]] = field(default_factory=dict)
    fragment_descriptions: dict[str, str] = field(default_factory=dict)

    def register_fragment(
        self,
        fingerprint: str,
        variant_id: str,
        description: str = "",
    ) -> None:
        """Register that a plan variant includes a given fragment.

        Parameters
        ----------
        fingerprint:
            Fragment fingerprint.
        variant_id:
            Plan variant that includes this fragment.
        description:
            Human-readable description of the fragment.

        """
        if fingerprint not in self.fragment_to_variants:
            self.fragment_to_variants[fingerprint] = set()
        self.fragment_to_variants[fingerprint].add(variant_id)
        if description:
            self.fragment_descriptions[fingerprint] = description

    def entangled_fragments(self, min_sharing: int = 2) -> dict[str, set[str]]:
        """Return fragments shared by at least ``min_sharing`` variants.

        These are the "entangled" sub-computations that benefit most
        from memoization.

        Parameters
        ----------
        min_sharing:
            Minimum number of variants sharing a fragment.

        Returns
        -------
        dict[str, set[str]]
            Fragment fingerprint -> set of variant IDs.

        """
        return {
            fp: variants
            for fp, variants in self.fragment_to_variants.items()
            if len(variants) >= min_sharing
        }

    def coherence_score(self) -> float:
        """Compute an overall coherence score for the multiverse.

        Higher scores indicate more shared computation, meaning the
        parallel exploration is more efficient.

        Returns
        -------
        float
            Score in [0, 1] where 1 means all fragments are shared
            by all variants.

        """
        if not self.fragment_to_variants:
            return 0.0

        total_refs = sum(len(vs) for vs in self.fragment_to_variants.values())
        max_possible = len(self.fragment_to_variants) * max(
            len(vs) for vs in self.fragment_to_variants.values()
        )
        if max_possible == 0:
            return 0.0
        return total_refs / max_possible


# ---------------------------------------------------------------------------
# MultiversePlanner: enumerate plan variants and build coherence graph
# ---------------------------------------------------------------------------


class MultiversePlanner:
    """Generates the multiverse of execution plan variants.

    Given a set of entities, relationships, and predicates involved in
    a query, the planner enumerates alternative execution strategies
    along three axes (join order, scan method, predicate placement)
    and constructs a coherence graph identifying shared sub-computations.

    Parameters
    ----------
    max_join_permutations : int
        Maximum number of join orderings to explore. For queries with
        many joins, this caps the combinatorial explosion (default: 24,
        corresponding to up to 4! permutations).
    scan_alternatives : list[str]
        Available scan strategies to consider.
    predicate_modes : list[str]
        Available predicate placement strategies.

    """

    def __init__(
        self,
        *,
        max_join_permutations: int = 24,
        scan_alternatives: list[str] | None = None,
        predicate_modes: list[str] | None = None,
    ) -> None:
        self.max_join_permutations = max_join_permutations
        self.scan_alternatives = scan_alternatives or [
            "full_scan",
            "index_lookup",
            "hash_probe",
        ]
        self.predicate_modes = predicate_modes or [
            "pushdown",
            "post_join",
        ]

    def enumerate_variants(
        self,
        join_keys: list[str],
        entities: list[str],
        predicates: list[str],
        *,
        cost_model: dict[str, float] | None = None,
    ) -> list[PlanVariant]:
        """Generate all plausible plan variants for a query.

        Parameters
        ----------
        join_keys:
            Entity/relationship names involved in joins.
        entities:
            Entity types that need scanning.
        predicates:
            Predicate expressions to place.
        cost_model:
            Optional mapping of ``"operation_type:target"`` to
            estimated cost. Used to compute ``estimated_cost``.

        Returns
        -------
        list[PlanVariant]
            All enumerated plan variants, sorted by estimated cost.

        """
        cost_model = cost_model or {}
        variants: list[PlanVariant] = []

        # Generate join order permutations (capped)
        join_perms = list(
            itertools.islice(
                itertools.permutations(join_keys),
                self.max_join_permutations,
            ),
        )

        # For each join order, consider scan strategy combinations
        scan_combos = list(
            itertools.product(
                self.scan_alternatives,
                repeat=len(entities),
            ),
        )

        # Cap scan combos to avoid exponential blowup
        max_scan_combos = max(
            1, self.max_join_permutations // max(1, len(join_perms))
        )
        scan_combos = scan_combos[:max_scan_combos]

        for join_order in join_perms:
            for scan_combo in scan_combos:
                scan_strats = dict(zip(entities, scan_combo, strict=False))

                # For each predicate, choose placement
                for pred_combo in itertools.product(
                    self.predicate_modes,
                    repeat=max(1, len(predicates)),
                ):
                    pred_placements = dict(
                        zip(predicates or ["_none"], pred_combo, strict=False),
                    )

                    # Build canonical description
                    desc_parts = [
                        f"join:{','.join(join_order)}",
                        *(f"scan:{e}={s}" for e, s in scan_strats.items()),
                        *(f"pred:{p}={m}" for p, m in pred_placements.items()),
                    ]
                    description = "|".join(desc_parts)

                    # Estimate cost
                    cost = self._estimate_cost(
                        join_order,
                        scan_strats,
                        pred_placements,
                        cost_model,
                    )

                    variant = PlanVariant(
                        plan_description=description,
                        join_order=join_order,
                        scan_strategies=scan_strats,
                        predicate_placements=pred_placements,
                        estimated_cost=cost,
                    )

                    # Fingerprint each sub-computation
                    for entity in entities:
                        variant.fingerprint_fragment(
                            f"scan:{entity}={scan_strats[entity]}",
                        )
                    for i, _jk in enumerate(join_order):
                        # Join fragment depends on preceding joins + scan
                        prefix = ",".join(join_order[: i + 1])
                        variant.fingerprint_fragment(f"join:{prefix}")

                    variants.append(variant)

        # Sort by estimated cost
        variants.sort(key=lambda v: v.estimated_cost)

        _logger.info(
            "Enumerated %d plan variants from %d join orders x %d scan combos",
            len(variants),
            len(join_perms),
            len(scan_combos),
        )
        return variants

    def build_coherence_graph(
        self,
        variants: list[PlanVariant],
    ) -> CoherenceGraph:
        """Construct the coherence graph from a set of plan variants.

        Parameters
        ----------
        variants:
            Plan variants generated by :meth:`enumerate_variants`.

        Returns
        -------
        CoherenceGraph
            Graph of shared sub-computations.

        """
        graph = CoherenceGraph()

        for variant in variants:
            for fp in variant.fragment_fingerprints:
                graph.register_fragment(fp, variant.variant_id)

        entangled = graph.entangled_fragments()
        _logger.info(
            "Coherence graph: %d fragments, %d entangled (score=%.3f)",
            len(graph.fragment_to_variants),
            len(entangled),
            graph.coherence_score(),
        )
        return graph

    def populate_multiverse(
        self,
        state: MultiverseState,
        variants: list[PlanVariant],
        coherence: CoherenceGraph,
    ) -> None:
        """Spawn universes from plan variants into a multiverse state.

        Registers coherence relationships and spawns one universe per
        variant, respecting the multiverse capacity limit.

        Parameters
        ----------
        state:
            The multiverse state to populate.
        variants:
            Plan variants to spawn as universes.
        coherence:
            Coherence graph for registering shared fragments.

        """
        variant_to_universe: dict[str, str] = {}

        for variant in variants:
            if state.superposition_size >= state.max_universes:
                _logger.info(
                    "Multiverse capacity reached (%d), stopping enumeration",
                    state.max_universes,
                )
                break

            universe = state.spawn_universe(
                decision_type="plan_variant",
                decision_value=variant.variant_id,
                cost_estimate=variant.estimated_cost,
                metadata={
                    "join_order": list(variant.join_order),
                    "scan_strategies": variant.scan_strategies,
                    "predicate_placements": variant.predicate_placements,
                },
            )
            universe.fingerprint_plan(variant.plan_description)
            variant_to_universe[variant.variant_id] = universe.universe_id

        # Register coherence in the multiverse state
        for fp, variant_ids in coherence.fragment_to_variants.items():
            universe_ids = {
                variant_to_universe[vid]
                for vid in variant_ids
                if vid in variant_to_universe
            }
            if len(universe_ids) >= 2:
                state.register_coherence(fp, universe_ids)

    def prune_dominated(
        self,
        variants: list[PlanVariant],
    ) -> list[PlanVariant]:
        """Remove strictly dominated plan variants.

        A variant is dominated if another variant has lower cost on
        every axis (join cost, scan cost, predicate cost).

        Parameters
        ----------
        variants:
            Full set of enumerated variants.

        Returns
        -------
        list[PlanVariant]
            Non-dominated (Pareto-optimal) variants.

        """
        if len(variants) <= 1:
            return variants

        # Simple dominance check based on total estimated cost
        # A more sophisticated version would check per-axis costs
        surviving: list[PlanVariant] = []
        costs_seen: set[str] = set()

        for variant in variants:
            # Deduplicate by plan fingerprint
            if variant.variant_id in costs_seen:
                continue
            costs_seen.add(variant.variant_id)
            surviving.append(variant)

        _logger.debug(
            "Pruned %d -> %d variants after dominance check",
            len(variants),
            len(surviving),
        )
        return surviving

    @staticmethod
    def _estimate_cost(
        join_order: tuple[str, ...],
        scan_strategies: dict[str, str],
        predicate_placements: dict[str, str],
        cost_model: dict[str, float],
    ) -> float:
        """Compute a simple cost estimate for a plan variant.

        Parameters
        ----------
        join_order:
            The join permutation.
        scan_strategies:
            Scan method per entity.
        predicate_placements:
            Predicate placement mode per predicate.
        cost_model:
            User-provided cost overrides.

        Returns
        -------
        float
            Estimated cost (lower is better).

        """
        # Default costs per operation type
        scan_costs = {
            "full_scan": 10.0,
            "index_lookup": 2.0,
            "hash_probe": 4.0,
        }
        pred_costs = {
            "pushdown": 0.5,  # cheap: filter early
            "post_join": 3.0,  # expensive: filter after join
        }

        total = 0.0

        # Scan costs
        for entity, strategy in scan_strategies.items():
            key = f"scan:{entity}"
            total += cost_model.get(key, scan_costs.get(strategy, 10.0))

        # Join costs (later joins are more expensive due to larger intermediates)
        for i, jk in enumerate(join_order):
            key = f"join:{jk}"
            base_cost = cost_model.get(key, 5.0)
            # Multiplicative increase for later joins (intermediate growth)
            total += base_cost * math.log2(i + 2)

        # Predicate costs
        for pred, mode in predicate_placements.items():
            key = f"pred:{pred}"
            total += cost_model.get(key, pred_costs.get(mode, 1.0))

        return total
