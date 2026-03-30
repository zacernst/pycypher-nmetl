"""Tests for multiverse plan enumeration and coherence analysis.

Tests cover:
- PlanVariant: construction, auto-ID generation, fragment fingerprinting
- CoherenceGraph: fragment registration, entanglement detection, scoring
- MultiversePlanner: variant enumeration, coherence graph construction,
  multiverse population, dominance pruning, and cost estimation
"""

from __future__ import annotations

import pytest
from fastopendata.multiverse.core import MultiverseState
from fastopendata.multiverse.planner import (
    CoherenceGraph,
    MultiversePlanner,
    PlanVariant,
)

# ---------------------------------------------------------------------------
# PlanVariant
# ---------------------------------------------------------------------------


class TestPlanVariant:
    def test_auto_variant_id(self) -> None:
        pv = PlanVariant(plan_description="join:A,B|scan:A=full_scan")
        assert len(pv.variant_id) == 12

    def test_no_auto_id_when_empty_description(self) -> None:
        pv = PlanVariant()
        assert pv.variant_id == ""

    def test_explicit_variant_id_preserved(self) -> None:
        pv = PlanVariant(variant_id="custom", plan_description="anything")
        assert pv.variant_id == "custom"

    def test_fingerprint_fragment(self) -> None:
        pv = PlanVariant(plan_description="test")
        fp = pv.fingerprint_fragment("scan:Person")
        assert len(fp) == 12
        assert fp in pv.fragment_fingerprints

    def test_fingerprint_fragment_deduplication(self) -> None:
        pv = PlanVariant(plan_description="test")
        fp1 = pv.fingerprint_fragment("scan:Person")
        fp2 = pv.fingerprint_fragment("scan:Person")
        assert fp1 == fp2
        assert pv.fragment_fingerprints.count(fp1) == 1

    def test_fingerprint_deterministic(self) -> None:
        pv1 = PlanVariant(plan_description="a")
        pv2 = PlanVariant(plan_description="b")
        assert pv1.fingerprint_fragment("scan:X") == pv2.fingerprint_fragment("scan:X")

    def test_default_cost_infinity(self) -> None:
        pv = PlanVariant()
        assert pv.estimated_cost == float("inf")


# ---------------------------------------------------------------------------
# CoherenceGraph
# ---------------------------------------------------------------------------


class TestCoherenceGraph:
    def test_register_fragment(self) -> None:
        cg = CoherenceGraph()
        cg.register_fragment("fp1", "v1", "scan:Person")
        assert "fp1" in cg.fragment_to_variants
        assert "v1" in cg.fragment_to_variants["fp1"]
        assert cg.fragment_descriptions["fp1"] == "scan:Person"

    def test_register_multiple_variants_same_fragment(self) -> None:
        cg = CoherenceGraph()
        cg.register_fragment("fp1", "v1")
        cg.register_fragment("fp1", "v2")
        assert cg.fragment_to_variants["fp1"] == {"v1", "v2"}

    def test_entangled_fragments_default_threshold(self) -> None:
        cg = CoherenceGraph()
        cg.register_fragment("shared", "v1")
        cg.register_fragment("shared", "v2")
        cg.register_fragment("unique", "v3")
        entangled = cg.entangled_fragments()
        assert "shared" in entangled
        assert "unique" not in entangled

    def test_entangled_fragments_custom_threshold(self) -> None:
        cg = CoherenceGraph()
        cg.register_fragment("fp1", "v1")
        cg.register_fragment("fp1", "v2")
        cg.register_fragment("fp1", "v3")
        assert "fp1" in cg.entangled_fragments(min_sharing=3)
        assert "fp1" not in cg.entangled_fragments(min_sharing=4)

    def test_coherence_score_empty(self) -> None:
        cg = CoherenceGraph()
        assert cg.coherence_score() == 0.0

    def test_coherence_score_perfect(self) -> None:
        cg = CoherenceGraph()
        cg.register_fragment("fp1", "v1")
        cg.register_fragment("fp1", "v2")
        cg.register_fragment("fp2", "v1")
        cg.register_fragment("fp2", "v2")
        # All fragments shared by all variants => score = 1.0
        assert cg.coherence_score() == pytest.approx(1.0)

    def test_coherence_score_partial(self) -> None:
        cg = CoherenceGraph()
        cg.register_fragment("fp1", "v1")
        cg.register_fragment("fp1", "v2")
        cg.register_fragment("fp2", "v1")  # only v1
        # total_refs = 3, max_possible = 2 fragments * 2 max_variants = 4
        assert cg.coherence_score() == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# MultiversePlanner
# ---------------------------------------------------------------------------


class TestMultiversePlanner:
    def test_enumerate_single_entity_no_joins(self) -> None:
        planner = MultiversePlanner()
        variants = planner.enumerate_variants(
            join_keys=[],
            entities=["Person"],
            predicates=[],
        )
        # With no joins, we still get scan strategy variants
        assert len(variants) > 0
        for v in variants:
            assert "Person" in v.scan_strategies

    def test_enumerate_with_joins(self) -> None:
        planner = MultiversePlanner(max_join_permutations=6)
        variants = planner.enumerate_variants(
            join_keys=["A", "B", "C"],
            entities=["Person"],
            predicates=[],
        )
        assert len(variants) > 0
        # Should have different join orders
        orders = {v.join_order for v in variants}
        assert len(orders) >= 2

    def test_enumerate_with_predicates(self) -> None:
        planner = MultiversePlanner(max_join_permutations=2)
        variants = planner.enumerate_variants(
            join_keys=["A"],
            entities=["Person"],
            predicates=["age > 30"],
        )
        modes = {v.predicate_placements.get("age > 30") for v in variants}
        assert "pushdown" in modes
        assert "post_join" in modes

    def test_enumerate_sorted_by_cost(self) -> None:
        planner = MultiversePlanner(max_join_permutations=4)
        variants = planner.enumerate_variants(
            join_keys=["A", "B"],
            entities=["X"],
            predicates=[],
        )
        costs = [v.estimated_cost for v in variants]
        assert costs == sorted(costs)

    def test_enumerate_fragments_populated(self) -> None:
        planner = MultiversePlanner(max_join_permutations=2)
        variants = planner.enumerate_variants(
            join_keys=["A"],
            entities=["Person"],
            predicates=[],
        )
        for v in variants:
            assert len(v.fragment_fingerprints) > 0

    def test_enumerate_with_cost_model(self) -> None:
        planner = MultiversePlanner(max_join_permutations=2)
        cheap = planner.enumerate_variants(
            join_keys=["A"],
            entities=["Person"],
            predicates=[],
            cost_model={"scan:Person": 0.1},
        )
        expensive = planner.enumerate_variants(
            join_keys=["A"],
            entities=["Person"],
            predicates=[],
            cost_model={"scan:Person": 1000.0},
        )
        assert cheap[0].estimated_cost < expensive[0].estimated_cost

    def test_build_coherence_graph(self) -> None:
        planner = MultiversePlanner(max_join_permutations=4)
        variants = planner.enumerate_variants(
            join_keys=["A", "B"],
            entities=["Person", "Company"],
            predicates=[],
        )
        graph = planner.build_coherence_graph(variants)
        assert len(graph.fragment_to_variants) > 0
        # With multiple variants sharing scan fragments, we expect entanglement
        entangled = graph.entangled_fragments()
        assert len(entangled) > 0

    def test_populate_multiverse(self) -> None:
        planner = MultiversePlanner(max_join_permutations=4)
        variants = planner.enumerate_variants(
            join_keys=["A"],
            entities=["Person"],
            predicates=[],
        )
        graph = planner.build_coherence_graph(variants)
        state = MultiverseState(max_universes=8)
        planner.populate_multiverse(state, variants, graph)
        assert state.superposition_size > 0
        assert state.superposition_size <= 8
        for u in state.active_universes:
            assert u.plan_fingerprint != ""

    def test_populate_multiverse_respects_capacity(self) -> None:
        planner = MultiversePlanner(max_join_permutations=10)
        variants = planner.enumerate_variants(
            join_keys=["A", "B"],
            entities=["X"],
            predicates=[],
        )
        state = MultiverseState(max_universes=3)
        graph = planner.build_coherence_graph(variants)
        planner.populate_multiverse(state, variants, graph)
        assert state.superposition_size <= 3

    def test_prune_dominated_empty(self) -> None:
        planner = MultiversePlanner()
        assert planner.prune_dominated([]) == []

    def test_prune_dominated_single(self) -> None:
        planner = MultiversePlanner()
        v = PlanVariant(plan_description="only one")
        assert planner.prune_dominated([v]) == [v]

    def test_prune_dominated_deduplicates(self) -> None:
        planner = MultiversePlanner()
        v1 = PlanVariant(plan_description="same plan")
        v2 = PlanVariant(plan_description="same plan")
        # Same description => same variant_id
        assert v1.variant_id == v2.variant_id
        result = planner.prune_dominated([v1, v2])
        assert len(result) == 1

    def test_estimate_cost_scan_strategies(self) -> None:
        cost = MultiversePlanner._estimate_cost(
            join_order=(),
            scan_strategies={"A": "index_lookup"},
            predicate_placements={},
            cost_model={},
        )
        cost_full = MultiversePlanner._estimate_cost(
            join_order=(),
            scan_strategies={"A": "full_scan"},
            predicate_placements={},
            cost_model={},
        )
        # index_lookup (2.0) < full_scan (10.0)
        assert cost < cost_full

    def test_estimate_cost_predicate_placement(self) -> None:
        cost_push = MultiversePlanner._estimate_cost(
            join_order=(),
            scan_strategies={},
            predicate_placements={"p1": "pushdown"},
            cost_model={},
        )
        cost_post = MultiversePlanner._estimate_cost(
            join_order=(),
            scan_strategies={},
            predicate_placements={"p1": "post_join"},
            cost_model={},
        )
        assert cost_push < cost_post
