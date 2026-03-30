"""Tests for multiverse core data structures.

Tests cover:
- UniverseStatus enum values and transitions
- BranchPoint creation and field defaults
- Universe properties (depth, is_active), fingerprinting, and decoherence
- CollapseResult construction
- MultiverseState: spawn, capacity enforcement, coherence registration,
  decoherence pruning, and coherence savings calculation
"""

from __future__ import annotations

import pytest
from fastopendata.multiverse.core import (
    BranchPoint,
    CollapseResult,
    MultiverseState,
    Universe,
    UniverseStatus,
)

# ---------------------------------------------------------------------------
# UniverseStatus
# ---------------------------------------------------------------------------


class TestUniverseStatus:
    def test_enum_values(self) -> None:
        assert UniverseStatus.NASCENT.value == "nascent"
        assert UniverseStatus.SUPERPOSED.value == "superposed"
        assert UniverseStatus.EXECUTING.value == "executing"
        assert UniverseStatus.COLLAPSED.value == "collapsed"
        assert UniverseStatus.DECOHERED.value == "decohered"

    def test_all_statuses_present(self) -> None:
        assert len(UniverseStatus) == 5


# ---------------------------------------------------------------------------
# BranchPoint
# ---------------------------------------------------------------------------


class TestBranchPoint:
    def test_creation(self) -> None:
        bp = BranchPoint(
            parent_universe_id="parent-1",
            decision_type="join_order",
            decision_value="A,B,C",
        )
        assert bp.parent_universe_id == "parent-1"
        assert bp.decision_type == "join_order"
        assert bp.decision_value == "A,B,C"
        assert isinstance(bp.timestamp, float)
        assert bp.metadata == {}

    def test_metadata_preserved(self) -> None:
        bp = BranchPoint(
            parent_universe_id="p",
            decision_type="scan",
            decision_value="index",
            metadata={"cost": 42.0},
        )
        assert bp.metadata["cost"] == 42.0


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------


class TestUniverse:
    def test_defaults(self) -> None:
        u = Universe()
        assert len(u.universe_id) == 12
        assert u.status == UniverseStatus.NASCENT
        assert u.plan_fingerprint == ""
        assert u.branch_history == []
        assert u.coherent_fragments == set()
        assert u.cost_estimate == float("inf")
        assert u.actual_cost is None
        assert u.result is None
        assert u.elapsed_ms == 0.0

    def test_depth_empty(self) -> None:
        u = Universe()
        assert u.depth == 0

    def test_depth_with_branches(self) -> None:
        bp1 = BranchPoint(
            parent_universe_id="root",
            decision_type="a",
            decision_value="1",
        )
        bp2 = BranchPoint(
            parent_universe_id="root",
            decision_type="b",
            decision_value="2",
        )
        u = Universe(branch_history=[bp1, bp2])
        assert u.depth == 2

    def test_is_active_nascent(self) -> None:
        assert Universe(status=UniverseStatus.NASCENT).is_active is True

    def test_is_active_superposed(self) -> None:
        assert Universe(status=UniverseStatus.SUPERPOSED).is_active is True

    def test_is_active_executing(self) -> None:
        assert Universe(status=UniverseStatus.EXECUTING).is_active is True

    def test_is_active_collapsed(self) -> None:
        assert Universe(status=UniverseStatus.COLLAPSED).is_active is False

    def test_is_active_decohered(self) -> None:
        assert Universe(status=UniverseStatus.DECOHERED).is_active is False

    def test_fingerprint_plan(self) -> None:
        u = Universe()
        fp = u.fingerprint_plan("scan:Person|join:KNOWS")
        assert len(fp) == 16
        assert u.plan_fingerprint == fp

    def test_fingerprint_plan_deterministic(self) -> None:
        u1 = Universe()
        u2 = Universe()
        assert u1.fingerprint_plan("same plan") == u2.fingerprint_plan("same plan")

    def test_fingerprint_plan_differs_for_different_plans(self) -> None:
        u = Universe()
        fp1 = u.fingerprint_plan("plan A")
        fp2 = u.fingerprint_plan("plan B")
        assert fp1 != fp2

    def test_decohere(self) -> None:
        u = Universe(status=UniverseStatus.EXECUTING)
        u.decohere("too expensive")
        assert u.status == UniverseStatus.DECOHERED
        assert u.metadata["decoherence_reason"] == "too expensive"
        assert "decohered_at" in u.metadata

    def test_unique_ids(self) -> None:
        ids = {Universe().universe_id for _ in range(50)}
        assert len(ids) == 50


# ---------------------------------------------------------------------------
# CollapseResult
# ---------------------------------------------------------------------------


class TestCollapseResult:
    def test_construction(self) -> None:
        cr = CollapseResult(
            selected_universe_id="abc",
            result={"rows": 10},
            total_universes=5,
            collapsed_universes=3,
            decohered_universes=2,
            coherence_savings_pct=25.0,
            collapse_reason="best cost",
            elapsed_ms=100.0,
        )
        assert cr.selected_universe_id == "abc"
        assert cr.result == {"rows": 10}
        assert cr.total_universes == 5
        assert cr.universe_costs == {}

    def test_universe_costs(self) -> None:
        cr = CollapseResult(
            selected_universe_id="x",
            result=None,
            total_universes=2,
            collapsed_universes=2,
            decohered_universes=0,
            coherence_savings_pct=0.0,
            collapse_reason="done",
            elapsed_ms=50.0,
            universe_costs={"u1": 10.0, "u2": 20.0},
        )
        assert cr.universe_costs["u1"] == 10.0


# ---------------------------------------------------------------------------
# MultiverseState
# ---------------------------------------------------------------------------


class TestMultiverseState:
    def test_empty_state(self) -> None:
        ms = MultiverseState()
        assert ms.active_universes == []
        assert ms.superposition_size == 0

    def test_spawn_universe_root(self) -> None:
        ms = MultiverseState()
        u = ms.spawn_universe(cost_estimate=10.0)
        assert u.universe_id in ms.universes
        assert u.status == UniverseStatus.SUPERPOSED
        assert u.cost_estimate == 10.0
        assert ms.superposition_size == 1

    def test_spawn_universe_with_parent(self) -> None:
        ms = MultiverseState()
        parent = ms.spawn_universe(cost_estimate=5.0)
        parent.coherent_fragments.add("frag1")
        child = ms.spawn_universe(
            parent_id=parent.universe_id,
            decision_type="join_order",
            decision_value="A,B",
            cost_estimate=8.0,
        )
        assert child.depth == 1
        assert child.branch_history[0].parent_universe_id == parent.universe_id
        assert "frag1" in child.coherent_fragments

    def test_spawn_nonexistent_parent_ignored(self) -> None:
        ms = MultiverseState()
        u = ms.spawn_universe(
            parent_id="nonexistent",
            decision_type="t",
            decision_value="v",
        )
        # Should still create the universe; parent lookup is a no-op
        assert u.depth == 1  # branch point still recorded
        assert u.coherent_fragments == set()

    def test_capacity_enforcement(self) -> None:
        ms = MultiverseState(max_universes=3)
        u1 = ms.spawn_universe(cost_estimate=10.0)
        u2 = ms.spawn_universe(cost_estimate=5.0)
        u3 = ms.spawn_universe(cost_estimate=3.0)
        assert ms.superposition_size == 3
        # Spawning a 4th should decohere the most expensive active one
        u4 = ms.spawn_universe(cost_estimate=1.0)
        assert ms.superposition_size == 3
        assert u1.status == UniverseStatus.DECOHERED

    def test_register_coherence(self) -> None:
        ms = MultiverseState()
        u1 = ms.spawn_universe(cost_estimate=1.0)
        u2 = ms.spawn_universe(cost_estimate=2.0)
        ms.register_coherence("frag_x", {u1.universe_id, u2.universe_id})
        assert "frag_x" in ms.coherence_map
        assert u1.universe_id in ms.coherence_map["frag_x"]
        assert "frag_x" in u1.coherent_fragments
        assert "frag_x" in u2.coherent_fragments

    def test_apply_decoherence_prunes_expensive(self) -> None:
        ms = MultiverseState(decoherence_threshold=2.0)
        _cheap = ms.spawn_universe(cost_estimate=1.0)
        expensive = ms.spawn_universe(cost_estimate=5.0)
        decohered = ms.apply_decoherence()
        assert expensive.universe_id in decohered
        assert expensive.status == UniverseStatus.DECOHERED

    def test_apply_decoherence_single_universe(self) -> None:
        ms = MultiverseState()
        ms.spawn_universe(cost_estimate=100.0)
        assert ms.apply_decoherence() == []

    def test_apply_decoherence_zero_cost(self) -> None:
        ms = MultiverseState()
        ms.spawn_universe(cost_estimate=0.0)
        ms.spawn_universe(cost_estimate=100.0)
        # best_cost <= 0 returns early
        assert ms.apply_decoherence() == []

    def test_coherence_savings_no_coherence(self) -> None:
        ms = MultiverseState()
        assert ms.coherence_savings() == 0.0

    def test_coherence_savings_with_sharing(self) -> None:
        ms = MultiverseState()
        u1 = ms.spawn_universe(cost_estimate=1.0)
        u2 = ms.spawn_universe(cost_estimate=2.0)
        u3 = ms.spawn_universe(cost_estimate=3.0)
        ms.register_coherence("f1", {u1.universe_id, u2.universe_id, u3.universe_id})
        savings = ms.coherence_savings()
        # 3 refs, 1 unique => (3-1)/3 = 66.67%
        assert savings == pytest.approx(66.67, abs=0.01)

    def test_coherence_savings_no_actual_sharing(self) -> None:
        ms = MultiverseState()
        u1 = ms.spawn_universe(cost_estimate=1.0)
        ms.register_coherence("f1", {u1.universe_id})
        # 1 ref, 1 unique => no savings
        assert ms.coherence_savings() == 0.0
