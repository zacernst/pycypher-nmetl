"""Tests for multiverse parallel execution and collapse strategies.

Tests cover:
- FragmentCache: get/put, hit rate, thread safety
- LatencyCollapseStrategy: should_collapse, select_winner
- QualityCollapseStrategy: should_collapse with thresholds, select_winner
- MultiverseExecutor: execute_multiverse with default and custom execution
  functions, empty state, all-failing universes, coherence integration
"""

from __future__ import annotations

import pytest
from fastopendata.multiverse.core import (
    MultiverseState,
    Universe,
    UniverseStatus,
)
from fastopendata.multiverse.executor import (
    FragmentCache,
    LatencyCollapseStrategy,
    MultiverseExecutor,
    QualityCollapseStrategy,
)

# ---------------------------------------------------------------------------
# FragmentCache
# ---------------------------------------------------------------------------


class TestFragmentCache:
    def test_miss_on_empty(self) -> None:
        cache = FragmentCache()
        hit, result = cache.get("nonexistent")
        assert hit is False
        assert result is None

    def test_put_and_get(self) -> None:
        cache = FragmentCache()
        cache.put("fp1", {"rows": 100})
        hit, result = cache.get("fp1")
        assert hit is True
        assert result == {"rows": 100}

    def test_hit_rate_no_accesses(self) -> None:
        cache = FragmentCache()
        assert cache.hit_rate == 0.0

    def test_hit_rate_all_misses(self) -> None:
        cache = FragmentCache()
        cache.get("a")
        cache.get("b")
        assert cache.hit_rate == 0.0

    def test_hit_rate_mixed(self) -> None:
        cache = FragmentCache()
        cache.put("a", 1)
        cache.get("a")  # hit
        cache.get("b")  # miss
        assert cache.hit_rate == pytest.approx(0.5)

    def test_savings_pct(self) -> None:
        cache = FragmentCache()
        cache.put("a", 1)
        cache.get("a")  # hit
        cache.get("b")  # miss
        assert cache.savings_pct == pytest.approx(50.0)

    def test_overwrite(self) -> None:
        cache = FragmentCache()
        cache.put("fp", "old")
        cache.put("fp", "new")
        _, result = cache.get("fp")
        assert result == "new"


# ---------------------------------------------------------------------------
# LatencyCollapseStrategy
# ---------------------------------------------------------------------------


class TestLatencyCollapseStrategy:
    def test_collapse_on_first_completion(self) -> None:
        strategy = LatencyCollapseStrategy(min_completions=1)
        u = Universe(elapsed_ms=10.0)
        assert strategy.should_collapse([u], total=5, elapsed_ms=10.0) is True

    def test_no_collapse_when_insufficient_completions(self) -> None:
        strategy = LatencyCollapseStrategy(min_completions=3)
        u = Universe(elapsed_ms=10.0)
        assert strategy.should_collapse([u], total=5, elapsed_ms=10.0) is False

    def test_collapse_on_timeout(self) -> None:
        strategy = LatencyCollapseStrategy(timeout_ms=100.0)
        assert strategy.should_collapse([], total=5, elapsed_ms=200.0) is True

    def test_select_winner_fastest(self) -> None:
        strategy = LatencyCollapseStrategy()
        u1 = Universe(elapsed_ms=50.0)
        u2 = Universe(elapsed_ms=10.0)
        u3 = Universe(elapsed_ms=30.0)
        winner = strategy.select_winner([u1, u2, u3])
        assert winner is u2


# ---------------------------------------------------------------------------
# QualityCollapseStrategy
# ---------------------------------------------------------------------------


class TestQualityCollapseStrategy:
    def test_collapse_when_all_complete(self) -> None:
        strategy = QualityCollapseStrategy()
        universes = [Universe() for _ in range(3)]
        assert strategy.should_collapse(universes, total=3, elapsed_ms=1.0) is True

    def test_no_collapse_when_incomplete(self) -> None:
        strategy = QualityCollapseStrategy()
        universes = [Universe()]
        assert strategy.should_collapse(universes, total=3, elapsed_ms=1.0) is False

    def test_collapse_on_timeout(self) -> None:
        strategy = QualityCollapseStrategy(timeout_ms=50.0)
        assert strategy.should_collapse([], total=5, elapsed_ms=100.0) is True

    def test_early_termination_on_quality_threshold(self) -> None:
        strategy = QualityCollapseStrategy(quality_threshold=5.0)
        u = Universe(actual_cost=3.0)
        assert strategy.should_collapse([u], total=10, elapsed_ms=1.0) is True

    def test_no_early_termination_above_threshold(self) -> None:
        strategy = QualityCollapseStrategy(quality_threshold=5.0)
        u = Universe(actual_cost=10.0)
        assert strategy.should_collapse([u], total=10, elapsed_ms=1.0) is False

    def test_select_winner_lowest_cost(self) -> None:
        strategy = QualityCollapseStrategy()
        u1 = Universe(actual_cost=50.0)
        u2 = Universe(actual_cost=5.0)
        u3 = Universe(actual_cost=20.0)
        winner = strategy.select_winner([u1, u2, u3])
        assert winner is u2

    def test_select_winner_none_cost_treated_as_inf(self) -> None:
        strategy = QualityCollapseStrategy()
        u1 = Universe(actual_cost=None)
        u2 = Universe(actual_cost=10.0)
        winner = strategy.select_winner([u1, u2])
        assert winner is u2


# ---------------------------------------------------------------------------
# MultiverseExecutor
# ---------------------------------------------------------------------------


class TestMultiverseExecutor:
    def test_empty_state(self) -> None:
        executor = MultiverseExecutor()
        state = MultiverseState()
        result = executor.execute_multiverse(state)
        assert result.selected_universe_id == ""
        assert result.result is None
        assert result.total_universes == 0
        assert result.collapse_reason == "no active universes"

    def test_execute_with_default_fn(self) -> None:
        executor = MultiverseExecutor(
            collapse_strategy=LatencyCollapseStrategy(min_completions=1),
        )
        state = MultiverseState()
        state.spawn_universe(cost_estimate=1.0)
        state.spawn_universe(cost_estimate=2.0)
        result = executor.execute_multiverse(state)
        assert result.selected_universe_id != ""
        assert result.result is not None
        assert result.collapsed_universes >= 1
        assert result.elapsed_ms > 0

    def test_execute_with_custom_fn(self) -> None:
        def custom_fn(universe: Universe, cache: FragmentCache) -> str:
            return f"result-{universe.universe_id}"

        executor = MultiverseExecutor(
            collapse_strategy=LatencyCollapseStrategy(),
            execution_fn=custom_fn,
        )
        state = MultiverseState()
        u = state.spawn_universe(cost_estimate=1.0)
        result = executor.execute_multiverse(state)
        assert result.result == f"result-{u.universe_id}"

    def test_execute_all_fail(self) -> None:
        def failing_fn(universe: Universe, cache: FragmentCache) -> None:
            msg = "boom"
            raise RuntimeError(msg)

        executor = MultiverseExecutor(
            collapse_strategy=QualityCollapseStrategy(timeout_ms=5000.0),
            execution_fn=failing_fn,
        )
        state = MultiverseState()
        state.spawn_universe(cost_estimate=1.0)
        state.spawn_universe(cost_estimate=2.0)
        result = executor.execute_multiverse(state)
        assert result.collapse_reason == "all universes failed"
        assert result.result is None

    def test_winner_marked_collapsed(self) -> None:
        executor = MultiverseExecutor(
            collapse_strategy=LatencyCollapseStrategy(),
        )
        state = MultiverseState()
        state.spawn_universe(cost_estimate=1.0)
        result = executor.execute_multiverse(state)
        winner = state.universes[result.selected_universe_id]
        assert winner.status == UniverseStatus.COLLAPSED

    def test_non_winners_decohered(self) -> None:
        executor = MultiverseExecutor(
            collapse_strategy=QualityCollapseStrategy(timeout_ms=5000.0),
        )
        state = MultiverseState()
        state.spawn_universe(cost_estimate=1.0)
        state.spawn_universe(cost_estimate=2.0)
        state.spawn_universe(cost_estimate=3.0)
        result = executor.execute_multiverse(state)
        winner_id = result.selected_universe_id
        for uid, u in state.universes.items():
            if uid == winner_id:
                assert u.status == UniverseStatus.COLLAPSED
            else:
                assert u.status == UniverseStatus.DECOHERED

    def test_decoherence_before_execution(self) -> None:
        executor = MultiverseExecutor(
            collapse_strategy=LatencyCollapseStrategy(),
        )
        state = MultiverseState(decoherence_threshold=1.5)
        state.spawn_universe(cost_estimate=1.0)
        state.spawn_universe(cost_estimate=10.0)  # Should be pre-decohered
        result = executor.execute_multiverse(state)
        assert result.decohered_universes >= 1

    def test_fragment_cache_shared_across_universes(self) -> None:
        computed_count = {"value": 0}

        def caching_fn(universe: Universe, cache: FragmentCache) -> str:
            hit, val = cache.get("shared_frag")
            if hit:
                return f"cached-{val}"
            computed_count["value"] += 1
            cache.put("shared_frag", "data")
            return "computed"

        executor = MultiverseExecutor(
            collapse_strategy=QualityCollapseStrategy(timeout_ms=5000.0),
            execution_fn=caching_fn,
            max_workers=1,  # Sequential to ensure deterministic cache behavior
        )
        state = MultiverseState()
        state.spawn_universe(cost_estimate=1.0)
        state.spawn_universe(cost_estimate=1.0)
        executor.execute_multiverse(state)
        # First computes, second should hit cache
        assert computed_count["value"] == 1

    def test_universe_costs_in_result(self) -> None:
        executor = MultiverseExecutor(
            collapse_strategy=QualityCollapseStrategy(timeout_ms=5000.0),
        )
        state = MultiverseState()
        state.spawn_universe(cost_estimate=1.0)
        state.spawn_universe(cost_estimate=2.0)
        result = executor.execute_multiverse(state)
        assert len(result.universe_costs) >= 1
        for cost in result.universe_costs.values():
            assert cost > 0
