"""Parallel speculative execution across universes with adaptive collapse.

This module implements the "execution and measurement" phase of the
multiverse framework. All active universes in the superposition are
executed in parallel (via thread-pool concurrency), and collapse
strategies determine when and how to select the winning result.

Architecture
------------

::

    MultiverseExecutor
    ├── execute_multiverse()     — run all universes in parallel
    │   ├── _execute_universe()  — run a single universe's plan
    │   ├── _check_coherence()   — reuse cached fragment results
    │   └── _apply_collapse()    — select the winning universe
    ├── collapse_strategies      — pluggable collapse policies
    └── fragment_cache           — memoized shared sub-computations

Collapse Strategies
-------------------

The "quantum measurement" that reduces the multiverse to a single
outcome is implemented as a pluggable :class:`CollapseStrategy`:

- :class:`LatencyCollapseStrategy` — first universe to finish wins
  (optimistic: good when plans have high variance in runtime).
- :class:`QualityCollapseStrategy` — wait for all universes, pick
  the one with lowest actual cost (conservative: good when you need
  the best possible result).

Custom strategies can implement early termination based on quality
thresholds, resource budgets, or other heuristics.

.. versionadded:: 0.0.30
"""

from __future__ import annotations

import abc
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from fastopendata.multiverse.core import (
    CollapseResult,
    MultiverseState,
    Universe,
    UniverseStatus,
)

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Collapse strategies: the "quantum measurement" decision
# ---------------------------------------------------------------------------


class CollapseStrategy(abc.ABC):
    """Base class for multiverse collapse strategies.

    A collapse strategy decides when to stop waiting for additional
    universes and which result to select. This is analogous to
    choosing a quantum measurement basis — different strategies
    project the superposition onto different outcome spaces.
    """

    @abc.abstractmethod
    def should_collapse(
        self,
        completed: list[Universe],
        total: int,
        elapsed_ms: float,
    ) -> bool:
        """Return True if the multiverse should collapse now.

        Parameters
        ----------
        completed:
            Universes that have finished executing.
        total:
            Total number of universes in the superposition.
        elapsed_ms:
            Wall-clock time since execution started.

        """

    @abc.abstractmethod
    def select_winner(self, completed: list[Universe]) -> Universe:
        """Select the winning universe from completed candidates.

        Parameters
        ----------
        completed:
            Universes that have finished executing.

        Returns
        -------
        Universe
            The selected winner.

        """


class LatencyCollapseStrategy(CollapseStrategy):
    """Collapse as soon as the first universe completes.

    Optimal for interactive queries where response time matters more
    than finding the absolute cheapest plan. Analogous to a "weak
    measurement" in quantum mechanics — you observe the first branch
    that decoheres into a definite state.

    Parameters
    ----------
    min_completions : int
        Minimum number of universes that must complete before
        collapse is allowed (default: 1).
    timeout_ms : float
        Maximum wall-clock time before forced collapse (default: 30s).

    """

    def __init__(
        self,
        *,
        min_completions: int = 1,
        timeout_ms: float = 30_000.0,
    ) -> None:
        self.min_completions = min_completions
        self.timeout_ms = timeout_ms

    def should_collapse(
        self,
        completed: list[Universe],
        total: int,
        elapsed_ms: float,
    ) -> bool:
        """Collapse after min_completions or timeout."""
        if elapsed_ms >= self.timeout_ms:
            return True
        return len(completed) >= self.min_completions

    def select_winner(self, completed: list[Universe]) -> Universe:
        """Select the fastest-completing universe."""
        return min(completed, key=lambda u: u.elapsed_ms)


class QualityCollapseStrategy(CollapseStrategy):
    """Wait for all universes, then select the lowest-cost result.

    Optimal for batch workloads where finding the best plan matters
    more than latency. Analogous to a "strong measurement" — you
    wait until all branches have fully decohered before observing.

    Parameters
    ----------
    quality_threshold : float
        If any universe achieves a cost below this threshold,
        collapse immediately (early termination). Default: 0
        (no early termination).
    timeout_ms : float
        Maximum wall-clock time before forced collapse (default: 60s).

    """

    def __init__(
        self,
        *,
        quality_threshold: float = 0.0,
        timeout_ms: float = 60_000.0,
    ) -> None:
        self.quality_threshold = quality_threshold
        self.timeout_ms = timeout_ms

    def should_collapse(
        self,
        completed: list[Universe],
        total: int,
        elapsed_ms: float,
    ) -> bool:
        """Collapse when all complete, quality threshold met, or timeout."""
        if elapsed_ms >= self.timeout_ms:
            return True
        if len(completed) >= total:
            return True
        # Early termination if quality threshold is met
        if self.quality_threshold > 0 and completed:
            best = min(u.actual_cost or float("inf") for u in completed)
            if best <= self.quality_threshold:
                return True
        return False

    def select_winner(self, completed: list[Universe]) -> Universe:
        """Select the universe with lowest actual cost."""
        return min(
            completed,
            key=lambda u: u.actual_cost if u.actual_cost is not None else float("inf"),
        )


# ---------------------------------------------------------------------------
# Fragment cache: memoized shared sub-computations (quantum entanglement)
# ---------------------------------------------------------------------------


@dataclass
class FragmentCache:
    """Thread-safe cache for memoized plan fragment results.

    When multiple universes share a sub-computation (e.g., the same
    entity scan), the first universe to compute it stores the result
    here. Subsequent universes read from the cache instead of
    re-executing — this is the computational realization of quantum
    entanglement (correlated outcomes across branches).

    Attributes
    ----------
    _store : dict[str, Any]
        Fingerprint -> cached result.
    _lock : threading.Lock
        Ensures thread-safe access.
    _hit_count : int
        Number of cache hits (shared computation reuses).
    _miss_count : int
        Number of cache misses (unique computations).

    """

    _store: dict[str, Any] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _hit_count: int = 0
    _miss_count: int = 0

    def get(self, fingerprint: str) -> tuple[bool, Any]:
        """Look up a cached fragment result.

        Parameters
        ----------
        fingerprint:
            Fragment fingerprint.

        Returns
        -------
        tuple[bool, Any]
            ``(True, result)`` on hit, ``(False, None)`` on miss.

        """
        with self._lock:
            if fingerprint in self._store:
                self._hit_count += 1
                return True, self._store[fingerprint]
            self._miss_count += 1
            return False, None

    def put(self, fingerprint: str, result: Any) -> None:
        """Store a fragment result in the cache.

        Parameters
        ----------
        fingerprint:
            Fragment fingerprint.
        result:
            The computed result to cache.

        """
        with self._lock:
            self._store[fingerprint] = result

    @property
    def hit_rate(self) -> float:
        """Cache hit rate as a fraction in [0, 1]."""
        total = self._hit_count + self._miss_count
        return self._hit_count / total if total > 0 else 0.0

    @property
    def savings_pct(self) -> float:
        """Percentage of computations saved via cache hits."""
        return self.hit_rate * 100.0


# ---------------------------------------------------------------------------
# MultiverseExecutor: parallel speculative execution engine
# ---------------------------------------------------------------------------


class MultiverseExecutor:
    """Execute query plans across parallel universes and collapse to the best.

    This is the central orchestrator of the multiverse framework. It:

    1. Takes a populated :class:`MultiverseState` (containing universes
       with their execution plans).
    2. Executes all active universes in parallel using a thread pool.
    3. Uses the :class:`FragmentCache` to share sub-computations
       across universes (coherence / entanglement).
    4. Applies a :class:`CollapseStrategy` to select the winning result.

    Parameters
    ----------
    collapse_strategy : CollapseStrategy
        Policy for when/how to collapse the multiverse.
    max_workers : int
        Maximum parallel execution threads (default: 8).
    execution_fn : Callable[[Universe, FragmentCache], Any] | None
        Function that executes a universe's plan and returns the
        result. If ``None``, a stub that returns the universe's
        metadata is used (for testing/development).

    """

    def __init__(
        self,
        *,
        collapse_strategy: CollapseStrategy | None = None,
        max_workers: int = 8,
        execution_fn: Callable[[Universe, FragmentCache], Any] | None = None,
    ) -> None:
        self.collapse_strategy = collapse_strategy or LatencyCollapseStrategy()
        self.max_workers = max_workers
        self.execution_fn = execution_fn or self._default_execution_fn
        self.fragment_cache = FragmentCache()

    def execute_multiverse(
        self,
        state: MultiverseState,
    ) -> CollapseResult:
        """Execute all active universes and collapse to the best result.

        Parameters
        ----------
        state:
            Populated multiverse state with active universes.

        Returns
        -------
        CollapseResult
            The collapsed outcome with the selected universe's result
            and multiverse diagnostics.

        """
        active = state.active_universes
        if not active:
            return CollapseResult(
                selected_universe_id="",
                result=None,
                total_universes=0,
                collapsed_universes=0,
                decohered_universes=0,
                coherence_savings_pct=0.0,
                collapse_reason="no active universes",
                elapsed_ms=0.0,
            )

        # Apply decoherence before execution (prune obviously bad universes)
        decohered_ids = state.apply_decoherence()
        active = state.active_universes

        _logger.info(
            "Executing multiverse: %d active universes, %d pre-decohered",
            len(active),
            len(decohered_ids),
        )

        for universe in active:
            universe.status = UniverseStatus.EXECUTING

        start_time = time.monotonic()
        completed = self._run_parallel(active, start_time)
        total_elapsed = (time.monotonic() - start_time) * 1000

        if not completed:
            return CollapseResult(
                selected_universe_id="",
                result=None,
                total_universes=len(state.universes),
                collapsed_universes=0,
                decohered_universes=len(decohered_ids) + len(active),
                coherence_savings_pct=state.coherence_savings(),
                collapse_reason="all universes failed",
                elapsed_ms=total_elapsed,
            )

        return self._build_collapse_result(state, completed, total_elapsed)

    def _run_parallel(
        self,
        active: list[Universe],
        start_time: float,
    ) -> list[Universe]:
        """Submit universes to a thread pool and collect results."""
        completed: list[Universe] = []
        lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_to_universe: dict[Future[Any], Universe] = {
                pool.submit(self._execute_universe, u): u for u in active
            }

            for future in as_completed(future_to_universe):
                universe = future_to_universe[future]
                elapsed_ms = (time.monotonic() - start_time) * 1000

                self._collect_result(future, universe, elapsed_ms, completed, lock)

                with lock:
                    if self.collapse_strategy.should_collapse(
                        completed,
                        len(active),
                        elapsed_ms,
                    ):
                        for f in future_to_universe:
                            if not f.done():
                                f.cancel()
                        break

        return completed

    @staticmethod
    def _collect_result(
        future: Future[Any],
        universe: Universe,
        elapsed_ms: float,
        completed: list[Universe],
        lock: threading.Lock,
    ) -> None:
        """Record a future's outcome on its universe."""
        try:
            result = future.result()
            universe.result = result
            universe.elapsed_ms = elapsed_ms
            universe.actual_cost = elapsed_ms
            with lock:
                completed.append(universe)
            _logger.debug(
                "Universe %s completed in %.1fms",
                universe.universe_id,
                elapsed_ms,
            )
        except Exception:
            universe.decohere(f"execution failed: {future.exception()}")
            _logger.warning(
                "Universe %s failed: %s",
                universe.universe_id,
                future.exception(),
            )

    def _build_collapse_result(
        self,
        state: MultiverseState,
        completed: list[Universe],
        total_elapsed: float,
    ) -> CollapseResult:
        """Select the winner and build the final CollapseResult."""
        winner = self.collapse_strategy.select_winner(completed)
        winner.status = UniverseStatus.COLLAPSED

        for universe in completed:
            if universe.universe_id != winner.universe_id:
                universe.decohere("not selected during collapse")

        universe_costs = {
            u.universe_id: u.actual_cost for u in completed if u.actual_cost is not None
        }
        decohered_count = sum(
            1 for u in state.universes.values() if u.status == UniverseStatus.DECOHERED
        )

        result = CollapseResult(
            selected_universe_id=winner.universe_id,
            result=winner.result,
            total_universes=len(state.universes),
            collapsed_universes=len(completed),
            decohered_universes=decohered_count,
            coherence_savings_pct=self.fragment_cache.savings_pct,
            collapse_reason=(
                f"selected by {type(self.collapse_strategy).__name__}: "
                f"cost={winner.actual_cost:.1f}ms"
            ),
            elapsed_ms=total_elapsed,
            universe_costs=universe_costs,
        )

        _logger.info(
            "Multiverse collapsed: winner=%s cost=%.1fms "
            "(%d/%d completed, %.1f%% coherence savings, %.1fms total)",
            winner.universe_id,
            winner.actual_cost or 0,
            len(completed),
            len(state.universes),
            result.coherence_savings_pct,
            total_elapsed,
        )

        return result

    def _execute_universe(self, universe: Universe) -> Any:
        """Execute a single universe's plan with fragment caching.

        Parameters
        ----------
        universe:
            The universe to execute.

        Returns
        -------
        Any
            The execution result.

        """
        return self.execution_fn(universe, self.fragment_cache)

    @staticmethod
    def _default_execution_fn(
        universe: Universe,
        cache: FragmentCache,
    ) -> dict[str, Any]:
        """Default stub execution function for testing.

        Simulates execution by returning universe metadata with
        fragment cache interaction.

        Parameters
        ----------
        universe:
            The universe being executed.
        cache:
            Fragment cache for shared computation.

        Returns
        -------
        dict[str, Any]
            Simulated execution result.

        """
        results: dict[str, Any] = {
            "universe_id": universe.universe_id,
            "plan_fingerprint": universe.plan_fingerprint,
            "fragments_computed": [],
            "fragments_cached": [],
        }

        for fp in universe.coherent_fragments:
            hit, _cached_result = cache.get(fp)
            if hit:
                results["fragments_cached"].append(fp)
            else:
                # Simulate computation
                computed = f"result_{fp}"
                cache.put(fp, computed)
                results["fragments_computed"].append(fp)

        return results
