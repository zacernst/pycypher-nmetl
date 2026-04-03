"""Machine-learning feedback loops for adaptive query optimization.

This module is the public API for the query learning subsystem.  It provides
a unified facade (:class:`QueryLearningStore`) and re-exports all components
from the two implementation modules:

* :mod:`pycypher.feedback_collector` — data collection (fingerprinting,
  selectivity tracking, join performance tracking, plan versioning)
* :mod:`pycypher.learning_algorithm` — decision-making (adaptive eviction,
  plan caching, semantic result caching, distributed sync)

All existing ``from pycypher.query_learning import X`` statements continue
to work unchanged.

Usage::

    store = QueryLearningStore()

    # Before execution: get learned plan adjustments
    fingerprint = store.fingerprint(query_ast)
    cached_plan = store.get_cached_plan(fingerprint)
    selectivity = store.get_learned_selectivity("Person", "age", ">")

    # After execution: record feedback
    store.record_selectivity("Person", "age", ">", estimated=0.33, actual=0.12)
    store.record_join_performance(
        strategy=JoinStrategy.HASH,
        left_rows=10000, right_rows=500,
        actual_rows=450, elapsed_ms=12.3,
    )
    store.cache_plan(fingerprint, plan)
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

# Re-export all public symbols from submodules so existing imports work.
from pycypher.feedback_collector import (
    JoinPerformanceTracker,
    PlanVersionTracker,
    PredicateSelectivityTracker,
    QueryFingerprint,
    QueryFingerprinter,
    _size_bucket,
)
from pycypher.learning_algorithm import (
    AdaptiveEvictionPolicy,
    AdaptivePlanCache,
    DistributedLearningSync,
    SemanticResultCache,
)

if TYPE_CHECKING:
    from pycypher.query_planner import AnalysisResult, JoinStrategy

__all__ = [
    "AdaptiveEvictionPolicy",
    "AdaptivePlanCache",
    "DistributedLearningSync",
    "JoinPerformanceTracker",
    "PlanVersionTracker",
    "PredicateSelectivityTracker",
    "QueryFingerprint",
    "QueryFingerprinter",
    "QueryLearningStore",
    "SemanticResultCache",
    "get_learning_store",
]


# ---------------------------------------------------------------------------
# QueryLearningStore — unified facade
# ---------------------------------------------------------------------------


class QueryLearningStore:
    """Unified facade coordinating all query learning components.

    Provides a single entry point for the query planner and executor to:
    - Fingerprint queries for plan reuse
    - Get/record learned predicate selectivities
    - Get/record join strategy performance
    - Cache and retrieve analysis results
    """

    def __init__(self) -> None:
        self.fingerprinter = QueryFingerprinter()
        self.selectivity_tracker = PredicateSelectivityTracker()
        self.join_tracker = JoinPerformanceTracker()
        self.plan_cache = AdaptivePlanCache()
        self.version_tracker = PlanVersionTracker()

    # -- Fingerprinting ------------------------------------------------------

    def fingerprint(self, query: Any) -> QueryFingerprint:
        """Compute a structural fingerprint for *query*."""
        return self.fingerprinter.fingerprint(query)

    # -- Plan caching --------------------------------------------------------

    def get_cached_plan(
        self,
        fingerprint: QueryFingerprint,
    ) -> AnalysisResult | None:
        """Look up a cached analysis for this fingerprint."""
        return self.plan_cache.get(fingerprint)

    def cache_plan(
        self,
        fingerprint: QueryFingerprint,
        analysis: AnalysisResult,
    ) -> None:
        """Cache an analysis result for future reuse."""
        self.plan_cache.put(fingerprint, analysis)

    # -- Selectivity learning ------------------------------------------------

    def record_selectivity(
        self,
        entity_type: str,
        prop: str,
        operator: str,
        *,
        estimated: float,
        actual: float,
    ) -> None:
        """Record observed predicate selectivity."""
        self.selectivity_tracker.record(
            entity_type,
            prop,
            operator,
            estimated=estimated,
            actual=actual,
        )

    def get_learned_selectivity(
        self,
        entity_type: str,
        prop: str,
        operator: str,
    ) -> float | None:
        """Get learned selectivity, or None if insufficient data."""
        return self.selectivity_tracker.get_learned_selectivity(
            entity_type,
            prop,
            operator,
        )

    # -- Join performance ----------------------------------------------------

    def record_join_performance(
        self,
        *,
        strategy: str,
        left_rows: int,
        right_rows: int,
        actual_output_rows: int,
        elapsed_ms: float,
    ) -> None:
        """Record join execution performance."""
        self.join_tracker.record(
            strategy=strategy,
            left_rows=left_rows,
            right_rows=right_rows,
            actual_output_rows=actual_output_rows,
            elapsed_ms=elapsed_ms,
        )

    def get_best_join_strategy(
        self,
        left_rows: int,
        right_rows: int,
    ) -> str | None:
        """Get historically best strategy for this input size pair."""
        return self.join_tracker.best_strategy(left_rows, right_rows)

    # -- Compound selectivity ------------------------------------------------

    def estimate_compound_selectivity(
        self,
        entity_type: str,
        predicates: list[tuple[str, str]],
        combinator: str = "AND",
    ) -> float | None:
        """Estimate compound selectivity for multiple predicates."""
        return self.selectivity_tracker.estimate_compound_selectivity(
            entity_type, predicates, combinator,
        )

    # -- Plan versioning -----------------------------------------------------

    def record_plan_version(
        self,
        fingerprint: QueryFingerprint,
        analysis: AnalysisResult,
    ) -> int:
        """Record a new plan version and return its version number."""
        return self.version_tracker.record_plan(fingerprint, analysis)

    def record_plan_execution(
        self,
        fingerprint: QueryFingerprint,
        version: int,
        *,
        elapsed_ms: float,
        rows_produced: int,
    ) -> None:
        """Record execution metrics for a plan version."""
        self.version_tracker.record_execution(
            fingerprint, version,
            elapsed_ms=elapsed_ms, rows_produced=rows_produced,
        )

    def get_plan_metrics(
        self,
        fingerprint: QueryFingerprint,
        version: int,
    ) -> dict[str, Any] | None:
        """Get execution metrics for a plan version."""
        return self.version_tracker.get_version_metrics(fingerprint, version)

    # -- Mutation invalidation -----------------------------------------------

    def invalidate_on_mutation(self) -> None:
        """Invalidate plan cache after data mutations."""
        self.plan_cache.invalidate()

    # -- Diagnostics ---------------------------------------------------------

    def diagnostics(self) -> dict[str, Any]:
        """Return a diagnostic snapshot of all learning components."""
        return {
            "plan_cache": self.plan_cache.stats,
            "selectivity_patterns": len(
                self.selectivity_tracker.tracked_patterns,
            ),
            "join_buckets_tracked": len(
                self.join_tracker._history,  # noqa: SLF001
            ),
            "plan_versions": len(self.version_tracker._versions),  # noqa: SLF001
            "correlation_pairs": len(
                self.selectivity_tracker._correlation_history,  # noqa: SLF001
            ),
        }

    def clear(self) -> None:
        """Reset all learning state."""
        self.selectivity_tracker.clear()
        self.join_tracker.clear()
        self.plan_cache.clear()
        self.version_tracker.clear()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_GLOBAL_STORE: QueryLearningStore | None = None
_STORE_LOCK = threading.Lock()


def get_learning_store() -> QueryLearningStore:
    """Return the module-level singleton ``QueryLearningStore``."""
    global _GLOBAL_STORE
    if _GLOBAL_STORE is None:
        with _STORE_LOCK:
            if _GLOBAL_STORE is None:
                _GLOBAL_STORE = QueryLearningStore()
    return _GLOBAL_STORE
