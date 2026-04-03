"""Learning algorithm components for adaptive query optimization.

Uses feedback collected by :mod:`pycypher.feedback_collector` to make
cache eviction, plan caching, semantic result sharing, and distributed
sync decisions.

Classes
-------
AdaptiveEvictionPolicy
    Frequency-recency scoring for ML-driven cache replacement.
AdaptivePlanCache
    Caches query analysis results by structural fingerprint.
SemanticResultCache
    Shares results between structurally similar queries.
DistributedLearningSync
    Serializes/deserializes learning state for multi-instance sharing.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

from pycypher.feedback_collector import QueryFingerprint

if TYPE_CHECKING:
    from pycypher.query_planner import AnalysisResult

__all__ = [
    "AdaptiveEvictionPolicy",
    "AdaptivePlanCache",
    "DistributedLearningSync",
    "SemanticResultCache",
]


# ---------------------------------------------------------------------------
# Constants (plan cache specific)
# ---------------------------------------------------------------------------

#: Maximum cached plans
_MAX_PLAN_CACHE: int = 256

#: Plan cache TTL in seconds (0 = no expiry)
_PLAN_CACHE_TTL_S: float = 300.0


# ---------------------------------------------------------------------------
# Adaptive Eviction Policy — ML-driven cache replacement
# ---------------------------------------------------------------------------


class AdaptiveEvictionPolicy:
    """Frequency-recency adaptive cache eviction policy.

    Combines access frequency and recency into a single eviction score.
    Higher score = more valuable = less likely to be evicted.

    Score = frequency_weight * log(access_count + 1) + recency_weight * recency_score

    Where recency_score decays exponentially with time since last access.
    """

    def __init__(
        self,
        *,
        frequency_weight: float = 0.6,
        recency_weight: float = 0.4,
        decay_half_life_s: float = 60.0,
    ) -> None:
        self._lock = threading.Lock()
        self._access_counts: dict[str, int] = {}
        self._last_access: dict[str, float] = {}
        self._frequency_weight = frequency_weight
        self._recency_weight = recency_weight
        self._decay_half_life = decay_half_life_s

    def record_access(self, key: str) -> None:
        """Record an access for the given cache key."""
        now = time.monotonic()
        with self._lock:
            self._access_counts[key] = self._access_counts.get(key, 0) + 1
            self._last_access[key] = now

    def eviction_score(self, key: str) -> float:
        """Compute eviction score for a key. Higher = more valuable."""
        now = time.monotonic()
        with self._lock:
            count = self._access_counts.get(key, 0)
            last = self._last_access.get(key, 0.0)

        if count == 0:
            return 0.0

        freq_score = math.log(count + 1)
        elapsed = now - last
        recency_score = math.exp(
            -0.693 * elapsed / self._decay_half_life,  # ln(2) ~ 0.693
        )

        return (
            self._frequency_weight * freq_score
            + self._recency_weight * recency_score
        )

    def select_eviction_candidate(self, keys: list[str]) -> str | None:
        """Select the best candidate for eviction (lowest score)."""
        if not keys:
            return None
        return min(keys, key=self.eviction_score)

    def remove(self, key: str) -> None:
        """Remove tracking data for an evicted key."""
        with self._lock:
            self._access_counts.pop(key, None)
            self._last_access.pop(key, None)

    def clear(self) -> None:
        """Reset all tracking data."""
        with self._lock:
            self._access_counts.clear()
            self._last_access.clear()


# ---------------------------------------------------------------------------
# Adaptive Plan Cache — reuse plans for structurally similar queries
# ---------------------------------------------------------------------------


@dataclass
class _CachedPlan:
    """A cached analysis result with metadata."""

    fingerprint: QueryFingerprint
    analysis: AnalysisResult
    created_at: float = field(default_factory=time.monotonic)
    hit_count: int = 0
    last_hit: float = field(default_factory=time.monotonic)


class AdaptivePlanCache:
    """Caches query analysis results keyed by structural fingerprint.

    Structurally similar queries (same clause structure, entity types,
    predicate shapes) reuse cached plans, avoiding repeated analysis.
    """

    def __init__(
        self,
        *,
        max_entries: int = _MAX_PLAN_CACHE,
        ttl_seconds: float = _PLAN_CACHE_TTL_S,
        eviction_policy: str = "lru",
    ) -> None:
        self._lock = threading.Lock()
        self._cache: dict[str, _CachedPlan] = {}
        self._max_entries = max_entries
        self._ttl = ttl_seconds
        self._total_hits = 0
        self._total_misses = 0
        self._eviction_policy_name = eviction_policy
        self._adaptive_eviction: AdaptiveEvictionPolicy | None = (
            AdaptiveEvictionPolicy() if eviction_policy == "adaptive" else None
        )

    def get(self, fingerprint: QueryFingerprint) -> AnalysisResult | None:
        """Look up a cached plan by fingerprint. Returns None on miss."""
        with self._lock:
            entry = self._cache.get(fingerprint.digest)
            if entry is None:
                self._total_misses += 1
                return None

            # Check TTL
            if self._ttl > 0:
                age = time.monotonic() - entry.created_at
                if age > self._ttl:
                    del self._cache[fingerprint.digest]
                    if self._adaptive_eviction:
                        self._adaptive_eviction.remove(fingerprint.digest)
                    self._total_misses += 1
                    return None

            entry.hit_count += 1
            entry.last_hit = time.monotonic()
            self._total_hits += 1

            if self._adaptive_eviction:
                self._adaptive_eviction.record_access(fingerprint.digest)

            return entry.analysis

    def put(
        self,
        fingerprint: QueryFingerprint,
        analysis: AnalysisResult,
    ) -> None:
        """Cache an analysis result for a fingerprint."""
        with self._lock:
            # Evict if at capacity
            if (
                len(self._cache) >= self._max_entries
                and fingerprint.digest not in self._cache
            ):
                if self._adaptive_eviction:
                    self._evict_adaptive()
                else:
                    self._evict_lru()

            self._cache[fingerprint.digest] = _CachedPlan(
                fingerprint=fingerprint,
                analysis=analysis,
            )

            if self._adaptive_eviction:
                self._adaptive_eviction.record_access(fingerprint.digest)

    def _evict_lru(self) -> None:
        """Evict the least-recently-used entry. Caller holds lock."""
        if not self._cache:
            return
        lru_key = min(self._cache, key=lambda k: self._cache[k].last_hit)
        del self._cache[lru_key]

    def _evict_adaptive(self) -> None:
        """Evict using ML-driven frequency-recency scoring. Caller holds lock."""
        if not self._cache or not self._adaptive_eviction:
            return
        candidate = self._adaptive_eviction.select_eviction_candidate(
            list(self._cache.keys()),
        )
        if candidate and candidate in self._cache:
            del self._cache[candidate]
            self._adaptive_eviction.remove(candidate)

    def invalidate(self) -> None:
        """Clear the entire plan cache (e.g. after mutations)."""
        with self._lock:
            self._cache.clear()

    @property
    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        with self._lock:
            total = self._total_hits + self._total_misses
            return {
                "entries": len(self._cache),
                "max_entries": self._max_entries,
                "hits": self._total_hits,
                "misses": self._total_misses,
                "hit_rate": (
                    self._total_hits / total if total > 0 else 0.0
                ),
            }

    def clear(self) -> None:
        """Drop all cached plans and reset stats."""
        with self._lock:
            self._cache.clear()
            self._total_hits = 0
            self._total_misses = 0


# ---------------------------------------------------------------------------
# Semantic Result Cache — similar query result sharing
# ---------------------------------------------------------------------------


@dataclass
class _SemanticCacheEntry:
    """A cached result with semantic metadata."""

    query: str
    parameters: dict[str, Any] | None
    fingerprint_digest: str
    result: Any
    is_parameter_independent: bool
    created_at: float = field(default_factory=time.monotonic)
    last_access: float = field(default_factory=time.monotonic)
    hit_count: int = 0


class SemanticResultCache:
    """Cache that shares results between structurally similar queries.

    For parameter-independent queries (aggregations, full scans), results
    are shared across all queries with the same structural fingerprint.
    For parameter-dependent queries, only exact query+params matches are returned.

    This provides higher hit rates for workloads with repeated query patterns
    that differ only in literal values.
    """

    def __init__(self, *, max_entries: int = 256) -> None:
        self._lock = threading.Lock()
        self._max_entries = max_entries
        # Exact match: (query, params_key) -> entry
        self._exact: dict[str, _SemanticCacheEntry] = {}
        # Semantic match: fingerprint_digest -> entry (parameter-independent only)
        self._semantic: dict[str, _SemanticCacheEntry] = {}
        self._exact_hits = 0
        self._semantic_hits = 0
        self._misses = 0

    @staticmethod
    def _params_key(parameters: dict[str, Any] | None) -> str:
        """Stable key for parameters."""
        if not parameters:
            return ""
        return str(sorted(parameters.items()))

    def _exact_key(self, query: str, parameters: dict[str, Any] | None) -> str:
        return f"{query}||{self._params_key(parameters)}"

    def put(
        self,
        query: str,
        parameters: dict[str, Any] | None,
        fingerprint_digest: str,
        result: Any,
        *,
        is_parameter_independent: bool = False,
    ) -> None:
        """Cache a query result."""
        entry = _SemanticCacheEntry(
            query=query,
            parameters=parameters,
            fingerprint_digest=fingerprint_digest,
            result=result,
            is_parameter_independent=is_parameter_independent,
        )

        with self._lock:
            exact_key = self._exact_key(query, parameters)

            # Evict if at capacity (count unique entries via _exact only)
            if (
                len(self._exact) >= self._max_entries
                and exact_key not in self._exact
            ):
                self._evict_lru()

            self._exact[exact_key] = entry

            if is_parameter_independent:
                self._semantic[fingerprint_digest] = entry

    def get(
        self,
        query: str,
        parameters: dict[str, Any] | None,
        fingerprint_digest: str,
    ) -> Any | None:
        """Look up a cached result. Tries exact match first, then semantic."""
        with self._lock:
            # 1. Try exact match
            exact_key = self._exact_key(query, parameters)
            entry = self._exact.get(exact_key)
            if entry is not None:
                entry.hit_count += 1
                entry.last_access = time.monotonic()
                self._exact_hits += 1
                return entry.result

            # 2. Try semantic match (fingerprint-based, parameter-independent only)
            sem_entry = self._semantic.get(fingerprint_digest)
            if sem_entry is not None and sem_entry.is_parameter_independent:
                sem_entry.hit_count += 1
                sem_entry.last_access = time.monotonic()
                self._semantic_hits += 1
                return sem_entry.result

            self._misses += 1
            return None

    def _evict_lru(self) -> None:
        """Evict least-recently-used entry. Caller holds lock."""
        if self._exact:
            lru_key = min(
                self._exact, key=lambda k: self._exact[k].last_access,
            )
            entry = self._exact.pop(lru_key)
            # Also remove from semantic index if present
            if (
                entry.is_parameter_independent
                and entry.fingerprint_digest in self._semantic
            ):
                self._semantic.pop(entry.fingerprint_digest, None)

    def invalidate(self) -> None:
        """Clear all cached results."""
        with self._lock:
            self._exact.clear()
            self._semantic.clear()

    @property
    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        with self._lock:
            total = self._exact_hits + self._semantic_hits + self._misses
            return {
                "entries": len(self._exact),
                "semantic_entries": len(self._semantic),
                "exact_hits": self._exact_hits,
                "semantic_hits": self._semantic_hits,
                "misses": self._misses,
                "hit_rate": (
                    (self._exact_hits + self._semantic_hits) / total
                    if total > 0
                    else 0.0
                ),
            }


# ---------------------------------------------------------------------------
# Distributed Learning Sync — multi-instance knowledge sharing
# ---------------------------------------------------------------------------


class DistributedLearningSync:
    """Serializes and deserializes learning state for multi-instance sharing.

    Allows multiple pycypher instances to share learned query optimization
    knowledge (selectivity patterns, join performance) without requiring
    shared memory or a database.

    State is versioned to support conflict resolution during merges.
    """

    def __init__(self, store: Any) -> None:
        """Initialize with a QueryLearningStore instance.

        Args:
            store: A QueryLearningStore (typed as Any to avoid circular import).
        """
        self._store = store
        self._version = 0
        self._lock = threading.Lock()

    def export_state(self) -> dict[str, Any]:
        """Export current learning state as a serializable dict."""
        with self._lock:
            self._version += 1

            # Export selectivity data
            selectivity_data: list[dict[str, Any]] = []
            tracker = self._store.selectivity_tracker
            with tracker._lock:  # noqa: SLF001
                for key, ema_val in tracker._ema.items():  # noqa: SLF001
                    count = tracker._observation_counts.get(key, 0)  # noqa: SLF001
                    selectivity_data.append({
                        "entity_type": key[0],
                        "property": key[1],
                        "operator": key[2],
                        "ema": ema_val,
                        "count": count,
                    })

            # Export join performance data
            join_data: list[dict[str, Any]] = []
            jt = self._store.join_tracker
            with jt._lock:  # noqa: SLF001
                for bucket_key, history in jt._history.items():  # noqa: SLF001
                    for obs in history:
                        join_data.append({
                            "left_bucket": bucket_key[0],
                            "right_bucket": bucket_key[1],
                            "strategy": obs.strategy,
                            "left_rows": obs.left_rows,
                            "right_rows": obs.right_rows,
                            "actual_output_rows": obs.actual_output_rows,
                            "elapsed_ms": obs.elapsed_ms,
                        })

            return {
                "version": self._version,
                "selectivity": selectivity_data,
                "join_performance": join_data,
            }

    def import_state(self, state: dict[str, Any]) -> bool:
        """Import learning state from another instance.

        Returns True if import was applied, False if state was stale.
        """
        with self._lock:
            incoming_version = state.get("version", 0)
            if incoming_version <= self._version - 1 and self._version > 1:
                # Stale state — reject
                return False

            # Import selectivity data
            for entry in state.get("selectivity", []):
                entity_type = entry["entity_type"]
                prop = entry["property"]
                operator = entry["operator"]
                ema = entry["ema"]
                count = entry["count"]

                # Only import if we have less data for this pattern
                existing = self._store.get_learned_selectivity(
                    entity_type, prop, operator,
                )
                if existing is None:
                    # Replay enough observations to establish the EMA
                    for _ in range(max(count, 3)):
                        self._store.record_selectivity(
                            entity_type, prop, operator,
                            estimated=ema, actual=ema,
                        )

            # Import join performance data
            for entry in state.get("join_performance", []):
                self._store.record_join_performance(
                    strategy=entry["strategy"],
                    left_rows=entry["left_rows"],
                    right_rows=entry["right_rows"],
                    actual_output_rows=entry["actual_output_rows"],
                    elapsed_ms=entry["elapsed_ms"],
                )

            return True
