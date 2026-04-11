"""Feedback collection components for adaptive query optimization.

Collects execution feedback (selectivity observations, join performance,
plan versioning) that drives the learning algorithms in
:mod:`pycypher.learning_algorithm`.

Classes
-------
QueryFingerprint
    Structural fingerprint dataclass for query similarity detection.
QueryFingerprinter
    Produces structural fingerprints by stripping literal values.
PredicateSelectivityTracker
    EMA-based selectivity learning per (entity, property, operator).
JoinPerformanceTracker
    Records join strategy performance for adaptive strategy selection.
PlanVersionTracker
    Tracks plan evolution and detects regressions.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.ast_models import ASTNode, Query
    from pycypher.query_planner import AnalysisResult

__all__ = [
    "JoinPerformanceTracker",
    "PlanVersionTracker",
    "PredicateSelectivityTracker",
    "QueryFingerprint",
    "QueryFingerprinter",
    "_size_bucket",
]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: EMA smoothing factor for online learning (higher = more weight on recent)
_EMA_ALPHA: float = 0.3

#: Maximum observations per predicate pattern
_MAX_SELECTIVITY_HISTORY: int = 64

#: Maximum observations per join strategy key
_MAX_JOIN_HISTORY: int = 64

#: Minimum observations before learning overrides heuristics
_MIN_OBSERVATIONS: int = 3

#: Confidence threshold — EMA correction only applied when confidence > this
_CONFIDENCE_THRESHOLD: float = 0.5


# ---------------------------------------------------------------------------
# Query Fingerprinting — structural similarity detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueryFingerprint:
    """Structural fingerprint of a Cypher query for similarity detection.

    Two queries with the same fingerprint have identical clause structure,
    entity/relationship types, and predicate shapes — differing only in
    literal values.

    Attributes:
        digest: SHA-256 hex digest of the structural representation.
        clause_signature: Human-readable clause type sequence.
        entity_types: Sorted entity types referenced.
        relationship_types: Sorted relationship types referenced.

    """

    digest: str
    clause_signature: str
    entity_types: tuple[str, ...]
    relationship_types: tuple[str, ...]


class QueryFingerprinter:
    """Produces structural fingerprints for query similarity detection.

    Strips literal values and extracts the query's structural skeleton
    so that ``MATCH (p:Person) WHERE p.age > 30`` and
    ``MATCH (p:Person) WHERE p.age > 50`` produce the same fingerprint.
    """

    def fingerprint(self, query: Query) -> QueryFingerprint:
        """Compute a structural fingerprint for *query*."""
        from pycypher.ast_models import (
            Match,
            NodePattern,
            RelationshipPattern,
            Return,
            With,
        )

        clause_types: list[str] = []
        entity_types: list[str] = []
        rel_types: list[str] = []
        predicate_shapes: list[str] = []

        for clause in query.clauses:
            clause_types.append(type(clause).__name__)

            if isinstance(clause, Match) and clause.pattern is not None:
                for path in clause.pattern.paths:
                    for element in path.elements:
                        if isinstance(element, NodePattern) and element.labels:
                            entity_types.extend(element.labels)
                        elif (
                            isinstance(element, RelationshipPattern)
                            and element.labels
                        ):
                            rel_types.extend(element.labels)

                if clause.where is not None:
                    predicate_shapes.append(
                        self._predicate_shape(clause.where),
                    )

            elif isinstance(clause, (Return, With)):
                has_distinct = getattr(clause, "distinct", False)
                has_limit = getattr(clause, "limit", None) is not None
                has_skip = getattr(clause, "skip", None) is not None
                has_order = getattr(clause, "order_by", None) is not None
                modifiers = []
                if has_distinct:
                    modifiers.append("DISTINCT")
                if has_order:
                    modifiers.append("ORDER")
                if has_skip:
                    modifiers.append("SKIP")
                if has_limit:
                    modifiers.append("LIMIT")
                if modifiers:
                    clause_types[-1] += f"[{','.join(modifiers)}]"

        clause_sig = " -> ".join(clause_types)
        entity_types_sorted = tuple(sorted(set(entity_types)))
        rel_types_sorted = tuple(sorted(set(rel_types)))

        # Build structural string for hashing
        parts = [
            clause_sig,
            "|".join(entity_types_sorted),
            "|".join(rel_types_sorted),
            "|".join(predicate_shapes),
        ]
        structural_str = "||".join(parts)
        digest = hashlib.sha256(structural_str.encode()).hexdigest()[:16]

        return QueryFingerprint(
            digest=digest,
            clause_signature=clause_sig,
            entity_types=entity_types_sorted,
            relationship_types=rel_types_sorted,
        )

    def _predicate_shape(self, expr: ASTNode) -> str:
        """Extract the structural shape of a predicate, stripping literals."""
        from pycypher.ast_models import (
            And,
            Comparison,
            Not,
            Or,
        )

        if isinstance(expr, And):
            parts = []
            for op in (
                getattr(expr, "left", None),
                getattr(expr, "right", None),
            ):
                if op is not None:
                    parts.append(self._predicate_shape(op))
            for op in getattr(expr, "operands", []):
                parts.append(self._predicate_shape(op))
            return f"AND({','.join(sorted(parts))})"

        if isinstance(expr, Or):
            parts = []
            for op in (
                getattr(expr, "left", None),
                getattr(expr, "right", None),
            ):
                if op is not None:
                    parts.append(self._predicate_shape(op))
            for op in getattr(expr, "operands", []):
                parts.append(self._predicate_shape(op))
            return f"OR({','.join(sorted(parts))})"

        if isinstance(expr, Not):
            child = getattr(expr, "operand", None)
            if child is not None:
                return f"NOT({self._predicate_shape(child)})"
            return "NOT(?)"

        if isinstance(expr, Comparison):
            op = getattr(expr, "operator", "?")
            left = getattr(expr, "left", None)
            right = getattr(expr, "right", None)
            left_shape = self._expr_shape(left) if left else "?"
            right_shape = self._expr_shape(right) if right else "?"
            return f"CMP({left_shape},{op},{right_shape})"

        return type(expr).__name__

    def _expr_shape(self, expr: Any) -> str:
        """Shape of a single expression node (property lookup -> type, literal -> $)."""
        from pycypher.ast_models import (
            FloatLiteral,
            IntegerLiteral,
            PropertyLookup,
            StringLiteral,
            Variable,
        )

        if isinstance(expr, PropertyLookup):
            var = getattr(expr, "expression", None)
            prop = getattr(expr, "property", "?")
            if isinstance(var, Variable):
                return f"{var.name}.{prop}"
            return f"?.{prop}"

        if isinstance(expr, Variable):
            return expr.name

        if isinstance(expr, (IntegerLiteral, FloatLiteral, StringLiteral)):
            return "$"  # Literal placeholder

        return type(expr).__name__


# ---------------------------------------------------------------------------
# Predicate Selectivity Tracker — learns actual selectivity
# ---------------------------------------------------------------------------


@dataclass
class _SelectivityObservation:
    """A single selectivity measurement."""

    estimated: float
    actual: float
    timestamp: float = field(default_factory=time.monotonic)


class PredicateSelectivityTracker:
    """Learns actual predicate selectivity from execution feedback.

    Tracks selectivity per (entity_type, property, operator) triple using
    an exponential moving average for fast convergence with recency bias.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # key: (entity_type, property, operator) -> deque of observations
        self._history: dict[
            tuple[str, str, str],
            deque[_SelectivityObservation],
        ] = {}
        # EMA values for fast lookup
        self._ema: dict[tuple[str, str, str], float] = {}
        self._observation_counts: dict[tuple[str, str, str], int] = {}
        # Correlation tracking for compound selectivity
        self._correlation_history: dict[
            tuple[str, tuple[str, str], tuple[str, str]],
            deque[float],
        ] = {}
        self._correlation_ema: dict[
            tuple[str, tuple[str, str], tuple[str, str]], float
        ] = {}
        self._correlation_counts: dict[
            tuple[str, tuple[str, str], tuple[str, str]], int
        ] = {}

    def record(
        self,
        entity_type: str,
        prop: str,
        operator: str,
        *,
        estimated: float,
        actual: float,
    ) -> None:
        """Record an observed selectivity for a predicate pattern."""
        key = (entity_type, prop, operator.strip().upper())
        obs = _SelectivityObservation(estimated=estimated, actual=actual)

        with self._lock:
            if key not in self._history:
                self._history[key] = deque(
                    maxlen=_MAX_SELECTIVITY_HISTORY,
                )
                self._ema[key] = actual
                self._observation_counts[key] = 0

            self._history[key].append(obs)
            self._observation_counts[key] += 1

            # Update EMA
            prev = self._ema[key]
            self._ema[key] = _EMA_ALPHA * actual + (1 - _EMA_ALPHA) * prev

    def get_learned_selectivity(
        self,
        entity_type: str,
        prop: str,
        operator: str,
    ) -> float | None:
        """Return the learned selectivity, or None if insufficient data.

        Returns None when fewer than ``_MIN_OBSERVATIONS`` have been
        recorded, allowing the caller to fall back to heuristic defaults.
        """
        key = (entity_type, prop, operator.strip().upper())
        with self._lock:
            count = self._observation_counts.get(key, 0)
            if count < _MIN_OBSERVATIONS:
                return None
            return self._ema.get(key)

    def correction_factor(
        self,
        entity_type: str,
        prop: str,
        operator: str,
        *,
        heuristic: float,
    ) -> float:
        """Return a multiplicative correction to apply to a heuristic estimate.

        If learned selectivity is 0.12 and heuristic is 0.33, returns
        0.12 / 0.33 ~ 0.36 so the caller can multiply their estimate.

        Returns 1.0 when insufficient data is available.
        """
        learned = self.get_learned_selectivity(entity_type, prop, operator)
        if learned is None or heuristic <= 0:
            return 1.0
        return max(0.01, min(100.0, learned / heuristic))

    @property
    def tracked_patterns(self) -> list[tuple[str, str, str]]:
        """Return all tracked (entity_type, property, operator) triples."""
        with self._lock:
            return list(self._history.keys())

    # -- Compound selectivity -------------------------------------------------

    def estimate_compound_selectivity(
        self,
        entity_type: str,
        predicates: list[tuple[str, str]],
        combinator: str = "AND",
    ) -> float | None:
        """Estimate compound selectivity for multiple predicates.

        Uses independence assumption (adjusted by learned correlation when
        available) to combine individual selectivities.

        Parameters
        ----------
        entity_type:
            The entity type (e.g. "Person").
        predicates:
            List of (property, operator) pairs.
        combinator:
            "AND" or "OR".

        Returns
        -------
        Estimated compound selectivity, or None if any predicate lacks data.
        """
        if not predicates:
            return None

        selectivities: list[float] = []
        for prop, op in predicates:
            s = self.get_learned_selectivity(entity_type, prop, op)
            if s is None:
                return None
            selectivities.append(s)

        if len(selectivities) == 1:
            return selectivities[0]

        combinator = combinator.strip().upper()

        if combinator == "AND":
            # Start with independence assumption: product of selectivities
            result = 1.0
            for s in selectivities:
                result *= s
            # Apply pairwise correlation corrections if available
            for i in range(len(predicates)):
                for j in range(i + 1, len(predicates)):
                    corr = self.get_correlation_factor(
                        entity_type, predicates[i], predicates[j],
                    )
                    if corr is not None:
                        result *= corr
            return max(0.0, min(1.0, result))

        if combinator == "OR":
            # Inclusion-exclusion: s1 + s2 - s1*s2 (generalized)
            result = 0.0
            for s in selectivities:
                result = result + s - result * s
            return max(0.0, min(1.0, result))

        return None

    def record_compound(
        self,
        entity_type: str,
        predicates: list[tuple[str, str]],
        *,
        estimated_compound: float,
        actual_compound: float,
    ) -> None:
        """Record observed compound selectivity to learn correlations.

        Compares the actual compound selectivity against what independence
        would predict, storing the ratio as a correlation factor.
        """
        if len(predicates) < 2:
            return

        # Compute independence-based prediction from individual EMA values
        individual_product = 1.0
        all_known = True
        for prop, op in predicates:
            s = self.get_learned_selectivity(entity_type, prop, op)
            if s is None or s <= 0:
                all_known = False
                break
            individual_product *= s

        if not all_known or individual_product <= 0:
            # Still store the raw observation for future use
            pass
        else:
            # Correlation factor = actual / independence_prediction
            corr_factor = actual_compound / individual_product

            # Store pairwise correlations
            for i in range(len(predicates)):
                for j in range(i + 1, len(predicates)):
                    key = self._correlation_key(
                        entity_type, predicates[i], predicates[j],
                    )
                    with self._lock:
                        if key not in self._correlation_history:
                            self._correlation_history[key] = deque(
                                maxlen=_MAX_SELECTIVITY_HISTORY,
                            )
                            self._correlation_ema[key] = corr_factor
                            self._correlation_counts[key] = 0

                        self._correlation_history[key].append(corr_factor)
                        self._correlation_counts[key] += 1

                        prev = self._correlation_ema[key]
                        self._correlation_ema[key] = (
                            _EMA_ALPHA * corr_factor
                            + (1 - _EMA_ALPHA) * prev
                        )

    def get_correlation_factor(
        self,
        entity_type: str,
        pred_a: tuple[str, str],
        pred_b: tuple[str, str],
    ) -> float | None:
        """Get learned correlation factor between two predicates.

        Returns a multiplicative factor: < 1.0 means positive correlation
        (AND selects fewer than independence), > 1.0 means negative correlation.
        Returns None if insufficient data.
        """
        key = self._correlation_key(entity_type, pred_a, pred_b)
        with self._lock:
            count = self._correlation_counts.get(key, 0)
            if count < _MIN_OBSERVATIONS:
                return None
            return self._correlation_ema.get(key)

    def _correlation_key(
        self,
        entity_type: str,
        pred_a: tuple[str, str],
        pred_b: tuple[str, str],
    ) -> tuple[str, tuple[str, str], tuple[str, str]]:
        """Canonical key for a predicate pair (sorted for symmetry)."""
        a = (pred_a[0], pred_a[1].strip().upper())
        b = (pred_b[0], pred_b[1].strip().upper())
        if a > b:
            a, b = b, a
        return (entity_type, a, b)

    def clear(self) -> None:
        """Drop all history."""
        with self._lock:
            self._history.clear()
            self._ema.clear()
            self._observation_counts.clear()
            self._correlation_history.clear()
            self._correlation_ema.clear()
            self._correlation_counts.clear()


# ---------------------------------------------------------------------------
# Join Performance Tracker — adaptive strategy selection
# ---------------------------------------------------------------------------


@dataclass
class _JoinObservation:
    """A single join execution measurement."""

    strategy: str  # JoinStrategy.value
    left_rows: int
    right_rows: int
    actual_output_rows: int
    elapsed_ms: float
    timestamp: float = field(default_factory=time.monotonic)


def _size_bucket(rows: int) -> str:
    """Bucket row count into a size category for strategy lookup."""
    if rows <= 100:
        return "tiny"
    if rows <= 10_000:
        return "small"
    if rows <= 1_000_000:
        return "medium"
    return "large"


class JoinPerformanceTracker:
    """Tracks join strategy performance for adaptive strategy selection.

    Records execution time and output accuracy per (size_bucket_left,
    size_bucket_right, strategy) triple.  Over time, this allows the
    planner to prefer strategies that performed well historically for
    similar input sizes.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # key: (left_bucket, right_bucket) -> list of observations
        self._history: dict[
            tuple[str, str],
            deque[_JoinObservation],
        ] = {}

    def record(
        self,
        *,
        strategy: str,
        left_rows: int,
        right_rows: int,
        actual_output_rows: int,
        elapsed_ms: float,
    ) -> None:
        """Record a join execution observation."""
        bucket_key = (_size_bucket(left_rows), _size_bucket(right_rows))
        obs = _JoinObservation(
            strategy=strategy,
            left_rows=left_rows,
            right_rows=right_rows,
            actual_output_rows=actual_output_rows,
            elapsed_ms=elapsed_ms,
        )
        with self._lock:
            if bucket_key not in self._history:
                self._history[bucket_key] = deque(maxlen=_MAX_JOIN_HISTORY)
            self._history[bucket_key].append(obs)

    def best_strategy(
        self,
        left_rows: int,
        right_rows: int,
    ) -> str | None:
        """Return the historically best-performing strategy for this size pair.

        Returns None when insufficient data is available (< _MIN_OBSERVATIONS
        for any strategy in this bucket).
        """
        bucket_key = (_size_bucket(left_rows), _size_bucket(right_rows))

        with self._lock:
            history = self._history.get(bucket_key)
            if not history:
                return None

        # Group by strategy, compute average elapsed_ms
        strategy_times: dict[str, list[float]] = {}
        for obs in history:
            if obs.strategy not in strategy_times:
                strategy_times[obs.strategy] = []
            strategy_times[obs.strategy].append(obs.elapsed_ms)

        # Only consider strategies with enough observations
        candidates: dict[str, float] = {}
        for strat, times in strategy_times.items():
            if len(times) >= _MIN_OBSERVATIONS:
                candidates[strat] = sum(times) / len(times)

        if not candidates:
            return None

        best = min(candidates, key=candidates.get)  # type: ignore[arg-type]
        LOGGER.debug(
            "Join performance tracker: best strategy for %s is %s (avg %.1fms)",
            bucket_key,
            best,
            candidates[best],
        )
        return best

    def strategy_stats(
        self,
        left_rows: int,
        right_rows: int,
    ) -> dict[str, dict[str, float]]:
        """Return per-strategy statistics for a size bucket.

        Returns a dict of strategy -> {avg_ms, count, output_accuracy}.
        """
        bucket_key = (_size_bucket(left_rows), _size_bucket(right_rows))

        with self._lock:
            history = self._history.get(bucket_key)
            if not history:
                return {}

        stats: dict[str, dict[str, Any]] = {}
        for obs in history:
            if obs.strategy not in stats:
                stats[obs.strategy] = {
                    "times": [],
                    "count": 0,
                    "output_ratios": [],
                }
            stats[obs.strategy]["times"].append(obs.elapsed_ms)
            stats[obs.strategy]["count"] += 1
            expected = min(obs.left_rows, obs.right_rows)
            if expected > 0:
                stats[obs.strategy]["output_ratios"].append(
                    obs.actual_output_rows / expected,
                )

        result: dict[str, dict[str, float]] = {}
        for strat, data in stats.items():
            times = data["times"]
            ratios = data["output_ratios"]
            result[strat] = {
                "avg_ms": sum(times) / len(times),
                "count": float(data["count"]),
                "output_accuracy": (
                    sum(ratios) / len(ratios) if ratios else 1.0
                ),
            }
        return result

    def clear(self) -> None:
        """Drop all history."""
        with self._lock:
            self._history.clear()


# ---------------------------------------------------------------------------
# Plan Version Tracker — track plan evolution and effectiveness
# ---------------------------------------------------------------------------


@dataclass
class _PlanVersion:
    """A versioned plan with execution metrics."""

    version: int
    analysis: AnalysisResult
    created_at: float = field(default_factory=time.monotonic)
    execution_times: list[float] = field(default_factory=list)
    rows_produced: list[int] = field(default_factory=list)


class PlanVersionTracker:
    """Tracks plan evolution and effectiveness metrics per query fingerprint.

    Each time a new plan is generated for a structurally similar query,
    the version is incremented.  Execution metrics are recorded per version
    to detect regressions and identify the best-performing plan.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # digest -> list of _PlanVersion (ordered by version)
        self._versions: dict[str, list[_PlanVersion]] = {}

    def record_plan(
        self,
        fingerprint: QueryFingerprint,
        analysis: AnalysisResult,
    ) -> int:
        """Record a new plan version. Returns the version number."""
        with self._lock:
            versions = self._versions.setdefault(fingerprint.digest, [])
            version = len(versions) + 1
            versions.append(
                _PlanVersion(version=version, analysis=analysis),
            )
            return version

    def record_execution(
        self,
        fingerprint: QueryFingerprint,
        version: int,
        *,
        elapsed_ms: float,
        rows_produced: int,
    ) -> None:
        """Record execution metrics for a specific plan version."""
        with self._lock:
            versions = self._versions.get(fingerprint.digest, [])
            for v in versions:
                if v.version == version:
                    v.execution_times.append(elapsed_ms)
                    v.rows_produced.append(rows_produced)
                    return

    def get_version_metrics(
        self,
        fingerprint: QueryFingerprint,
        version: int,
    ) -> dict[str, Any] | None:
        """Get execution metrics for a specific plan version."""
        with self._lock:
            versions = self._versions.get(fingerprint.digest, [])
            for v in versions:
                if v.version == version:
                    if not v.execution_times:
                        return {
                            "version": v.version,
                            "execution_count": 0,
                            "avg_elapsed_ms": 0.0,
                            "avg_rows": 0.0,
                        }
                    return {
                        "version": v.version,
                        "execution_count": len(v.execution_times),
                        "avg_elapsed_ms": (
                            sum(v.execution_times) / len(v.execution_times)
                        ),
                        "avg_rows": (
                            sum(v.rows_produced) / len(v.rows_produced)
                            if v.rows_produced
                            else 0.0
                        ),
                    }
            return None

    def best_version(self, fingerprint: QueryFingerprint) -> int | None:
        """Return the version number of the best-performing plan.

        "Best" = lowest average execution time among versions with data.
        Returns None if no versions have execution metrics.
        """
        with self._lock:
            versions = self._versions.get(fingerprint.digest, [])
            if not versions:
                return None

            best: int | None = None
            best_avg: float = float("inf")
            for v in versions:
                if v.execution_times:
                    avg = sum(v.execution_times) / len(v.execution_times)
                    if avg < best_avg:
                        best_avg = avg
                        best = v.version
            return best

    def get_history(
        self, fingerprint: QueryFingerprint,
    ) -> list[dict[str, Any]]:
        """Get full version history for a fingerprint."""
        with self._lock:
            versions = self._versions.get(fingerprint.digest, [])
            return [
                {
                    "version": v.version,
                    "cardinalities": v.analysis.clause_cardinalities,
                    "execution_count": len(v.execution_times),
                    "avg_elapsed_ms": (
                        sum(v.execution_times) / len(v.execution_times)
                        if v.execution_times
                        else None
                    ),
                }
                for v in versions
            ]

    def detect_regression(
        self, fingerprint: QueryFingerprint, version: int,
    ) -> bool:
        """Detect if a plan version is a regression vs the previous best.

        Returns True if the given version's average execution time is
        worse than the best previous version by >10%.
        """
        with self._lock:
            versions = self._versions.get(fingerprint.digest, [])
            if not versions:
                return False

            target: _PlanVersion | None = None
            for v in versions:
                if v.version == version:
                    target = v
                    break

            if target is None or not target.execution_times:
                return False

            target_avg = sum(target.execution_times) / len(target.execution_times)

            # Find best average among other versions
            best_other_avg: float = float("inf")
            for v in versions:
                if v.version != version and v.execution_times:
                    avg = sum(v.execution_times) / len(v.execution_times)
                    if avg < best_other_avg:
                        best_other_avg = avg

            if best_other_avg == float("inf"):
                return False  # No other versions to compare against

            # Regression if >10% worse than best other
            return target_avg > best_other_avg * 1.1

    def clear(self) -> None:
        """Drop all version history."""
        with self._lock:
            self._versions.clear()
