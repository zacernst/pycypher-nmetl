"""Lightweight in-process query metrics collector.

Aggregates query execution statistics — counts, timing percentiles, error
rates, and slow-query detection — without requiring an external metrics
backend.  All data lives in-process and is designed for diagnostic access
via ``QueryMetrics.snapshot()``.

The collector is **thread-safe** (uses a :class:`threading.Lock`) for
compatibility with the free-threaded Python 3.14t build.

Enable/disable via the ``PYCYPHER_METRICS_ENABLED`` environment variable
(default: enabled).  When disabled, all recording methods are no-ops.

Usage::

    from shared.metrics import QUERY_METRICS

    # Record a completed query
    QUERY_METRICS.record_query(
        query_id="abc123",
        elapsed_s=0.042,
        rows=150,
        clauses=["Match", "Return"],
    )

    # Record an error
    QUERY_METRICS.record_error(
        query_id="def456",
        error_type="TypeError",
        elapsed_s=0.003,
    )

    # Get a point-in-time snapshot
    stats = QUERY_METRICS.snapshot()
    print(stats)

Attributes:
    QUERY_METRICS: Module-level singleton collector instance.
    SLOW_QUERY_THRESHOLD_S: Queries exceeding this duration trigger a
        warning log.  Configurable via ``PYCYPHER_SLOW_QUERY_MS`` env var
        (default 1000 ms).
"""

from __future__ import annotations

import logging
import os
import resource
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

_logger = logging.getLogger(__name__)


def _parse_slow_query_ms() -> float:
    """Parse PYCYPHER_SLOW_QUERY_MS env var safely, defaulting to 1000ms."""
    raw = os.environ.get("PYCYPHER_SLOW_QUERY_MS", "1000")
    try:
        val = int(raw)
    except (ValueError, TypeError):
        _logger.warning(
            "Invalid PYCYPHER_SLOW_QUERY_MS=%r, using default 1000ms",
            raw,
        )
        val = 1000
    return val / 1000.0


SLOW_QUERY_THRESHOLD_S: float = _parse_slow_query_ms()

_ENABLED: bool = os.environ.get(
    "PYCYPHER_METRICS_ENABLED",
    "1",
).lower() not in (
    "0",
    "false",
    "no",
)

# Maximum number of recent timing samples to keep (rolling window).
_MAX_SAMPLES: int = 1000

# macOS reports ru_maxrss in bytes; Linux in kilobytes.
_RUSAGE_DIVISOR: float = (
    1024.0 * 1024.0 if os.uname().sysname == "Darwin" else 1024.0
)


def get_rss_mb() -> float:
    """Return current process RSS in megabytes using :mod:`resource`.

    Uses :func:`resource.getrusage` (stdlib) for zero external dependencies.
    Returns 0.0 if unavailable.
    """
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / _RUSAGE_DIVISOR
    except (AttributeError, OSError):
        return 0.0


@dataclass(frozen=True)
class MetricsSnapshot:
    """Immutable point-in-time view of collected metrics.

    Attributes:
        total_queries: Total number of successfully executed queries.
        total_errors: Total number of failed queries.
        slow_queries: Number of queries that exceeded the slow-query threshold.
        error_counts: Breakdown of errors by exception type name.
        clause_counts: How many times each clause type was executed.
        timing_p50_ms: Median query execution time in milliseconds.
        timing_p90_ms: 90th percentile execution time in milliseconds.
        timing_p99_ms: 99th percentile execution time in milliseconds.
        timing_max_ms: Maximum observed execution time in milliseconds.
        timing_min_ms: Minimum observed execution time in milliseconds.
        total_rows_returned: Cumulative rows returned across all queries.
        uptime_s: Seconds since the collector was created.
    """

    total_queries: int
    total_errors: int
    slow_queries: int
    error_counts: dict[str, int]
    clause_counts: dict[str, int]
    timing_p50_ms: float
    timing_p90_ms: float
    timing_p99_ms: float
    timing_max_ms: float
    timing_min_ms: float
    total_rows_returned: int
    uptime_s: float
    queries_per_second: float
    rows_per_second: float
    recent_queries_per_second: float
    memory_delta_p50_mb: float
    memory_delta_p90_mb: float
    memory_delta_max_mb: float
    clause_timing_p50_ms: dict[str, float]
    clause_timing_p90_ms: dict[str, float]
    clause_timing_max_ms: dict[str, float]
    parse_time_p50_ms: float
    parse_time_p90_ms: float
    parse_time_max_ms: float
    error_rate: float
    recent_error_rate: float
    planner_accuracy_ratio_p50: float
    planner_mae_mb: float
    plan_time_p50_ms: float
    plan_time_p90_ms: float
    plan_time_max_ms: float
    result_cache_hits: int = 0
    result_cache_misses: int = 0
    result_cache_hit_rate: float = 0.0
    result_cache_size_mb: float = 0.0
    result_cache_entries: int = 0
    result_cache_evictions: int = 0

    def summary(self) -> str:
        """Return a human-readable diagnostic summary.

        Suitable for logging, dashboard display, or REPL inspection.
        """
        total_ops = self.total_queries + self.total_errors
        lines = [
            f"Queries: {self.total_queries} ok, {self.total_errors} errors ({total_ops} total)",
            f"Error rate: {self.error_rate:.1%} overall, {self.recent_error_rate:.1%} recent",
            f"Timing p50={self.timing_p50_ms:.1f}ms  p90={self.timing_p90_ms:.1f}ms  p99={self.timing_p99_ms:.1f}ms  max={self.timing_max_ms:.1f}ms",
            f"Throughput: {self.queries_per_second:.1f} qps, {self.rows_per_second:.0f} rows/s",
            f"Slow queries: {self.slow_queries}",
            f"Memory delta p50={self.memory_delta_p50_mb:.1f}MB  max={self.memory_delta_max_mb:.1f}MB",
            f"Parse time p50={self.parse_time_p50_ms:.1f}ms  max={self.parse_time_max_ms:.1f}ms",
            f"Plan time p50={self.plan_time_p50_ms:.1f}ms  max={self.plan_time_max_ms:.1f}ms",
        ]
        if self.clause_timing_p50_ms:
            clause_parts = [
                f"{k}={v:.1f}ms"
                for k, v in sorted(self.clause_timing_p50_ms.items())
            ]
            lines.append(f"Clause p50: {', '.join(clause_parts)}")
        if self.planner_mae_mb > 0:
            lines.append(
                f"Planner MAE={self.planner_mae_mb:.2f}MB  accuracy_ratio_p50={self.planner_accuracy_ratio_p50:.2f}",
            )
        cache_total = self.result_cache_hits + self.result_cache_misses
        if cache_total > 0:
            lines.append(
                f"Cache: {self.result_cache_hits} hits, {self.result_cache_misses} misses "
                f"({self.result_cache_hit_rate:.1%} hit rate), "
                f"{self.result_cache_entries} entries, {self.result_cache_size_mb:.1f}MB, "
                f"{self.result_cache_evictions} evictions",
            )
        lines.append(f"Uptime: {self.uptime_s:.0f}s")
        return "\n".join(lines)

    def health_status(self) -> str:
        """Return automated health classification.

        Returns:
            ``"healthy"`` — error rate < 5% and slow query rate < 10%.
            ``"degraded"`` — error rate 5–20% or slow query rate 10–30%.
            ``"unhealthy"`` — error rate > 20% or slow query rate > 30%.
        """
        total_ops = self.total_queries + self.total_errors
        if total_ops == 0:
            return "healthy"

        slow_rate = (
            self.slow_queries / self.total_queries
            if self.total_queries > 0
            else 0.0
        )

        if self.error_rate > 0.20 or slow_rate > 0.30:
            return "unhealthy"
        if self.error_rate > 0.05 or slow_rate > 0.10:
            return "degraded"
        return "healthy"

    def diagnostic_report(self) -> str:
        """Return a detailed diagnostic report with actionable recommendations.

        Analyses performance degradation, error patterns, clause hotspots,
        cache efficiency, and resource pressure.  Designed for production
        debugging — each section includes concrete recommendations when
        problems are detected.

        Returns:
            A multi-section diagnostic report string.

        """
        sections: list[str] = []
        status = self.health_status()
        total_ops = self.total_queries + self.total_errors

        # ── Header ──
        sections.append(
            f"=== Query Execution Diagnostic Report ===\n"
            f"Status: {status.upper()}  |  "
            f"Uptime: {self.uptime_s:.0f}s  |  "
            f"Total operations: {total_ops}"
        )

        if total_ops == 0:
            sections.append("\nNo queries recorded yet.")
            return "\n".join(sections)

        # ── Performance Degradation Detection ──
        perf_lines: list[str] = ["\n--- Performance Analysis ---"]
        if self.timing_p99_ms > 0 and self.timing_p50_ms > 0:
            tail_ratio = self.timing_p99_ms / self.timing_p50_ms
            perf_lines.append(
                f"Latency spread: p50={self.timing_p50_ms:.1f}ms  "
                f"p90={self.timing_p90_ms:.1f}ms  "
                f"p99={self.timing_p99_ms:.1f}ms  "
                f"max={self.timing_max_ms:.1f}ms"
            )
            if tail_ratio > 10:
                perf_lines.append(
                    f"  WARNING: p99/p50 ratio is {tail_ratio:.1f}x — "
                    "high tail latency suggests occasional expensive queries "
                    "or resource contention"
                )
            elif tail_ratio > 5:
                perf_lines.append(
                    f"  NOTE: p99/p50 ratio is {tail_ratio:.1f}x — "
                    "moderate tail latency variance"
                )

        if self.recent_queries_per_second > 0 and self.queries_per_second > 0:
            rate_ratio = (
                self.recent_queries_per_second / self.queries_per_second
            )
            if rate_ratio < 0.5:
                perf_lines.append(
                    f"  WARNING: Recent throughput ({self.recent_queries_per_second:.1f} qps) "
                    f"is {rate_ratio:.0%} of overall ({self.queries_per_second:.1f} qps) — "
                    "possible throughput degradation"
                )

        slow_rate = (
            self.slow_queries / self.total_queries
            if self.total_queries > 0
            else 0.0
        )
        if slow_rate > 0:
            perf_lines.append(
                f"Slow queries: {self.slow_queries}/{self.total_queries} "
                f"({slow_rate:.1%}) exceeded {SLOW_QUERY_THRESHOLD_S * 1000:.0f}ms threshold"
            )
            if slow_rate > 0.10:
                perf_lines.append(
                    "  ACTION: Review slow query patterns — consider adding "
                    "LIMIT clauses or WHERE filters to reduce scan scope"
                )
        sections.append("\n".join(perf_lines))

        # ── Error Pattern Analysis ──
        if self.total_errors > 0:
            err_lines: list[str] = ["\n--- Error Analysis ---"]
            err_lines.append(
                f"Error rate: {self.error_rate:.1%} overall, "
                f"{self.recent_error_rate:.1%} recent  "
                f"({self.total_errors} failures)"
            )
            if (
                self.recent_error_rate > self.error_rate * 1.5
                and self.recent_error_rate > 0.05
            ):
                err_lines.append(
                    "  WARNING: Recent error rate is increasing — "
                    "check for new query patterns or data issues"
                )
            if self.error_counts:
                sorted_errors = sorted(
                    self.error_counts.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )
                err_lines.append("Error breakdown:")
                for err_type, count in sorted_errors[:5]:
                    pct = count / self.total_errors * 100
                    err_lines.append(f"  {err_type}: {count} ({pct:.0f}%)")
            sections.append("\n".join(err_lines))

        # ── Clause Hotspot Identification ──
        if self.clause_timing_p50_ms:
            clause_lines: list[str] = ["\n--- Clause Hotspots ---"]
            sorted_clauses = sorted(
                self.clause_timing_p50_ms.items(),
                key=lambda x: x[1],
                reverse=True,
            )
            for clause_name, p50 in sorted_clauses:
                p90 = self.clause_timing_p90_ms.get(clause_name, 0.0)
                max_t = self.clause_timing_max_ms.get(clause_name, 0.0)
                count = self.clause_counts.get(clause_name, 0)
                clause_lines.append(
                    f"  {clause_name:20s}  p50={p50:7.1f}ms  "
                    f"p90={p90:7.1f}ms  max={max_t:7.1f}ms  "
                    f"count={count}"
                )
            if sorted_clauses and sorted_clauses[0][1] > 100:
                clause_lines.append(
                    f"  HOTSPOT: {sorted_clauses[0][0]} is the slowest clause — "
                    "focus optimization here for maximum impact"
                )
            sections.append("\n".join(clause_lines))

        # ── Cache Efficiency ──
        cache_total = self.result_cache_hits + self.result_cache_misses
        if cache_total > 0:
            cache_lines: list[str] = ["\n--- Cache Efficiency ---"]
            cache_lines.append(
                f"Hit rate: {self.result_cache_hit_rate:.1%}  "
                f"({self.result_cache_hits} hits / {cache_total} lookups)"
            )
            cache_lines.append(
                f"Entries: {self.result_cache_entries}  "
                f"Size: {self.result_cache_size_mb:.1f}MB  "
                f"Evictions: {self.result_cache_evictions}"
            )
            if self.result_cache_hit_rate < 0.3 and cache_total > 10:
                cache_lines.append(
                    "  NOTE: Low cache hit rate — queries may have high "
                    "cardinality or unique parameters reducing cache reuse"
                )
            if self.result_cache_evictions > cache_total * 0.5:
                cache_lines.append(
                    "  WARNING: High eviction rate — consider increasing "
                    "cache size to reduce recomputation"
                )
            sections.append("\n".join(cache_lines))

        # ── Resource Pressure ──
        if self.memory_delta_max_mb > 0:
            mem_lines: list[str] = ["\n--- Resource Pressure ---"]
            mem_lines.append(
                f"Memory delta: p50={self.memory_delta_p50_mb:.1f}MB  "
                f"p90={self.memory_delta_p90_mb:.1f}MB  "
                f"max={self.memory_delta_max_mb:.1f}MB"
            )
            if self.memory_delta_max_mb > 500:
                mem_lines.append(
                    "  WARNING: Peak memory delta exceeds 500MB — "
                    "queries with large intermediate results may cause OOM"
                )
            if self.parse_time_max_ms > 100:
                mem_lines.append(
                    f"  NOTE: Max parse time {self.parse_time_max_ms:.1f}ms — "
                    "complex queries may benefit from AST caching"
                )
            if self.plan_time_max_ms > 100:
                mem_lines.append(
                    f"  NOTE: Max plan time {self.plan_time_max_ms:.1f}ms — "
                    "consider simplifying join patterns"
                )
            sections.append("\n".join(mem_lines))

        # ── Planner Accuracy ──
        if self.planner_mae_mb > 0:
            plan_lines: list[str] = ["\n--- Planner Accuracy ---"]
            plan_lines.append(
                f"Memory estimate MAE: {self.planner_mae_mb:.2f}MB  "
                f"Accuracy ratio p50: {self.planner_accuracy_ratio_p50:.2f}"
            )
            if self.planner_accuracy_ratio_p50 > 2.0:
                plan_lines.append(
                    "  WARNING: Planner overestimates memory by 2x+ — "
                    "may reject queries that would fit in budget"
                )
            elif self.planner_accuracy_ratio_p50 < 0.5:
                plan_lines.append(
                    "  WARNING: Planner underestimates memory by 2x+ — "
                    "queries may exceed budget unexpectedly"
                )
            sections.append("\n".join(plan_lines))

        return "\n".join(sections)

    def to_dict(self) -> dict[str, Any]:
        """Return snapshot as a flat dictionary for JSON serialization.

        Useful for programmatic access, log aggregation pipelines, and
        external monitoring integrations.

        Returns:
            A dictionary with all metric fields as key-value pairs.

        """
        from dataclasses import asdict

        return asdict(self)


@dataclass
class QueryMetrics:
    """Thread-safe in-process query metrics collector.

    Records query execution statistics and provides diagnostic snapshots.
    All mutating methods acquire ``_lock`` before modifying internal state.
    """

    _total_queries: int = 0
    _total_errors: int = 0
    _slow_queries: int = 0
    _total_rows: int = 0
    _error_counts: Counter[str] = field(default_factory=Counter)
    _clause_counts: Counter[str] = field(default_factory=Counter)
    _timings_ms: list[float] = field(default_factory=list)
    _memory_deltas_mb: list[float] = field(default_factory=list)
    _clause_timings_ms: dict[str, list[float]] = field(default_factory=dict)
    _parse_times_ms: list[float] = field(default_factory=list)
    _query_timestamps: list[float] = field(default_factory=list)
    _error_timestamps: list[float] = field(default_factory=list)
    _planner_accuracy_ratios: list[float] = field(default_factory=list)
    _planner_abs_errors_mb: list[float] = field(default_factory=list)
    _plan_times_ms: list[float] = field(default_factory=list)
    _cache_stats: dict[str, Any] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _created_at: float = field(default_factory=time.monotonic)

    def record_query(
        self,
        *,
        query_id: str,
        elapsed_s: float,
        rows: int = 0,
        clauses: list[str] | None = None,
        memory_delta_mb: float | None = None,
        clause_timings_ms: dict[str, float] | None = None,
        parse_time_ms: float | None = None,
        estimated_memory_mb: float | None = None,
        plan_time_ms: float | None = None,
    ) -> None:
        """Record a successfully completed query.

        Args:
            query_id: Correlation ID for the query.
            elapsed_s: Wall-clock execution time in seconds.
            rows: Number of rows in the result set.
            clauses: List of clause type names executed (e.g. ``["Match", "Return"]``).
            memory_delta_mb: RSS memory change during query execution (MB).
            clause_timings_ms: Per-clause-type execution times in milliseconds.
            parse_time_ms: Time spent parsing the query string (ms).
            estimated_memory_mb: Planner's memory estimate (MB) for accuracy tracking.
            plan_time_ms: Time spent in query planning phase (ms).
        """
        if not _ENABLED:
            return

        elapsed_ms = elapsed_s * 1000.0
        is_slow = elapsed_s > SLOW_QUERY_THRESHOLD_S

        now = time.monotonic()
        with self._lock:
            self._total_queries += 1
            self._total_rows += rows
            if len(self._query_timestamps) >= _MAX_SAMPLES:
                self._query_timestamps = self._query_timestamps[
                    _MAX_SAMPLES // 2 :
                ]
            self._query_timestamps.append(now)
            if is_slow:
                self._slow_queries += 1

            # Rolling window — discard oldest samples when full.
            if len(self._timings_ms) >= _MAX_SAMPLES:
                self._timings_ms = self._timings_ms[_MAX_SAMPLES // 2 :]
            self._timings_ms.append(elapsed_ms)

            if memory_delta_mb is not None:
                if len(self._memory_deltas_mb) >= _MAX_SAMPLES:
                    self._memory_deltas_mb = self._memory_deltas_mb[
                        _MAX_SAMPLES // 2 :
                    ]
                self._memory_deltas_mb.append(memory_delta_mb)

            if clauses:
                for clause_name in clauses:
                    self._clause_counts[clause_name] += 1

            if clause_timings_ms:
                for clause_name, timing in clause_timings_ms.items():
                    samples = self._clause_timings_ms.setdefault(
                        clause_name,
                        [],
                    )
                    if len(samples) >= _MAX_SAMPLES:
                        self._clause_timings_ms[clause_name] = samples[
                            _MAX_SAMPLES // 2 :
                        ]
                    self._clause_timings_ms[clause_name].append(timing)

            if parse_time_ms is not None:
                if len(self._parse_times_ms) >= _MAX_SAMPLES:
                    self._parse_times_ms = self._parse_times_ms[
                        _MAX_SAMPLES // 2 :
                    ]
                self._parse_times_ms.append(parse_time_ms)

            if estimated_memory_mb is not None and memory_delta_mb is not None:
                abs_err = abs(estimated_memory_mb - memory_delta_mb)
                ratio = (
                    estimated_memory_mb / memory_delta_mb
                    if memory_delta_mb != 0.0
                    else 0.0
                )
                if len(self._planner_accuracy_ratios) >= _MAX_SAMPLES:
                    self._planner_accuracy_ratios = (
                        self._planner_accuracy_ratios[_MAX_SAMPLES // 2 :]
                    )
                self._planner_accuracy_ratios.append(ratio)
                if len(self._planner_abs_errors_mb) >= _MAX_SAMPLES:
                    self._planner_abs_errors_mb = self._planner_abs_errors_mb[
                        _MAX_SAMPLES // 2 :
                    ]
                self._planner_abs_errors_mb.append(abs_err)

            if plan_time_ms is not None:
                if len(self._plan_times_ms) >= _MAX_SAMPLES:
                    self._plan_times_ms = self._plan_times_ms[
                        _MAX_SAMPLES // 2 :
                    ]
                self._plan_times_ms.append(plan_time_ms)

        if is_slow:
            _logger.warning(
                "SLOW QUERY  query_id=%s  elapsed=%.1fms  rows=%d  threshold=%.0fms",
                query_id,
                elapsed_ms,
                rows,
                SLOW_QUERY_THRESHOLD_S * 1000,
            )

    def record_error(
        self,
        *,
        query_id: str,
        error_type: str,
        elapsed_s: float,
    ) -> None:
        """Record a failed query execution.

        Args:
            query_id: Correlation ID for the query.
            error_type: Exception class name (e.g. ``"TypeError"``).
            elapsed_s: Wall-clock time before failure in seconds.
        """
        if not _ENABLED:
            return

        now = time.monotonic()
        with self._lock:
            self._total_errors += 1
            self._error_counts[error_type] += 1
            if len(self._error_timestamps) >= _MAX_SAMPLES:
                self._error_timestamps = self._error_timestamps[
                    _MAX_SAMPLES // 2 :
                ]
            self._error_timestamps.append(now)

    def update_cache_stats(self, stats: dict[str, Any]) -> None:
        """Update the latest result cache statistics.

        Called after each query execution with the output of
        ``ResultCache.stats()``.

        Args:
            stats: Dict with keys like ``result_cache_hits``,
                ``result_cache_misses``, ``result_cache_hit_rate``, etc.
        """
        if not _ENABLED:
            return
        with self._lock:
            self._cache_stats = dict(stats)

    def snapshot(self) -> MetricsSnapshot:
        """Return an immutable point-in-time view of all collected metrics.

        Returns:
            A :class:`MetricsSnapshot` with current aggregated statistics.
        """
        _RATE_WINDOW_S = 60.0
        now = time.monotonic()
        with self._lock:
            timings = list(self._timings_ms)
            mem_deltas = list(self._memory_deltas_mb)
            parse_times = list(self._parse_times_ms)

            uptime_s = now - self._created_at

            # Throughput rates.
            queries_per_second = (
                self._total_queries / uptime_s if uptime_s > 0 else 0.0
            )
            rows_per_second = (
                self._total_rows / uptime_s if uptime_s > 0 else 0.0
            )

            # Time-windowed recent rate: count queries in last 60s.
            cutoff = now - _RATE_WINDOW_S
            recent_count = sum(
                1 for ts in self._query_timestamps if ts >= cutoff
            )
            recent_queries_per_second = recent_count / _RATE_WINDOW_S

            # Error rates.
            total_ops = self._total_queries + self._total_errors
            error_rate = (
                self._total_errors / total_ops if total_ops > 0 else 0.0
            )
            recent_errors = sum(
                1 for ts in self._error_timestamps if ts >= cutoff
            )
            recent_total_ops = recent_count + recent_errors
            recent_error_rate = (
                recent_errors / recent_total_ops
                if recent_total_ops > 0
                else 0.0
            )

            # Build per-clause-type timing percentiles.
            ct_p50: dict[str, float] = {}
            ct_p90: dict[str, float] = {}
            ct_max: dict[str, float] = {}
            for clause_name, samples in self._clause_timings_ms.items():
                s = list(samples)
                ct_p50[clause_name] = _percentile(s, 0.50)
                ct_p90[clause_name] = _percentile(s, 0.90)
                ct_max[clause_name] = max(s) if s else 0.0

            return MetricsSnapshot(
                total_queries=self._total_queries,
                total_errors=self._total_errors,
                slow_queries=self._slow_queries,
                error_counts=dict(self._error_counts),
                clause_counts=dict(self._clause_counts),
                timing_p50_ms=_percentile(timings, 0.50),
                timing_p90_ms=_percentile(timings, 0.90),
                timing_p99_ms=_percentile(timings, 0.99),
                timing_max_ms=max(timings) if timings else 0.0,
                timing_min_ms=min(timings) if timings else 0.0,
                total_rows_returned=self._total_rows,
                uptime_s=uptime_s,
                queries_per_second=queries_per_second,
                rows_per_second=rows_per_second,
                recent_queries_per_second=recent_queries_per_second,
                memory_delta_p50_mb=_percentile(mem_deltas, 0.50),
                memory_delta_p90_mb=_percentile(mem_deltas, 0.90),
                memory_delta_max_mb=max(mem_deltas) if mem_deltas else 0.0,
                clause_timing_p50_ms=ct_p50,
                clause_timing_p90_ms=ct_p90,
                clause_timing_max_ms=ct_max,
                parse_time_p50_ms=_percentile(parse_times, 0.50),
                parse_time_p90_ms=_percentile(parse_times, 0.90),
                parse_time_max_ms=max(parse_times) if parse_times else 0.0,
                error_rate=error_rate,
                recent_error_rate=recent_error_rate,
                planner_accuracy_ratio_p50=_percentile(
                    list(self._planner_accuracy_ratios),
                    0.50,
                ),
                planner_mae_mb=(
                    sum(self._planner_abs_errors_mb)
                    / len(self._planner_abs_errors_mb)
                    if self._planner_abs_errors_mb
                    else 0.0
                ),
                plan_time_p50_ms=_percentile(list(self._plan_times_ms), 0.50),
                plan_time_p90_ms=_percentile(list(self._plan_times_ms), 0.90),
                plan_time_max_ms=max(self._plan_times_ms)
                if self._plan_times_ms
                else 0.0,
                result_cache_hits=self._cache_stats.get(
                    "result_cache_hits",
                    0,
                ),
                result_cache_misses=self._cache_stats.get(
                    "result_cache_misses",
                    0,
                ),
                result_cache_hit_rate=self._cache_stats.get(
                    "result_cache_hit_rate",
                    0.0,
                ),
                result_cache_size_mb=self._cache_stats.get(
                    "result_cache_size_mb",
                    0.0,
                ),
                result_cache_entries=self._cache_stats.get(
                    "result_cache_entries",
                    0,
                ),
                result_cache_evictions=self._cache_stats.get(
                    "result_cache_evictions",
                    0,
                ),
            )

    def reset(self) -> None:
        """Reset all counters and timings to zero.

        Useful for test isolation or periodic metric rotation.
        """
        with self._lock:
            self._total_queries = 0
            self._total_errors = 0
            self._slow_queries = 0
            self._total_rows = 0
            self._error_counts.clear()
            self._clause_counts.clear()
            self._timings_ms.clear()
            self._memory_deltas_mb.clear()
            self._clause_timings_ms.clear()
            self._parse_times_ms.clear()
            self._query_timestamps.clear()
            self._error_timestamps.clear()
            self._planner_accuracy_ratios.clear()
            self._planner_abs_errors_mb.clear()
            self._plan_times_ms.clear()
            self._cache_stats.clear()
            self._created_at = time.monotonic()


def _percentile(data: list[float], pct: float) -> float:
    """Compute a percentile from a sorted list of floats.

    Args:
        data: List of observed values (will be sorted in-place).
        pct: Percentile as a fraction (e.g. 0.95 for p95).

    Returns:
        The percentile value, or 0.0 if data is empty.
    """
    if not data:
        return 0.0
    try:
        return statistics.quantiles(data, n=100)[max(0, int(pct * 100) - 1)]
    except statistics.StatisticsError:
        return data[0] if data else 0.0


# Module-level singleton.
QUERY_METRICS = QueryMetrics()
