"""Query profiling and bottleneck analysis tools.

Traces individual query execution to identify hot spots, track memory
allocation, and generate optimization recommendations.  Built on top
of the per-clause timing infrastructure in :mod:`shared.metrics`.

Usage::

    from pycypher.query_profiler import QueryProfiler
    from pycypher.star import Star

    profiler = QueryProfiler(Star())
    report = profiler.profile("MATCH (p:Person) RETURN p.name")
    print(report)
    print(report.hotspot)          # Slowest clause type
    print(report.recommendations)  # Optimization suggestions
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from shared.logger import LOGGER

# Recommendation thresholds (milliseconds).
_SLOW_PARSE_MS = 50.0
_SLOW_PLAN_MS = 20.0
_SLOW_CLAUSE_MS = 100.0
_LARGE_RESULT_ROWS = 10_000


@dataclass(frozen=True)
class ProfileReport:
    """Profiling result for a single query execution.

    Attributes:
        query: The original Cypher query string.
        total_time_ms: Total wall-clock time including parse + plan + execute.
        parse_time_ms: Time spent parsing the query string.
        plan_time_ms: Time spent in the query planner.
        clause_timings: Per-clause-type execution times in milliseconds.
        row_count: Number of rows in the result set.
        hotspot: The clause type that consumed the most time, or ``None``.
        recommendations: List of optimization suggestions based on the profile.
        memory_delta_mb: RSS change during execution (MB).

    """

    query: str
    total_time_ms: float
    parse_time_ms: float
    plan_time_ms: float
    clause_timings: dict[str, float]
    row_count: int
    hotspot: str | None
    recommendations: list[str]
    memory_delta_mb: float = 0.0
    backend_timings: dict[str, dict[str, float]] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return a human-readable profile report."""
        lines = [
            f"Query Profile: {self.query[:80]}",
            f"Total: {self.total_time_ms:.1f}ms  (parse={self.parse_time_ms:.1f}ms, plan={self.plan_time_ms:.1f}ms)",
            f"Rows: {self.row_count}  Memory delta: {self.memory_delta_mb:.1f}MB",
            "Clause breakdown:",
        ]
        for clause, timing in sorted(
            self.clause_timings.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            pct = (
                (timing / self.total_time_ms * 100)
                if self.total_time_ms > 0
                else 0
            )
            marker = " <-- HOTSPOT" if clause == self.hotspot else ""
            lines.append(f"  {clause}: {timing:.1f}ms ({pct:.0f}%){marker}")
        if self.backend_timings:
            lines.append("Backend operations:")
            for op, stats in sorted(
                self.backend_timings.items(),
                key=lambda x: x[1].get("total_ms", 0),
                reverse=True,
            ):
                count = int(stats.get("count", 0))
                total = stats.get("total_ms", 0.0)
                lines.append(f"  {op}: {total:.1f}ms ({count} calls)")
        if self.recommendations:
            lines.append("Recommendations:")
            for rec in self.recommendations:
                lines.append(f"  - {rec}")
        return "\n".join(lines)


@dataclass
class QueryProfiler:
    """Profiles query execution and generates bottleneck analysis.

    Wraps a :class:`~pycypher.star.Star` instance and instruments
    each query execution to collect detailed timing data.

    Args:
        star: The Star instance to profile queries against.

    """

    star: Any
    backend: Any = None
    history: list[ProfileReport] = field(default_factory=list)

    def profile(
        self,
        query: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> ProfileReport:
        """Execute and profile a query.

        Args:
            query: Cypher query string.
            parameters: Optional named query parameters.

        Returns:
            A :class:`ProfileReport` with timing breakdown and recommendations.

        """
        from shared.metrics import get_rss_mb

        LOGGER.debug("QueryProfiler.profile: starting  query=%r", query[:80])
        rss_before = get_rss_mb()
        t0 = time.perf_counter()

        # Reset per-query state on the star.
        self.star._last_clause_timings = {}

        result = self.star.execute_query(query, parameters=parameters)

        total_ms = (time.perf_counter() - t0) * 1000.0
        rss_after = get_rss_mb()
        mem_delta = rss_after - rss_before

        # Extract timing data from the star's instrumentation.
        clause_timings: dict[str, float] = dict(
            getattr(self.star, "_last_clause_timings", {}),
        )
        parse_ms = (
            getattr(self.star, "_last_parse_time_ms", 0.0)
            if hasattr(self.star, "_last_parse_time_ms")
            else 0.0
        )
        plan_ms = (
            getattr(self.star, "_last_plan_time_ms", 0.0)
            if hasattr(self.star, "_last_plan_time_ms")
            else 0.0
        )

        row_count = len(result) if isinstance(result, pd.DataFrame) else 0

        # Identify hotspot.
        hotspot = (
            max(clause_timings, key=lambda k: clause_timings.get(k, 0.0))
            if clause_timings
            else None
        )

        # Generate recommendations (including optimizer-aware hints).
        opt_plan = getattr(self.star, "_last_optimization_plan", None)
        recommendations = _generate_recommendations(
            query=query,
            total_ms=total_ms,
            parse_ms=parse_ms,
            plan_ms=plan_ms,
            clause_timings=clause_timings,
            row_count=row_count,
            hotspot=hotspot,
            optimization_plan=opt_plan,
        )

        # Collect backend operation timings if an InstrumentedBackend is attached.
        backend_timings: dict[str, dict[str, float]] = {}
        if self.backend is not None and hasattr(
            self.backend,
            "timing_summary",
        ):
            backend_timings = self.backend.timing_summary()

        report = ProfileReport(
            query=query,
            total_time_ms=total_ms,
            parse_time_ms=parse_ms,
            plan_time_ms=plan_ms,
            clause_timings=clause_timings,
            row_count=row_count,
            hotspot=hotspot,
            recommendations=recommendations,
            memory_delta_mb=mem_delta,
            backend_timings=backend_timings,
        )

        self.history.append(report)
        LOGGER.debug(
            "QueryProfiler.profile: done  total=%.1fms  rows=%d  hotspot=%s  recs=%d",
            total_ms,
            row_count,
            hotspot or "none",
            len(recommendations),
        )
        return report

    def metrics_summary(self) -> dict[str, Any]:
        """Return a combined diagnostic summary of clause and backend timings.

        Aggregates all profile reports in history into a single summary with:

        - ``query_count``: Number of profiled queries.
        - ``clause_timings``: Aggregated per-clause total milliseconds.
        - ``backend_timings``: Latest backend operation timing summary.

        Returns:
            Dict with combined clause-level and operation-level metrics.

        """
        clause_totals: dict[str, float] = {}
        for report in self.history:
            for clause, ms in report.clause_timings.items():
                clause_totals[clause] = clause_totals.get(clause, 0.0) + ms

        backend_timings: dict[str, dict[str, float]] = {}
        if self.backend is not None and hasattr(
            self.backend,
            "timing_summary",
        ):
            backend_timings = self.backend.timing_summary()

        return {
            "query_count": len(self.history),
            "clause_timings": clause_totals,
            "backend_timings": backend_timings,
        }

    def clear_history(self) -> None:
        """Clear all stored profile reports."""
        self.history.clear()


def _generate_recommendations(
    *,
    query: str,
    total_ms: float,
    parse_ms: float,
    plan_ms: float,
    clause_timings: dict[str, float],
    row_count: int,
    hotspot: str | None,
    optimization_plan: Any = None,
) -> list[str]:
    """Generate optimization recommendations from profile data.

    Combines timing-based heuristics, query structure analysis, and
    optimizer output to produce actionable suggestions.

    Args:
        query: The original Cypher query string.
        total_ms: Total execution time.
        parse_ms: Parse phase time.
        plan_ms: Planning phase time.
        clause_timings: Per-clause timing breakdown.
        row_count: Result row count.
        hotspot: The slowest clause type.
        optimization_plan: Optional :class:`OptimizationPlan` from the
            query optimizer, used for cardinality and rule-based hints.

    Returns:
        List of recommendation strings.

    """
    recs: list[str] = []
    query_upper = query.upper()

    # --- Timing-based recommendations ---

    if parse_ms > _SLOW_PARSE_MS:
        recs.append(
            f"Parse time ({parse_ms:.0f}ms) is high. "
            "Consider caching parsed ASTs for repeated queries.",
        )

    if plan_ms > _SLOW_PLAN_MS:
        recs.append(
            f"Planning time ({plan_ms:.0f}ms) is high. "
            "Complex query structure may benefit from simplification.",
        )

    if hotspot == "Match" and clause_timings.get("Match", 0) > _SLOW_CLAUSE_MS:
        recs.append(
            f"MATCH clause is the bottleneck ({clause_timings['Match']:.0f}ms). "
            "Consider adding WHERE predicates to reduce scan scope.",
        )

    if (
        hotspot == "Return"
        and clause_timings.get("Return", 0) > _SLOW_CLAUSE_MS
    ):
        recs.append(
            f"RETURN clause is slow ({clause_timings['Return']:.0f}ms). "
            "Consider reducing projected columns or adding LIMIT.",
        )

    if row_count > _LARGE_RESULT_ROWS:
        recs.append(
            f"Large result set ({row_count} rows). "
            "Consider adding LIMIT or more selective WHERE predicates.",
        )

    # --- Query structure anti-pattern detection ---

    match_count = query_upper.count("MATCH")
    if match_count > 2:
        recs.append(
            f"Query has {match_count} MATCH clauses. "
            "Multiple MATCH patterns may cause expensive cross-products.",
        )

    # Anti-pattern: MATCH without WHERE on large results
    has_where = "WHERE" in query_upper
    has_limit = "LIMIT" in query_upper
    if not has_where and not has_limit and row_count > 1000:
        recs.append(
            "Query has no WHERE or LIMIT clause and returned "
            f"{row_count:,} rows. Add filters to avoid full scans.",
        )

    # Anti-pattern: RETURN * (projects all columns, often unnecessary)
    if "RETURN *" in query_upper.replace("  ", " "):
        recs.append(
            "RETURN * projects all properties. "
            "Specify only needed columns to reduce memory and I/O.",
        )

    # Anti-pattern: ORDER BY without LIMIT (sorts entire result set)
    if "ORDER BY" in query_upper and not has_limit:
        recs.append(
            "ORDER BY without LIMIT sorts the entire result set. "
            "Add LIMIT to avoid sorting rows that won't be used.",
        )

    # Anti-pattern: multiple disconnected MATCH patterns (Cartesian product)
    if match_count >= 2 and "," not in query_upper.split("RETURN")[0]:
        # Multiple MATCH without shared variables risk Cartesian products.
        # This is a heuristic — the optimizer detects this more precisely.
        pass  # Covered by match_count > 2 check above

    # --- Optimizer-aware recommendations ---

    if optimization_plan is not None:
        hints = getattr(optimization_plan, "hints", {})
        applied = getattr(optimization_plan, "applied_rules", [])

        # Cardinality-based backend suggestion
        cardinality_estimates = hints.get("cardinality_estimates", {})
        if cardinality_estimates:
            max_card = max(cardinality_estimates.values(), default=0)
            if max_card > 100_000:
                recs.append(
                    f"High estimated cardinality ({max_card:,.0f} rows). "
                    "Consider using backend='duckdb' or backend='auto' "
                    "for analytical workloads.",
                )

        # Filter pushdown opportunity
        filter_count = hints.get("filter_pushdown_count", 0)
        if "FilterPushdown" in applied and filter_count > 0:
            recs.append(
                f"Optimizer pushed down {filter_count} filter(s). "
                "Query benefits from early filtering — keep WHERE "
                "predicates close to MATCH patterns.",
            )

        # Limit pushdown
        limit_value = hints.get("limit_pushdown_value")
        if limit_value is not None and "LimitPushdown" in applied:
            recs.append(
                f"LIMIT {limit_value} was pushed down to reduce "
                "intermediate result sizes.",
            )

        # Join reordering applied — inform user
        if "JoinReordering" in applied:
            optimal_order = hints.get("optimal_match_order", [])
            if optimal_order:
                recs.append(
                    "Optimizer reordered joins for efficiency. "
                    f"Optimal order: {' → '.join(optimal_order)}.",
                )

        # Index scan candidates
        index_candidates = hints.get("index_scan_candidates", [])
        if index_candidates:
            recs.append(
                f"Index scan candidates detected: "
                f"{', '.join(str(c) for c in index_candidates[:3])}. "
                "Ensure graph indexes are built for these properties.",
            )

        # No rules applied on a slow query — query may not be optimizable
        if not applied and total_ms > 500:
            recs.append(
                "No optimizer rules applied on a slow query. "
                "Consider restructuring the query to enable "
                "filter pushdown or join reordering.",
            )

    return recs


def analyze_workload(history: list[ProfileReport]) -> list[str]:
    """Analyze a collection of profile reports for workload-level patterns.

    Examines aggregate query patterns to suggest system-level tuning
    rather than per-query fixes.

    Args:
        history: List of profile reports from a :class:`QueryProfiler`.

    Returns:
        List of workload-level tuning recommendations.

    """
    if not history:
        return []

    recs: list[str] = []
    n = len(history)

    # Aggregate metrics
    total_times = [r.total_time_ms for r in history]
    row_counts = [r.row_count for r in history]
    parse_times = [r.parse_time_ms for r in history]

    avg_time = sum(total_times) / n
    max_time = max(total_times)
    avg_rows = sum(row_counts) / n

    # Repeated slow parse suggests AST caching would help
    slow_parses = sum(1 for t in parse_times if t > _SLOW_PARSE_MS)
    if slow_parses > n * 0.3:
        recs.append(
            f"{slow_parses}/{n} queries ({slow_parses / n:.0%}) have slow "
            "parse times. Enable query AST caching for repeated patterns.",
        )

    # Tail latency: if p99 >> p50, some queries are outliers
    sorted_times = sorted(total_times)
    p50 = sorted_times[n // 2]
    p99_idx = min(int(n * 0.99), n - 1)
    p99 = sorted_times[p99_idx]
    if p50 > 0 and p99 / p50 > 10 and n >= 10:
        recs.append(
            f"High tail latency: p99={p99:.0f}ms vs p50={p50:.0f}ms "
            f"({p99 / p50:.0f}x ratio). Investigate outlier queries.",
        )

    # Consistently large results suggest missing pagination
    large_results = sum(1 for r in row_counts if r > _LARGE_RESULT_ROWS)
    if large_results > n * 0.5:
        recs.append(
            f"{large_results}/{n} queries return >10K rows. "
            "Consider implementing pagination with SKIP/LIMIT.",
        )

    # Backend suggestion based on average workload size
    if avg_rows > 50_000:
        recs.append(
            f"Average result size is {avg_rows:,.0f} rows. "
            "Consider switching to backend='auto' or backend='duckdb' "
            "for better large-dataset performance.",
        )

    # Clause hotspot concentration
    hotspot_counts: dict[str, int] = {}
    for r in history:
        if r.hotspot:
            hotspot_counts[r.hotspot] = hotspot_counts.get(r.hotspot, 0) + 1
    if hotspot_counts:
        dominant = max(hotspot_counts, key=lambda k: hotspot_counts[k])
        dominant_pct = hotspot_counts[dominant] / n
        if dominant_pct > 0.7:
            recs.append(
                f"{dominant} clause is the bottleneck in {dominant_pct:.0%} "
                f"of queries. Focus optimization efforts on {dominant} "
                "performance.",
            )

    return recs
