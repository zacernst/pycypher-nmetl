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

        # Generate recommendations.
        recommendations = _generate_recommendations(
            query=query,
            total_ms=total_ms,
            parse_ms=parse_ms,
            plan_ms=plan_ms,
            clause_timings=clause_timings,
            row_count=row_count,
            hotspot=hotspot,
        )

        # Collect backend operation timings if an InstrumentedBackend is attached.
        backend_timings: dict[str, dict[str, float]] = {}
        if self.backend is not None and hasattr(
            self.backend, "timing_summary"
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
            self.backend, "timing_summary"
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
) -> list[str]:
    """Generate optimization recommendations from profile data.

    Args:
        query: The original query string.
        total_ms: Total execution time.
        parse_ms: Parse phase time.
        plan_ms: Planning phase time.
        clause_timings: Per-clause timing breakdown.
        row_count: Result row count.
        hotspot: The slowest clause type.

    Returns:
        List of recommendation strings.
    """
    recs: list[str] = []

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

    # Check for multiple MATCH clauses (potential cross-product).
    match_count = query.upper().count("MATCH")
    if match_count > 2:
        recs.append(
            f"Query has {match_count} MATCH clauses. "
            "Multiple MATCH patterns may cause expensive cross-products.",
        )

    return recs
