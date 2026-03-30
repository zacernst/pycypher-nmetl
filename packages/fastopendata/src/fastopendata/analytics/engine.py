"""Performance analysis engine.

:class:`AnalyticsEngine` consumes collected :class:`QueryMetric` data
and produces actionable summaries: latency percentiles, bottleneck
identification, trend detection, and optimization recommendations.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any

from fastopendata.analytics.collector import MetricsCollector, QueryMetric, QueryStatus


@dataclass
class LatencyBreakdown:
    """Latency percentile statistics.

    Attributes
    ----------
    p50 : float
        Median latency in ms.
    p90 : float
        90th percentile latency in ms.
    p95 : float
        95th percentile latency in ms.
    p99 : float
        99th percentile latency in ms.
    mean : float
        Mean latency in ms.
    min : float
        Minimum observed latency in ms.
    max : float
        Maximum observed latency in ms.
    count : int
        Number of samples.

    """

    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    mean: float = 0.0
    min: float = 0.0
    max: float = 0.0
    count: int = 0

    def to_dict(self) -> dict[str, float | int]:
        """Serialize to dictionary."""
        return {
            "p50_ms": round(self.p50, 3),
            "p90_ms": round(self.p90, 3),
            "p95_ms": round(self.p95, 3),
            "p99_ms": round(self.p99, 3),
            "mean_ms": round(self.mean, 3),
            "min_ms": round(self.min, 3),
            "max_ms": round(self.max, 3),
            "count": self.count,
        }


@dataclass
class Bottleneck:
    """An identified performance bottleneck.

    Attributes
    ----------
    category : str
        Type of bottleneck (e.g. "slow_parse", "high_exec_ratio").
    severity : str
        One of "low", "medium", "high", "critical".
    description : str
        Human-readable explanation.
    affected_queries : int
        Number of queries exhibiting this bottleneck.
    recommendation : str
        Suggested remediation.

    """

    category: str
    severity: str
    description: str
    affected_queries: int = 0
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "category": self.category,
            "severity": self.severity,
            "description": self.description,
            "affected_queries": self.affected_queries,
            "recommendation": self.recommendation,
        }


@dataclass
class TrendPoint:
    """A single data point in a time-series trend.

    Attributes
    ----------
    bucket_start : float
        Epoch timestamp for the start of this bucket.
    query_count : int
        Number of queries in this bucket.
    avg_latency_ms : float
        Average latency in this bucket.
    error_count : int
        Number of errors in this bucket.
    p95_latency_ms : float
        95th percentile latency in this bucket.

    """

    bucket_start: float
    query_count: int = 0
    avg_latency_ms: float = 0.0
    error_count: int = 0
    p95_latency_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "bucket_start": self.bucket_start,
            "query_count": self.query_count,
            "avg_latency_ms": round(self.avg_latency_ms, 3),
            "error_count": self.error_count,
            "p95_latency_ms": round(self.p95_latency_ms, 3),
        }


@dataclass
class PerformanceSummary:
    """Complete performance analysis result.

    Attributes
    ----------
    total_queries : int
        Total queries analyzed.
    error_rate : float
        Fraction of queries that failed.
    latency : LatencyBreakdown
        Latency statistics.
    bottlenecks : list[Bottleneck]
        Identified bottlenecks.
    trends : list[TrendPoint]
        Time-bucketed performance trend.
    slowest_queries : list[dict[str, Any]]
        Top N slowest query summaries.
    recommendations : list[str]
        Prioritized optimization recommendations.

    """

    total_queries: int = 0
    error_rate: float = 0.0
    latency: LatencyBreakdown = field(default_factory=LatencyBreakdown)
    bottlenecks: list[Bottleneck] = field(default_factory=list)
    trends: list[TrendPoint] = field(default_factory=list)
    slowest_queries: list[dict[str, Any]] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dictionary."""
        return {
            "total_queries": self.total_queries,
            "error_rate": round(self.error_rate, 4),
            "latency": self.latency.to_dict(),
            "bottlenecks": [b.to_dict() for b in self.bottlenecks],
            "trends": [t.to_dict() for t in self.trends],
            "slowest_queries": self.slowest_queries,
            "recommendations": self.recommendations,
        }


class AnalyticsEngine:
    """Analyzes collected query metrics to produce performance summaries.

    Parameters
    ----------
    collector : MetricsCollector
        The metrics source.
    slow_threshold_ms : float
        Queries slower than this are flagged as slow.
    trend_bucket_seconds : float
        Width of time buckets for trend analysis.

    """

    def __init__(
        self,
        collector: MetricsCollector,
        *,
        slow_threshold_ms: float = 1000.0,
        trend_bucket_seconds: float = 60.0,
    ) -> None:
        self._collector = collector
        self._slow_threshold_ms = slow_threshold_ms
        self._trend_bucket_seconds = trend_bucket_seconds

    @property
    def collector(self) -> MetricsCollector:
        """The underlying metrics collector."""
        return self._collector

    def summary(self, last_n: int | None = None) -> PerformanceSummary:
        """Generate a performance summary from collected metrics.

        Parameters
        ----------
        last_n : int | None
            If provided, only analyze the most recent *last_n* metrics.

        Returns
        -------
        PerformanceSummary
            Complete analysis result.

        """
        metrics = self._collector.all_metrics()
        if last_n is not None:
            metrics = metrics[-last_n:]

        if not metrics:
            return PerformanceSummary()

        successful = [m for m in metrics if m.status == QueryStatus.SUCCESS]
        errors = [m for m in metrics if m.status != QueryStatus.SUCCESS]

        latency = self._compute_latency(successful)
        bottlenecks = self._detect_bottlenecks(metrics)
        trends = self._compute_trends(metrics)
        slowest = self._find_slowest(successful, n=10)
        recommendations = self._generate_recommendations(
            metrics,
            latency,
            bottlenecks,
        )

        return PerformanceSummary(
            total_queries=len(metrics),
            error_rate=len(errors) / len(metrics) if metrics else 0.0,
            latency=latency,
            bottlenecks=bottlenecks,
            trends=trends,
            slowest_queries=slowest,
            recommendations=recommendations,
        )

    def _compute_latency(self, metrics: list[QueryMetric]) -> LatencyBreakdown:
        """Compute latency percentiles from successful queries."""
        if not metrics:
            return LatencyBreakdown()

        latencies = sorted(m.total_ms for m in metrics)
        n = len(latencies)

        return LatencyBreakdown(
            p50=self._percentile(latencies, 0.50),
            p90=self._percentile(latencies, 0.90),
            p95=self._percentile(latencies, 0.95),
            p99=self._percentile(latencies, 0.99),
            mean=statistics.mean(latencies),
            min=latencies[0],
            max=latencies[-1],
            count=n,
        )

    def _detect_bottlenecks(self, metrics: list[QueryMetric]) -> list[Bottleneck]:
        """Identify performance bottlenecks from metric patterns."""
        bottlenecks: list[Bottleneck] = []

        successful = [m for m in metrics if m.status == QueryStatus.SUCCESS]
        if not successful:
            return bottlenecks

        # Detect slow queries
        slow = [m for m in successful if m.total_ms > self._slow_threshold_ms]
        if slow:
            pct = len(slow) / len(successful) * 100
            severity = "critical" if pct > 20 else "high" if pct > 10 else "medium"
            bottlenecks.append(
                Bottleneck(
                    category="slow_queries",
                    severity=severity,
                    description=(
                        f"{len(slow)} queries ({pct:.1f}%) exceed "
                        f"{self._slow_threshold_ms}ms threshold"
                    ),
                    affected_queries=len(slow),
                    recommendation=(
                        "Review slow queries for missing indexes or "
                        "unoptimized join patterns."
                    ),
                ),
            )

        # Detect parse-heavy queries (parse > 30% of total)
        parse_heavy = [
            m
            for m in successful
            if m.parse_ms > 0 and m.total_ms > 0 and m.parse_ms / m.total_ms > 0.3
        ]
        if parse_heavy:
            bottlenecks.append(
                Bottleneck(
                    category="parse_bottleneck",
                    severity="medium",
                    description=(
                        f"{len(parse_heavy)} queries spend >30% of time in parsing"
                    ),
                    affected_queries=len(parse_heavy),
                    recommendation=(
                        "Consider query caching or prepared statements to "
                        "reduce repeated parsing overhead."
                    ),
                ),
            )

        # Detect execution-heavy queries (exec > 80% of total)
        exec_heavy = [
            m
            for m in successful
            if m.exec_ms > 0 and m.total_ms > 0 and m.exec_ms / m.total_ms > 0.8
        ]
        if len(exec_heavy) > len(successful) * 0.5:
            bottlenecks.append(
                Bottleneck(
                    category="execution_bottleneck",
                    severity="high",
                    description=(
                        f"{len(exec_heavy)} queries are execution-dominated "
                        f"(>80% time in execution)"
                    ),
                    affected_queries=len(exec_heavy),
                    recommendation=(
                        "Investigate join ordering and filter pushdown "
                        "to reduce execution cost."
                    ),
                ),
            )

        # Detect high error rate
        errors = [m for m in metrics if m.status != QueryStatus.SUCCESS]
        if errors and len(errors) / len(metrics) > 0.05:
            pct = len(errors) / len(metrics) * 100
            severity = "critical" if pct > 20 else "high" if pct > 10 else "medium"
            bottlenecks.append(
                Bottleneck(
                    category="high_error_rate",
                    severity=severity,
                    description=f"{len(errors)} queries ({pct:.1f}%) failed",
                    affected_queries=len(errors),
                    recommendation=(
                        "Review error messages for common patterns. "
                        "Check for invalid query syntax or missing types."
                    ),
                ),
            )

        return bottlenecks

    def _compute_trends(self, metrics: list[QueryMetric]) -> list[TrendPoint]:
        """Bucket metrics into time windows for trend analysis."""
        if not metrics:
            return []

        bucket_width = self._trend_bucket_seconds
        min_ts = min(m.timestamp for m in metrics)
        max_ts = max(m.timestamp for m in metrics)

        # Build buckets
        buckets: dict[float, list[QueryMetric]] = {}
        current = min_ts
        while current <= max_ts:
            buckets[current] = []
            current += bucket_width

        # Assign metrics to buckets
        for m in metrics:
            bucket_start = (
                min_ts + int((m.timestamp - min_ts) / bucket_width) * bucket_width
            )
            if bucket_start not in buckets:
                buckets[bucket_start] = []
            buckets[bucket_start].append(m)

        # Compute per-bucket stats
        trends: list[TrendPoint] = []
        for bucket_start in sorted(buckets):
            bucket_metrics = buckets[bucket_start]
            if not bucket_metrics:
                trends.append(TrendPoint(bucket_start=bucket_start))
                continue

            latencies = [m.total_ms for m in bucket_metrics]
            error_count = sum(
                1 for m in bucket_metrics if m.status != QueryStatus.SUCCESS
            )

            trends.append(
                TrendPoint(
                    bucket_start=bucket_start,
                    query_count=len(bucket_metrics),
                    avg_latency_ms=statistics.mean(latencies),
                    error_count=error_count,
                    p95_latency_ms=self._percentile(sorted(latencies), 0.95),
                ),
            )

        return trends

    def _find_slowest(
        self,
        metrics: list[QueryMetric],
        n: int = 10,
    ) -> list[dict[str, Any]]:
        """Return the N slowest query summaries."""
        sorted_metrics = sorted(metrics, key=lambda m: m.total_ms, reverse=True)
        return [m.to_dict() for m in sorted_metrics[:n]]

    def _generate_recommendations(
        self,
        metrics: list[QueryMetric],
        latency: LatencyBreakdown,
        bottlenecks: list[Bottleneck],
    ) -> list[str]:
        """Generate prioritized optimization recommendations."""
        recommendations: list[str] = []

        # Based on bottleneck severity
        critical = [b for b in bottlenecks if b.severity == "critical"]
        high = [b for b in bottlenecks if b.severity == "high"]

        for b in critical:
            recommendations.append(f"CRITICAL: {b.recommendation}")
        for b in high:
            recommendations.append(f"HIGH: {b.recommendation}")

        # Latency-based recommendations
        if latency.p99 > 0 and latency.p99 > latency.p50 * 10:
            recommendations.append(
                "Large p99/p50 latency ratio detected — investigate outlier "
                "queries that may be scanning large datasets without filters.",
            )

        if latency.count > 0 and latency.mean > self._slow_threshold_ms * 0.5:
            recommendations.append(
                "Average latency is approaching the slow threshold — consider "
                "enabling query plan caching or optimizing frequently executed queries.",
            )

        # Error-based recommendations
        error_count = sum(1 for m in metrics if m.status != QueryStatus.SUCCESS)
        if error_count > 0:
            timeout_count = sum(1 for m in metrics if m.status == QueryStatus.TIMEOUT)
            if timeout_count > 0:
                recommendations.append(
                    f"{timeout_count} query timeouts detected — consider increasing "
                    "timeout limits or optimizing complex queries.",
                )

        return recommendations

    @staticmethod
    def _percentile(sorted_values: list[float], p: float) -> float:
        """Compute the p-th percentile from a sorted list.

        Parameters
        ----------
        sorted_values : list[float]
            Pre-sorted values.
        p : float
            Percentile as a fraction (0.0–1.0).

        Returns
        -------
        float
            The interpolated percentile value.

        """
        if not sorted_values:
            return 0.0
        n = len(sorted_values)
        if n == 1:
            return sorted_values[0]
        idx = p * (n - 1)
        lower = int(idx)
        upper = min(lower + 1, n - 1)
        frac = idx - lower
        return sorted_values[lower] + frac * (
            sorted_values[upper] - sorted_values[lower]
        )
