"""Configurable load generation for PyCypher stress testing.

Provides realistic query workloads, synthetic dataset generation at
configurable scales, and resource monitoring utilities used by all
load test modules.
"""

from __future__ import annotations

import gc
import os
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from pycypher.ingestion import ContextBuilder
from pycypher.relational_models import Context

# ---------------------------------------------------------------------------
# Synthetic dataset generation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GraphScale:
    """Defines the scale of a synthetic graph for load testing."""

    name: str
    person_count: int
    company_count: int
    knows_edges: int
    works_at_edges: int

    @property
    def total_entities(self) -> int:
        return self.person_count + self.company_count

    @property
    def total_edges(self) -> int:
        return self.knows_edges + self.works_at_edges


# Pre-defined scales — each roughly 3-5x the previous.
SCALE_MICRO = GraphScale("micro", 50, 10, 100, 30)
SCALE_SMALL = GraphScale("small", 500, 50, 2_000, 200)
SCALE_MEDIUM = GraphScale("medium", 5_000, 200, 25_000, 2_000)
SCALE_LARGE = GraphScale("large", 50_000, 1_000, 250_000, 20_000)
SCALE_STRESS = GraphScale("stress", 200_000, 5_000, 1_000_000, 100_000)


def build_graph(scale: GraphScale, *, seed: int = 42) -> Context:
    """Build a synthetic social-network graph at the given scale.

    Returns a fully populated :class:`Context` with Person entities,
    Company entities, KNOWS relationships, and WORKS_AT relationships.

    Args:
        scale: Graph size specification.
        seed: RNG seed for reproducibility.

    Returns:
        Populated execution context.
    """
    rng = np.random.default_rng(seed)

    # --- Persons ---
    person_ids = [f"p{i}" for i in range(scale.person_count)]
    person_df = pd.DataFrame(
        {
            "__ID__": person_ids,
            "name": [f"Person_{i}" for i in range(scale.person_count)],
            "age": rng.integers(18, 80, size=scale.person_count).tolist(),
        }
    )

    # --- Companies ---
    company_ids = [f"c{i}" for i in range(scale.company_count)]
    company_df = pd.DataFrame(
        {
            "__ID__": company_ids,
            "name": [f"Company_{i}" for i in range(scale.company_count)],
            "size": rng.integers(
                10, 10_000, size=scale.company_count
            ).tolist(),
        }
    )

    # --- KNOWS edges (person -> person) ---
    knows_src = rng.choice(person_ids, size=scale.knows_edges).tolist()
    knows_tgt = rng.choice(person_ids, size=scale.knows_edges).tolist()
    knows_df = pd.DataFrame(
        {
            "__SOURCE__": knows_src,
            "__TARGET__": knows_tgt,
            "since": rng.integers(2000, 2025, size=scale.knows_edges).tolist(),
        }
    )

    # --- WORKS_AT edges (person -> company) ---
    works_src = rng.choice(person_ids, size=scale.works_at_edges).tolist()
    works_tgt = rng.choice(company_ids, size=scale.works_at_edges).tolist()
    works_df = pd.DataFrame(
        {
            "__SOURCE__": works_src,
            "__TARGET__": works_tgt,
            "role": rng.choice(
                ["Engineer", "Manager", "Analyst", "Director"],
                size=scale.works_at_edges,
            ).tolist(),
        }
    )

    ctx = (
        ContextBuilder()
        .add_entity("Person", person_df, id_col="__ID__")
        .add_entity("Company", company_df, id_col="__ID__")
        .add_relationship(
            "KNOWS", knows_df, source_col="__SOURCE__", target_col="__TARGET__"
        )
        .add_relationship(
            "WORKS_AT",
            works_df,
            source_col="__SOURCE__",
            target_col="__TARGET__",
        )
        .build()
    )
    return ctx


# ---------------------------------------------------------------------------
# Query workload definitions
# ---------------------------------------------------------------------------

#: Queries ordered roughly by expected cost (cheap -> expensive).
QUERY_WORKLOAD: list[dict[str, Any]] = [
    {
        "name": "simple_return",
        "query": "MATCH (p:Person) RETURN p.name LIMIT 10",
        "category": "trivial",
    },
    {
        "name": "filtered_scan",
        "query": "MATCH (p:Person) WHERE p.age > 50 RETURN p.name, p.age",
        "category": "scan",
    },
    {
        "name": "single_hop",
        "query": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 100",
        "category": "join",
    },
    {
        "name": "aggregation",
        "query": "MATCH (p:Person) RETURN avg(p.age), count(p)",
        "category": "aggregation",
    },
    {
        "name": "two_hop",
        "query": (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name, c.name LIMIT 100"
        ),
        "category": "multi_join",
    },
    {
        "name": "cross_type_join",
        "query": (
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN p.name, c.name LIMIT 100"
        ),
        "category": "join",
    },
    {
        "name": "grouped_aggregation",
        "query": (
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN c.name, count(p), avg(p.age)"
        ),
        "category": "aggregation",
    },
    {
        "name": "order_by",
        "query": "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 50",
        "category": "sort",
    },
    {
        "name": "distinct",
        "query": (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.name"
        ),
        "category": "distinct",
    },
    {
        "name": "variable_length_bounded",
        "query": (
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "RETURN a.name, b.name LIMIT 200"
        ),
        "category": "bfs",
    },
]


# ---------------------------------------------------------------------------
# Resource monitoring
# ---------------------------------------------------------------------------


@dataclass
class ExecutionMetrics:
    """Metrics collected from a single query execution."""

    query_name: str
    elapsed_s: float
    peak_rss_mb: float
    rss_delta_mb: float
    row_count: int
    success: bool
    error: str | None = None


@dataclass
class LoadTestReport:
    """Aggregated results from a load test run."""

    scale: str
    total_queries: int
    total_elapsed_s: float
    metrics: list[ExecutionMetrics] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for m in self.metrics if m.success)

    @property
    def failure_count(self) -> int:
        return sum(1 for m in self.metrics if not m.success)

    @property
    def success_rate(self) -> float:
        if self.total_queries == 0:
            return 0.0
        return self.success_count / self.total_queries

    @property
    def throughput_qps(self) -> float:
        if self.total_elapsed_s == 0:
            return 0.0
        return self.total_queries / self.total_elapsed_s

    @property
    def p50_latency_s(self) -> float:
        times = sorted(m.elapsed_s for m in self.metrics if m.success)
        if not times:
            return 0.0
        return times[len(times) // 2]

    @property
    def p99_latency_s(self) -> float:
        times = sorted(m.elapsed_s for m in self.metrics if m.success)
        if not times:
            return 0.0
        idx = min(int(len(times) * 0.99), len(times) - 1)
        return times[idx]

    @property
    def max_peak_rss_mb(self) -> float:
        if not self.metrics:
            return 0.0
        return max(m.peak_rss_mb for m in self.metrics)


def _get_rss_mb() -> float:
    """Return current process RSS in MB."""
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except ImportError:
        # Fallback: read /proc on Linux, rough estimate elsewhere
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        # ru_maxrss is in KB on Linux, bytes on macOS
        import sys

        if sys.platform == "darwin":
            return usage.ru_maxrss / (1024 * 1024)
        return usage.ru_maxrss / 1024


def execute_workload(
    star: Any,
    queries: list[dict[str, Any]],
    *,
    iterations: int = 1,
    timeout_per_query: float | None = 10.0,
) -> LoadTestReport:
    """Execute a query workload and collect metrics.

    Args:
        star: A ``Star`` instance to execute queries against.
        queries: List of query dicts (must have 'name' and 'query' keys).
        iterations: Number of times to repeat the full workload.
        timeout_per_query: Per-query timeout in seconds.

    Returns:
        Aggregated load test report.
    """
    all_metrics: list[ExecutionMetrics] = []
    t0 = time.perf_counter()

    for _iteration in range(iterations):
        for qdef in queries:
            gc.collect()
            rss_before = _get_rss_mb()
            qt0 = time.perf_counter()
            try:
                result = star.execute_query(
                    qdef["query"],
                    timeout_seconds=timeout_per_query,
                )
                elapsed = time.perf_counter() - qt0
                rss_after = _get_rss_mb()
                all_metrics.append(
                    ExecutionMetrics(
                        query_name=qdef["name"],
                        elapsed_s=elapsed,
                        peak_rss_mb=rss_after,
                        rss_delta_mb=rss_after - rss_before,
                        row_count=len(result),
                        success=True,
                    )
                )
            except Exception as exc:
                elapsed = time.perf_counter() - qt0
                rss_after = _get_rss_mb()
                all_metrics.append(
                    ExecutionMetrics(
                        query_name=qdef["name"],
                        elapsed_s=elapsed,
                        peak_rss_mb=rss_after,
                        rss_delta_mb=rss_after - rss_before,
                        row_count=0,
                        success=False,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )

    total_elapsed = time.perf_counter() - t0
    return LoadTestReport(
        scale="custom",
        total_queries=len(all_metrics),
        total_elapsed_s=total_elapsed,
        metrics=all_metrics,
    )
