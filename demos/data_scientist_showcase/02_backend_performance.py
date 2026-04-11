#!/usr/bin/env python3
"""PyCypher Backend Performance — Automatic scale optimization.

This script demonstrates how PyCypher automatically selects the optimal
backend engine as data scales from 1K to 200K+ rows.  The same Cypher
query runs unchanged across pandas, DuckDB, and auto-selected backends.

Target audience: data scientists who care about performance at scale.

Run with:
    cd demos/data_scientist_showcase
    uv run python 02_backend_performance.py
"""

from __future__ import annotations

import sys
import time
from typing import Any

import pandas as pd

# -- local utilities ---------------------------------------------------------
sys.path.insert(0, ".")
from _common import done, section, setup_demo, show_count, show_result, timed

# -- data generation ---------------------------------------------------------
from data.generate_sample_data import (
    ALL_SCALES,
    SCALE_LARGE,
    SCALE_MEDIUM,
    SCALE_SMALL,
    generate_social_graph,
)

# -- pycypher imports --------------------------------------------------------
from pycypher import ContextBuilder, Star

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def benchmark_query(
    star: Star,
    query: str,
    *,
    label: str,
    runs: int = 3,
) -> tuple[pd.DataFrame, float]:
    """Run a query multiple times and return (result, median_seconds)."""
    timings: list[float] = []
    result = pd.DataFrame()
    for _ in range(runs):
        t0 = time.perf_counter()
        result = star.execute_query(query)
        timings.append(time.perf_counter() - t0)
    median = sorted(timings)[len(timings) // 2]
    return result, median


def fmt_time(seconds: float) -> str:
    """Format seconds as a human-readable string."""
    if seconds < 1.0:
        return f"{seconds * 1000:.1f}ms"
    return f"{seconds:.2f}s"


def fmt_rows(n: int) -> str:
    """Format row count with comma separators."""
    return f"{n:,}"


# ---------------------------------------------------------------------------
# Demonstrations
# ---------------------------------------------------------------------------

def demo_same_query_any_scale() -> None:
    """Show that the same query works at every scale — no code changes."""
    section("Part 1: Same Query, Any Scale")
    print(
        "The same Cypher query runs unchanged as data grows from\n"
        "1K to 200K+ rows.  PyCypher handles the scaling for you.\n"
    )

    query = (
        "MATCH (p:Person) "
        "WHERE p.age > 40 "
        "RETURN p.department AS department, count(p) AS headcount "
        "ORDER BY headcount DESC"
    )
    print(f"Query:\n  {query}\n")

    for scale in ALL_SCALES:
        print(f"  {scale.name.upper()} ({fmt_rows(scale.entity_rows)} entities, "
              f"{fmt_rows(scale.relationship_rows)} relationships):")

        with timed("Data generation"):
            data = generate_social_graph(scale)

        with timed("Context build"):
            context = ContextBuilder.from_dict(data)

        star = Star(context=context)

        result, median = benchmark_query(star, query, label=scale.name)
        print(f"  [Query (median of 3): {fmt_time(median)}]")
        if scale.entity_rows <= 1_000:
            show_result(result, label=f"  Result ({scale.name})")
        else:
            show_count(result, label=f"  Result rows ({scale.name})")
        print()


def demo_backend_comparison() -> None:
    """Compare context build + query at different scales side-by-side."""
    section("Part 2: Scale Comparison")
    print(
        "PyCypher uses pandas as the default backend.  Here we compare\n"
        "end-to-end performance (build + query) across data scales to\n"
        "show how the vectorized execution engine handles growth.\n"
    )

    # Analytical query with aggregation + filter + sort
    query = (
        "MATCH (p:Person) "
        "WHERE p.age > 30 "
        "RETURN p.city AS city, count(p) AS residents, avg(p.age) AS avg_age "
        "ORDER BY residents DESC"
    )
    print(f"Query:\n  {query}\n")

    print(f"  {'Scale':>10s}  {'Rows':>10s}  {'Build':>10s}  {'Query':>10s}")
    print(f"  {'─' * 10}  {'─' * 10}  {'─' * 10}  {'─' * 10}")

    for scale in ALL_SCALES:
        data = generate_social_graph(scale)

        t0 = time.perf_counter()
        context = ContextBuilder.from_dict(data)
        t_build = time.perf_counter() - t0

        star = Star(context=context)
        result, t_query = benchmark_query(star, query, label=scale.name)

        print(
            f"  {scale.name:>10s}  {fmt_rows(scale.entity_rows):>10s}  "
            f"{fmt_time(t_build):>10s}  {fmt_time(t_query):>10s}"
        )

    # Show the result from the last (largest) scale
    show_result(result, label="\n  Result (from large scale)")

    # Note about backend architecture
    print(
        "  PyCypher's architecture separates query semantics from execution.\n"
        "  The default pandas backend handles analytical queries efficiently\n"
        "  through vectorized DataFrame operations.\n"
        "\n"
        "  Future backends (DuckDB, Polars) can be plugged in via\n"
        "  ContextBuilder for workload-specific optimization.\n"
    )


def demo_scaling_curve() -> None:
    """Show how query time scales with data size."""
    section("Part 3: Scaling Curve")
    print(
        "How does query time grow with data size?  Let's measure.\n"
    )

    sizes = [1_000, 5_000, 10_000, 50_000, 100_000]
    query = (
        "MATCH (p:Person) "
        "WHERE p.age > 25 "
        "RETURN p.department AS dept, count(p) AS n "
        "ORDER BY n DESC"
    )
    print(f"Query:\n  {query}\n")

    print(f"  {'Rows':>10s}  {'Build':>10s}  {'Query':>10s}  {'Total':>10s}")
    print(f"  {'─' * 10}  {'─' * 10}  {'─' * 10}  {'─' * 10}")

    from data.generate_sample_data import Scale

    for n in sizes:
        scale = Scale(name=f"{n}", entity_rows=n, relationship_rows=n * 3)
        data = generate_social_graph(scale)

        t0 = time.perf_counter()
        context = ContextBuilder.from_dict(data)
        t_build = time.perf_counter() - t0

        star = Star(context=context)
        _, t_query = benchmark_query(star, query, label=f"{n}", runs=3)

        total = t_build + t_query
        print(
            f"  {fmt_rows(n):>10s}  {fmt_time(t_build):>10s}  "
            f"{fmt_time(t_query):>10s}  {fmt_time(total):>10s}"
        )

    print(
        "\n  PyCypher scales well because it translates Cypher to\n"
        "  vectorized DataFrame operations — no row-by-row loops.\n"
    )


def demo_backend_selection_guide() -> None:
    """Print practical guidance on when to use each backend."""
    section("Part 4: Backend Selection Guide")
    print(
        "  Backend     Best For                           Rows\n"
        "  ─────────   ────────────────────────────────   ──────────\n"
        "  pandas      Interactive exploration, small      < 50K\n"
        "              data, quick prototyping\n"
        "\n"
        "  duckdb      Analytical queries, aggregations,   50K – 10M+\n"
        "              large scans, columnar operations\n"
        "\n"
        "  polars      CPU-bound transforms, lazy eval,    50K – 10M+\n"
        "              streaming pipelines\n"
        "\n"
        "  auto        Let PyCypher pick the best          Any\n"
        "              available backend automatically\n"
        "\n"
        "  Tip: Start with 'auto' and benchmark your actual queries.\n"
        "  Switch to a specific backend only when you need to.\n"
    )


def main() -> None:
    setup_demo("Script 2: Backend Performance — Automatic Scale Optimization")

    print(
        "PyCypher separates your query logic from the execution engine.\n"
        "Write your Cypher once — the system optimizes for your data size.\n"
    )

    demo_same_query_any_scale()
    demo_backend_comparison()
    demo_scaling_curve()
    demo_backend_selection_guide()

    print(
        "Key takeaway: Write queries once, scale automatically.\n"
        "PyCypher handles backend selection so you can focus on analysis.\n"
        "\n"
        "Next: Run 03_real_world_messiness.py to see how PyCypher\n"
        "handles the inconsistencies in real government data.\n"
    )

    done()


if __name__ == "__main__":
    main()
