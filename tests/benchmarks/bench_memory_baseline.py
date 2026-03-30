"""Memory profiling baseline for PyCypher query execution.

Measures memory usage and timing at key materialization points across
queries of increasing complexity and dataset sizes.  Run directly::

    uv run python tests/benchmarks/bench_memory_baseline.py

Or as a pytest file (markers prevent accidental CI runs)::

    uv run pytest tests/benchmarks/bench_memory_baseline.py -v -s

Produces a JSON report at ``tests/benchmarks/baseline_report.json``.
"""

from __future__ import annotations

import gc
import json
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPORT_PATH = Path(__file__).parent / "baseline_report.json"

# Dataset scale factors — number of entities per type
SCALE_FACTORS: list[int] = [100, 1_000, 10_000, 50_000]

# Queries at increasing complexity
QUERIES: dict[str, str] = {
    "simple_scan": "MATCH (n:Person) RETURN n.name",
    "filtered_scan": "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
    "single_hop": ("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name, m.name"),
    "filtered_hop": (
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) "
        "WHERE n.age > 25 RETURN n.name, m.name, r.since"
    ),
    "two_hop": (
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
        "RETURN a.name, c.name"
    ),
    "aggregation_count": ("MATCH (n:Person) RETURN n.dept, count(n) AS cnt"),
    "aggregation_avg": ("MATCH (n:Person) RETURN n.dept, avg(n.salary) AS avg_sal"),
    "varlength_path": (
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name"
    ),
}


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------


def generate_persons(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
    """Generate a Person entity DataFrame with *n* rows."""
    depts = ["eng", "mktg", "sales", "ops", "hr"]
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n + 1),
            "name": [f"Person_{i}" for i in range(1, n + 1)],
            "age": rng.integers(18, 65, size=n),
            "dept": rng.choice(depts, size=n),
            "salary": rng.integers(40_000, 200_000, size=n),
        },
    )


def generate_knows(
    n_persons: int,
    *,
    avg_degree: int = 5,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate a KNOWS relationship DataFrame.

    Creates approximately ``n_persons * avg_degree`` directed edges.
    """
    n_edges = n_persons * avg_degree
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    # Remove self-loops
    mask = sources != targets
    sources = sources[mask]
    targets = targets[mask]
    n_actual = len(sources)
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n_actual + 1),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": rng.integers(2000, 2026, size=n_actual),
        },
    )


def build_context(n_persons: int, *, rng: np.random.Generator) -> Context:
    """Build a Context with *n_persons* Person entities and KNOWS relationships."""
    persons_df = generate_persons(n_persons, rng=rng)
    knows_df = generate_knows(n_persons, rng=rng)

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(persons_df.columns),
        source_obj_attribute_map={c: c for c in persons_df.columns if c != "__ID__"},
        attribute_map={c: c for c in persons_df.columns if c != "__ID__"},
        source_obj=persons_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows_df.columns),
        source_obj_attribute_map={
            c: c
            for c in knows_df.columns
            if c not in {"__ID__", "__SOURCE__", "__TARGET__"}
        },
        attribute_map={
            c: c
            for c in knows_df.columns
            if c not in {"__ID__", "__SOURCE__", "__TARGET__"}
        },
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


# ---------------------------------------------------------------------------
# Measurement infrastructure
# ---------------------------------------------------------------------------


@dataclass
class Measurement:
    """Single benchmark measurement."""

    scale: int
    query_name: str
    query_text: str
    peak_memory_bytes: int
    net_memory_bytes: int
    elapsed_seconds: float
    result_rows: int
    result_columns: int
    error: str | None = None


@dataclass
class MaterializationPoint:
    """Memory snapshot at a specific materialization point."""

    point_name: str
    scale: int
    memory_bytes: int
    description: str


@dataclass
class BenchmarkReport:
    """Full benchmark report."""

    timestamp: str = ""
    python_version: str = ""
    measurements: list[Measurement] = field(default_factory=list)
    materialization_points: list[MaterializationPoint] = field(
        default_factory=list,
    )
    scaling_analysis: dict[str, Any] = field(default_factory=dict)


def measure_query(
    star: Star,
    query_name: str,
    query_text: str,
    scale: int,
) -> Measurement:
    """Execute a query and measure memory + timing."""
    gc.collect()
    tracemalloc.start()

    snapshot_before = tracemalloc.take_snapshot()
    t0 = time.perf_counter()

    try:
        result = star.execute_query(query_text)
        elapsed = time.perf_counter() - t0
        snapshot_after = tracemalloc.take_snapshot()

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Compute net allocation
        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        net = sum(s.size_diff for s in stats if s.size_diff > 0)

        return Measurement(
            scale=scale,
            query_name=query_name,
            query_text=query_text,
            peak_memory_bytes=peak,
            net_memory_bytes=net,
            elapsed_seconds=round(elapsed, 4),
            result_rows=len(result),
            result_columns=len(result.columns),
        )
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        tracemalloc.stop()
        return Measurement(
            scale=scale,
            query_name=query_name,
            query_text=query_text,
            peak_memory_bytes=0,
            net_memory_bytes=0,
            elapsed_seconds=round(elapsed, 4),
            result_rows=0,
            result_columns=0,
            error=f"{type(exc).__name__}: {exc}",
        )


def measure_materialization_points(
    scale: int,
    *,
    rng: np.random.Generator,
) -> list[MaterializationPoint]:
    """Measure memory at key materialization points for a given scale."""
    points: list[MaterializationPoint] = []

    # 1. Data generation baseline
    gc.collect()
    tracemalloc.start()
    persons_df = generate_persons(scale, rng=rng)
    knows_df = generate_knows(scale, rng=rng)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    points.append(
        MaterializationPoint(
            point_name="data_generation",
            scale=scale,
            memory_bytes=peak,
            description="Raw DataFrame creation (Person + KNOWS)",
        ),
    )

    # 2. Context construction
    gc.collect()
    tracemalloc.start()
    ctx = build_context(scale, rng=rng)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    points.append(
        MaterializationPoint(
            point_name="context_construction",
            scale=scale,
            memory_bytes=peak,
            description="Context + EntityTable + RelationshipTable wrapping",
        ),
    )

    # 3. Entity scan (BindingFrame creation)
    from pycypher.binding_frame import EntityScan

    gc.collect()
    tracemalloc.start()
    scan = EntityScan(entity_type="Person", var_name="n")
    bf = scan.scan(ctx)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    points.append(
        MaterializationPoint(
            point_name="entity_scan",
            scale=scale,
            memory_bytes=peak,
            description="EntityScan.scan() → BindingFrame with all Person IDs",
        ),
    )

    # 4. Relationship scan
    from pycypher.binding_frame import RelationshipScan

    gc.collect()
    tracemalloc.start()
    rel_scan = RelationshipScan(
        rel_type="KNOWS",
        rel_var="r",
    )
    rel_bf = rel_scan.scan(ctx)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    points.append(
        MaterializationPoint(
            point_name="relationship_scan",
            scale=scale,
            memory_bytes=peak,
            description="RelationshipScan.scan() → BindingFrame with all KNOWS relationships",
        ),
    )

    # 5. Join operation
    gc.collect()
    tracemalloc.start()
    joined = bf.join(rel_bf, "n", rel_scan.src_col)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    points.append(
        MaterializationPoint(
            point_name="join_operation",
            scale=scale,
            memory_bytes=peak,
            description="BindingFrame.join() via pd.merge (entity-to-relationship)",
        ),
    )

    # 6. Property lookup
    gc.collect()
    tracemalloc.start()
    _ = joined.get_property("n", "name")
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    points.append(
        MaterializationPoint(
            point_name="property_lookup",
            scale=scale,
            memory_bytes=peak,
            description="BindingFrame.get_property() with cache population",
        ),
    )

    return points


# ---------------------------------------------------------------------------
# Scaling analysis
# ---------------------------------------------------------------------------


def compute_scaling_factors(
    measurements: list[Measurement],
) -> dict[str, Any]:
    """Estimate how memory and time scale with dataset size."""
    analysis: dict[str, Any] = {}

    query_names = sorted({m.query_name for m in measurements})
    for qname in query_names:
        qm = sorted(
            [m for m in measurements if m.query_name == qname and m.error is None],
            key=lambda x: x.scale,
        )
        if len(qm) < 2:
            continue

        scales = [m.scale for m in qm]
        peaks = [m.peak_memory_bytes for m in qm]
        times = [m.elapsed_seconds for m in qm]
        rows = [m.result_rows for m in qm]

        # Estimate memory scaling: ratio of peak memory / scale
        mem_per_entity = [p / s for p, s in zip(peaks, scales)]
        time_per_entity = [t / s for t, s in zip(times, scales)]

        # Estimate scaling order by log-log slope
        if scales[-1] > scales[0] and peaks[-1] > 0 and peaks[0] > 0:
            log_scale_ratio = np.log(scales[-1] / scales[0])
            log_mem_ratio = np.log(max(peaks[-1], 1) / max(peaks[0], 1))
            log_time_ratio = np.log(max(times[-1], 1e-9) / max(times[0], 1e-9))
            mem_order = round(log_mem_ratio / log_scale_ratio, 2)
            time_order = round(log_time_ratio / log_scale_ratio, 2)
        else:
            mem_order = None
            time_order = None

        analysis[qname] = {
            "scales": scales,
            "peak_memory_bytes": peaks,
            "elapsed_seconds": times,
            "result_rows": rows,
            "bytes_per_entity": [round(x, 1) for x in mem_per_entity],
            "seconds_per_entity": [round(x, 8) for x in time_per_entity],
            "estimated_memory_order": mem_order,
            "estimated_time_order": time_order,
        }

    return analysis


# ---------------------------------------------------------------------------
# Main benchmark runner
# ---------------------------------------------------------------------------


def run_benchmark(
    scales: list[int] | None = None,
    queries: dict[str, str] | None = None,
) -> BenchmarkReport:
    """Run the full benchmark suite and return a report."""
    scales = scales or SCALE_FACTORS
    queries = queries or QUERIES
    rng = np.random.default_rng(42)

    report = BenchmarkReport(
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
        python_version=sys.version,
    )

    print(f"{'=' * 70}")
    print("PyCypher Memory Baseline Benchmark")
    print(f"{'=' * 70}")
    print(f"Scales: {scales}")
    print(f"Queries: {list(queries.keys())}")
    print()

    for scale in scales:
        print(f"\n--- Scale: {scale:,} entities ---")

        # Build context for this scale
        ctx = build_context(scale, rng=rng)
        star = Star(context=ctx)

        # Measure materialization points
        mat_points = measure_materialization_points(scale, rng=rng)
        report.materialization_points.extend(mat_points)
        for mp in mat_points:
            print(
                f"  [{mp.point_name}] {mp.memory_bytes / 1024:.1f} KB — {mp.description}",
            )

        # Measure queries
        for qname, qtext in queries.items():
            m = measure_query(star, qname, qtext, scale)
            report.measurements.append(m)

            status = "OK" if m.error is None else f"ERR: {m.error[:60]}"
            print(
                f"  {qname:30s}  "
                f"peak={m.peak_memory_bytes / 1024:>10.1f} KB  "
                f"time={m.elapsed_seconds:>8.4f}s  "
                f"rows={m.result_rows:>8,}  "
                f"{status}",
            )

    # Scaling analysis
    report.scaling_analysis = compute_scaling_factors(report.measurements)

    print(f"\n{'=' * 70}")
    print("Scaling Analysis (log-log slope: 1.0=linear, 2.0=quadratic)")
    print(f"{'=' * 70}")
    for qname, analysis in report.scaling_analysis.items():
        mem_order = analysis.get("estimated_memory_order", "?")
        time_order = analysis.get("estimated_time_order", "?")
        print(
            f"  {qname:30s}  memory~O(n^{mem_order})  time~O(n^{time_order})",
        )

    return report


def save_report(report: BenchmarkReport) -> None:
    """Save the report to JSON."""
    data = asdict(report)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(data, indent=2, default=str))
    print(f"\nReport saved to {REPORT_PATH}")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point."""
    report = run_benchmark()
    save_report(report)


if __name__ == "__main__":
    main()
