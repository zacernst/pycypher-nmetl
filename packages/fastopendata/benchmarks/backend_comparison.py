"""DuckDB vs Pandas backend benchmark on a 1M-row pipeline.

Builds a synthetic 1-million-row ``GraphPipeline`` and times the same
``MATCH (x:X) RETURN count(x)`` query under both the Pandas and DuckDB
backends. Prints a summary table to stdout — there are no assertions,
this is meant for human eyes (and CI artefact attachments).

Run from the monorepo root::

    uv run python packages/fastopendata/benchmarks/backend_comparison.py

Optional flags::

    --rows N          Total entity rows (default: 1_000_000)
    --warmup K        Untimed warmup runs per backend (default: 1)
    --iterations N    Timed iterations per backend (default: 3)

The script intentionally does NOT use pytest. Phase 4 acceptance treats
the benchmark as a one-shot measurement, not a regression gate — a CI
gate would need stable infra and statistical thresholds the integration
suite doesn't provide.
"""

from __future__ import annotations

import argparse
import statistics
import time

import pandas as pd

from fastopendata.pipeline import GraphPipeline


def _build_pipeline(n_rows: int) -> GraphPipeline:
    """Create a pipeline with a single ``X`` entity holding ``n_rows`` rows.

    Mixed numeric + string columns approximate real census/contract data
    well enough that the COUNT query touches a representative working set.
    """
    df = pd.DataFrame({
        "__ID__": list(range(n_rows)),
        "category": [f"cat_{i % 50}" for i in range(n_rows)],
        "value": [float(i % 1000) for i in range(n_rows)],
    })

    pipeline = GraphPipeline()
    pipeline.add_entity_dataframe("X", df, id_col="__ID__")
    return pipeline


_QUERIES: list[tuple[str, str]] = [
    # (label, cypher) — both queries take a ``$threshold`` parameter so we
    # can dodge Star's per-(query, params) result cache by varying the
    # bound value across timed iterations. Without that, the second call
    # would always be a cache hit and we'd be benchmarking the cache.
    (
        "filter_count",
        "MATCH (x:X) WHERE x.value > $threshold RETURN count(x) AS total",
    ),
    (
        "filter_with_return",
        "MATCH (x:X) WHERE x.value > $threshold "
        "RETURN x.category AS cat, x.value AS v LIMIT 100",
    ),
]


def _time_query(
    pipeline: GraphPipeline,
    backend: str,
    cypher: str,
    *,
    warmup: int,
    iterations: int,
) -> tuple[float, list[float]]:
    """Build a ``Star`` for *backend* and time *cypher*.

    Each timed iteration uses a different ``$threshold`` value so the
    Star result cache cannot serve the call. Returns
    ``(median_ms, all_runs_ms)``.
    """
    star = pipeline.build_star(backend=backend)

    # Untimed warmup so the JIT / pandas type inference has settled.
    for i in range(warmup):
        star.execute_query(cypher, parameters={"threshold": 100 + i})

    timings_ms: list[float] = []
    for i in range(iterations):
        # A unique threshold per iteration → unique cache key → cache miss.
        threshold = 200 + i * 137
        t0 = time.perf_counter()
        star.execute_query(cypher, parameters={"threshold": threshold})
        t1 = time.perf_counter()
        timings_ms.append((t1 - t0) * 1000.0)

    return statistics.median(timings_ms), timings_ms


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--rows", type=int, default=1_000_000,
        help="Total entity rows (default: 1,000,000).",
    )
    parser.add_argument(
        "--warmup", type=int, default=1,
        help="Untimed warmup runs per backend (default: 1).",
    )
    parser.add_argument(
        "--iterations", type=int, default=3,
        help="Timed iterations per backend (default: 3).",
    )
    args = parser.parse_args()

    print("=" * 72)
    print("DuckDB vs Pandas backend benchmark — fastopendata GraphPipeline")
    print("=" * 72)
    print(f"  rows       : {args.rows:,}")
    print(f"  warmup     : {args.warmup}")
    print(f"  iterations : {args.iterations}")
    print()

    print("Building pipeline...")
    t0 = time.perf_counter()
    pipeline = _build_pipeline(args.rows)
    build_ms = (time.perf_counter() - t0) * 1000.0
    print(f"  pipeline build: {build_ms:.1f} ms")
    print()

    # results[(label, backend)] -> (median_ms, runs_ms)
    results: dict[tuple[str, str], tuple[float, list[float]]] = {}
    for label, cypher in _QUERIES:
        print(f"Query: {label}  →  {cypher}")
        for backend in ("pandas", "duckdb"):
            try:
                median_ms, runs = _time_query(
                    pipeline, backend, cypher,
                    warmup=args.warmup, iterations=args.iterations,
                )
            except Exception as exc:  # pragma: no cover
                print(f"  backend={backend:<7} FAILED: {exc!r}")
                results[(label, backend)] = (float("nan"), [])
                continue
            print(
                f"  backend={backend:<7} median={median_ms:7.2f} ms  "
                f"runs={[round(r, 2) for r in runs]}"
            )
            results[(label, backend)] = (median_ms, runs)
        print()

    print("Summary")
    print("-" * 72)
    print(
        f"{'query':<14} {'backend':<8} {'rows':>12} "
        f"{'median (ms)':>14} {'min (ms)':>10} {'max (ms)':>10}"
    )
    for label, _ in _QUERIES:
        for backend in ("pandas", "duckdb"):
            median_ms, runs = results.get(
                (label, backend), (float("nan"), []),
            )
            if not runs:
                print(
                    f"{label:<14} {backend:<8} {args.rows:>12,} "
                    f"{'FAILED':>14} {'-':>10} {'-':>10}"
                )
                continue
            print(
                f"{label:<14} {backend:<8} {args.rows:>12,} "
                f"{median_ms:>14.2f} {min(runs):>10.2f} {max(runs):>10.2f}"
            )
    print("-" * 72)

    # Per-query speedup commentary.
    for label, _ in _QUERIES:
        p_med, _ = results.get((label, "pandas"), (float("nan"), []))
        d_med, _ = results.get((label, "duckdb"), (float("nan"), []))
        if p_med <= 0 or d_med <= 0 or p_med != p_med or d_med != d_med:
            # NaN or zero — comparison meaningless.
            continue
        if d_med < p_med:
            print(f"  {label}: DuckDB is {p_med / d_med:.2f}x faster than Pandas")
        else:
            print(f"  {label}: Pandas is {d_med / p_med:.2f}x faster than DuckDB")


if __name__ == "__main__":
    main()
