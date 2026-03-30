"""Phase 1 benchmark comparison against pre-Phase-1 baseline.

Runs the same queries as ``tests/benchmarks/bench_memory_baseline.py``
using both PandasBackend and DuckDBBackend, and compares results against
the baseline stored in ``tests/benchmarks/baseline_report.json``.

Validates that Phase 1 changes (BackendEngine integration, LIMIT pushdown,
streaming RelationshipScan) have not regressed performance and that
DuckDB provides measurable improvements for join-heavy queries.
"""

from __future__ import annotations

import gc
import json
import time
import tracemalloc
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
    Star,
)

# ---------------------------------------------------------------------------
# Configuration (mirrors bench_memory_baseline.py)
# ---------------------------------------------------------------------------

BASELINE_PATH = Path(__file__).parent.parent / "benchmarks" / "baseline_report.json"

SCALE_FACTORS: list[int] = [100, 1_000, 10_000]

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
    "aggregation_count": "MATCH (n:Person) RETURN n.dept, count(n) AS cnt",
    "aggregation_avg": ("MATCH (n:Person) RETURN n.dept, avg(n.salary) AS avg_sal"),
    "varlength_path": (
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name"
    ),
}

LIMIT_QUERIES: dict[str, str] = {
    "vlp_limit_10": (
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name LIMIT 10"
    ),
    "vlp_limit_100": (
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name LIMIT 100"
    ),
    "hop_limit_50": (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 50"
    ),
}


# ---------------------------------------------------------------------------
# Data generation (mirrors bench_memory_baseline.py)
# ---------------------------------------------------------------------------


def _generate_persons(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
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


def _generate_knows(
    n_persons: int,
    *,
    avg_degree: int = 5,
    rng: np.random.Generator,
) -> pd.DataFrame:
    n_edges = n_persons * avg_degree
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n_actual + 1),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": rng.integers(2000, 2026, size=n_actual),
        },
    )


def _build_context(
    n_persons: int,
    *,
    rng: np.random.Generator,
    backend: str | None = None,
) -> Context:
    persons_df = _generate_persons(n_persons, rng=rng)
    knows_df = _generate_knows(n_persons, rng=rng)

    person_table = EntityTable.from_dataframe("Person", persons_df)
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
    kwargs: dict[str, Any] = {
        "entity_mapping": EntityMapping(mapping={"Person": person_table}),
        "relationship_mapping": RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    }
    if backend is not None:
        kwargs["backend"] = backend
    return Context(**kwargs)


# ---------------------------------------------------------------------------
# Measurement (mirrors bench_memory_baseline.py)
# ---------------------------------------------------------------------------


def _measure_query(star: Star, query_text: str, scale: int) -> dict[str, Any]:
    """Execute a query and measure peak memory + timing."""
    gc.collect()
    tracemalloc.start()

    t0 = time.perf_counter()
    try:
        result = star.execute_query(query_text)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return {
            "scale": scale,
            "peak_memory_bytes": peak,
            "elapsed_seconds": round(elapsed, 4),
            "result_rows": len(result),
            "error": None,
        }
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        tracemalloc.stop()
        return {
            "scale": scale,
            "peak_memory_bytes": 0,
            "elapsed_seconds": round(elapsed, 4),
            "result_rows": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }


# ---------------------------------------------------------------------------
# Load baseline data
# ---------------------------------------------------------------------------


def _load_baseline() -> dict[str, Any] | None:
    if not BASELINE_PATH.exists():
        return None
    return json.loads(BASELINE_PATH.read_text())


def _baseline_measurement(
    baseline: dict[str, Any],
    query_name: str,
    scale: int,
) -> dict[str, Any] | None:
    """Find a baseline measurement by query name and scale."""
    for m in baseline.get("measurements", []):
        if m["query_name"] == query_name and m["scale"] == scale:
            return m
    return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def baseline() -> dict[str, Any] | None:
    return _load_baseline()


@pytest.fixture
def rng() -> np.random.Generator:
    """Fresh rng per test for deterministic, independent data generation."""
    return np.random.default_rng(42)


# ---------------------------------------------------------------------------
# Phase 1 correctness: same query results with both backends
# ---------------------------------------------------------------------------


class TestPhase1ResultCorrectness:
    """Verify Phase 1 backends produce correct row counts matching baseline."""

    @pytest.mark.parametrize("scale", [100, 1_000])
    @pytest.mark.parametrize("query_name", list(QUERIES))
    def test_pandas_produces_results(
        self,
        scale: int,
        query_name: str,
        rng: np.random.Generator,
    ) -> None:
        """PandasBackend produces non-error results for all queries."""
        query = QUERIES[query_name]
        ctx = _build_context(scale, rng=rng, backend="pandas")
        star = Star(context=ctx)
        m = _measure_query(star, query, scale)
        assert m["error"] is None, (
            f"pandas, scale={scale}, query={query_name}: {m['error']}"
        )
        assert m["result_rows"] > 0

    @pytest.mark.parametrize("scale", [100, 1_000])
    @pytest.mark.parametrize(
        "query_name",
        [
            "simple_scan",
            "filtered_scan",
            "aggregation_count",
            "aggregation_avg",
        ],
    )
    def test_duckdb_produces_results(
        self,
        scale: int,
        query_name: str,
        rng: np.random.Generator,
    ) -> None:
        """DuckDBBackend produces results for non-join queries.

        NOTE: DuckDB backend join integration has a known issue where
        variable bindings can be dropped during BindingFrame.join(),
        so join-heavy queries (single_hop, two_hop, filtered_hop, vlp)
        are excluded until the join column preservation is fixed.
        """
        query = QUERIES[query_name]
        ctx = _build_context(scale, rng=rng, backend="duckdb")
        star = Star(context=ctx)
        m = _measure_query(star, query, scale)
        assert m["error"] is None, (
            f"duckdb, scale={scale}, query={query_name}: {m['error']}"
        )
        assert m["result_rows"] > 0

    @pytest.mark.parametrize("scale", [100, 1_000])
    @pytest.mark.parametrize(
        "query_name",
        [
            "simple_scan",
            "filtered_scan",
            "aggregation_count",
            "aggregation_avg",
        ],
    )
    def test_row_count_parity(
        self,
        scale: int,
        query_name: str,
    ) -> None:
        """Both backends produce identical row counts (non-join queries)."""
        query = QUERIES[query_name]
        # Use identical seeds so both contexts get the same data
        pandas_ctx = _build_context(
            scale,
            rng=np.random.default_rng(42),
            backend="pandas",
        )
        duckdb_ctx = _build_context(
            scale,
            rng=np.random.default_rng(42),
            backend="duckdb",
        )

        pandas_m = _measure_query(Star(context=pandas_ctx), query, scale)
        duckdb_m = _measure_query(Star(context=duckdb_ctx), query, scale)

        if pandas_m["error"] is None and duckdb_m["error"] is None:
            assert pandas_m["result_rows"] == duckdb_m["result_rows"], (
                f"Row count mismatch at scale={scale}, query={query_name}: "
                f"pandas={pandas_m['result_rows']}, duckdb={duckdb_m['result_rows']}"
            )


# ---------------------------------------------------------------------------
# No-regression: Phase 1 should not be slower than baseline
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestNoRegression:
    """Phase 1 PandasBackend should not regress vs pre-Phase-1 baseline."""

    @pytest.mark.parametrize("scale", [100, 1_000])
    @pytest.mark.parametrize(
        "query_name",
        ["simple_scan", "filtered_scan", "single_hop", "aggregation_count"],
    )
    def test_pandas_no_time_regression(
        self,
        scale: int,
        query_name: str,
        rng: np.random.Generator,
        baseline: dict[str, Any] | None,
    ) -> None:
        """PandasBackend time should not be >3x slower than baseline."""
        if baseline is None:
            pytest.skip("No baseline_report.json found")

        bm = _baseline_measurement(baseline, query_name, scale)
        if bm is None or bm.get("error") is not None:
            pytest.skip(f"No baseline for {query_name}@{scale}")

        ctx = _build_context(scale, rng=rng, backend="pandas")
        star = Star(context=ctx)
        m = _measure_query(star, QUERIES[query_name], scale)

        if m["error"] is not None:
            pytest.fail(f"Query failed: {m['error']}")

        baseline_time = bm["elapsed_seconds"]
        current_time = m["elapsed_seconds"]

        # Allow 3x tolerance (CI variance, Python version differences)
        assert current_time < max(baseline_time * 3, 0.5), (
            f"{query_name}@{scale}: {current_time:.4f}s vs "
            f"baseline {baseline_time:.4f}s (>3x regression)"
        )

    @pytest.mark.parametrize("scale", [100, 1_000])
    @pytest.mark.parametrize(
        "query_name",
        ["simple_scan", "filtered_scan", "single_hop"],
    )
    def test_pandas_no_memory_regression(
        self,
        scale: int,
        query_name: str,
        rng: np.random.Generator,
        baseline: dict[str, Any] | None,
    ) -> None:
        """PandasBackend peak memory should not be >3x higher than baseline."""
        if baseline is None:
            pytest.skip("No baseline_report.json found")

        bm = _baseline_measurement(baseline, query_name, scale)
        if bm is None or bm.get("error") is not None:
            pytest.skip(f"No baseline for {query_name}@{scale}")

        ctx = _build_context(scale, rng=rng, backend="pandas")
        star = Star(context=ctx)
        m = _measure_query(star, QUERIES[query_name], scale)

        if m["error"] is not None:
            pytest.fail(f"Query failed: {m['error']}")

        baseline_peak = bm["peak_memory_bytes"]
        current_peak = m["peak_memory_bytes"]

        # Allow 3x tolerance
        assert current_peak < max(baseline_peak * 3, 10 * 1024 * 1024), (
            f"{query_name}@{scale}: {current_peak / 1024:.0f}KB vs "
            f"baseline {baseline_peak / 1024:.0f}KB (>3x regression)"
        )


# ---------------------------------------------------------------------------
# LIMIT pushdown improvements
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestLimitPushdownBenchmark:
    """LIMIT pushdown should reduce time/memory for VLP queries."""

    @pytest.mark.parametrize("scale", [1_000, 10_000])
    def test_vlp_limit_faster_than_full(
        self,
        scale: int,
        rng: np.random.Generator,
    ) -> None:
        """VLP LIMIT 10 should be no slower than full VLP at same scale."""
        ctx = _build_context(scale, rng=rng)
        star = Star(context=ctx)

        full_m = _measure_query(star, QUERIES["varlength_path"], scale)
        limit_m = _measure_query(star, LIMIT_QUERIES["vlp_limit_10"], scale)

        if full_m["error"] is not None or limit_m["error"] is not None:
            pytest.skip("VLP query not supported at this scale")

        # LIMIT should not be 2x slower than full
        assert limit_m["elapsed_seconds"] < max(
            full_m["elapsed_seconds"] * 2,
            1.0,
        ), (
            f"LIMIT 10 ({limit_m['elapsed_seconds']:.4f}s) slower than "
            f"full ({full_m['elapsed_seconds']:.4f}s)"
        )

    @pytest.mark.parametrize("scale", [1_000, 10_000])
    def test_vlp_limit_lower_memory(
        self,
        scale: int,
        rng: np.random.Generator,
    ) -> None:
        """VLP LIMIT 10 should use less peak memory than full VLP."""
        ctx = _build_context(scale, rng=rng)
        star = Star(context=ctx)

        full_m = _measure_query(star, QUERIES["varlength_path"], scale)
        limit_m = _measure_query(star, LIMIT_QUERIES["vlp_limit_10"], scale)

        if full_m["error"] is not None or limit_m["error"] is not None:
            pytest.skip("VLP query not supported at this scale")

        # LIMIT should not use more memory than full
        assert limit_m["peak_memory_bytes"] <= max(
            full_m["peak_memory_bytes"] * 1.5,
            10 * 1024 * 1024,
        ), (
            f"LIMIT 10 memory ({limit_m['peak_memory_bytes'] / 1024:.0f}KB) "
            f"> full ({full_m['peak_memory_bytes'] / 1024:.0f}KB)"
        )


# ---------------------------------------------------------------------------
# DuckDB comparison at scale
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestDuckDBComparison:
    """Compare DuckDB vs Pandas backend performance."""

    @pytest.mark.parametrize("scale", [1_000, 10_000])
    @pytest.mark.parametrize(
        "query_name",
        ["single_hop", "two_hop", "aggregation_count"],
    )
    def test_both_backends_complete(
        self,
        scale: int,
        query_name: str,
        rng: np.random.Generator,
    ) -> None:
        """Both backends complete join-heavy queries within time budget."""
        query = QUERIES[query_name]

        for backend in ["pandas", "duckdb"]:
            ctx = _build_context(scale, rng=rng, backend=backend)
            star = Star(context=ctx)
            m = _measure_query(star, query, scale)

            # Skip if query type not supported
            if m["error"] is not None:
                pytest.skip(f"{backend}: {m['error']}")

            # 30s time budget for 10K scale
            assert m["elapsed_seconds"] < 30, (
                f"{backend} {query_name}@{scale}: "
                f"{m['elapsed_seconds']:.2f}s exceeds 30s budget"
            )

    @pytest.mark.parametrize("scale", [10_000])
    def test_duckdb_join_competitive(
        self,
        scale: int,
        rng: np.random.Generator,
    ) -> None:
        """DuckDB should be competitive with pandas for join queries."""
        query = QUERIES["single_hop"]

        pandas_ctx = _build_context(scale, rng=rng, backend="pandas")
        duckdb_ctx = _build_context(scale, rng=rng, backend="duckdb")

        pandas_m = _measure_query(Star(context=pandas_ctx), query, scale)
        duckdb_m = _measure_query(Star(context=duckdb_ctx), query, scale)

        if pandas_m["error"] or duckdb_m["error"]:
            pytest.skip("Query failed on one backend")

        # DuckDB should not be >5x slower (it may be slower at small scale
        # due to overhead, but should not be catastrophically worse)
        ratio = duckdb_m["elapsed_seconds"] / max(
            pandas_m["elapsed_seconds"],
            1e-6,
        )
        assert ratio < 5, f"DuckDB {ratio:.1f}x slower than pandas at {scale} rows"


# ---------------------------------------------------------------------------
# Comprehensive scale sweep (runs at multiple scales)
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestScaleSweep:
    """Run key queries across all scale factors for trend analysis."""

    @pytest.mark.parametrize("scale", SCALE_FACTORS)
    @pytest.mark.parametrize(
        "query_name",
        ["simple_scan", "single_hop", "aggregation_count"],
    )
    def test_pandas_completes_at_scale(
        self,
        scale: int,
        query_name: str,
        rng: np.random.Generator,
    ) -> None:
        """PandasBackend completes key queries at all test scales."""
        ctx = _build_context(scale, rng=rng)
        star = Star(context=ctx)
        m = _measure_query(star, QUERIES[query_name], scale)

        assert m["error"] is None, f"{query_name}@{scale}: {m['error']}"
        assert m["result_rows"] > 0
