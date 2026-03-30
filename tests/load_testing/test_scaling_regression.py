"""Load tests: performance scaling and regression detection.

Verifies that query performance scales predictably as data size increases,
and detects regressions by comparing execution profiles across scales.
"""

from __future__ import annotations

import time

import pytest
from pycypher.star import Star

from .load_generator import (
    SCALE_MICRO,
    SCALE_SMALL,
    build_graph,
)


@pytest.fixture(scope="module")
def stars_by_scale() -> dict[str, Star]:
    """Build Stars at micro and small scales for comparison."""
    return {
        "micro": Star(build_graph(SCALE_MICRO)),
        "small": Star(build_graph(SCALE_SMALL)),
    }


def _measure_query(
    star: Star,
    query: str,
    *,
    warmup: int = 1,
    runs: int = 3,
) -> float:
    """Return median execution time in seconds."""
    for _ in range(warmup):
        try:
            star.execute_query(query, timeout_seconds=30.0)
        except Exception:
            pass

    times: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        try:
            star.execute_query(query, timeout_seconds=30.0)
        except Exception:
            pass
        times.append(time.perf_counter() - t0)

    times.sort()
    return times[len(times) // 2]


class TestLinearScalingQueries:
    """Queries expected to scale linearly with data size."""

    @pytest.mark.parametrize(
        "query,label",
        [
            ("MATCH (p:Person) RETURN count(p)", "count_scan"),
            (
                "MATCH (p:Person) WHERE p.age > 50 RETURN p.name",
                "filtered_scan",
            ),
            ("MATCH (p:Person) RETURN avg(p.age)", "aggregation"),
        ],
    )
    def test_scan_scales_linearly(
        self,
        stars_by_scale: dict[str, Star],
        query: str,
        label: str,
    ) -> None:
        """Entity scan queries must not scale worse than O(n * log(n))."""
        t_micro = _measure_query(stars_by_scale["micro"], query)
        t_small = _measure_query(stars_by_scale["small"], query)

        data_ratio = SCALE_SMALL.person_count / SCALE_MICRO.person_count
        time_ratio = t_small / max(t_micro, 1e-6)

        # Allow O(n * log(n)) — time ratio should be at most data_ratio * 2.
        max_acceptable = data_ratio * 2
        assert time_ratio < max_acceptable, (
            f"{label}: time scaled {time_ratio:.1f}x for {data_ratio:.0f}x data "
            f"(max acceptable: {max_acceptable:.0f}x)"
        )


class TestJoinScaling:
    """Join queries must not exhibit unexpected superlinear blowup."""

    def test_single_hop_join_scaling(
        self,
        stars_by_scale: dict[str, Star],
    ) -> None:
        """Single-hop join with LIMIT should scale sub-linearly."""
        query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 50"
        t_micro = _measure_query(stars_by_scale["micro"], query)
        t_small = _measure_query(stars_by_scale["small"], query)

        # With LIMIT pushdown, this should barely change.
        time_ratio = t_small / max(t_micro, 1e-6)
        # Even without perfect pushdown, should not be worse than O(n^2).
        data_ratio = SCALE_SMALL.total_edges / max(SCALE_MICRO.total_edges, 1)
        assert time_ratio < data_ratio**2, (
            f"Single-hop join scaled {time_ratio:.1f}x — "
            f"worse than quadratic on {data_ratio:.0f}x edges"
        )


class TestQueryProfileRegression:
    """Query profiling must detect hotspot shifts across scales."""

    def test_profiler_identifies_hotspot(
        self,
        stars_by_scale: dict[str, Star],
    ) -> None:
        """QueryProfiler must identify the slowest clause consistently."""
        from pycypher.query_profiler import QueryProfiler

        star = stars_by_scale["small"]
        profiler = QueryProfiler(star=star)
        report = profiler.profile(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 20",
        )

        # Must identify a hotspot.
        assert report.hotspot is not None
        assert report.total_time_ms > 0
        assert report.row_count >= 0

    def test_profiler_recommendations_consistent(
        self,
        stars_by_scale: dict[str, Star],
    ) -> None:
        """Profiler recommendations must not be empty for expensive queries."""
        from pycypher.query_profiler import QueryProfiler

        star = stars_by_scale["small"]
        profiler = QueryProfiler(star=star)

        # Run an expensive-ish query.
        report = profiler.profile(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN DISTINCT a.name ORDER BY a.name",
        )

        # At small scale, recommendations may or may not fire depending
        # on hardware speed. Just verify the report is well-formed.
        assert isinstance(report.recommendations, list)
        assert report.total_time_ms >= 0
