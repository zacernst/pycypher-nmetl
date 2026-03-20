"""TDD tests for QueryProfiler + InstrumentedBackend integration.

Verifies that the profiler aggregates clause-level and operation-level
observability into a unified diagnostic summary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pytest
from pycypher.backend_engine import InstrumentedBackend, PandasBackend
from pycypher.query_profiler import ProfileReport, QueryProfiler


@dataclass(frozen=True)
class _FakeReport:
    """Minimal stand-in for a profile report used in metrics_summary tests."""

    clause_timings: dict[str, float]
    backend_timings: dict[str, dict[str, float]]
    total_time_ms: float


class TestProfileReportBackendTimings:
    """ProfileReport must include backend operation timings."""

    def test_backend_timings_field_exists(self) -> None:
        report = ProfileReport(
            query="MATCH (n) RETURN n",
            total_time_ms=10.0,
            parse_time_ms=1.0,
            plan_time_ms=1.0,
            clause_timings={"Match": 5.0, "Return": 3.0},
            row_count=10,
            hotspot="Match",
            recommendations=[],
            memory_delta_mb=0.1,
            backend_timings={
                "join": {"count": 2, "total_ms": 3.5},
                "filter": {"count": 1, "total_ms": 0.8},
            },
        )
        assert "join" in report.backend_timings
        assert report.backend_timings["join"]["count"] == 2
        assert report.backend_timings["join"]["total_ms"] == 3.5

    def test_backend_timings_default_empty(self) -> None:
        report = ProfileReport(
            query="MATCH (n) RETURN n",
            total_time_ms=10.0,
            parse_time_ms=1.0,
            plan_time_ms=1.0,
            clause_timings={},
            row_count=0,
            hotspot=None,
            recommendations=[],
        )
        assert report.backend_timings == {}

    def test_str_includes_backend_timings(self) -> None:
        report = ProfileReport(
            query="MATCH (n) RETURN n",
            total_time_ms=10.0,
            parse_time_ms=1.0,
            plan_time_ms=1.0,
            clause_timings={"Match": 5.0},
            row_count=10,
            hotspot="Match",
            recommendations=[],
            backend_timings={"join": {"count": 3, "total_ms": 4.2}},
        )
        text = str(report)
        assert "Backend" in text or "backend" in text
        assert "join" in text


class TestQueryProfilerBackendIntegration:
    """QueryProfiler must collect InstrumentedBackend timings when available."""

    def test_profiler_accepts_backend_parameter(self) -> None:
        """QueryProfiler constructor accepts an optional instrumented backend."""
        ib = InstrumentedBackend(PandasBackend())
        profiler = QueryProfiler(star=_make_fake_star(), backend=ib)
        assert profiler.backend is ib

    def test_profiler_backend_defaults_none(self) -> None:
        profiler = QueryProfiler(star=_make_fake_star())
        assert profiler.backend is None

    def test_profile_collects_backend_timings(self) -> None:
        """When an InstrumentedBackend is attached, profile() captures its timing_summary."""
        ib = InstrumentedBackend(PandasBackend())
        # Simulate some backend work before profiling
        star = _make_fake_star(backend=ib)
        profiler = QueryProfiler(star=star, backend=ib)
        report = profiler.profile("MATCH (n) RETURN n")
        # backend_timings should be a dict (possibly empty if fake star doesn't use backend)
        assert isinstance(report.backend_timings, dict)


class TestMetricsSummary:
    """QueryProfiler.metrics_summary() combines clause + backend timings."""

    def test_metrics_summary_structure(self) -> None:
        ib = InstrumentedBackend(PandasBackend())
        star = _make_fake_star(backend=ib)
        profiler = QueryProfiler(star=star, backend=ib)
        profiler.profile("MATCH (n) RETURN n")
        summary = profiler.metrics_summary()
        assert "query_count" in summary
        assert "clause_timings" in summary
        assert "backend_timings" in summary
        assert summary["query_count"] == 1

    def test_metrics_summary_empty_when_no_history(self) -> None:
        profiler = QueryProfiler(star=_make_fake_star())
        summary = profiler.metrics_summary()
        assert summary["query_count"] == 0
        assert summary["clause_timings"] == {}
        assert summary["backend_timings"] == {}

    def test_metrics_summary_aggregates_multiple_profiles(self) -> None:
        ib = InstrumentedBackend(PandasBackend())
        star = _make_fake_star(backend=ib)
        profiler = QueryProfiler(star=star, backend=ib)
        profiler.profile("MATCH (n) RETURN n")
        profiler.profile("MATCH (n) RETURN n")
        summary = profiler.metrics_summary()
        assert summary["query_count"] == 2


# ---------------------------------------------------------------------------
# Helpers — fake Star for unit-testing without real query execution
# ---------------------------------------------------------------------------


@dataclass
class _FakeStar:
    """Minimal Star stand-in that returns an empty DataFrame."""

    _last_clause_timings: dict[str, float] = field(default_factory=dict)
    _last_parse_time_ms: float = 1.0
    _last_plan_time_ms: float = 0.5
    _backend: Any = None

    def execute_query(
        self,
        query: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Return empty DataFrame, simulating a trivial query."""
        self._last_clause_timings = {"Match": 2.0, "Return": 1.0}
        # If we have a backend, do a trivial operation to generate metrics
        if self._backend is not None:
            df = pd.DataFrame({"__ID__": ["a", "b"]})
            self._backend.filter(df, df["__ID__"] == "a")
        return pd.DataFrame()


def _make_fake_star(backend: InstrumentedBackend | None = None) -> _FakeStar:
    return _FakeStar(_backend=backend)
