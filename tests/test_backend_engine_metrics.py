"""TDD tests for backend engine metrics instrumentation.

Verifies that backend operations (join, filter, scan, aggregate, sort)
are instrumented with timing, logging, and QUERY_METRICS integration.
"""

from __future__ import annotations

import logging
import time

import pandas as pd
import pytest
from pycypher.backend_engine import (
    InstrumentedBackend,
    PandasBackend,
    select_backend,
)
from pycypher.constants import ID_COLUMN
from shared.metrics import QUERY_METRICS


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """Reset QUERY_METRICS before each test."""
    QUERY_METRICS.reset()


@pytest.fixture()
def backend() -> InstrumentedBackend:
    """Return an instrumented pandas backend."""
    return InstrumentedBackend(PandasBackend())


@pytest.fixture()
def sample_frame() -> pd.DataFrame:
    """Return a small test DataFrame with ID column."""
    return pd.DataFrame(
        {ID_COLUMN: ["a", "b", "c"], "name": ["Alice", "Bob", "Carol"]}
    )


@pytest.fixture()
def sample_right_frame() -> pd.DataFrame:
    """Return a second test DataFrame for joins."""
    return pd.DataFrame({ID_COLUMN: ["b", "c", "d"], "age": [30, 40, 50]})


class TestInstrumentedBackendDelegation:
    """InstrumentedBackend must delegate all operations to the inner backend."""

    def test_delegates_scan_entity(self, backend: InstrumentedBackend) -> None:
        source = pd.DataFrame({ID_COLUMN: ["x", "y"], "val": [1, 2]})
        result = backend.scan_entity(source, "Thing")
        assert list(result.columns) == [ID_COLUMN]
        assert len(result) == 2

    def test_delegates_filter(
        self, backend: InstrumentedBackend, sample_frame: pd.DataFrame
    ) -> None:
        mask = sample_frame["name"] == "Alice"
        result = backend.filter(sample_frame, mask)
        assert len(result) == 1

    def test_delegates_join(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        sample_right_frame: pd.DataFrame,
    ) -> None:
        result = backend.join(sample_frame, sample_right_frame, on=ID_COLUMN)
        assert len(result) == 2  # "b" and "c"

    def test_delegates_aggregate(self, backend: InstrumentedBackend) -> None:
        df = pd.DataFrame({"group": ["a", "a", "b"], "val": [1, 2, 3]})
        result = backend.aggregate(df, ["group"], {"total": ("val", "sum")})
        assert len(result) == 2

    def test_delegates_sort(
        self, backend: InstrumentedBackend, sample_frame: pd.DataFrame
    ) -> None:
        result = backend.sort(sample_frame, by=["name"])
        assert result["name"].iloc[0] == "Alice"

    def test_delegates_name_property(
        self, backend: InstrumentedBackend
    ) -> None:
        assert backend.name == "pandas"


class TestOperationTimingMetrics:
    """Each backend operation must record its timing in operation_timings."""

    def test_join_records_timing(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        sample_right_frame: pd.DataFrame,
    ) -> None:
        backend.join(sample_frame, sample_right_frame, on=ID_COLUMN)
        timings = backend.operation_timings
        assert "join" in timings
        assert len(timings["join"]) == 1
        assert timings["join"][0] >= 0.0

    def test_filter_records_timing(
        self, backend: InstrumentedBackend, sample_frame: pd.DataFrame
    ) -> None:
        mask = sample_frame["name"] == "Bob"
        backend.filter(sample_frame, mask)
        assert "filter" in backend.operation_timings
        assert len(backend.operation_timings["filter"]) == 1

    def test_scan_entity_records_timing(
        self, backend: InstrumentedBackend
    ) -> None:
        source = pd.DataFrame({ID_COLUMN: ["a"], "x": [1]})
        backend.scan_entity(source, "Node")
        assert "scan_entity" in backend.operation_timings

    def test_aggregate_records_timing(
        self, backend: InstrumentedBackend
    ) -> None:
        df = pd.DataFrame({"g": ["a", "b"], "v": [1, 2]})
        backend.aggregate(df, ["g"], {"s": ("v", "sum")})
        assert "aggregate" in backend.operation_timings

    def test_sort_records_timing(
        self, backend: InstrumentedBackend, sample_frame: pd.DataFrame
    ) -> None:
        backend.sort(sample_frame, by=["name"])
        assert "sort" in backend.operation_timings

    def test_multiple_operations_accumulate(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        sample_right_frame: pd.DataFrame,
    ) -> None:
        mask = sample_frame["name"] == "Alice"
        backend.filter(sample_frame, mask)
        backend.filter(sample_frame, mask)
        backend.join(sample_frame, sample_right_frame, on=ID_COLUMN)
        assert len(backend.operation_timings["filter"]) == 2
        assert len(backend.operation_timings["join"]) == 1


class TestOperationCounts:
    """Each backend operation must increment an operation counter."""

    def test_operation_counts(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        sample_right_frame: pd.DataFrame,
    ) -> None:
        mask = sample_frame["name"] == "Alice"
        backend.filter(sample_frame, mask)
        backend.filter(sample_frame, mask)
        backend.join(sample_frame, sample_right_frame, on=ID_COLUMN)
        counts = backend.operation_counts
        assert counts["filter"] == 2
        assert counts["join"] == 1


class TestOperationLogging:
    """Backend operations must emit DEBUG-level log messages."""

    def test_join_logs_at_debug(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        sample_right_frame: pd.DataFrame,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger="shared.logger"):
            backend.join(sample_frame, sample_right_frame, on=ID_COLUMN)
        assert any("join" in r.message.lower() for r in caplog.records)

    def test_filter_logs_at_debug(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mask = sample_frame["name"] == "Alice"
        with caplog.at_level(logging.DEBUG, logger="shared.logger"):
            backend.filter(sample_frame, mask)
        assert any("filter" in r.message.lower() for r in caplog.records)


class TestTimingSummary:
    """InstrumentedBackend must provide a timing summary dict."""

    def test_timing_summary_structure(
        self,
        backend: InstrumentedBackend,
        sample_frame: pd.DataFrame,
        sample_right_frame: pd.DataFrame,
    ) -> None:
        mask = sample_frame["name"] == "Alice"
        backend.filter(sample_frame, mask)
        backend.join(sample_frame, sample_right_frame, on=ID_COLUMN)
        summary = backend.timing_summary()
        assert "filter" in summary
        assert "join" in summary
        # Each entry should have count and total_ms
        assert summary["filter"]["count"] == 1
        assert summary["filter"]["total_ms"] >= 0.0
        assert summary["join"]["count"] == 1

    def test_empty_summary(self, backend: InstrumentedBackend) -> None:
        summary = backend.timing_summary()
        assert summary == {}


class TestSelectBackendInstrumented:
    """select_backend with instrument=True must return InstrumentedBackend."""

    def test_select_backend_instrumented(self) -> None:
        engine = select_backend(hint="pandas", instrument=True)
        assert isinstance(engine, InstrumentedBackend)
        assert engine.name == "pandas"

    def test_select_backend_default_not_instrumented(self) -> None:
        engine = select_backend(hint="pandas")
        assert isinstance(engine, PandasBackend)
        assert not isinstance(engine, InstrumentedBackend)
