"""Tests for the continuous metrics monitoring orchestration.

Covers ContinuousMonitor lifecycle, periodic collection, dashboard
integration, alerting evaluation, regression detection feeding,
exporter dispatch, and callback invocation.
"""

from __future__ import annotations

import time

import pytest
from _perf_helpers import perf_threshold


def _make_monitor(**kwargs):
    """Create a ContinuousMonitor with a fresh QueryMetrics instance."""
    from shared.continuous_monitor import ContinuousMonitor
    from shared.metrics import QueryMetrics

    metrics = kwargs.pop("metrics", None) or QueryMetrics()
    return ContinuousMonitor(metrics=metrics, **kwargs), metrics


def _populate_metrics(metrics, n_ok=10, n_err=0):
    """Record some queries into a metrics collector."""
    for i in range(n_ok):
        metrics.record_query(
            query_id=f"ok-{i}",
            elapsed_s=0.01 * (i + 1),
            rows=i * 10,
            clauses=["Match", "Return"],
        )
    for i in range(n_err):
        metrics.record_error(
            query_id=f"err-{i}",
            error_type="TypeError",
            elapsed_s=0.001,
        )


class TestContinuousMonitorImport:
    """Basic importability and instantiation."""

    def test_import(self):
        from shared.continuous_monitor import ContinuousMonitor

        assert ContinuousMonitor is not None

    def test_create_with_defaults(self):
        monitor, _ = _make_monitor()
        assert monitor is not None
        assert not monitor.is_running
        assert monitor.cycle_count == 0
        assert monitor.last_snapshot is None


class TestCollectOnce:
    """Synchronous single-cycle collection."""

    def test_collect_once_returns_snapshot_dict(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=5)
        result = monitor.collect_once()
        assert isinstance(result, dict)
        assert "total_queries" in result
        assert result["total_queries"] == 5

    def test_collect_once_updates_cycle_count(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=3)
        monitor.collect_once()
        assert monitor.cycle_count == 1
        monitor.collect_once()
        assert monitor.cycle_count == 2

    def test_collect_once_records_in_dashboard(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=5)
        monitor.collect_once()
        assert len(monitor.dashboard.history) == 1
        assert monitor.dashboard.latest()["total_queries"] == 5

    def test_collect_once_updates_last_snapshot(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=5)
        monitor.collect_once()
        snap = monitor.last_snapshot
        assert snap is not None
        assert snap["total_queries"] == 5


class TestDashboardIntegration:
    """Dashboard time-series integration."""

    def test_multiple_cycles_build_trend(self):
        from shared.metrics import QueryMetrics

        metrics = QueryMetrics()
        monitor, _ = _make_monitor(metrics=metrics)

        for i in range(5):
            metrics.record_query(
                query_id=f"q-{i}",
                elapsed_s=0.01,
                rows=10,
            )
            monitor.collect_once()

        trend = monitor.dashboard.trend("total_queries")
        assert len(trend) == 5
        # Each cycle sees cumulative queries: 1, 2, 3, 4, 5
        assert trend == [1, 2, 3, 4, 5]

    def test_custom_dashboard_instance(self):
        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=3)
        monitor, metrics = _make_monitor(dashboard=dd)
        _populate_metrics(metrics, n_ok=2)
        monitor.collect_once()
        assert len(dd.history) == 1


class TestAlertingIntegration:
    """Alert rule evaluation during collection."""

    def test_alerts_fire_on_threshold_breach(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        am = AlertManager()
        am.add_rule(
            AlertRule(
                name="high_errors",
                metric="total_errors",
                threshold=5,
                operator="gte",
                severity=AlertSeverity.WARNING,
            )
        )

        monitor, metrics = _make_monitor(alert_manager=am)
        _populate_metrics(metrics, n_ok=10, n_err=6)
        monitor.collect_once()

        history = am.alert_history()
        assert len(history) == 1
        assert history[0].rule_name == "high_errors"

    def test_no_alerts_when_within_thresholds(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        am = AlertManager()
        am.add_rule(
            AlertRule(
                name="high_errors",
                metric="total_errors",
                threshold=100,
                operator="gt",
                severity=AlertSeverity.CRITICAL,
            )
        )

        monitor, metrics = _make_monitor(alert_manager=am)
        _populate_metrics(metrics, n_ok=10, n_err=2)
        monitor.collect_once()

        assert len(am.alert_history()) == 0


class TestRegressionDetectorIntegration:
    """Regression detector baseline feeding."""

    def test_regression_metrics_fed_to_detector(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=10)

        for _ in range(5):
            monitor.collect_once()

        rd = monitor.regression_detector
        assert rd.sample_count("timing_p50_ms") == 5
        assert rd.sample_count("error_rate") == 5

    def test_custom_regression_metrics(self):
        monitor, metrics = _make_monitor(
            regression_metrics=("total_queries",)
        )
        _populate_metrics(metrics, n_ok=5)
        monitor.collect_once()

        rd = monitor.regression_detector
        assert rd.sample_count("total_queries") == 1
        assert rd.sample_count("timing_p50_ms") == 0

    def test_check_regressions_returns_results(self):
        from shared.regression_detector import RegressionDetector

        rd = RegressionDetector(min_baseline_samples=3)
        monitor, metrics = _make_monitor(regression_detector=rd)
        _populate_metrics(metrics, n_ok=10)

        # Build baseline with several cycles.
        for _ in range(5):
            monitor.collect_once()

        results = monitor.check_regressions()
        # With consistent metrics, results should exist but not be regressions.
        for metric, result in results.items():
            if result is not None:
                assert not result.is_regression


class TestExporterIntegration:
    """Exporter dispatch during collection."""

    def test_exporters_called_on_collect(self):
        exported = []

        class FakeExporter:
            name = "fake"

            def export(self, snapshot):
                exported.append(snapshot)

        monitor, metrics = _make_monitor(exporters=[FakeExporter()])
        _populate_metrics(metrics, n_ok=3)
        monitor.collect_once()

        assert len(exported) == 1

    def test_exporter_failure_does_not_stop_collection(self):
        class FailingExporter:
            name = "failing"

            def export(self, snapshot):
                raise RuntimeError("export failed")

        monitor, metrics = _make_monitor(exporters=[FailingExporter()])
        _populate_metrics(metrics, n_ok=3)
        # Should not raise.
        monitor.collect_once()
        assert monitor.cycle_count == 1


class TestCallbacks:
    """on_cycle callback invocation."""

    def test_callback_receives_snapshot(self):
        received = []
        monitor, metrics = _make_monitor()
        monitor.on_cycle(lambda snap: received.append(snap))
        _populate_metrics(metrics, n_ok=5)
        monitor.collect_once()

        assert len(received) == 1
        assert received[0]["total_queries"] == 5

    def test_callback_failure_does_not_stop_collection(self):
        def bad_callback(snap):
            raise ValueError("callback error")

        monitor, metrics = _make_monitor()
        monitor.on_cycle(bad_callback)
        _populate_metrics(metrics, n_ok=3)
        # Should not raise.
        monitor.collect_once()
        assert monitor.cycle_count == 1


class TestBackgroundLoop:
    """Background thread lifecycle."""

    def test_start_and_stop(self):
        monitor, metrics = _make_monitor(interval_s=0.05)
        _populate_metrics(metrics, n_ok=3)

        monitor.start()
        assert monitor.is_running
        time.sleep(0.2)
        monitor.stop()

        assert not monitor.is_running
        assert monitor.cycle_count >= 2

    def test_start_twice_raises(self):
        monitor, _ = _make_monitor(interval_s=1.0)
        monitor.start()
        try:
            with pytest.raises(RuntimeError, match="already running"):
                monitor.start()
        finally:
            monitor.stop()

    def test_stop_without_start_is_safe(self):
        monitor, _ = _make_monitor()
        monitor.stop()  # Should not raise.

    def test_dashboard_populated_by_background_loop(self):
        monitor, metrics = _make_monitor(interval_s=0.05)
        _populate_metrics(metrics, n_ok=5)
        monitor.start()
        time.sleep(0.25)
        monitor.stop()

        assert len(monitor.dashboard.history) >= 2


class TestStatusSummary:
    """Status summary for diagnostics."""

    def test_status_summary_structure(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=5)
        monitor.collect_once()
        status = monitor.status_summary()

        assert isinstance(status, dict)
        assert "running" in status
        assert "cycle_count" in status
        assert "interval_s" in status
        assert "total_alerts" in status
        assert "regression_baselines" in status
        assert "exporter_count" in status
        assert status["cycle_count"] == 1
        assert status["running"] is False

    def test_status_summary_before_any_collection(self):
        monitor, _ = _make_monitor()
        status = monitor.status_summary()
        assert status["cycle_count"] == 0
        assert status["health_status"] is None


class TestCollectionPerformance:
    """Collection cycle performance bounds."""

    def test_single_cycle_under_10ms(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=100)

        start = time.perf_counter()
        monitor.collect_once()
        elapsed = time.perf_counter() - start

        assert elapsed < perf_threshold(0.01)

    def test_hundred_cycles_under_500ms(self):
        monitor, metrics = _make_monitor()
        _populate_metrics(metrics, n_ok=50)

        start = time.perf_counter()
        for _ in range(100):
            monitor.collect_once()
        elapsed = time.perf_counter() - start

        assert elapsed < perf_threshold(0.5)
