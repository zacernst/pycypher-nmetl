"""Continuous metrics monitoring orchestration.

Ties together the metrics collector, dashboard data store, alert manager,
regression detector, and exporters into a single periodic monitoring loop.

Usage::

    from shared.continuous_monitor import ContinuousMonitor
    from shared.metrics import QUERY_METRICS

    monitor = ContinuousMonitor(metrics=QUERY_METRICS, interval_s=10.0)
    monitor.start()   # non-blocking, runs in background thread

    # Later...
    monitor.stop()

The monitor periodically:

1. Takes a snapshot from :class:`~shared.metrics.QueryMetrics`.
2. Records it in a :class:`~shared.dashboard.DashboardData` for trend tracking.
3. Evaluates configured :class:`~shared.alerting.AlertRule` rules.
4. Feeds the :class:`~shared.regression_detector.RegressionDetector` baseline.
5. Pushes to configured exporters.

"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from shared.alerting import AlertManager, AlertResult
from shared.dashboard import DashboardData
from shared.metrics import QueryMetrics
from shared.regression_detector import RegressionDetector, RegressionResult

_logger = logging.getLogger(__name__)

# Metrics fed into the regression detector by default.
_DEFAULT_REGRESSION_METRICS = (
    "timing_p50_ms",
    "timing_p90_ms",
    "timing_p99_ms",
    "error_rate",
    "memory_delta_p50_mb",
)


class ContinuousMonitor:
    """Orchestrates periodic metric collection, alerting, and export.

    Args:
        metrics: The :class:`~shared.metrics.QueryMetrics` collector to poll.
        interval_s: Seconds between collection cycles.  Defaults to 10.
        dashboard: Optional pre-existing dashboard data store.  A new one
            is created if not provided.
        alert_manager: Optional alert manager.  A new one is created if not
            provided.
        regression_detector: Optional regression detector.  A new one is
            created if not provided.
        exporters: Optional list of exporter instances.  Each must have an
            ``export(snapshot)`` method.
        regression_metrics: Metric names to track for regression detection.
            Defaults to latency, error rate, and memory metrics.

    """

    def __init__(
        self,
        *,
        metrics: QueryMetrics,
        interval_s: float = 10.0,
        dashboard: DashboardData | None = None,
        alert_manager: AlertManager | None = None,
        regression_detector: RegressionDetector | None = None,
        exporters: list[Any] | None = None,
        regression_metrics: tuple[str, ...] = _DEFAULT_REGRESSION_METRICS,
    ) -> None:
        self._metrics = metrics
        self._interval_s = interval_s
        self._dashboard = dashboard or DashboardData()
        self._alert_manager = alert_manager or AlertManager()
        self._regression_detector = regression_detector or RegressionDetector()
        self._exporters = list(exporters or [])
        self._regression_metrics = regression_metrics

        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._cycle_count = 0
        self._last_snapshot: dict[str, Any] | None = None
        self._lock = threading.Lock()
        self._on_cycle_callbacks: list[Callable[[dict[str, Any]], None]] = []

    # -- Public properties ---------------------------------------------------

    @property
    def dashboard(self) -> DashboardData:
        """The dashboard data store receiving metric snapshots."""
        return self._dashboard

    @property
    def alert_manager(self) -> AlertManager:
        """The alert manager evaluating threshold rules."""
        return self._alert_manager

    @property
    def regression_detector(self) -> RegressionDetector:
        """The regression detector tracking metric baselines."""
        return self._regression_detector

    @property
    def interval_s(self) -> float:
        """Collection interval in seconds."""
        return self._interval_s

    @property
    def cycle_count(self) -> int:
        """Number of completed collection cycles."""
        with self._lock:
            return self._cycle_count

    @property
    def is_running(self) -> bool:
        """Whether the background monitor thread is running."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def last_snapshot(self) -> dict[str, Any] | None:
        """The most recent collected snapshot dict, or ``None``."""
        with self._lock:
            return dict(self._last_snapshot) if self._last_snapshot else None

    # -- Callbacks -----------------------------------------------------------

    def on_cycle(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Register a callback invoked after each collection cycle.

        The callback receives the snapshot dict.
        """
        self._on_cycle_callbacks.append(callback)

    # -- Lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Start the background monitoring thread.

        Raises :exc:`RuntimeError` if already running.
        """
        if self.is_running:
            raise RuntimeError("ContinuousMonitor is already running")
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="continuous-monitor"
        )
        self._thread.start()
        _logger.info(
            "ContinuousMonitor started  interval=%.1fs", self._interval_s
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the background monitoring thread.

        Args:
            timeout: Maximum seconds to wait for the thread to finish.

        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        _logger.info("ContinuousMonitor stopped  cycles=%d", self._cycle_count)

    def collect_once(self) -> dict[str, Any]:
        """Run a single collection cycle synchronously.

        Useful for testing or on-demand metric collection without
        starting the background loop.

        Returns:
            The snapshot dict from this cycle.

        """
        return self._collect()

    # -- Internal ------------------------------------------------------------

    def _run_loop(self) -> None:
        """Background loop: collect → sleep → repeat."""
        while not self._stop_event.is_set():
            try:
                self._collect()
            except Exception:
                _logger.exception("ContinuousMonitor collection cycle failed")
            self._stop_event.wait(timeout=self._interval_s)

    def _collect(self) -> dict[str, Any]:
        """Execute one full collection cycle."""
        snapshot = self._metrics.snapshot()
        snap_dict = snapshot.to_dict()

        # 1. Record in dashboard for trend tracking.
        self._dashboard.record_snapshot(snap_dict)

        # 2. Evaluate alert rules.
        self._alert_manager.evaluate(snap_dict)

        # 3. Feed regression detector baseline.
        for metric_name in self._regression_metrics:
            value = snap_dict.get(metric_name)
            if value is not None and isinstance(value, (int, float)):
                self._regression_detector.record(metric_name, value)

        # 4. Push to exporters.
        for exporter in self._exporters:
            try:
                exporter.export(snapshot)
            except Exception:
                _logger.warning(
                    "Exporter %s failed",
                    getattr(exporter, "name", type(exporter).__name__),
                    exc_info=True,
                )

        # 5. Update internal state.
        with self._lock:
            self._cycle_count += 1
            self._last_snapshot = snap_dict

        # 6. Invoke callbacks.
        for cb in self._on_cycle_callbacks:
            try:
                cb(snap_dict)
            except Exception:
                _logger.warning("on_cycle callback failed", exc_info=True)

        return snap_dict

    def check_regressions(
        self,
        snapshot: dict[str, Any] | None = None,
    ) -> dict[str, RegressionResult | None]:
        """Check tracked metrics for regressions against baseline.

        Args:
            snapshot: Optional snapshot dict to check.  Uses the latest
                collected snapshot if not provided.

        Returns:
            Dict mapping metric name to its regression result (or ``None``
            if insufficient baseline data).

        """
        snap = snapshot or self.last_snapshot
        if snap is None:
            return {}
        values = {
            k: snap[k]
            for k in self._regression_metrics
            if k in snap and isinstance(snap[k], (int, float))
        }
        return self._regression_detector.check_all(values)

    def status_summary(self) -> dict[str, Any]:
        """Return a concise status dict for diagnostic display.

        Includes cycle count, running state, latest health status, alert
        count, and regression detector sample counts.
        """
        snap = self.last_snapshot
        return {
            "running": self.is_running,
            "cycle_count": self.cycle_count,
            "interval_s": self._interval_s,
            "health_status": snap.get("health_status") if snap else None,
            "total_alerts": len(self._alert_manager.alert_history()),
            "regression_baselines": {
                metric: self._regression_detector.sample_count(metric)
                for metric in self._regression_metrics
            },
            "exporter_count": len(self._exporters),
        }
