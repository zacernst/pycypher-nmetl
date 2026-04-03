"""Statistical performance regression detection engine.

Detects regressions by comparing new metric values against a baseline
distribution using z-score analysis.  Thread-safe for concurrent use
in monitoring pipelines.

Usage::

    from shared.regression_detector import RegressionDetector

    detector = RegressionDetector(min_baseline_samples=30)

    # Build baseline from normal operation
    for sample in historical_latencies:
        detector.record("timing_p50_ms", sample)

    # Check a new observation
    result = detector.check("timing_p50_ms", current_latency)
    if result and result.is_regression:
        print(f"Regression detected: {result.severity}")

"""

from __future__ import annotations

import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RegressionResult:
    """Immutable result of a regression check.

    Attributes:
        is_regression: Whether the value is a statistically significant
            regression (increase above baseline).
        metric_name: Name of the metric that was checked.
        current_value: The observed value being tested.
        baseline_mean: Mean of the baseline samples.
        baseline_std: Standard deviation of the baseline samples.
        z_score: Number of standard deviations above the mean.
        severity: ``"info"`` / ``"warning"`` / ``"critical"`` based on
            how far the value deviates.

    """

    is_regression: bool
    metric_name: str
    current_value: float
    baseline_mean: float
    baseline_std: float
    z_score: float
    severity: str


class RegressionDetector:
    """Statistical regression detection using z-score analysis.

    Records baseline samples per metric and detects regressions when new
    values exceed a configurable z-score threshold above the baseline mean.

    Args:
        min_baseline_samples: Minimum samples required before detection
            is active.  Defaults to 20.
        max_samples: Maximum rolling-window size per metric.  Defaults
            to 500.
        z_score_threshold: Z-score above which a value is flagged as a
            regression.  Defaults to 2.5.

    """

    def __init__(
        self,
        *,
        min_baseline_samples: int = 20,
        max_samples: int = 500,
        z_score_threshold: float = 2.0,
    ) -> None:
        self._min_baseline = min_baseline_samples
        self._max_samples = max_samples
        self._z_threshold = z_score_threshold
        self._samples: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def record(self, metric: str, value: float) -> None:
        """Record a baseline sample for *metric*."""
        with self._lock:
            samples = self._samples.setdefault(metric, [])
            if len(samples) >= self._max_samples:
                # Evict oldest half to maintain rolling window.
                self._samples[metric] = samples[self._max_samples // 2 :]
                samples = self._samples[metric]
            samples.append(value)

    def sample_count(self, metric: str) -> int:
        """Return number of recorded samples for *metric*."""
        with self._lock:
            return len(self._samples.get(metric, []))

    def check(self, metric: str, value: float) -> RegressionResult | None:
        """Check whether *value* represents a regression for *metric*.

        Returns ``None`` if there are insufficient baseline samples.
        """
        with self._lock:
            samples = self._samples.get(metric)
            if not samples or len(samples) < self._min_baseline:
                return None
            data = list(samples)

        mean = statistics.mean(data)
        std = statistics.stdev(data) if len(data) > 1 else 0.0

        if std == 0.0:
            # All samples identical — any deviation is a regression.
            z_score = float("inf") if value > mean else 0.0
        else:
            z_score = (value - mean) / std

        is_regression = z_score > self._z_threshold
        severity = self._classify_severity(z_score)

        return RegressionResult(
            is_regression=is_regression,
            metric_name=metric,
            current_value=value,
            baseline_mean=mean,
            baseline_std=std,
            z_score=z_score,
            severity=severity,
        )

    def check_all(
        self, metrics: dict[str, float]
    ) -> dict[str, RegressionResult | None]:
        """Check multiple metrics at once.

        Returns a dict mapping metric name to its regression result
        (or ``None`` if insufficient baseline data).
        """
        return {
            name: self.check(name, value)
            for name, value in metrics.items()
        }

    def baseline_stats(self, metric: str) -> dict[str, Any] | None:
        """Return summary statistics for *metric*'s baseline.

        Returns ``None`` if no samples have been recorded.
        """
        with self._lock:
            samples = self._samples.get(metric)
            if not samples:
                return None
            data = list(samples)

        return {
            "mean": statistics.mean(data),
            "std": statistics.stdev(data) if len(data) > 1 else 0.0,
            "count": len(data),
            "min": min(data),
            "max": max(data),
        }

    def reset(self, metric: str) -> None:
        """Clear all samples for *metric*."""
        with self._lock:
            self._samples.pop(metric, None)

    def reset_all(self) -> None:
        """Clear all recorded samples."""
        with self._lock:
            self._samples.clear()

    def to_dict(self) -> dict[str, Any]:
        """Serialize detector state to a dictionary."""
        with self._lock:
            metrics_data = {}
            for name, samples in self._samples.items():
                data = list(samples)
                metrics_data[name] = {
                    "samples": data,
                    "count": len(data),
                    "mean": statistics.mean(data) if data else 0.0,
                    "std": (
                        statistics.stdev(data) if len(data) > 1 else 0.0
                    ),
                }
        return {
            "min_baseline_samples": self._min_baseline,
            "max_samples": self._max_samples,
            "z_score_threshold": self._z_threshold,
            "metrics": metrics_data,
        }

    @staticmethod
    def _classify_severity(z_score: float) -> str:
        """Classify severity based on z-score magnitude."""
        if z_score > 5.0:
            return "critical"
        if z_score > 3.0:
            return "warning"
        return "info"
