"""Query performance regression detection.

Tracks per-query-fingerprint latency baselines and detects when a query
starts consistently running slower than its historical norm. Integrates
with :class:`~fastopendata.analytics.collector.MetricsCollector` for
data and produces alerts when regressions are identified.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from fastopendata.analytics.collector import MetricsCollector, QueryStatus

_logger = logging.getLogger(__name__)

# Strip literals and whitespace to fingerprint structurally identical queries.
_LITERAL_RE = re.compile(
    r"""(?:'[^']*'|"[^"]*"|\b\d+(?:\.\d+)?\b)""",
)
_WHITESPACE_RE = re.compile(r"\s+")


def query_fingerprint(query: str) -> str:
    """Compute a structural fingerprint for a Cypher query.

    Replaces string/numeric literals with placeholders and normalizes
    whitespace so that ``MATCH (n) WHERE n.age > 30`` and
    ``MATCH (n) WHERE n.age > 50`` share the same fingerprint.

    Parameters
    ----------
    query : str
        The raw Cypher query text.

    Returns
    -------
    str
        A short hex digest identifying the query structure.

    """
    normalized = _LITERAL_RE.sub("?", query)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip().upper()
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


@dataclass
class RegressionAlert:
    """A detected performance regression for a query fingerprint.

    Attributes
    ----------
    fingerprint : str
        Query structure fingerprint.
    sample_query : str
        An example query matching this fingerprint.
    baseline_ms : float
        Historical baseline latency (mean of baseline window).
    current_ms : float
        Current latency (mean of recent window).
    ratio : float
        current_ms / baseline_ms — how much slower.
    severity : str
        Classification: "warning" (2x–5x), "critical" (5x+).
    detected_at : float
        Epoch timestamp when regression was detected.

    """

    fingerprint: str
    sample_query: str
    baseline_ms: float
    current_ms: float
    ratio: float
    severity: str
    detected_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "fingerprint": self.fingerprint,
            "sample_query": self.sample_query,
            "baseline_ms": round(self.baseline_ms, 2),
            "current_ms": round(self.current_ms, 2),
            "ratio": round(self.ratio, 2),
            "severity": self.severity,
            "detected_at": self.detected_at,
        }


class RegressionDetector:
    """Detects query performance regressions by tracking per-fingerprint baselines.

    Maintains a sliding window of latencies for each query fingerprint.
    When the recent window's mean latency exceeds the baseline by more
    than ``threshold_ratio``, a regression alert is generated.

    Parameters
    ----------
    collector : MetricsCollector
        Source of query metrics.
    baseline_window : int
        Number of initial observations to establish the baseline.
    recent_window : int
        Number of recent observations to compare against baseline.
    threshold_ratio : float
        Minimum current/baseline ratio to trigger an alert (default 2.0 = 2x slower).
    critical_ratio : float
        Ratio above which the alert is classified as critical (default 5.0).

    """

    def __init__(
        self,
        collector: MetricsCollector,
        *,
        baseline_window: int = 20,
        recent_window: int = 5,
        threshold_ratio: float = 2.0,
        critical_ratio: float = 5.0,
    ) -> None:
        self._collector = collector
        self._baseline_window = baseline_window
        self._recent_window = recent_window
        self._threshold_ratio = threshold_ratio
        self._critical_ratio = critical_ratio
        # Per-fingerprint latency history
        self._history: dict[str, deque[float]] = {}
        # Per-fingerprint sample query text
        self._samples: dict[str, str] = {}
        # Active alerts (fingerprint -> most recent alert)
        self._alerts: dict[str, RegressionAlert] = {}
        # Tracks which metrics have been ingested
        self._last_ingested_count = 0

    @property
    def alerts(self) -> list[RegressionAlert]:
        """All active regression alerts."""
        return list(self._alerts.values())

    @property
    def tracked_fingerprint_count(self) -> int:
        """Number of distinct query fingerprints being tracked."""
        return len(self._history)

    def ingest(self) -> list[RegressionAlert]:
        """Ingest new metrics from the collector and check for regressions.

        Pulls any metrics not yet seen, updates per-fingerprint history,
        and returns any new regression alerts detected.

        Returns
        -------
        list[RegressionAlert]
            Newly detected regressions (may be empty).

        """
        all_metrics = self._collector.all_metrics()
        new_metrics = all_metrics[self._last_ingested_count :]
        self._last_ingested_count = len(all_metrics)

        new_alerts: list[RegressionAlert] = []
        for metric in new_metrics:
            if metric.status != QueryStatus.SUCCESS:
                continue
            fp = query_fingerprint(metric.query_text)
            if fp not in self._history:
                self._history[fp] = deque(
                    maxlen=self._baseline_window + self._recent_window,
                )
            self._history[fp].append(metric.total_ms)
            self._samples[fp] = metric.query_text

            alert = self._check_fingerprint(fp)
            if alert is not None:
                new_alerts.append(alert)

        return new_alerts

    def check_all(self) -> list[RegressionAlert]:
        """Check all tracked fingerprints for regressions.

        Unlike :meth:`ingest`, this does not pull new data — it
        re-evaluates existing history.

        Returns
        -------
        list[RegressionAlert]
            All currently active regressions.

        """
        self._alerts.clear()
        for fp in self._history:
            self._check_fingerprint(fp)
        return self.alerts

    def clear(self) -> None:
        """Reset all tracking state."""
        self._history.clear()
        self._samples.clear()
        self._alerts.clear()
        self._last_ingested_count = 0

    def _check_fingerprint(self, fp: str) -> RegressionAlert | None:
        """Check a single fingerprint for regression."""
        history = self._history.get(fp)
        if history is None:
            return None

        total = len(history)
        min_needed = self._baseline_window + self._recent_window
        if total < min_needed:
            # Not enough data to compare
            return None

        history_list = list(history)
        baseline = history_list[: self._baseline_window]
        recent = history_list[-self._recent_window :]

        baseline_mean = sum(baseline) / len(baseline)
        recent_mean = sum(recent) / len(recent)

        if baseline_mean <= 0:
            return None

        ratio = recent_mean / baseline_mean

        if ratio < self._threshold_ratio:
            # No regression — clear any existing alert
            self._alerts.pop(fp, None)
            return None

        severity = "critical" if ratio >= self._critical_ratio else "warning"

        alert = RegressionAlert(
            fingerprint=fp,
            sample_query=self._samples.get(fp, ""),
            baseline_ms=baseline_mean,
            current_ms=recent_mean,
            ratio=ratio,
            severity=severity,
        )
        self._alerts[fp] = alert

        _logger.warning(
            "Query regression detected: fingerprint=%s ratio=%.1fx "
            "baseline=%.1fms current=%.1fms severity=%s",
            fp,
            ratio,
            baseline_mean,
            recent_mean,
            severity,
        )

        return alert
