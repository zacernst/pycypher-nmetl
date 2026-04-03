"""Configurable performance alerting system.

Evaluates metric values against user-defined threshold rules and fires
alerts with pluggable notification handlers.  Thread-safe with cooldown
support to prevent alert storms.

Usage::

    from shared.alerting import AlertManager, AlertRule, AlertSeverity

    manager = AlertManager(cooldown_seconds=300)
    manager.add_rule(AlertRule(
        name="high_p99",
        metric="timing_p99_ms",
        threshold=500.0,
        operator="gt",
        severity=AlertSeverity.CRITICAL,
    ))
    manager.on_alert(lambda a: print(f"ALERT: {a.rule_name}"))

    alerts = manager.evaluate(snapshot.to_dict())

"""

from __future__ import annotations

import enum
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

_logger = logging.getLogger(__name__)


class AlertSeverity(enum.Enum):
    """Alert severity levels, ordered by importance."""

    INFO = 1
    WARNING = 2
    CRITICAL = 3


@dataclass(frozen=True)
class AlertRule:
    """A threshold rule for a single metric.

    Attributes:
        name: Unique rule identifier.
        metric: Metric name to evaluate (e.g. ``"timing_p99_ms"``).
        threshold: Numeric threshold value.
        operator: Comparison operator — ``"gt"`` (default), ``"lt"``,
            ``"gte"``, ``"lte"``.
        severity: Alert severity when the rule fires.

    """

    name: str
    metric: str
    threshold: float
    severity: AlertSeverity
    operator: str = "gt"


@dataclass(frozen=True)
class AlertResult:
    """An alert that has been fired.

    Attributes:
        rule_name: Name of the rule that triggered.
        metric_name: Metric that exceeded its threshold.
        current_value: The observed metric value.
        threshold: The threshold that was violated.
        severity: Alert severity level.
        timestamp: Time the alert fired (``time.time()``).

    """

    rule_name: str
    metric_name: str
    current_value: float
    threshold: float
    severity: AlertSeverity
    timestamp: float

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary for JSON export."""
        return {
            "rule_name": self.rule_name,
            "metric_name": self.metric_name,
            "current_value": self.current_value,
            "threshold": self.threshold,
            "severity": self.severity.name,
            "timestamp": self.timestamp,
        }


_OPERATORS: dict[str, Callable[[float, float], bool]] = {
    "gt": lambda v, t: v > t,
    "lt": lambda v, t: v < t,
    "gte": lambda v, t: v >= t,
    "lte": lambda v, t: v <= t,
}


class AlertManager:
    """Evaluates alert rules against metric values.

    Args:
        cooldown_seconds: Minimum seconds between repeated alerts for
            the same rule.  Defaults to 0 (no cooldown).
        max_history: Maximum number of alert results to keep in history.
            Defaults to 1000.

    """

    def __init__(
        self,
        *,
        cooldown_seconds: float = 0.0,
        max_history: int = 1000,
    ) -> None:
        self._rules: list[AlertRule] = []
        self._handlers: list[Callable[[AlertResult], None]] = []
        self._last_fired: dict[str, float] = {}
        self._history: list[AlertResult] = []
        self._cooldown = cooldown_seconds
        self._max_history = max_history
        self._lock = threading.Lock()

    @property
    def rules(self) -> list[AlertRule]:
        """Return a copy of the current rule list."""
        with self._lock:
            return list(self._rules)

    def add_rule(self, rule: AlertRule) -> None:
        """Register an alert rule."""
        with self._lock:
            self._rules.append(rule)

    def remove_rule(self, name: str) -> None:
        """Remove a rule by name."""
        with self._lock:
            self._rules = [r for r in self._rules if r.name != name]

    def on_alert(self, handler: Callable[[AlertResult], None]) -> None:
        """Register a notification handler called when alerts fire."""
        with self._lock:
            self._handlers.append(handler)

    def evaluate(self, metrics: dict[str, float]) -> list[AlertResult]:
        """Evaluate all rules against the given metric values.

        Returns a list of alerts that fired.  Respects cooldown periods
        and invokes registered handlers for each alert.
        """
        now = time.time()
        fired: list[AlertResult] = []

        with self._lock:
            rules = list(self._rules)
            handlers = list(self._handlers)

        for rule in rules:
            value = metrics.get(rule.metric)
            if value is None:
                continue

            comparator = _OPERATORS.get(rule.operator, _OPERATORS["gt"])
            if not comparator(value, rule.threshold):
                continue

            # Cooldown check.
            with self._lock:
                last = self._last_fired.get(rule.name, 0.0)
                if self._cooldown > 0 and (now - last) < self._cooldown:
                    continue
                self._last_fired[rule.name] = now

            alert = AlertResult(
                rule_name=rule.name,
                metric_name=rule.metric,
                current_value=value,
                threshold=rule.threshold,
                severity=rule.severity,
                timestamp=now,
            )
            fired.append(alert)

            with self._lock:
                self._history.append(alert)
                if len(self._history) > self._max_history:
                    self._history = self._history[-self._max_history :]

            for handler in handlers:
                try:
                    handler(alert)
                except Exception:
                    _logger.exception(
                        "Alert handler failed for rule %s", rule.name
                    )

        return fired

    def alert_history(self) -> list[AlertResult]:
        """Return a copy of the alert history."""
        with self._lock:
            return list(self._history)
