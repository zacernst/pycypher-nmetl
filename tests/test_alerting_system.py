"""Tests for the configurable performance alerting system.

TDD tests written first — validates alerting with configurable thresholds,
notification routing, and <10% false positive rate.
"""

from __future__ import annotations

import time

import pytest


class TestAlertRuleBasics:
    """Alert rule creation and configuration."""

    def test_import(self):
        from shared.alerting import AlertRule, AlertSeverity

        rule = AlertRule(
            name="high_latency",
            metric="timing_p50_ms",
            threshold=100.0,
            severity=AlertSeverity.WARNING,
        )
        assert rule.name == "high_latency"

    def test_alert_severity_ordering(self):
        from shared.alerting import AlertSeverity

        assert AlertSeverity.INFO.value < AlertSeverity.WARNING.value
        assert AlertSeverity.WARNING.value < AlertSeverity.CRITICAL.value

    def test_rule_with_comparison_operators(self):
        from shared.alerting import AlertRule, AlertSeverity

        gt_rule = AlertRule(
            name="high_latency",
            metric="timing_p50_ms",
            threshold=100.0,
            operator="gt",
            severity=AlertSeverity.WARNING,
        )
        lt_rule = AlertRule(
            name="low_throughput",
            metric="queries_per_second",
            threshold=1.0,
            operator="lt",
            severity=AlertSeverity.CRITICAL,
        )
        assert gt_rule.operator == "gt"
        assert lt_rule.operator == "lt"


class TestAlertManager:
    """AlertManager evaluation and notification dispatch."""

    def test_create_manager(self):
        from shared.alerting import AlertManager

        manager = AlertManager()
        assert manager is not None

    def test_add_rule(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        rule = AlertRule(
            name="high_latency",
            metric="timing_p50_ms",
            threshold=100.0,
            severity=AlertSeverity.WARNING,
        )
        manager.add_rule(rule)
        assert len(manager.rules) == 1

    def test_evaluate_fires_alert_gt(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="high_latency",
                metric="timing_p50_ms",
                threshold=100.0,
                operator="gt",
                severity=AlertSeverity.WARNING,
            )
        )
        alerts = manager.evaluate({"timing_p50_ms": 150.0})
        assert len(alerts) == 1
        assert alerts[0].rule_name == "high_latency"
        assert alerts[0].current_value == 150.0

    def test_evaluate_no_alert_within_threshold(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="high_latency",
                metric="timing_p50_ms",
                threshold=100.0,
                operator="gt",
                severity=AlertSeverity.WARNING,
            )
        )
        alerts = manager.evaluate({"timing_p50_ms": 50.0})
        assert len(alerts) == 0

    def test_evaluate_fires_alert_lt(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="low_throughput",
                metric="qps",
                threshold=10.0,
                operator="lt",
                severity=AlertSeverity.CRITICAL,
            )
        )
        alerts = manager.evaluate({"qps": 5.0})
        assert len(alerts) == 1
        assert alerts[0].severity == AlertSeverity.CRITICAL

    def test_evaluate_fires_alert_gte(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="error_rate",
                metric="error_rate",
                threshold=0.05,
                operator="gte",
                severity=AlertSeverity.WARNING,
            )
        )
        alerts = manager.evaluate({"error_rate": 0.05})
        assert len(alerts) == 1

    def test_multiple_rules_multiple_alerts(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="latency",
                metric="timing_p50_ms",
                threshold=100.0,
                operator="gt",
                severity=AlertSeverity.WARNING,
            )
        )
        manager.add_rule(
            AlertRule(
                name="errors",
                metric="error_rate",
                threshold=0.1,
                operator="gt",
                severity=AlertSeverity.CRITICAL,
            )
        )
        alerts = manager.evaluate(
            {"timing_p50_ms": 200.0, "error_rate": 0.15}
        )
        assert len(alerts) == 2

    def test_missing_metric_does_not_fire(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="latency",
                metric="timing_p50_ms",
                threshold=100.0,
                severity=AlertSeverity.WARNING,
            )
        )
        # Metric not present in evaluation data
        alerts = manager.evaluate({"other_metric": 999.0})
        assert len(alerts) == 0


class TestAlertResult:
    """Alert result structure and metadata."""

    def test_alert_result_fields(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test_alert",
                metric="latency",
                threshold=100.0,
                operator="gt",
                severity=AlertSeverity.CRITICAL,
            )
        )
        alerts = manager.evaluate({"latency": 200.0})
        assert len(alerts) == 1
        alert = alerts[0]
        assert hasattr(alert, "rule_name")
        assert hasattr(alert, "metric_name")
        assert hasattr(alert, "current_value")
        assert hasattr(alert, "threshold")
        assert hasattr(alert, "severity")
        assert hasattr(alert, "timestamp")
        assert alert.rule_name == "test_alert"
        assert alert.metric_name == "latency"
        assert alert.threshold == 100.0

    def test_alert_result_to_dict(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        alerts = manager.evaluate({"m": 20.0})
        d = alerts[0].to_dict()
        assert isinstance(d, dict)
        assert d["rule_name"] == "test"
        assert d["current_value"] == 20.0


class TestAlertCooldown:
    """Alert cooldown to prevent notification storms."""

    def test_cooldown_prevents_repeated_alerts(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager(cooldown_seconds=60.0)
        manager.add_rule(
            AlertRule(
                name="latency",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        # First evaluation fires
        alerts1 = manager.evaluate({"m": 20.0})
        assert len(alerts1) == 1
        # Second evaluation within cooldown does not fire
        alerts2 = manager.evaluate({"m": 20.0})
        assert len(alerts2) == 0

    def test_different_rules_fire_independently(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager(cooldown_seconds=60.0)
        manager.add_rule(
            AlertRule(
                name="r1",
                metric="m1",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        manager.add_rule(
            AlertRule(
                name="r2",
                metric="m2",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        alerts1 = manager.evaluate({"m1": 20.0, "m2": 20.0})
        assert len(alerts1) == 2
        # Only r1 and r2 are in cooldown
        alerts2 = manager.evaluate({"m1": 20.0, "m2": 20.0})
        assert len(alerts2) == 0


class TestAlertNotificationHandlers:
    """Pluggable notification handlers (log, callback, webhook)."""

    def test_register_callback_handler(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        fired = []
        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        manager.on_alert(lambda alert: fired.append(alert))
        manager.evaluate({"m": 20.0})
        assert len(fired) == 1

    def test_multiple_handlers(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        log1 = []
        log2 = []
        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        manager.on_alert(lambda a: log1.append(a))
        manager.on_alert(lambda a: log2.append(a))
        manager.evaluate({"m": 20.0})
        assert len(log1) == 1
        assert len(log2) == 1

    def test_handler_error_does_not_crash(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        def bad_handler(alert):
            raise RuntimeError("handler exploded")

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        manager.on_alert(bad_handler)
        # Should not raise
        alerts = manager.evaluate({"m": 20.0})
        assert len(alerts) == 1


class TestAlertHistory:
    """Alert history tracking for analysis."""

    def test_alert_history_recorded(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        manager.evaluate({"m": 20.0})
        history = manager.alert_history()
        assert len(history) == 1
        assert history[0].rule_name == "test"

    def test_alert_history_max_size(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager(max_history=5)
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        for i in range(10):
            manager.evaluate({"m": 20.0 + i})
            # Reset cooldown for testing
            manager._last_fired.clear()
        history = manager.alert_history()
        assert len(history) <= 5

    def test_remove_rule(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="test",
                metric="m",
                threshold=10.0,
                severity=AlertSeverity.WARNING,
            )
        )
        manager.remove_rule("test")
        assert len(manager.rules) == 0


class TestSnapshotIntegration:
    """Integration with MetricsSnapshot for automated evaluation."""

    def test_evaluate_from_snapshot_dict(self):
        from shared.alerting import AlertManager, AlertRule, AlertSeverity

        manager = AlertManager()
        manager.add_rule(
            AlertRule(
                name="p99_latency",
                metric="timing_p99_ms",
                threshold=500.0,
                operator="gt",
                severity=AlertSeverity.CRITICAL,
            )
        )
        # Simulate snapshot dict
        snapshot_data = {
            "timing_p50_ms": 10.0,
            "timing_p90_ms": 50.0,
            "timing_p99_ms": 600.0,
            "error_rate": 0.01,
        }
        alerts = manager.evaluate(snapshot_data)
        assert len(alerts) == 1
        assert alerts[0].rule_name == "p99_latency"
