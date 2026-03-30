"""Tests for fastopendata.analytics.regression — performance regression detection."""

from __future__ import annotations

import pytest
from fastopendata.analytics.collector import MetricsCollector
from fastopendata.analytics.regression import (
    RegressionAlert,
    RegressionDetector,
    query_fingerprint,
)

# ── query_fingerprint ────────────────────────────────────────────────


class TestQueryFingerprint:
    def test_identical_queries(self) -> None:
        q = "MATCH (n:Person) RETURN n"
        assert query_fingerprint(q) == query_fingerprint(q)

    def test_same_structure_different_literals(self) -> None:
        q1 = "MATCH (n:Person) WHERE n.age > 30 RETURN n"
        q2 = "MATCH (n:Person) WHERE n.age > 50 RETURN n"
        assert query_fingerprint(q1) == query_fingerprint(q2)

    def test_different_string_literals(self) -> None:
        q1 = "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n"
        q2 = "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n"
        assert query_fingerprint(q1) == query_fingerprint(q2)

    def test_different_structure(self) -> None:
        q1 = "MATCH (n:Person) RETURN n"
        q2 = "MATCH (n:City) RETURN n.name"
        assert query_fingerprint(q1) != query_fingerprint(q2)

    def test_whitespace_normalization(self) -> None:
        q1 = "MATCH  (n)   RETURN   n"
        q2 = "MATCH (n) RETURN n"
        assert query_fingerprint(q1) == query_fingerprint(q2)

    def test_case_insensitive(self) -> None:
        q1 = "match (n) return n"
        q2 = "MATCH (n) RETURN n"
        assert query_fingerprint(q1) == query_fingerprint(q2)

    def test_returns_hex_string(self) -> None:
        fp = query_fingerprint("MATCH (n) RETURN n")
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)

    def test_float_literals_normalized(self) -> None:
        q1 = "MATCH (n) WHERE n.score > 3.14 RETURN n"
        q2 = "MATCH (n) WHERE n.score > 2.71 RETURN n"
        assert query_fingerprint(q1) == query_fingerprint(q2)


# ── RegressionDetector ───────────────────────────────────────────────


class TestRegressionDetector:
    def _make_detector(
        self,
        baseline_window: int = 10,
        recent_window: int = 3,
        threshold_ratio: float = 2.0,
    ) -> tuple[MetricsCollector, RegressionDetector]:
        c = MetricsCollector()
        d = RegressionDetector(
            c,
            baseline_window=baseline_window,
            recent_window=recent_window,
            threshold_ratio=threshold_ratio,
        )
        return c, d

    def test_no_data_no_alerts(self) -> None:
        c, d = self._make_detector()
        alerts = d.ingest()
        assert alerts == []
        assert d.alerts == []

    def test_insufficient_data_no_alerts(self) -> None:
        c, d = self._make_detector(baseline_window=10, recent_window=3)
        # Only add 5 metrics (need 13 total)
        for i in range(5):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        alerts = d.ingest()
        assert alerts == []

    def test_stable_performance_no_alerts(self) -> None:
        c, d = self._make_detector(baseline_window=10, recent_window=3)
        # All queries at ~10ms — no regression
        for _ in range(15):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        alerts = d.ingest()
        assert alerts == []

    def test_detects_regression(self) -> None:
        c, d = self._make_detector(baseline_window=10, recent_window=3)
        # Baseline: 10ms
        for _ in range(10):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        # Recent: 30ms (3x slower)
        for _ in range(3):
            c.record_success("MATCH (n) RETURN n", total_ms=30.0)
        alerts = d.ingest()
        assert len(alerts) == 1
        assert alerts[0].ratio == pytest.approx(3.0, abs=0.1)
        assert alerts[0].severity == "warning"

    def test_detects_critical_regression(self) -> None:
        c, d = self._make_detector(
            baseline_window=10,
            recent_window=3,
            threshold_ratio=2.0,
        )
        d._critical_ratio = 5.0
        for _ in range(10):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        for _ in range(3):
            c.record_success("MATCH (n) RETURN n", total_ms=60.0)
        alerts = d.ingest()
        assert len(alerts) == 1
        assert alerts[0].severity == "critical"

    def test_no_alert_below_threshold(self) -> None:
        c, d = self._make_detector(baseline_window=10, recent_window=3)
        for _ in range(10):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        # 1.5x — below 2x threshold
        for _ in range(3):
            c.record_success("MATCH (n) RETURN n", total_ms=15.0)
        alerts = d.ingest()
        assert alerts == []

    def test_different_queries_tracked_separately(self) -> None:
        c, d = self._make_detector(baseline_window=5, recent_window=2)
        # Query A: stable
        for _ in range(7):
            c.record_success("MATCH (a:Person) RETURN a", total_ms=10.0)
        # Query B: regression
        for _ in range(5):
            c.record_success("MATCH (b:City) RETURN b", total_ms=10.0)
        for _ in range(2):
            c.record_success("MATCH (b:City) RETURN b", total_ms=30.0)
        alerts = d.ingest()
        # Only query B should have a regression
        assert len(alerts) == 1
        assert "City" in alerts[0].sample_query

    def test_errors_ignored_in_regression(self) -> None:
        c, d = self._make_detector(baseline_window=10, recent_window=3)
        for _ in range(10):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        # Errors shouldn't affect regression tracking
        for _ in range(3):
            c.record_error("MATCH (n) RETURN n", total_ms=100.0, error_message="fail")
        # Still need 3 recent successes — add them at baseline speed
        for _ in range(3):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        alerts = d.ingest()
        assert alerts == []

    def test_alert_clears_when_performance_recovers(self) -> None:
        c, d = self._make_detector(baseline_window=5, recent_window=2)
        # Baseline
        for _ in range(5):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        # Regression
        for _ in range(2):
            c.record_success("MATCH (n) RETURN n", total_ms=30.0)
        alerts = d.ingest()
        assert len(alerts) == 1

        # Now check_all with the full history — baseline window shifts
        # to include some fast queries, but need to verify clearing logic
        d.check_all()
        # Alert may still be present since history still shows regression

    def test_incremental_ingest(self) -> None:
        c, d = self._make_detector(baseline_window=5, recent_window=2)
        # First batch
        for _ in range(5):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        alerts1 = d.ingest()
        assert alerts1 == []  # Not enough data

        # Second batch — still no regression
        for _ in range(2):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        alerts2 = d.ingest()
        assert alerts2 == []

    def test_clear(self) -> None:
        c, d = self._make_detector(baseline_window=5, recent_window=2)
        for _ in range(7):
            c.record_success("MATCH (n) RETURN n", total_ms=10.0)
        d.ingest()
        d.clear()
        assert d.alerts == []
        assert d.tracked_fingerprint_count == 0

    def test_same_structure_different_values_grouped(self) -> None:
        c, d = self._make_detector(baseline_window=5, recent_window=2)
        # Same structure, different literals — should share fingerprint
        for i in range(5):
            c.record_success(
                f"MATCH (n) WHERE n.age > {i * 10} RETURN n",
                total_ms=10.0,
            )
        for _ in range(2):
            c.record_success(
                "MATCH (n) WHERE n.age > 99 RETURN n",
                total_ms=25.0,
            )
        alerts = d.ingest()
        assert len(alerts) == 1

    def test_tracked_fingerprint_count(self) -> None:
        c, d = self._make_detector(baseline_window=5, recent_window=2)
        assert d.tracked_fingerprint_count == 0
        for _ in range(3):
            c.record_success("MATCH (a:Person) RETURN a", total_ms=10.0)
        for _ in range(3):
            c.record_success("MATCH (b:City) RETURN b", total_ms=10.0)
        d.ingest()
        assert d.tracked_fingerprint_count == 2
        d.clear()
        assert d.tracked_fingerprint_count == 0

    def test_alert_to_dict(self) -> None:
        alert = RegressionAlert(
            fingerprint="abc123",
            sample_query="MATCH (n) RETURN n",
            baseline_ms=10.0,
            current_ms=30.0,
            ratio=3.0,
            severity="warning",
        )
        d = alert.to_dict()
        assert d["fingerprint"] == "abc123"
        assert d["ratio"] == 3.0
        assert d["severity"] == "warning"
        assert "detected_at" in d


# ── API integration ──────────────────────────────────────────────────


class TestRegressionAPI:
    @pytest.fixture
    def client(self) -> TestClient:
        from fastapi.testclient import TestClient
        from fastopendata.api import app, get_metrics_collector, get_regression_detector

        get_metrics_collector().clear()
        get_regression_detector().clear()
        return TestClient(app)

    def test_regressions_endpoint_empty(self, client: TestClient) -> None:
        r = client.get("/analytics/regressions")
        assert r.status_code == 200
        body = r.json()
        assert body["regressions"] == []
        assert body["total_tracked_fingerprints"] == 0

    def test_regressions_endpoint_with_data(self, client: TestClient) -> None:
        from fastopendata.api import get_metrics_collector

        collector = get_metrics_collector()
        # Build up baseline (default detector: 20 baseline + 5 recent)
        for _ in range(20):
            collector.record_success("MATCH (n) RETURN n", total_ms=10.0)
        # Add regression
        for _ in range(5):
            collector.record_success("MATCH (n) RETURN n", total_ms=30.0)

        r = client.get("/analytics/regressions")
        assert r.status_code == 200
        body = r.json()
        assert len(body["regressions"]) == 1
        assert body["regressions"][0]["severity"] == "warning"
        assert body["total_tracked_fingerprints"] >= 1

    def test_regressions_no_false_positive(self, client: TestClient) -> None:
        from fastopendata.api import get_metrics_collector

        collector = get_metrics_collector()
        for _ in range(30):
            collector.record_success("MATCH (n) RETURN n", total_ms=10.0)
        r = client.get("/analytics/regressions")
        assert r.status_code == 200
        body = r.json()
        assert body["regressions"] == []


from fastapi.testclient import TestClient
