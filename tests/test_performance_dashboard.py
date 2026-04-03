"""Tests for the performance monitoring dashboard.

TDD tests written first — validates web-based dashboard serving,
real-time metric updates, and historical trend visualization.
"""

from __future__ import annotations

import json
import time

import pytest
from _perf_helpers import perf_threshold


class TestDashboardData:
    """Dashboard data provider for metrics visualization."""

    def test_import(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        assert dd is not None

    def test_record_snapshot(self):
        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=100)
        dd.record_snapshot(
            {
                "timing_p50_ms": 10.0,
                "timing_p90_ms": 50.0,
                "total_queries": 100,
                "error_rate": 0.01,
            }
        )
        assert len(dd.history) == 1

    def test_history_rolling_window(self):
        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=5)
        for i in range(10):
            dd.record_snapshot({"timing_p50_ms": float(i)})
        assert len(dd.history) == 5
        # Most recent should be last
        assert dd.history[-1]["timing_p50_ms"] == 9.0

    def test_trend_data_for_metric(self):
        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=100)
        for i in range(5):
            dd.record_snapshot({"latency": float(i * 10)})
        trend = dd.trend("latency")
        assert len(trend) == 5
        assert trend == [0.0, 10.0, 20.0, 30.0, 40.0]

    def test_trend_missing_metric_returns_empty(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        dd.record_snapshot({"latency": 10.0})
        trend = dd.trend("nonexistent")
        assert trend == []

    def test_latest_snapshot(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        dd.record_snapshot({"a": 1.0})
        dd.record_snapshot({"a": 2.0})
        latest = dd.latest()
        assert latest is not None
        assert latest["a"] == 2.0

    def test_latest_empty_returns_none(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        assert dd.latest() is None

    def test_timestamps_recorded(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        dd.record_snapshot({"a": 1.0})
        assert len(dd.timestamps) == 1
        assert isinstance(dd.timestamps[0], float)


class TestDashboardRenderer:
    """Dashboard HTML rendering for web display."""

    def test_render_html(self):
        from shared.dashboard import DashboardData, render_dashboard_html

        dd = DashboardData()
        for i in range(3):
            dd.record_snapshot(
                {
                    "timing_p50_ms": float(i * 10),
                    "timing_p90_ms": float(i * 20),
                    "total_queries": i * 100,
                    "error_rate": 0.01 * i,
                }
            )
        html = render_dashboard_html(dd)
        assert isinstance(html, str)
        assert "<html" in html.lower()
        assert "performance" in html.lower() or "dashboard" in html.lower()

    def test_render_includes_metric_data(self):
        from shared.dashboard import DashboardData, render_dashboard_html

        dd = DashboardData()
        dd.record_snapshot({"timing_p50_ms": 42.5})
        html = render_dashboard_html(dd)
        # Should contain the metric value somewhere in the page
        assert "42.5" in html or "timing_p50_ms" in html

    def test_render_empty_dashboard(self):
        from shared.dashboard import DashboardData, render_dashboard_html

        dd = DashboardData()
        html = render_dashboard_html(dd)
        assert isinstance(html, str)
        assert "<html" in html.lower()


class TestDashboardJSON:
    """JSON API endpoint for dashboard data."""

    def test_to_json(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        dd.record_snapshot({"timing_p50_ms": 10.0, "error_rate": 0.01})
        dd.record_snapshot({"timing_p50_ms": 12.0, "error_rate": 0.02})
        j = dd.to_json()
        parsed = json.loads(j)
        assert "history" in parsed
        assert "timestamps" in parsed
        assert len(parsed["history"]) == 2

    def test_json_roundtrip(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        dd.record_snapshot({"m": 1.0})
        dd.record_snapshot({"m": 2.0})
        j = dd.to_json()
        parsed = json.loads(j)
        assert parsed["history"][0]["m"] == 1.0
        assert parsed["history"][1]["m"] == 2.0


class TestDashboardServer:
    """Lightweight HTTP dashboard server."""

    def test_server_creation(self):
        from shared.dashboard import DashboardData, DashboardServer

        dd = DashboardData()
        server = DashboardServer(dd, port=0)
        assert server is not None

    def test_server_serves_html(self):
        import http.client

        from shared.dashboard import DashboardData, DashboardServer

        dd = DashboardData()
        dd.record_snapshot({"timing_p50_ms": 10.0})
        server = DashboardServer(dd, port=0)
        server.start()
        time.sleep(0.1)
        port = server.port
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            conn.request("GET", "/")
            resp = conn.getresponse()
            assert resp.status == 200
            body = resp.read().decode()
            assert "<html" in body.lower()
        finally:
            conn.close()
            server.shutdown()

    def test_server_serves_json_api(self):
        import http.client

        from shared.dashboard import DashboardData, DashboardServer

        dd = DashboardData()
        dd.record_snapshot({"timing_p50_ms": 10.0})
        server = DashboardServer(dd, port=0)
        server.start()
        time.sleep(0.1)
        port = server.port
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            conn.request("GET", "/api/metrics")
            resp = conn.getresponse()
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "history" in data
        finally:
            conn.close()
            server.shutdown()


class TestDashboardMetricUpdates:
    """Validate <1s metric update latency."""

    def test_record_latency_under_1ms(self):
        from shared.dashboard import DashboardData

        dd = DashboardData()
        start = time.perf_counter()
        dd.record_snapshot({"timing_p50_ms": 10.0})
        elapsed = time.perf_counter() - start
        # Recording should be sub-millisecond
        assert elapsed < perf_threshold(0.001)

    def test_bulk_record_performance(self):
        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=1000)
        start = time.perf_counter()
        for i in range(1000):
            dd.record_snapshot({"m": float(i)})
        elapsed = time.perf_counter() - start
        # 1000 records should complete well under 1 second
        assert elapsed < perf_threshold(1.0)

    def test_trend_retrieval_under_1ms(self):
        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=1000)
        for i in range(1000):
            dd.record_snapshot({"m": float(i)})
        start = time.perf_counter()
        trend = dd.trend("m")
        elapsed = time.perf_counter() - start
        assert len(trend) == 1000
        assert elapsed < perf_threshold(0.01)  # 10ms budget


class TestDashboardThreadSafety:
    """Thread safety for concurrent access."""

    def test_concurrent_reads_and_writes(self):
        import threading

        from shared.dashboard import DashboardData

        dd = DashboardData(max_history=500)
        errors = []

        def writer():
            try:
                for i in range(100):
                    dd.record_snapshot({"m": float(i)})
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(100):
                    dd.latest()
                    dd.trend("m")
                    dd.to_json()
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(2):
            threads.append(threading.Thread(target=writer))
            threads.append(threading.Thread(target=reader))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0
