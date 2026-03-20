"""Tests for metrics export adapters (Prometheus, StatsD, JSON).

Verifies that each exporter correctly transforms MetricsSnapshot
data into the target format without requiring external services.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from shared.exporters import (
    JSONFileExporter,
    PrometheusExporter,
    StatsDExporter,
    export_once,
    get_exporters,
)
from shared.metrics import QueryMetrics

# ---------------------------------------------------------------------------
# Fixture: a realistic MetricsSnapshot
# ---------------------------------------------------------------------------


@pytest.fixture
def snapshot() -> MagicMock:
    """Create a mock MetricsSnapshot with realistic values."""
    snap = MagicMock()
    snap.total_queries = 1000
    snap.total_errors = 15
    snap.slow_queries = 8
    snap.total_rows_returned = 50_000
    snap.timing_p50_ms = 12.5
    snap.timing_p90_ms = 35.2
    snap.timing_p99_ms = 120.8
    snap.timing_max_ms = 450.0
    snap.queries_per_second = 16.7
    snap.recent_queries_per_second = 14.2
    snap.error_rate = 0.015
    snap.recent_error_rate = 0.02
    snap.memory_delta_p50_mb = 2.1
    snap.memory_delta_max_mb = 45.3
    snap.parse_time_p50_ms = 1.2
    snap.plan_time_p50_ms = 0.8
    snap.result_cache_hits = 300
    snap.result_cache_misses = 700
    snap.result_cache_hit_rate = 0.30
    snap.result_cache_evictions = 50
    snap.error_counts = {"TypeError": 10, "ValueError": 5}
    snap.clause_timing_p50_ms = {"Match": 8.5, "Return": 3.2}
    snap.uptime_s = 60.0
    snap.health_status.return_value = "healthy"
    snap.to_dict.return_value = {
        "total_queries": 1000,
        "total_errors": 15,
        "timing_p50_ms": 12.5,
    }
    return snap


@pytest.fixture
def real_snapshot() -> object:
    """Create a real MetricsSnapshot from QueryMetrics."""
    metrics = QueryMetrics()
    for i in range(10):
        metrics.record_query(
            query_id=f"q-{i}",
            elapsed_s=0.01 * (i + 1),
            rows=100 * (i + 1),
            clauses=["Match", "Return"],
        )
    metrics.record_error(
        query_id="q-err",
        error_type="TypeError",
        elapsed_s=0.005,
    )
    return metrics.snapshot()


# ---------------------------------------------------------------------------
# PrometheusExporter
# ---------------------------------------------------------------------------


class TestPrometheusExporter:
    """Verify Prometheus text exposition format output."""

    def test_render_contains_help_and_type(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert "# HELP pycypher_queries_total" in text
        assert "# TYPE pycypher_queries_total counter" in text

    def test_render_contains_core_metrics(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert "pycypher_queries_total 1000" in text
        assert "pycypher_errors_total 15" in text
        assert "pycypher_slow_queries_total 8" in text
        assert "pycypher_rows_returned_total 50000" in text

    def test_render_contains_latency_gauges(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert "pycypher_query_duration_p50_ms 12.5" in text
        assert "pycypher_query_duration_p90_ms 35.2" in text
        assert "pycypher_query_duration_p99_ms 120.8" in text

    def test_render_contains_error_breakdown(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert 'pycypher_errors_by_type{type="TypeError"} 10' in text
        assert 'pycypher_errors_by_type{type="ValueError"} 5' in text

    def test_render_contains_clause_timing(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert 'pycypher_clause_duration_p50_ms{clause="Match"} 8.5' in text
        assert 'pycypher_clause_duration_p50_ms{clause="Return"} 3.2' in text

    def test_render_contains_cache_stats(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert "pycypher_cache_hits_total 300" in text
        assert "pycypher_cache_misses_total 700" in text
        assert "pycypher_cache_hit_rate 0.3" in text

    def test_render_contains_health_status(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(snapshot)
        assert "pycypher_health_status 0" in text

    def test_custom_prefix(self, snapshot: MagicMock) -> None:
        exporter = PrometheusExporter(prefix="myapp")
        text = exporter.render(snapshot)
        assert "myapp_queries_total 1000" in text
        assert "pycypher_" not in text

    def test_no_cache_stats_when_zero(self) -> None:
        snap = MagicMock()
        snap.total_queries = 0
        snap.total_errors = 0
        snap.slow_queries = 0
        snap.total_rows_returned = 0
        snap.timing_p50_ms = 0.0
        snap.timing_p90_ms = 0.0
        snap.timing_p99_ms = 0.0
        snap.timing_max_ms = 0.0
        snap.queries_per_second = 0.0
        snap.recent_queries_per_second = 0.0
        snap.error_rate = 0.0
        snap.recent_error_rate = 0.0
        snap.memory_delta_p50_mb = 0.0
        snap.memory_delta_max_mb = 0.0
        snap.parse_time_p50_ms = 0.0
        snap.plan_time_p50_ms = 0.0
        snap.result_cache_hits = 0
        snap.result_cache_misses = 0
        snap.error_counts = {}
        snap.clause_timing_p50_ms = {}
        snap.uptime_s = 0.0
        snap.health_status.return_value = "healthy"

        exporter = PrometheusExporter()
        text = exporter.render(snap)
        assert "cache_hits_total" not in text

    def test_render_with_real_snapshot(self, real_snapshot: object) -> None:
        exporter = PrometheusExporter()
        text = exporter.render(real_snapshot)
        assert "pycypher_queries_total 10" in text
        assert "pycypher_errors_total 1" in text

    def test_exporter_name(self) -> None:
        assert PrometheusExporter().name == "prometheus"

    def test_export_logs(
        self,
        snapshot: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import logging

        with caplog.at_level(logging.INFO, logger="shared.exporters"):
            PrometheusExporter().export(snapshot)
        assert "Prometheus metrics export" in caplog.text


# ---------------------------------------------------------------------------
# StatsDExporter
# ---------------------------------------------------------------------------


class TestStatsDExporter:
    """Verify StatsD UDP metric emission."""

    def test_default_host_and_port(self) -> None:
        exporter = StatsDExporter()
        assert exporter._host == "127.0.0.1"
        assert exporter._port == 8125

    def test_custom_host_and_port(self) -> None:
        exporter = StatsDExporter(host="statsd.local", port=9125)
        assert exporter._host == "statsd.local"
        assert exporter._port == 9125

    def test_env_var_config(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PYCYPHER_STATSD_HOST": "metrics.prod",
                "PYCYPHER_STATSD_PORT": "9999",
            },
        ):
            exporter = StatsDExporter()
            assert exporter._host == "metrics.prod"
            assert exporter._port == 9999

    def test_export_sends_udp_packets(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = StatsDExporter()
        mock_sock = MagicMock(spec=socket.socket)
        exporter._sock = mock_sock

        exporter.export(snapshot)

        assert mock_sock.sendto.call_count > 0
        first_call = mock_sock.sendto.call_args_list[0]
        data = first_call[0][0].decode("utf-8")
        assert "pycypher.queries.total:1000|c" == data

    def test_export_handles_socket_error(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = StatsDExporter()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.sendto.side_effect = OSError("connection refused")
        exporter._sock = mock_sock

        # Should not raise
        exporter.export(snapshot)

    def test_exporter_name(self) -> None:
        assert StatsDExporter().name == "statsd"

    def test_health_status_sent(self, snapshot: MagicMock) -> None:
        exporter = StatsDExporter()
        mock_sock = MagicMock(spec=socket.socket)
        exporter._sock = mock_sock

        exporter.export(snapshot)

        all_data = [
            call[0][0].decode("utf-8")
            for call in mock_sock.sendto.call_args_list
        ]
        assert "pycypher.health_status:0|g" in all_data


# ---------------------------------------------------------------------------
# JSONFileExporter
# ---------------------------------------------------------------------------


class TestJSONFileExporter:
    """Verify JSON-lines file export."""

    def test_appends_jsonl(
        self,
        snapshot: MagicMock,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / "metrics.jsonl"
        exporter = JSONFileExporter(path=str(path))

        exporter.export(snapshot)
        exporter.export(snapshot)

        lines = path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        data = json.loads(lines[0])
        assert data["total_queries"] == 1000
        assert "_timestamp" in data
        assert data["_health"] == "healthy"

    def test_handles_write_error(
        self,
        snapshot: MagicMock,
    ) -> None:
        exporter = JSONFileExporter(path="/nonexistent/dir/metrics.jsonl")
        # Should not raise
        exporter.export(snapshot)

    def test_default_path(self) -> None:
        exporter = JSONFileExporter()
        assert exporter._path == Path("metrics.jsonl")

    def test_env_var_path(self) -> None:
        with patch.dict(
            os.environ,
            {"PYCYPHER_METRICS_JSON_PATH": "/tmp/custom.jsonl"},
        ):
            exporter = JSONFileExporter()
            assert exporter._path == Path("/tmp/custom.jsonl")

    def test_exporter_name(self) -> None:
        assert JSONFileExporter().name == "json"

    def test_with_real_snapshot(
        self,
        real_snapshot: object,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / "real_metrics.jsonl"
        exporter = JSONFileExporter(path=str(path))

        exporter.export(real_snapshot)

        data = json.loads(
            path.read_text(encoding="utf-8").strip(),
        )
        assert data["total_queries"] == 10
        assert data["total_errors"] == 1


# ---------------------------------------------------------------------------
# get_exporters factory
# ---------------------------------------------------------------------------


class TestGetExporters:
    """Verify auto-configuration from environment variables."""

    def test_empty_env_returns_empty(self) -> None:
        with patch.dict(os.environ, {"PYCYPHER_METRICS_EXPORT": ""}):
            assert get_exporters() == []

    def test_no_env_returns_empty(self) -> None:
        env = os.environ.copy()
        env.pop("PYCYPHER_METRICS_EXPORT", None)
        with patch.dict(os.environ, env, clear=True):
            assert get_exporters() == []

    def test_single_exporter(self) -> None:
        with patch.dict(
            os.environ,
            {"PYCYPHER_METRICS_EXPORT": "prometheus"},
        ):
            exporters = get_exporters()
            assert len(exporters) == 1
            assert isinstance(exporters[0], PrometheusExporter)

    def test_multiple_exporters(self) -> None:
        with patch.dict(
            os.environ,
            {"PYCYPHER_METRICS_EXPORT": "prometheus,json,statsd"},
        ):
            exporters = get_exporters()
            assert len(exporters) == 3
            types = {type(e) for e in exporters}
            assert types == {
                PrometheusExporter,
                JSONFileExporter,
                StatsDExporter,
            }

    def test_unknown_exporter_skipped(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import logging

        with (
            caplog.at_level(logging.WARNING, logger="shared.exporters"),
            patch.dict(
                os.environ,
                {"PYCYPHER_METRICS_EXPORT": "prometheus,foobar"},
            ),
        ):
            exporters = get_exporters()
            assert len(exporters) == 1
            assert "Unknown metrics exporter" in caplog.text

    def test_whitespace_handling(self) -> None:
        with patch.dict(
            os.environ,
            {"PYCYPHER_METRICS_EXPORT": " prometheus , json "},
        ):
            exporters = get_exporters()
            assert len(exporters) == 2


# ---------------------------------------------------------------------------
# export_once convenience function
# ---------------------------------------------------------------------------


class TestExportOnce:
    """Verify export_once pushes to all configured exporters."""

    def test_pushes_to_all_exporters(
        self,
        snapshot: MagicMock,
    ) -> None:
        with patch.dict(
            os.environ,
            {"PYCYPHER_METRICS_EXPORT": "prometheus,statsd"},
        ):
            with (
                patch.object(
                    PrometheusExporter,
                    "export",
                ) as prom_mock,
                patch.object(
                    StatsDExporter,
                    "export",
                ) as statsd_mock,
            ):
                export_once(snapshot)
                prom_mock.assert_called_once_with(snapshot)
                statsd_mock.assert_called_once_with(snapshot)

    def test_continues_on_exporter_error(
        self,
        snapshot: MagicMock,
    ) -> None:
        with patch.dict(
            os.environ,
            {"PYCYPHER_METRICS_EXPORT": "prometheus,json"},
        ):
            with (
                patch.object(
                    PrometheusExporter,
                    "export",
                    side_effect=RuntimeError("boom"),
                ),
                patch.object(
                    JSONFileExporter,
                    "export",
                ) as json_mock,
            ):
                export_once(snapshot)
                json_mock.assert_called_once_with(snapshot)
