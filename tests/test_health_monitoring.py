"""Tests for the monitoring and health check framework.

Covers:
- nmetl health CLI subcommand (exit codes, JSON output, verbose mode)
- HTTP health server endpoints (/health, /ready, /metrics)
- Health status classification from metrics
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from pycypher.nmetl_cli import cli

# ---------------------------------------------------------------------------
# nmetl health CLI tests
# ---------------------------------------------------------------------------


class TestHealthCLI:
    """Test the ``nmetl health`` CLI subcommand."""

    def test_health_returns_json(self) -> None:
        """--json flag produces valid JSON with expected structure."""
        runner = CliRunner()
        result = runner.invoke(cli, ["health", "--json"])
        # Exit code 0 = healthy (no queries recorded)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "status" in data
        assert "checks" in data
        assert "metrics" in data["checks"]
        assert "memory" in data["checks"]
        assert "uptime" in data["checks"]

    def test_health_default_healthy(self) -> None:
        """With no queries, health should be 'healthy' (exit code 0)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["health"])
        assert result.exit_code == 0
        assert "HEALTHY" in result.output

    def test_health_verbose_includes_rss(self) -> None:
        """Verbose mode includes RSS memory detail."""
        runner = CliRunner()
        result = runner.invoke(cli, ["health", "-v"])
        assert result.exit_code == 0
        assert "rss_mb" in result.output

    def test_health_json_structure(self) -> None:
        """JSON output has correct nested structure."""
        runner = CliRunner()
        result = runner.invoke(cli, ["health", "--json"])
        data = json.loads(result.output)

        # Each check has a status field
        for check_name, check_data in data["checks"].items():
            assert "status" in check_data, (
                f"Check '{check_name}' missing status"
            )


# ---------------------------------------------------------------------------
# Health status classification tests
# ---------------------------------------------------------------------------


class TestHealthStatusClassification:
    """Test that health_status() classifies correctly."""

    def test_no_queries_is_healthy(self) -> None:
        """Zero queries should classify as healthy."""
        from shared.metrics import QueryMetrics

        collector = QueryMetrics()
        snap = collector.snapshot()
        assert snap.health_status() == "healthy"

    def test_high_error_rate_is_unhealthy(self) -> None:
        """Error rate > 20% should classify as unhealthy."""
        from shared.metrics import QueryMetrics

        collector = QueryMetrics()
        # Record 70 successes and 30 errors -> 30% error rate
        for i in range(70):
            collector.record_query(
                query_id=f"ok-{i}", elapsed_s=0.01, rows=1, clauses=["Match"]
            )
        for i in range(30):
            collector.record_error(
                query_id=f"err-{i}", error_type="TypeError", elapsed_s=0.01
            )
        snap = collector.snapshot()
        assert snap.health_status() == "unhealthy"

    def test_moderate_error_rate_is_degraded(self) -> None:
        """Error rate 5-20% should classify as degraded."""
        from shared.metrics import QueryMetrics

        collector = QueryMetrics()
        # Record 90 successes and 10 errors -> 10% error rate
        for i in range(90):
            collector.record_query(
                query_id=f"ok-{i}", elapsed_s=0.01, rows=1, clauses=["Match"]
            )
        for i in range(10):
            collector.record_error(
                query_id=f"err-{i}", error_type="TypeError", elapsed_s=0.01
            )
        snap = collector.snapshot()
        assert snap.health_status() == "degraded"


# ---------------------------------------------------------------------------
# HTTP health server tests
# ---------------------------------------------------------------------------


class TestHealthServer:
    """Test the HTTP health check server."""

    @staticmethod
    def _start_health_server() -> tuple[Any, int]:
        """Start a health server on an OS-assigned free port and return (server, port)."""
        from http.server import HTTPServer

        from pycypher.health_server import _HealthHandler

        server = HTTPServer(("127.0.0.1", 0), _HealthHandler)
        port = server.server_address[1]
        server_thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
        )
        server_thread.start()
        time.sleep(0.1)
        return server, port

    def test_health_endpoint_returns_200(self) -> None:
        """GET /health returns 200 with healthy status."""
        from http.client import HTTPConnection

        server, port = self._start_health_server()
        try:
            conn = HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/health")
            resp = conn.getresponse()
            body = json.loads(resp.read())

            assert resp.status == 200
            assert body["status"] == "healthy"
            conn.close()
        finally:
            server.shutdown()

    def test_ready_endpoint_returns_200(self) -> None:
        """GET /ready returns 200 when healthy."""
        from http.client import HTTPConnection

        server, port = self._start_health_server()
        try:
            conn = HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/ready")
            resp = conn.getresponse()
            body = json.loads(resp.read())

            assert resp.status == 200
            assert body["status"] in {"healthy", "degraded"}
            conn.close()
        finally:
            server.shutdown()

    def test_metrics_endpoint_returns_text(self) -> None:
        """GET /metrics returns Prometheus text format or fallback."""
        from http.client import HTTPConnection

        server, port = self._start_health_server()
        try:
            conn = HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/metrics")
            resp = conn.getresponse()
            body = resp.read().decode()

            assert resp.status == 200
            # Should contain either prometheus metrics or fallback comment
            assert "pycypher" in body or "# No metrics" in body
            conn.close()
        finally:
            server.shutdown()

    def test_404_for_unknown_path(self) -> None:
        """GET /unknown returns 404."""
        from http.client import HTTPConnection

        server, port = self._start_health_server()
        try:
            conn = HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/unknown")
            resp = conn.getresponse()

            assert resp.status == 404
            conn.close()
        finally:
            server.shutdown()
