"""Tests for the health server HTTP endpoints.

Validates /health, /ready, /metrics endpoints for container orchestration
integration (Docker/Kubernetes liveness and readiness probes).
"""

from __future__ import annotations

import json
import urllib.request
from http.server import HTTPServer
from threading import Thread
from unittest.mock import patch

import pytest
from pycypher.health_server import _HealthHandler, run_health_server

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _start_server(port: int) -> tuple[HTTPServer, Thread]:
    """Start a health server on *port* in a background thread."""
    server = HTTPServer(("127.0.0.1", port), _HealthHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _get(port: int, path: str) -> tuple[int, dict[str, str], bytes]:
    """Issue a GET request and return (status_code, headers, body)."""
    url = f"http://127.0.0.1:{port}{path}"
    req = urllib.request.Request(url)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Use a port range unlikely to collide with other tests.
_PORT_COUNTER = 18400


@pytest.fixture
def health_port():
    """Allocate a unique port for each test."""
    global _PORT_COUNTER
    _PORT_COUNTER += 1
    return _PORT_COUNTER


@pytest.fixture
def server(health_port: int):
    """Start a health server and yield the port; shut down on cleanup."""
    srv, _thread = _start_server(health_port)
    yield health_port
    srv.shutdown()


# ---------------------------------------------------------------------------
# /health endpoint
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    """Liveness probe: /health."""

    def test_healthy_returns_200(self, server: int) -> None:
        """Default state (no queries) is healthy → 200."""
        status, headers, body = _get(server, "/health")
        assert status == 200
        payload = json.loads(body)
        assert payload["status"] == "healthy"

    def test_healthy_content_type_json(self, server: int) -> None:
        """Response Content-Type is application/json."""
        _status, headers, _body = _get(server, "/health")
        assert "application/json" in headers.get("Content-Type", "")

    def test_unhealthy_returns_503(self, server: int) -> None:
        """When health_status() reports unhealthy, /health returns 503."""
        with patch(
            "pycypher.health_server._HealthHandler._get_health_status",
            return_value="unhealthy",
        ):
            status, _headers, body = _get(server, "/health")
            assert status == 503
            payload = json.loads(body)
            assert payload["status"] == "unhealthy"

    def test_degraded_returns_200(self, server: int) -> None:
        """Degraded is still alive → 200 for liveness."""
        with patch(
            "pycypher.health_server._HealthHandler._get_health_status",
            return_value="degraded",
        ):
            status, _headers, body = _get(server, "/health")
            assert status == 200
            payload = json.loads(body)
            assert payload["status"] == "degraded"


# ---------------------------------------------------------------------------
# /ready endpoint
# ---------------------------------------------------------------------------


class TestReadyEndpoint:
    """Readiness probe: /ready."""

    def test_healthy_returns_200(self, server: int) -> None:
        """Healthy → ready (200)."""
        status, _headers, body = _get(server, "/ready")
        assert status == 200
        payload = json.loads(body)
        assert payload["status"] == "healthy"

    def test_degraded_returns_200(self, server: int) -> None:
        """Degraded is still ready → 200."""
        with patch(
            "pycypher.health_server._HealthHandler._get_health_status",
            return_value="degraded",
        ):
            status, _headers, body = _get(server, "/ready")
            assert status == 200
            payload = json.loads(body)
            assert payload["status"] == "degraded"

    def test_unhealthy_returns_503(self, server: int) -> None:
        """Unhealthy → not ready (503)."""
        with patch(
            "pycypher.health_server._HealthHandler._get_health_status",
            return_value="unhealthy",
        ):
            status, _headers, body = _get(server, "/ready")
            assert status == 503
            payload = json.loads(body)
            assert payload["status"] == "unhealthy"

    def test_ready_content_type_json(self, server: int) -> None:
        """Response Content-Type is application/json."""
        _status, headers, _body = _get(server, "/ready")
        assert "application/json" in headers.get("Content-Type", "")


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------


class TestMetricsEndpoint:
    """Prometheus text exposition: /metrics."""

    def test_metrics_returns_200(self, server: int) -> None:
        """/metrics always returns 200."""
        status, _headers, _body = _get(server, "/metrics")
        assert status == 200

    def test_metrics_content_type_text(self, server: int) -> None:
        """Metrics Content-Type is text/plain."""
        _status, headers, _body = _get(server, "/metrics")
        assert "text/plain" in headers.get("Content-Type", "")

    def test_metrics_fallback_when_no_exporter(self, server: int) -> None:
        """When PrometheusExporter is unavailable, returns a fallback body."""
        with patch(
            "pycypher.health_server._HealthHandler._handle_metrics",
        ) as mock_handle:
            # Simulate ImportError path by calling the real method with
            # PrometheusExporter import failing.
            pass  # We'll test the fallback differently

        # Actually patch the import to fail
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "shared.exporters":
                raise ImportError("test: no exporter")
            return original_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", side_effect=mock_import):
            status, _headers, body = _get(server, "/metrics")
            assert status == 200
            assert b"No metrics exporter" in body


# ---------------------------------------------------------------------------
# 404 for unknown paths
# ---------------------------------------------------------------------------


class TestUnknownPaths:
    """Unknown paths should return 404."""

    def test_root_returns_404(self, server: int) -> None:
        """/ is not a health endpoint."""
        status, _headers, _body = _get(server, "/")
        assert status == 404

    def test_unknown_path_returns_404(self, server: int) -> None:
        """/foo returns 404."""
        status, _headers, _body = _get(server, "/foo")
        assert status == 404


# ---------------------------------------------------------------------------
# Content-Length header
# ---------------------------------------------------------------------------


class TestContentLength:
    """Verify Content-Length header is set correctly."""

    def test_health_content_length(self, server: int) -> None:
        """Content-Length matches actual body size."""
        _status, headers, body = _get(server, "/health")
        assert int(headers["Content-Length"]) == len(body)

    def test_ready_content_length(self, server: int) -> None:
        """Content-Length matches actual body size."""
        _status, headers, body = _get(server, "/ready")
        assert int(headers["Content-Length"]) == len(body)

    def test_metrics_content_length(self, server: int) -> None:
        """Content-Length matches actual body size."""
        _status, headers, body = _get(server, "/metrics")
        assert int(headers["Content-Length"]) == len(body)


# ---------------------------------------------------------------------------
# run_health_server function
# ---------------------------------------------------------------------------


class TestRunHealthServer:
    """Test the run_health_server entry point."""

    def test_keyboard_interrupt_shuts_down(self) -> None:
        """run_health_server exits cleanly on KeyboardInterrupt."""
        with (
            patch.object(
                HTTPServer, "serve_forever", side_effect=KeyboardInterrupt
            ),
            patch.object(HTTPServer, "server_close") as mock_close,
        ):
            run_health_server(port=18499)
            mock_close.assert_called_once()


# ---------------------------------------------------------------------------
# Log suppression
# ---------------------------------------------------------------------------


class TestLogSuppression:
    """Verify access logs go through Python logging, not stderr."""

    def test_log_message_uses_logger(self, server: int) -> None:
        """Access log messages are routed through LOGGER.debug."""
        with patch("pycypher.health_server.LOGGER") as mock_logger:
            _get(server, "/health")
            # The handler calls log_message which routes to LOGGER.debug
            # We just verify it was called (at least once for the request).
            assert mock_logger.debug.called
