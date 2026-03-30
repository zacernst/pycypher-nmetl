"""Lightweight HTTP health check server for container orchestration.

Provides liveness and readiness probes for Docker/Kubernetes deployments
using only Python stdlib (no external dependencies).

Endpoints:

- ``GET /health`` — Liveness probe.  Returns 200 if the process is alive,
  503 if metrics report *unhealthy* status.
- ``GET /ready`` — Readiness probe.  Returns 200 if *healthy* or *degraded*,
  503 only if *unhealthy*.
- ``GET /metrics`` — Prometheus text exposition format (if queries recorded).

Start via CLI::

    nmetl health-server --port 8079

Or programmatically::

    from pycypher.health_server import run_health_server
    run_health_server(host="0.0.0.0", port=8079)
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from shared.logger import LOGGER


class _HealthHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health check endpoints."""

    def do_GET(self) -> None:
        """Handle GET requests for health, readiness, and metrics."""
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/ready":
            self._handle_ready()
        elif self.path == "/metrics":
            self._handle_metrics()
        else:
            self.send_error(404, "Not Found")

    def _get_health_status(self) -> str:
        """Get current health status from metrics collector."""
        from shared.metrics import QUERY_METRICS

        snap = QUERY_METRICS.snapshot()
        return snap.health_status()

    def _handle_health(self) -> None:
        """Liveness probe: 200 unless unhealthy."""
        status = self._get_health_status()
        code = 503 if status == "unhealthy" else 200
        body = json.dumps({"status": status}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_ready(self) -> None:
        """Readiness probe: 200 if healthy/degraded, 503 if unhealthy."""
        status = self._get_health_status()
        code = 503 if status == "unhealthy" else 200
        body = json.dumps({"status": status}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_metrics(self) -> None:
        """Prometheus text exposition endpoint."""
        try:
            from shared.exporters import PrometheusExporter
            from shared.metrics import QUERY_METRICS

            snap = QUERY_METRICS.snapshot()
            exporter = PrometheusExporter()
            body = exporter.render(snap).encode()
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/plain; version=0.0.4; charset=utf-8",
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except ImportError:
            # PrometheusExporter not available
            body = b"# No metrics exporter available\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Route access logs through Python logging instead of stderr."""
        LOGGER.debug(format, *args)


def run_health_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8079,
) -> None:
    """Start the health check HTTP server (blocking).

    Args:
        host: Address to bind to.  Use ``"0.0.0.0"`` for all interfaces.
        port: TCP port number.

    Raises:
        KeyboardInterrupt: Cleanly shuts down on Ctrl-C.

    """
    server = HTTPServer((host, port), _HealthHandler)
    LOGGER.info("Health server listening on %s:%d", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("Health server shutting down")
        server.server_close()
