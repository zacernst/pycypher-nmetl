"""Performance monitoring dashboard with web-based visualization.

Collects metric snapshots over time and serves a lightweight HTML dashboard
with real-time charts and a JSON API.

Usage::

    from shared.dashboard import DashboardData, DashboardServer

    dd = DashboardData(max_history=1000)

    # Record periodic snapshots
    dd.record_snapshot(metrics.snapshot().to_dict())

    # Serve dashboard on port 8080
    server = DashboardServer(dd, port=8080)
    server.start()  # non-blocking, runs in background thread

"""

from __future__ import annotations

import html
import http.server
import json
import threading
import time
from typing import Any


class DashboardData:
    """Thread-safe time-series store for dashboard metric snapshots.

    Args:
        max_history: Maximum number of snapshots to retain.
            Defaults to 500.

    """

    def __init__(self, *, max_history: int = 500) -> None:
        self._history: list[dict[str, Any]] = []
        self._timestamps: list[float] = []
        self._max_history = max_history
        self._lock = threading.Lock()

    def record_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Record a metric snapshot with the current timestamp."""
        now = time.time()
        with self._lock:
            self._history.append(dict(snapshot))
            self._timestamps.append(now)
            if len(self._history) > self._max_history:
                excess = len(self._history) - self._max_history
                self._history = self._history[excess:]
                self._timestamps = self._timestamps[excess:]

    @property
    def history(self) -> list[dict[str, Any]]:
        """Return a copy of the snapshot history."""
        with self._lock:
            return list(self._history)

    @property
    def timestamps(self) -> list[float]:
        """Return a copy of the timestamp list."""
        with self._lock:
            return list(self._timestamps)

    def latest(self) -> dict[str, Any] | None:
        """Return the most recent snapshot, or ``None`` if empty."""
        with self._lock:
            return dict(self._history[-1]) if self._history else None

    def trend(self, metric: str) -> list[float]:
        """Return time-series values for *metric* across all snapshots."""
        with self._lock:
            return [
                snap[metric]
                for snap in self._history
                if metric in snap
            ]

    def to_json(self) -> str:
        """Serialize history and timestamps to JSON."""
        with self._lock:
            return json.dumps(
                {
                    "history": list(self._history),
                    "timestamps": list(self._timestamps),
                },
                default=str,
            )


def render_dashboard_html(data: DashboardData) -> str:
    """Render a self-contained HTML dashboard page.

    Uses inline JavaScript with lightweight canvas-based charts.
    No external dependencies required.
    """
    latest = data.latest()
    history = data.history
    timestamps = data.timestamps

    # Build metric summary table rows.
    metric_rows = ""
    if latest:
        for key, value in sorted(latest.items()):
            esc_key = html.escape(str(key))
            esc_val = html.escape(f"{value}")
            metric_rows += (
                f"<tr><td>{esc_key}</td><td>{esc_val}</td></tr>\n"
            )

    # Build trend data for charting.
    trend_keys = []
    if history:
        # Pick numeric metrics for trend charts.
        for key in sorted(history[0].keys()):
            if isinstance(history[0].get(key), (int, float)):
                trend_keys.append(key)

    trend_json = {}
    for key in trend_keys[:8]:
        trend_json[key] = data.trend(key)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PyCypher Performance Dashboard</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 0; padding: 20px;
         background: #f5f5f5; color: #333; }}
  h1 {{ color: #1a1a2e; margin-bottom: 5px; }}
  .subtitle {{ color: #666; margin-bottom: 20px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
           gap: 16px; margin-bottom: 20px; }}
  .card {{ background: white; border-radius: 8px; padding: 16px;
           box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .card h3 {{ margin: 0 0 12px 0; font-size: 14px; color: #666; }}
  .card .value {{ font-size: 28px; font-weight: 700; color: #1a1a2e; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ text-align: left; padding: 8px 12px; border-bottom: 1px solid #eee; }}
  th {{ background: #f8f9fa; font-weight: 600; }}
  canvas {{ width: 100%; height: 120px; }}
  .chart-container {{ margin-top: 8px; }}
  .refresh {{ color: #888; font-size: 12px; }}
</style>
</head>
<body>
<h1>PyCypher Performance Dashboard</h1>
<p class="subtitle">Real-time query performance monitoring</p>
<p class="refresh">Snapshots: {len(history)} | Last update: {
    time.strftime('%H:%M:%S', time.localtime(timestamps[-1]))
    if timestamps else 'N/A'
}</p>

<div class="grid">
  <div class="card">
    <h3>Latency p50</h3>
    <div class="value">{latest.get('timing_p50_ms', 'N/A') if latest else 'N/A'} ms</div>
  </div>
  <div class="card">
    <h3>Latency p90</h3>
    <div class="value">{latest.get('timing_p90_ms', 'N/A') if latest else 'N/A'} ms</div>
  </div>
  <div class="card">
    <h3>Total Queries</h3>
    <div class="value">{latest.get('total_queries', 'N/A') if latest else 'N/A'}</div>
  </div>
  <div class="card">
    <h3>Error Rate</h3>
    <div class="value">{
        f"{latest.get('error_rate', 0) * 100:.1f}%"
        if latest and 'error_rate' in latest else 'N/A'
    }</div>
  </div>
</div>

<div class="card" style="margin-bottom: 20px;">
  <h3>Current Metrics</h3>
  <table>
    <thead><tr><th>Metric</th><th>Value</th></tr></thead>
    <tbody>{metric_rows if metric_rows else '<tr><td colspan="2">No data</td></tr>'}
    </tbody>
  </table>
</div>

<div class="grid">
{"".join(f'''
  <div class="card">
    <h3>{html.escape(key)}</h3>
    <div class="chart-container">
      <canvas id="chart-{html.escape(key)}" width="300" height="120"></canvas>
    </div>
  </div>
''' for key in trend_json)}
</div>

<script>
const trendData = {json.dumps(trend_json)};

function drawChart(canvasId, data) {{
  const canvas = document.getElementById(canvasId);
  if (!canvas || !data.length) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  const pad = 5;
  const mn = Math.min(...data), mx = Math.max(...data);
  const range = mx - mn || 1;

  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = '#4361ee';
  ctx.lineWidth = 2;
  ctx.beginPath();
  data.forEach((v, i) => {{
    const x = pad + (i / Math.max(data.length - 1, 1)) * (w - 2 * pad);
    const y = h - pad - ((v - mn) / range) * (h - 2 * pad);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }});
  ctx.stroke();
}}

Object.entries(trendData).forEach(([key, values]) => {{
  drawChart('chart-' + key, values);
}});
</script>
</body>
</html>"""


class _DashboardHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for the dashboard."""

    dashboard_data: DashboardData

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/metrics":
            body = self.server._dashboard_data.to_json().encode()  # type: ignore[attr-defined]
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = render_dashboard_html(
                self.server._dashboard_data  # type: ignore[attr-defined]
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default request logging."""


class DashboardServer:
    """Lightweight HTTP server for the performance dashboard.

    Args:
        data: The :class:`DashboardData` instance to serve.
        port: TCP port to bind.  Use 0 for auto-assignment.

    """

    def __init__(self, data: DashboardData, *, port: int = 8080) -> None:
        self._data = data
        self._httpd = http.server.HTTPServer(
            ("127.0.0.1", port), _DashboardHandler
        )
        self._httpd._dashboard_data = data  # type: ignore[attr-defined]
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        """Return the actual bound port."""
        return self._httpd.server_address[1]

    def serve_once(self) -> None:
        """Handle a single request (useful for testing)."""
        self._httpd.handle_request()

    def start(self) -> None:
        """Start serving in a background daemon thread."""
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True
        )
        self._thread.start()

    def shutdown(self) -> None:
        """Shut down the server."""
        self._httpd.shutdown()
        if self._thread:
            self._thread.join(timeout=5)
