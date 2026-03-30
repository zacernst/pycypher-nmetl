"""Pluggable metrics export adapters for external monitoring systems.

Bridges the in-process :class:`~shared.metrics.QueryMetrics` collector to
external observability backends — Prometheus, StatsD/Datadog, and JSON file
export.  All exporters work with zero external dependencies by default
(Prometheus text format and JSON need only stdlib).

Configuration is entirely via environment variables:

- ``PYCYPHER_METRICS_EXPORT`` — comma-separated list of active exporters
  (e.g. ``prometheus,statsd,json``).  Default: none.
- ``PYCYPHER_METRICS_PREFIX`` — metric name prefix (default: ``pycypher``).
- ``PYCYPHER_STATSD_HOST`` / ``PYCYPHER_STATSD_PORT`` — StatsD endpoint.
- ``PYCYPHER_METRICS_JSON_PATH`` — path for JSON file export.
- ``PYCYPHER_METRICS_EXPORT_INTERVAL_S`` — push interval (default: ``60``).

Usage::

    from shared.exporters import get_exporters, export_once

    # Auto-configured from env vars
    exporters = get_exporters()

    # Push current metrics to all configured backends
    from shared.metrics import QUERY_METRICS
    export_once(QUERY_METRICS.snapshot())

    # Or get Prometheus text format directly
    from shared.exporters import PrometheusExporter
    exporter = PrometheusExporter()
    print(exporter.render(QUERY_METRICS.snapshot()))
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any, Protocol

_logger = logging.getLogger(__name__)

_PREFIX: str = os.environ.get("PYCYPHER_METRICS_PREFIX", "pycypher")


# ---------------------------------------------------------------------------
# Exporter protocol
# ---------------------------------------------------------------------------


class MetricsExporter(Protocol):
    """Protocol for metrics export adapters."""

    @property
    def name(self) -> str:
        """Human-readable exporter name."""
        ...

    def export(self, snapshot: Any) -> None:
        """Push a :class:`~shared.metrics.MetricsSnapshot` to the backend.

        Args:
            snapshot: A metrics snapshot from ``QueryMetrics.snapshot()``.

        """
        ...


# ---------------------------------------------------------------------------
# Prometheus text format exporter (stdlib only)
# ---------------------------------------------------------------------------


class PrometheusExporter:
    """Export metrics in Prometheus text exposition format.

    Generates ``text/plain; version=0.0.4`` output suitable for scraping
    by Prometheus, VictoriaMetrics, or any OpenMetrics-compatible collector.

    Does not require any external dependencies — generates the text format
    directly.  For a full Prometheus client with push-gateway support, see
    ``prometheus_client``.

    Args:
        prefix: Metric name prefix.  Default: ``pycypher``.

    """

    def __init__(self, prefix: str = _PREFIX) -> None:
        self._prefix = prefix

    @property
    def name(self) -> str:
        """Exporter name."""
        return "prometheus"

    def render(self, snapshot: Any) -> str:
        """Render a MetricsSnapshot as Prometheus text exposition format.

        Args:
            snapshot: A :class:`~shared.metrics.MetricsSnapshot`.

        Returns:
            Multi-line string in Prometheus text format.

        """
        p = self._prefix
        lines: list[str] = []

        def _gauge(name: str, help_text: str, value: float | int) -> None:
            lines.append(f"# HELP {p}_{name} {help_text}")
            lines.append(f"# TYPE {p}_{name} gauge")
            lines.append(f"{p}_{name} {value}")

        def _counter(name: str, help_text: str, value: float | int) -> None:
            lines.append(f"# HELP {p}_{name} {help_text}")
            lines.append(f"# TYPE {p}_{name} counter")
            lines.append(f"{p}_{name} {value}")

        # Core counters
        _counter(
            "queries_total",
            "Total successfully executed queries.",
            snapshot.total_queries,
        )
        _counter(
            "errors_total",
            "Total failed queries.",
            snapshot.total_errors,
        )
        _counter(
            "slow_queries_total",
            "Queries exceeding the slow-query threshold.",
            snapshot.slow_queries,
        )
        _counter(
            "rows_returned_total",
            "Total rows returned across all queries.",
            snapshot.total_rows_returned,
        )

        # Latency gauges
        _gauge(
            "query_duration_p50_ms",
            "Median query execution time in milliseconds.",
            round(snapshot.timing_p50_ms, 2),
        )
        _gauge(
            "query_duration_p90_ms",
            "90th percentile query execution time in milliseconds.",
            round(snapshot.timing_p90_ms, 2),
        )
        _gauge(
            "query_duration_p99_ms",
            "99th percentile query execution time in milliseconds.",
            round(snapshot.timing_p99_ms, 2),
        )
        _gauge(
            "query_duration_max_ms",
            "Maximum query execution time in milliseconds.",
            round(snapshot.timing_max_ms, 2),
        )

        # Throughput
        _gauge(
            "queries_per_second",
            "Overall query throughput.",
            round(snapshot.queries_per_second, 3),
        )
        _gauge(
            "recent_queries_per_second",
            "Windowed recent query throughput.",
            round(snapshot.recent_queries_per_second, 3),
        )

        # Error rates
        _gauge(
            "error_rate",
            "Overall error rate (0.0-1.0).",
            round(snapshot.error_rate, 4),
        )
        _gauge(
            "recent_error_rate",
            "Windowed recent error rate (0.0-1.0).",
            round(snapshot.recent_error_rate, 4),
        )

        # Memory
        _gauge(
            "memory_delta_p50_mb",
            "Median query memory delta in MB.",
            round(snapshot.memory_delta_p50_mb, 2),
        )
        _gauge(
            "memory_delta_max_mb",
            "Max query memory delta in MB.",
            round(snapshot.memory_delta_max_mb, 2),
        )

        # Parse and plan times
        _gauge(
            "parse_time_p50_ms",
            "Median parse time in milliseconds.",
            round(snapshot.parse_time_p50_ms, 2),
        )
        _gauge(
            "plan_time_p50_ms",
            "Median plan time in milliseconds.",
            round(snapshot.plan_time_p50_ms, 2),
        )

        # Cache stats
        cache_total = snapshot.result_cache_hits + snapshot.result_cache_misses
        if cache_total > 0:
            _counter(
                "cache_hits_total",
                "Result cache hits.",
                snapshot.result_cache_hits,
            )
            _counter(
                "cache_misses_total",
                "Result cache misses.",
                snapshot.result_cache_misses,
            )
            _gauge(
                "cache_hit_rate",
                "Result cache hit rate (0.0-1.0).",
                round(snapshot.result_cache_hit_rate, 4),
            )
            _counter(
                "cache_evictions_total",
                "Result cache evictions.",
                snapshot.result_cache_evictions,
            )

        # Per-error-type breakdown
        for err_type, count in snapshot.error_counts.items():
            safe_type = err_type.replace('"', "")
            lines.append(
                f'{p}_errors_by_type{{type="{safe_type}"}} {count}',
            )

        # Per-clause timing
        for clause, p50 in snapshot.clause_timing_p50_ms.items():
            safe_clause = clause.replace('"', "")
            lines.append(
                f'{p}_clause_duration_p50_ms{{clause="{safe_clause}"}} {round(p50, 2)}',
            )

        # Uptime
        _gauge(
            "uptime_seconds",
            "Seconds since metrics collector was created.",
            round(snapshot.uptime_s, 1),
        )

        # Health status as numeric (healthy=0, degraded=1, unhealthy=2)
        status_map = {"healthy": 0, "degraded": 1, "unhealthy": 2}
        _gauge(
            "health_status",
            "Health status (0=healthy, 1=degraded, 2=unhealthy).",
            status_map.get(snapshot.health_status(), 0),
        )

        lines.append("")  # trailing newline
        return "\n".join(lines)

    def export(self, snapshot: Any) -> None:
        """Log the Prometheus text output at INFO level.

        In production, mount an HTTP endpoint that calls
        :meth:`render` instead.  This method is for development/debugging.

        Args:
            snapshot: A :class:`~shared.metrics.MetricsSnapshot`.

        """
        text = self.render(snapshot)
        _logger.info(
            "Prometheus metrics export:\n%s",
            text,
        )


# ---------------------------------------------------------------------------
# StatsD exporter (stdlib UDP socket only)
# ---------------------------------------------------------------------------


class StatsDExporter:
    """Push metrics to a StatsD-compatible daemon via UDP.

    Uses a plain UDP socket (stdlib only) — no external dependencies.
    Compatible with StatsD, Datadog Agent, Telegraf, and similar collectors.

    Args:
        host: StatsD host.  Default from ``PYCYPHER_STATSD_HOST`` or
            ``127.0.0.1``.
        port: StatsD port.  Default from ``PYCYPHER_STATSD_PORT`` or
            ``8125``.
        prefix: Metric name prefix.

    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        prefix: str = _PREFIX,
    ) -> None:
        self._host = host or os.environ.get(
            "PYCYPHER_STATSD_HOST",
            "127.0.0.1",
        )
        self._port = port or int(
            os.environ.get("PYCYPHER_STATSD_PORT", "8125"),
        )
        self._prefix = prefix
        self._sock: socket.socket | None = None

    @property
    def name(self) -> str:
        """Exporter name."""
        return "statsd"

    def _send(self, metric: str, value: float | int, mtype: str) -> None:
        """Send a single metric line over UDP."""
        line = f"{self._prefix}.{metric}:{value}|{mtype}"
        try:
            if self._sock is None:
                self._sock = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_DGRAM,
                )
            self._sock.sendto(
                line.encode("utf-8"),
                (self._host, self._port),
            )
        except OSError:
            _logger.debug(
                "StatsD send failed  host=%s  port=%d",
                self._host,
                self._port,
                exc_info=True,
            )

    def export(self, snapshot: Any) -> None:
        """Push metrics to StatsD.

        Args:
            snapshot: A :class:`~shared.metrics.MetricsSnapshot`.

        """
        self._send("queries.total", snapshot.total_queries, "c")
        self._send("errors.total", snapshot.total_errors, "c")
        self._send("slow_queries.total", snapshot.slow_queries, "c")
        self._send("rows.total", snapshot.total_rows_returned, "c")
        self._send("query.duration.p50_ms", snapshot.timing_p50_ms, "g")
        self._send("query.duration.p90_ms", snapshot.timing_p90_ms, "g")
        self._send("query.duration.p99_ms", snapshot.timing_p99_ms, "g")
        self._send("query.duration.max_ms", snapshot.timing_max_ms, "g")
        self._send("qps", snapshot.queries_per_second, "g")
        self._send("error_rate", snapshot.error_rate, "g")
        self._send("memory.delta_p50_mb", snapshot.memory_delta_p50_mb, "g")
        self._send("memory.delta_max_mb", snapshot.memory_delta_max_mb, "g")
        self._send("parse.p50_ms", snapshot.parse_time_p50_ms, "g")
        self._send("plan.p50_ms", snapshot.plan_time_p50_ms, "g")

        status_map = {"healthy": 0, "degraded": 1, "unhealthy": 2}
        self._send(
            "health_status",
            status_map.get(snapshot.health_status(), 0),
            "g",
        )

        _logger.debug(
            "StatsD export complete  host=%s  port=%d",
            self._host,
            self._port,
        )


# ---------------------------------------------------------------------------
# JSON file exporter (stdlib only)
# ---------------------------------------------------------------------------


class JSONFileExporter:
    """Append metrics snapshots to a JSON-lines file.

    Each export appends a single JSON line with a timestamp, suitable for
    ingestion by ELK, Datadog Logs, Loki, or any log aggregation pipeline.

    Args:
        path: Output file path.  Default from ``PYCYPHER_METRICS_JSON_PATH``
            or ``metrics.jsonl``.

    """

    def __init__(self, path: str | None = None) -> None:
        self._path = Path(
            path
            or os.environ.get("PYCYPHER_METRICS_JSON_PATH", "metrics.jsonl"),
        )

    @property
    def name(self) -> str:
        """Exporter name."""
        return "json"

    def export(self, snapshot: Any) -> None:
        """Append a JSON-lines entry for the snapshot.

        Args:
            snapshot: A :class:`~shared.metrics.MetricsSnapshot`.

        """
        entry = snapshot.to_dict()
        entry["_timestamp"] = time.time()
        entry["_health"] = snapshot.health_status()
        try:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, default=str) + "\n")
            _logger.debug("JSON metrics exported to %s", self._path)
        except OSError:
            _logger.warning(
                "Failed to write metrics to %s",
                self._path,
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# Factory: auto-configure exporters from environment
# ---------------------------------------------------------------------------


_EXPORTER_REGISTRY: dict[str, type] = {
    "prometheus": PrometheusExporter,
    "statsd": StatsDExporter,
    "json": JSONFileExporter,
}


def get_exporters() -> list[MetricsExporter]:
    """Auto-configure exporters from ``PYCYPHER_METRICS_EXPORT`` env var.

    Returns:
        List of configured :class:`MetricsExporter` instances.
        Empty list if no exporters are configured.

    Example::

        # In .env:  PYCYPHER_METRICS_EXPORT=prometheus,json
        exporters = get_exporters()
        # Returns [PrometheusExporter(), JSONFileExporter()]

    """
    raw = os.environ.get("PYCYPHER_METRICS_EXPORT", "")
    if not raw.strip():
        return []

    names = [n.strip().lower() for n in raw.split(",") if n.strip()]
    result: list[MetricsExporter] = []
    for name in names:
        cls = _EXPORTER_REGISTRY.get(name)
        if cls is None:
            _logger.warning(
                "Unknown metrics exporter %r — skipping.  Available: %s",
                name,
                ", ".join(sorted(_EXPORTER_REGISTRY)),
            )
            continue
        result.append(cls())
    return result


def export_once(snapshot: Any) -> None:
    """Push a snapshot to all configured exporters.

    Convenience function that calls :func:`get_exporters` and invokes
    ``export()`` on each.  Errors in individual exporters are logged
    but do not propagate.

    Args:
        snapshot: A :class:`~shared.metrics.MetricsSnapshot`.

    """
    for exporter in get_exporters():
        try:
            exporter.export(snapshot)
        except Exception:
            _logger.warning(
                "Metrics export failed for %s",
                exporter.name,
                exc_info=True,
            )
