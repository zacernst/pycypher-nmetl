"""System monitoring and configuration commands for nmetl CLI."""

from __future__ import annotations

import os
import sys
from typing import Any

import click


# Registry of config knobs: (env_var, description, default_display)
CONFIG_REGISTRY: list[tuple[str, str, str]] = [
    # --- Query execution ---
    ("PYCYPHER_QUERY_TIMEOUT_S", "Query timeout (seconds)", "None (no limit)"),
    ("PYCYPHER_MAX_CROSS_JOIN_ROWS", "Cross-join row ceiling", "1,000,000"),
    ("PYCYPHER_MAX_UNBOUNDED_PATH_HOPS", "Max BFS hops for [*] paths", "20"),
    (
        "PYCYPHER_MAX_COMPLEXITY_SCORE",
        "Complexity gate (0=disabled)",
        "0 (disabled)",
    ),
    (
        "PYCYPHER_COMPLEXITY_WARN_THRESHOLD",
        "Complexity warning threshold",
        "0 (disabled)",
    ),
    (
        "PYCYPHER_RATE_LIMIT_QPS",
        "Max queries/sec (0=disabled)",
        "0 (disabled)",
    ),
    ("PYCYPHER_RATE_LIMIT_BURST", "Rate limit burst size", "10"),
    # --- Caching ---
    ("PYCYPHER_RESULT_CACHE_MAX_MB", "Result cache size (MB)", "100"),
    ("PYCYPHER_RESULT_CACHE_TTL_S", "Cache TTL (seconds, 0=no expiry)", "0"),
    ("PYCYPHER_AST_CACHE_MAX", "Parsed AST cache size (LRU)", "1024"),
    # --- Security limits ---
    ("PYCYPHER_MAX_QUERY_SIZE_BYTES", "Max query size (bytes)", "1,048,576"),
    ("PYCYPHER_MAX_QUERY_NESTING_DEPTH", "Max AST nesting depth", "200"),
    (
        "PYCYPHER_MAX_COLLECTION_SIZE",
        "Max collection/string size",
        "1,000,000",
    ),
    # --- Logging and observability ---
    ("PYCYPHER_LOG_LEVEL", "Log level (DEBUG/INFO/WARNING/ERROR)", "WARNING"),
    ("PYCYPHER_LOG_FORMAT", "Log format (rich or json)", "rich"),
    ("PYCYPHER_AUDIT_LOG", "Audit logging (1/true/yes to enable)", "disabled"),
    (
        "PYCYPHER_METRICS_ENABLED",
        "In-process metrics (0/false to disable)",
        "1 (enabled)",
    ),
    ("PYCYPHER_SLOW_QUERY_MS", "Slow query threshold (ms)", "1000"),
    (
        "PYCYPHER_OTEL_ENABLED",
        "OpenTelemetry tracing (1/true/yes)",
        "0 (disabled)",
    ),
    # --- REPL ---
    ("PYCYPHER_REPL_MAX_ROWS", "REPL max displayed rows", "50"),
]


# ---------------------------------------------------------------------------
# Implementation functions (called by both cli/ and nmetl_cli.py wrappers)
# ---------------------------------------------------------------------------


def metrics_impl(*, as_json: bool, diagnostic: bool) -> None:
    """Show current query execution metrics."""
    import json

    from shared.metrics import QUERY_METRICS

    snap = QUERY_METRICS.snapshot()

    if as_json:
        click.echo(json.dumps(snap.to_dict(), indent=2, default=str))
    elif diagnostic:
        click.echo(snap.diagnostic_report())
    else:
        click.echo(f"Health: {snap.health_status()}")
        click.echo(snap.summary())


def config_impl(*, as_json: bool) -> None:
    """Show all configuration settings and their current values."""
    import json

    entries = []
    for env_var, description, default_display in CONFIG_REGISTRY:
        raw = os.environ.get(env_var)
        entries.append(
            {
                "variable": env_var,
                "value": raw if raw is not None else default_display,
                "source": "env" if raw is not None else "default",
                "description": description,
            },
        )

    if as_json:
        click.echo(json.dumps(entries, indent=2))
    else:
        click.echo("\nPyCypher Configuration\n")
        for entry in entries:
            marker = "*" if entry["source"] == "env" else " "
            click.echo(
                f"  {marker} {entry['variable']:<38} "
                f"{entry['value']:<18} {entry['description']}",
            )
        click.echo(
            "\n  * = set via environment variable\n"
            "  Set variables with: export PYCYPHER_<NAME>=<value>\n",
        )


def health_impl(*, as_json: bool, verbose_health: bool) -> None:
    """Run health checks and report operational status."""
    import json as json_mod

    from shared.metrics import QUERY_METRICS

    checks: dict[str, dict[str, Any]] = {}

    # 1. Metrics health
    snap = QUERY_METRICS.snapshot()
    metrics_status = snap.health_status()
    checks["metrics"] = {
        "status": metrics_status,
        "total_queries": snap.total_queries,
        "total_errors": snap.total_errors,
        "error_rate": round(snap.error_rate, 4),
    }

    # 2. System resources
    try:
        import resource as _resource

        rusage = _resource.getrusage(_resource.RUSAGE_SELF)
        mem_mb = rusage.ru_maxrss / (1024 * 1024)  # macOS returns bytes
        if sys.platform == "linux":
            mem_mb = rusage.ru_maxrss / 1024  # Linux returns KB

        # Heuristic: flag if RSS > 2GB
        mem_status = "healthy" if mem_mb < 2048 else "degraded"
        checks["memory"] = {
            "status": mem_status,
            "rss_mb": round(mem_mb, 1),
        }
    except Exception:  # noqa: BLE001 — graceful degradation for health check
        from shared.logger import LOGGER as _logger

        _logger.debug("Memory health check failed", exc_info=True)
        checks["memory"] = {"status": "unknown", "rss_mb": None}

    # 3. Process uptime
    checks["uptime"] = {
        "status": "healthy",
        "uptime_s": round(snap.uptime_s, 1),
    }

    # 4. Cache efficiency (if queries have been run)
    if snap.total_queries > 0:
        total_cache_ops = snap.result_cache_hits + snap.result_cache_misses
        cache_hit_rate = (
            snap.result_cache_hits / total_cache_ops
            if total_cache_ops > 0
            else 0.0
        )
        checks["cache"] = {
            "status": "healthy",
            "hit_rate": round(cache_hit_rate, 4),
            "evictions": snap.result_cache_evictions,
        }

    # Overall status: worst of all checks
    status_order = {"healthy": 0, "degraded": 1, "unhealthy": 2, "unknown": 1}
    overall = max(
        (c.get("status", "unknown") for c in checks.values()),
        key=lambda s: status_order.get(s, 1),
    )

    report = {"status": overall, "checks": checks}

    if as_json:
        click.echo(json_mod.dumps(report, indent=2, default=str))
    else:
        status_icon = {"healthy": "+", "degraded": "~", "unhealthy": "!"}
        click.echo(
            f"[{status_icon.get(overall, '?')}] Overall: {overall.upper()}",
        )
        for name, check in checks.items():
            icon = status_icon.get(check.get("status", "?"), "?")
            detail_parts = [
                f"{k}={v}"
                for k, v in check.items()
                if k != "status" and (verbose_health or k not in {"rss_mb"})
            ]
            detail = f"  ({', '.join(detail_parts)})" if detail_parts else ""
            click.echo(
                f"  [{icon}] {name}: {check.get('status', 'unknown')}{detail}",
            )

    # Exit code reflects health status
    sys.exit(status_order.get(overall, 1))


def health_server_impl(*, port: int, bind: str) -> None:
    """Start a lightweight HTTP health check endpoint."""
    from pycypher.health_server import run_health_server

    click.echo(f"Starting health server on {bind}:{port}...")
    click.echo(f"  GET http://{bind}:{port}/health")
    click.echo(f"  GET http://{bind}:{port}/ready")
    click.echo(f"  GET http://{bind}:{port}/metrics")
    run_health_server(host=bind, port=port)


# ---------------------------------------------------------------------------
# Click command wrappers
# ---------------------------------------------------------------------------


@click.command("metrics")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output metrics as JSON for programmatic consumption.",
)
@click.option(
    "--diagnostic",
    is_flag=True,
    default=False,
    help="Show detailed diagnostic report with recommendations.",
)
def metrics(*, as_json: bool, diagnostic: bool) -> None:
    """Show current query execution metrics."""
    metrics_impl(as_json=as_json, diagnostic=diagnostic)


@click.command("config")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output configuration as JSON.",
)
def config(*, as_json: bool) -> None:
    """Show all configuration settings and their current values."""
    config_impl(as_json=as_json)


@click.command("show-config")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"], case_sensitive=False),
    default="table",
    help="Output format (default: table).",
)
def show_config(output_format: str) -> None:
    """Show effective runtime configuration.

    Displays all pycypher configuration values as resolved from environment
    variables, including which values differ from their defaults.
    """
    import json as json_mod

    from pycypher import config as cfg

    # (env_var, current_value, default_value)
    settings: list[tuple[str, str, object, object]] = [
        (
            "PYCYPHER_QUERY_TIMEOUT_S",
            "Query timeout (s)",
            cfg.QUERY_TIMEOUT_S,
            None,
        ),
        (
            "PYCYPHER_MAX_CROSS_JOIN_ROWS",
            "Max cross-join rows",
            cfg.MAX_CROSS_JOIN_ROWS,
            1_000_000,
        ),
        (
            "PYCYPHER_RESULT_CACHE_MAX_MB",
            "Result cache max (MB)",
            cfg.RESULT_CACHE_MAX_MB,
            100,
        ),
        (
            "PYCYPHER_RESULT_CACHE_TTL_S",
            "Result cache TTL (s)",
            cfg.RESULT_CACHE_TTL_S,
            0.0,
        ),
        (
            "PYCYPHER_MAX_UNBOUNDED_PATH_HOPS",
            "Max unbounded path hops",
            cfg.MAX_UNBOUNDED_PATH_HOPS,
            20,
        ),
        (
            "PYCYPHER_AST_CACHE_MAX",
            "AST cache max entries",
            cfg.AST_CACHE_MAX_ENTRIES,
            1024,
        ),
        (
            "PYCYPHER_MAX_QUERY_SIZE_BYTES",
            "Max query size (bytes)",
            cfg.MAX_QUERY_SIZE_BYTES,
            1_048_576,
        ),
        (
            "PYCYPHER_MAX_QUERY_NESTING_DEPTH",
            "Max query nesting depth",
            cfg.MAX_QUERY_NESTING_DEPTH,
            200,
        ),
        (
            "PYCYPHER_MAX_COLLECTION_SIZE",
            "Max collection size",
            cfg.MAX_COLLECTION_SIZE,
            1_000_000,
        ),
        (
            "PYCYPHER_MAX_COMPLEXITY_SCORE",
            "Max complexity score",
            cfg.MAX_COMPLEXITY_SCORE,
            None,
        ),
        (
            "PYCYPHER_COMPLEXITY_WARN_THRESHOLD",
            "Complexity warn threshold",
            cfg.COMPLEXITY_WARN_THRESHOLD,
            None,
        ),
    ]

    if output_format == "json":
        data = {}
        for env_var, _label, value, default in settings:
            raw_env = os.environ.get(env_var)
            data[env_var] = {
                "value": value,
                "default": default,
                "source": "env" if raw_env is not None else "default",
            }
        click.echo(json_mod.dumps(data, indent=2, default=str))
    else:
        # Table format
        click.echo("Effective PyCypher Configuration")
        click.echo("=" * 64)
        for env_var, label, value, default in settings:
            raw_env = os.environ.get(env_var)
            source = "  (env)" if raw_env is not None else ""
            display_val = "None (no limit)" if value is None else str(value)
            click.echo(f"  {label:<30} {display_val:<16}{source}")
        click.echo()
        click.echo("Set environment variables to override defaults.")
        click.echo("Example: export PYCYPHER_QUERY_TIMEOUT_S=30")


@click.command("health")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output health report as JSON.",
)
@click.option(
    "--verbose",
    "-v",
    "verbose_health",
    is_flag=True,
    default=False,
    help="Include system resource details.",
)
def health(*, as_json: bool, verbose_health: bool) -> None:
    """Run health checks and report operational status."""
    health_impl(as_json=as_json, verbose_health=verbose_health)


@click.command("health-server")
@click.option(
    "--port",
    default=8079,
    type=int,
    help="Port to listen on (default: 8079).",
)
@click.option(
    "--bind",
    default="127.0.0.1",
    help="Address to bind to (default: 127.0.0.1).",
)
def health_server(*, port: int, bind: str) -> None:
    """Start a lightweight HTTP health check endpoint."""
    health_server_impl(port=port, bind=bind)
