"""System monitoring and configuration commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path

import click


@click.command()
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"], case_sensitive=False),
    default="table",
    help="Output format for metrics display.",
)
@click.option(
    "--query-id",
    type=str,
    default=None,
    help="Show metrics for a specific query ID only.",
)
def metrics(output_format: str, query_id: str | None) -> None:
    """Display performance and execution metrics."""
    # Import the original implementation
    from pycypher.nmetl_cli import metrics as _original_metrics

    # Delegate to original implementation
    _original_metrics(output_format, query_id)


@click.command()
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--section",
    type=click.Choice(
        [
            "project",
            "sources",
            "queries",
            "outputs",
            "all",
        ],
        case_sensitive=False,
    ),
    default="all",
    help="Show only a specific configuration section.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["yaml", "json", "summary"], case_sensitive=False),
    default="summary",
    help="Output format for configuration display.",
)
def config(config: Path, section: str, output_format: str) -> None:
    """Display and validate pipeline configuration."""
    # Import the original implementation
    from pycypher.nmetl_cli import config as _original_config

    # Delegate to original implementation
    _original_config(config, section, output_format)


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
    import os

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


@click.command()
@click.option(
    "--check",
    type=click.Choice(
        [
            "dependencies",
            "memory",
            "disk",
            "network",
            "all",
        ],
        case_sensitive=False,
    ),
    default="all",
    help="Run specific health checks (default: all).",
)
@click.option(
    "--timeout",
    type=int,
    default=30,
    help="Timeout in seconds for health checks (default: 30).",
)
def health(check: str, timeout: int) -> None:
    """Check system health and dependencies."""
    # Import the original implementation
    from pycypher.nmetl_cli import health as _original_health

    # Delegate to original implementation
    _original_health(check, timeout)


@click.command("health-server")
@click.option(
    "--host",
    type=str,
    default="localhost",
    help="Host to bind the health check server to (default: localhost).",
)
@click.option(
    "--port",
    type=int,
    default=8080,
    help="Port to bind the health check server to (default: 8080).",
)
@click.option(
    "--interval",
    type=int,
    default=60,
    help="Health check interval in seconds (default: 60).",
)
def health_server(host: str, port: int, interval: int) -> None:
    """Start a health check HTTP server."""
    # Import the original implementation
    from pycypher.nmetl_cli import health_server as _original_health_server

    # Delegate to original implementation
    _original_health_server(host, port, interval)
