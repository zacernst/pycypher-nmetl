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
    type=click.Choice([
        "project", "sources", "queries", "outputs", "all"
    ], case_sensitive=False),
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


@click.command()
@click.option(
    "--check",
    type=click.Choice([
        "dependencies", "memory", "disk", "network", "all"
    ], case_sensitive=False),
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