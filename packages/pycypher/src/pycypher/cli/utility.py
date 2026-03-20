"""Utility commands for nmetl CLI."""

from __future__ import annotations

import click


@click.command("compat-check")
@click.option(
    "--component",
    type=click.Choice([
        "python", "pandas", "duckdb", "neo4j", "all"
    ], case_sensitive=False),
    default="all",
    help="Check compatibility for specific components (default: all).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed version and compatibility information.",
)
def compat_check(component: str, verbose: bool) -> None:
    """Check compatibility with system dependencies."""
    # Import the original implementation
    from pycypher.nmetl_cli import compat_check as _original_compat_check

    # Delegate to original implementation
    _original_compat_check(component, verbose)