"""Schema and metadata commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path

import click


@click.command()
@click.option(
    "--category",
    type=click.Choice([
        "string", "math", "list", "aggregation", "temporal",
        "type", "graph", "all"
    ], case_sensitive=False),
    default="all",
    help="Filter functions by category (default: show all).",
)
@click.option(
    "--search",
    type=str,
    default=None,
    help="Search for functions containing this substring (case-insensitive).",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"], case_sensitive=False),
    default="table",
    help="Output format for function list.",
)
def functions(category: str, search: str | None, output_format: str) -> None:
    """List available Cypher functions by category."""
    # Import the original implementation
    from pycypher.nmetl_cli import functions as _original_functions

    # Delegate to original implementation
    _original_functions(category, search, output_format)


@click.command()
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "dot"], case_sensitive=False),
    default="table",
    help="Output format for schema information.",
)
@click.option(
    "--include-properties",
    is_flag=True,
    default=False,
    help="Include property information in schema output.",
)
def schema(config: Path, output_format: str, include_properties: bool) -> None:
    """Display schema information for the pipeline config."""
    # Import the original implementation
    from pycypher.nmetl_cli import schema as _original_schema

    # Delegate to original implementation
    _original_schema(config, output_format, include_properties)