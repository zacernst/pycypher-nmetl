"""Query processing and parsing commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path

import click


@click.command()
@click.argument("cypher_query", type=str)
@click.option(
    "--as-json",
    is_flag=True,
    default=False,
    help="Output the AST as formatted JSON instead of pretty-printed structure.",
)
@click.option(
    "--run-validation",
    is_flag=True,
    default=False,
    help="Run semantic validation on the parsed AST.",
)
def parse(cypher_query: str, *, as_json: bool, run_validation: bool) -> None:
    """Parse a Cypher query and display the resulting AST."""
    # Import the original implementation
    from pycypher.nmetl_cli import parse as _original_parse

    # Delegate to original implementation
    _original_parse(
        cypher_query, as_json=as_json, run_validation=run_validation
    )


@click.command()
@click.argument("cypher_query", type=str)
@click.option(
    "--entity",
    "-e",
    multiple=True,
    metavar="TYPE:PATH[:ID_COL]",
    help=(
        "Load an entity table from a CSV or Parquet file. "
        "TYPE is the entity type name, PATH is the file path, "
        "and ID_COL is the optional ID column name (defaults to 'id')."
    ),
)
@click.option(
    "--relationship",
    "-r",
    multiple=True,
    metavar="TYPE:PATH:SRC_COL:TGT_COL[:ID_COL]",
    help=(
        "Load a relationship table from a CSV or Parquet file. "
        "TYPE is the relationship type, PATH is the file path, "
        "SRC_COL and TGT_COL are source/target ID columns, "
        "and ID_COL is the optional relationship ID column."
    ),
)
@click.option(
    "--parameter",
    "-p",
    multiple=True,
    metavar="NAME=VALUE",
    help="Set a query parameter (e.g., -p 'limit=10').",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Write results to a CSV file instead of stdout.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit the number of result rows displayed.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "csv", "json"], case_sensitive=False),
    default="table",
    help="Output format for results.",
)
def query(
    cypher_query: str,
    entity: tuple[str, ...],
    relationship: tuple[str, ...],
    parameter: tuple[str, ...],
    output: Path | None,
    limit: int | None,
    output_format: str,
) -> None:
    """Execute a Cypher query against ad-hoc data sources."""
    # Import the original implementation
    from pycypher.nmetl_cli import query as _original_query

    # Delegate to original implementation
    _original_query(
        cypher_query,
        entity,
        relationship,
        parameter,
        output,
        limit,
        output_format,
    )


@click.command("format-query")
@click.argument("cypher_query", type=str)
@click.option(
    "--indent",
    type=int,
    default=2,
    help="Number of spaces per indentation level (default: 2).",
)
@click.option(
    "--compact",
    is_flag=True,
    default=False,
    help="Use compact formatting with minimal whitespace.",
)
def format_query(cypher_query: str, indent: int, compact: bool) -> None:
    """Format and pretty-print a Cypher query."""
    # Import the original implementation
    from pycypher.nmetl_cli import format_query as _original_format_query

    # Delegate to original implementation
    _original_format_query(cypher_query, indent, compact)
