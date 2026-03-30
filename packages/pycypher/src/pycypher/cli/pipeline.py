"""Pipeline management commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path

import click


@click.command()
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help=(
        "Parse and validate the config and list queries in dependency order, "
        "but do not load any data or execute any queries."
    ),
)
@click.option(
    "--query-id",
    "query_ids",
    multiple=True,
    metavar="ID",
    help=(
        "Run only the queries with these IDs.  May be repeated to select "
        "multiple queries.  Defaults to all queries in the config."
    ),
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help=(
        "Override the output directory for all sinks.  Relative paths in "
        "the config file are resolved from this directory instead of the "
        "directory containing the config file."
    ),
)
@click.option(
    "--on-error",
    type=click.Choice(["fail", "warn", "skip"], case_sensitive=False),
    default=None,
    help=(
        "Override the error-handling policy defined in the config.  "
        "'fail' aborts immediately, 'warn' logs and continues, "
        "'skip' silently continues."
    ),
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose logging output.",
)
def run(
    config: Path,
    dry_run: bool,
    query_ids: tuple[str, ...],
    output_dir: Path | None,
    on_error: str | None,
    verbose: bool,
) -> None:
    """Run the ETL pipeline defined in CONFIG.

    This is a modularized version of the main pipeline runner.
    Implementation details are preserved from the original nmetl_cli.py.
    """
    # Import the original implementation
    from pycypher.nmetl_cli import run as _original_run

    # Delegate to original implementation
    _original_run(config, dry_run, query_ids, output_dir, on_error, verbose)


@click.command()
@click.argument("config", type=click.Path(path_type=Path))
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show a detailed breakdown of every source, query, and output.",
)
def validate(config: Path, verbose: bool) -> None:
    """Validate the pipeline config file at CONFIG."""
    # Import the original implementation
    from pycypher.nmetl_cli import validate as _original_validate

    # Delegate to original implementation
    _original_validate(config, verbose)


@click.command("list-queries")
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--deps",
    is_flag=True,
    default=False,
    help="Show dependency analysis and execution order.",
)
def list_queries(config: Path, *, deps: bool) -> None:
    """List all queries defined in the CONFIG file."""
    # Import the original implementation
    from pycypher.nmetl_cli import list_queries as _original_list_queries

    # Delegate to original implementation
    _original_list_queries(config, deps=deps)
