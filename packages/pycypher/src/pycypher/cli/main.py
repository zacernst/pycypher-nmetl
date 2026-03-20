"""Modular CLI entry point for nmetl."""

from __future__ import annotations

import click

from .interactive import repl
from .pipeline import list_queries, run, validate
from .query import format_query, parse, query
from .schema import functions, schema
from .system import config, health, health_server, metrics
from .utility import compat_check


@click.group()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose output.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable debug mode with detailed error traces.",
)
def cli(*, verbose: bool, debug: bool) -> None:
    """nmetl: ETL pipeline runner powered by PyCypher.

    Run Cypher-based ETL pipelines against CSV, Parquet, and other data sources.
    Build graph data models and execute complex transformations with declarative
    Cypher queries.
    """
    if debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)
    elif verbose:
        import logging
        logging.basicConfig(level=logging.INFO)


# Register pipeline commands
cli.add_command(run)
cli.add_command(validate)
cli.add_command(list_queries)

# Register query processing commands
cli.add_command(parse)
cli.add_command(query)
cli.add_command(format_query)

# Register schema and metadata commands
cli.add_command(functions)
cli.add_command(schema)

# Register system monitoring commands
cli.add_command(metrics)
cli.add_command(config)
cli.add_command(health)
cli.add_command(health_server)

# Register interactive commands
cli.add_command(repl)

# Register utility commands
cli.add_command(compat_check)


if __name__ == "__main__":
    cli()