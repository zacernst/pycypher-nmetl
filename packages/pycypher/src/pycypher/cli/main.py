"""Modular CLI entry point for nmetl."""

from __future__ import annotations

import difflib

import click

from .interactive import repl
from .pipeline import list_queries, run, validate
from .query import format_query, parse, query
from .schema import functions, schema
from .security import security_check
from .system import config, health, health_server, metrics, show_config
from .utility import compat_check


class SuggestingGroup(click.Group):
    """Click group that suggests close matches for mistyped commands."""

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str | None, click.Command | None, list[str]]:
        try:
            return super().resolve_command(ctx, args)
        except click.UsageError as exc:
            cmd_name = args[0] if args else ""
            matches = difflib.get_close_matches(
                cmd_name, self.list_commands(ctx), n=3, cutoff=0.6
            )
            if matches:
                suggestion = ", ".join(f"'{m}'" for m in matches)
                raise click.UsageError(
                    f"No such command '{cmd_name}'. Did you mean: {suggestion}?"
                ) from exc
            raise


@click.group(cls=SuggestingGroup)
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
cli.add_command(show_config)

# Register interactive commands
cli.add_command(repl)

# Register security commands
cli.add_command(security_check)

# Register utility commands
cli.add_command(compat_check)


if __name__ == "__main__":
    cli()
