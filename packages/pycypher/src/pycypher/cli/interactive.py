"""Interactive commands for nmetl CLI."""

from __future__ import annotations

import click

# ---------------------------------------------------------------------------
# Implementation
# ---------------------------------------------------------------------------


def repl_impl(
    entity_specs: tuple[str, ...],
    rel_specs: tuple[str, ...],
    default_id_col: str | None,
) -> None:
    """Start an interactive Cypher query REPL."""
    from pycypher.repl import CypherRepl

    shell = CypherRepl(
        entity_specs=list(entity_specs),
        rel_specs=list(rel_specs),
        default_id_col=default_id_col,
    )
    shell.cmdloop()


# ---------------------------------------------------------------------------
# Click command wrapper
# ---------------------------------------------------------------------------


@click.command("repl")
@click.option(
    "--entity",
    "entity_specs",
    multiple=True,
    metavar="SPEC",
    help=(
        "Entity source in the form 'Label=path/to/file.csv' or "
        "'Label=path/to/file.csv:id_col'.  May be repeated."
    ),
)
@click.option(
    "--rel",
    "rel_specs",
    multiple=True,
    metavar="SPEC",
    help=(
        "Relationship source in the form 'REL=path.csv:src_col:tgt_col'.  "
        "May be repeated."
    ),
)
@click.option(
    "--id-col",
    "default_id_col",
    default=None,
    metavar="COL",
    help="Default ID column name for entity sources.",
)
def repl(
    entity_specs: tuple[str, ...],
    rel_specs: tuple[str, ...],
    default_id_col: str | None,
) -> None:
    r"""Start an interactive Cypher query REPL.

    Provides a readline-enabled interactive shell for exploring graph
    data with Cypher queries.  Supports query history, schema inspection,
    EXPLAIN/PROFILE prefixes, and dot-commands.

    \b
    Examples:
      nmetl repl --entity Person=people.csv
      nmetl repl --entity Person=people.csv --rel KNOWS=knows.csv:from_id:to_id
      nmetl repl --entity Person=people.csv:id --id-col id

    \b
    Inside the REPL:
      MATCH (p:Person) RETURN p.name;
      EXPLAIN MATCH (p:Person) RETURN p.name
      .schema     — show loaded entity types
      .help       — show all commands
    """
    repl_impl(entity_specs, rel_specs, default_id_col)
