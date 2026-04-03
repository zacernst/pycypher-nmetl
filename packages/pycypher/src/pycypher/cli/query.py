"""Query processing and parsing commands for nmetl CLI."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import NoReturn

import click


def _cli_error(message: str, *, exit_code: int = 1) -> NoReturn:
    """Print an error message to stderr and exit."""
    from pycypher.cli.common import cli_error

    cli_error(message, exit_code=exit_code)


# ---------------------------------------------------------------------------
# Implementation functions
# ---------------------------------------------------------------------------


def parse_impl(
    cypher_query: str, *, as_json: bool, run_validation: bool
) -> None:
    """Parse a Cypher query and display its AST structure."""
    import json as json_mod

    from lark.exceptions import LarkError, ParseError, UnexpectedInput

    from pycypher.exceptions import CypherSyntaxError
    from pycypher.grammar_parser import GrammarParser

    if not cypher_query or not cypher_query.strip():
        _cli_error("CYPHER_QUERY must not be empty.")

    parser = GrammarParser()

    try:
        ast_result = parser.parse_to_ast(cypher_query)
    except CypherSyntaxError as exc:
        _cli_error(str(exc))
    except (SyntaxError, LarkError, UnexpectedInput, ParseError) as exc:
        _cli_error(f"Cypher syntax error: {exc}")

    if as_json:
        click.echo(json_mod.dumps(ast_result, indent=2, default=str))
    else:
        click.echo(ast_result)

    if run_validation:
        from pycypher.semantic_validator import validate_query as _validate

        errors = _validate(cypher_query)
        if errors:
            click.echo("\nValidation issues:")
            for err in errors:
                click.echo(f"  [{err.severity.value}] {err.message}")
        else:
            click.echo("\nNo validation issues found.")


def format_query_impl(
    query_text: str,
    *,
    check: bool,
    lint: bool,
) -> None:
    """Format a Cypher query with consistent style."""
    from pycypher.query_formatter import format_query, lint_query

    if lint:
        issues = lint_query(query_text)
        if issues:
            for issue in issues:
                click.echo(
                    f"  L{issue.line}:{issue.column} "
                    f"[{issue.severity}] {issue.message}",
                )
            sys.exit(1)
        else:
            click.echo("No issues found.")
        return

    formatted = format_query(query_text)

    if check:
        if formatted != query_text:
            click.echo("Query needs formatting.", err=True)
            click.echo(formatted)
            sys.exit(1)
        else:
            click.echo("Query is well-formatted.")
    else:
        click.echo(formatted)


def _explain_query(cypher_query: str) -> None:
    """Show the typed AST and validation results without executing."""
    import time

    from pycypher.ast_converter import ASTConverter
    from pycypher.grammar_parser import GrammarParser

    parser = GrammarParser()

    t0 = time.perf_counter()
    parse_tree = parser.parse(cypher_query)
    parse_ms = (time.perf_counter() - t0) * 1000.0

    t1 = time.perf_counter()
    raw_ast = parser.transformer.transform(parse_tree)
    converter = ASTConverter()
    typed_ast = converter.convert(raw_ast)
    convert_ms = (time.perf_counter() - t1) * 1000.0

    click.echo(f"\nParse: {parse_ms:.1f}ms  Convert: {convert_ms:.1f}ms")
    click.echo(f"Root: {type(typed_ast).__name__}")
    click.echo(f"\n{typed_ast.pretty()}")

    from pycypher.semantic_validator import SemanticValidator

    validator = SemanticValidator()
    errors = validator.validate(typed_ast)
    if errors:
        click.echo("\nValidation errors:")
        for err in errors:
            click.echo(f"  - {err}")
    else:
        click.echo("\nValidation: OK")


def _profile_query(
    cypher_query: str,
    entity: tuple[str, ...],
    relationship: tuple[str, ...],
    parameter: tuple[str, ...],
) -> None:
    """Execute query with profiling and display detailed breakdown."""
    from pycypher.ingestion.context_builder import ContextBuilder
    from pycypher.nmetl_cli import _parse_entity_arg, _parse_rel_arg
    from pycypher.query_profiler import QueryProfiler
    from pycypher.star import Star

    if not entity and not relationship:
        click.echo("Error: --profile requires at least one --entity source.")
        return

    builder = ContextBuilder()
    for spec in entity:
        label, path, id_col = _parse_entity_arg(spec)
        builder.add_entity(label, path, id_col=id_col)
    for spec in relationship:
        rel_type, path, src_col, tgt_col = _parse_rel_arg(spec)
        builder.add_relationship(rel_type, path, source_col=src_col, target_col=tgt_col)

    ctx = builder.build()
    star = Star(context=ctx)

    params: dict[str, str] = {}
    for p in parameter:
        if "=" in p:
            key, val = p.split("=", 1)
            params[key] = val

    profiler = QueryProfiler(star=star)
    report = profiler.profile(cypher_query, parameters=params or None)

    click.echo(f"\n{report}")


# ---------------------------------------------------------------------------
# Click command wrappers
# ---------------------------------------------------------------------------


@click.command()
@click.argument("cypher_query")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output the AST as formatted JSON.",
)
@click.option(
    "--validate",
    "run_validation",
    is_flag=True,
    default=False,
    help="Run semantic validation and report any errors.",
)
def parse(cypher_query: str, *, as_json: bool, run_validation: bool) -> None:
    """Parse a Cypher query and display its AST structure."""
    parse_impl(cypher_query, as_json=as_json, run_validation=run_validation)


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
@click.option(
    "--suggestions",
    is_flag=True,
    default=False,
    help="Show similar query patterns from the suggestion engine.",
)
@click.option(
    "--hints",
    is_flag=True,
    default=False,
    help="Show performance hints and optimization suggestions.",
)
@click.option(
    "--profile",
    is_flag=True,
    default=False,
    help="Run with detailed profiling (timing breakdown, memory, recommendations).",
)
@click.option(
    "--explain",
    is_flag=True,
    default=False,
    help="Show the typed AST and validation without executing the query.",
)
def query(
    cypher_query: str,
    entity: tuple[str, ...],
    relationship: tuple[str, ...],
    parameter: tuple[str, ...],
    output: Path | None,
    limit: int | None,
    output_format: str,
    suggestions: bool,
    hints: bool,
    profile: bool,
    explain: bool,
) -> None:
    """Execute a Cypher query against ad-hoc data sources."""
    # Handle --explain: show AST without executing
    if explain:
        _explain_query(cypher_query)
        return

    if profile:
        _profile_query(cypher_query, entity, relationship, parameter)
    else:
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

    # Show performance hints if requested
    if hints:
        from pycypher.cli_intelligence import (
            PerformanceHintEngine,
            format_hints,
        )

        hint_engine = PerformanceHintEngine()
        hint_results = hint_engine.analyze(cypher_query)
        hint_output = format_hints(hint_results)
        if hint_output:
            click.echo(hint_output)

    # Show query suggestions if requested
    if suggestions:
        from pycypher.cli_intelligence import (
            QuerySuggestionEngine,
            format_suggestions,
        )

        suggestion_engine = QuerySuggestionEngine.with_common_patterns()
        suggestion_results = suggestion_engine.suggest(cypher_query)
        suggestion_output = format_suggestions(suggestion_results)
        if suggestion_output:
            click.echo(suggestion_output)


@click.command("format-query")
@click.argument("query_text")
@click.option(
    "--check",
    is_flag=True,
    default=False,
    help="Check formatting without modifying.  Exits 1 if changes needed.",
)
@click.option(
    "--lint",
    is_flag=True,
    default=False,
    help="Show lint warnings (lowercase keywords, trailing whitespace).",
)
def format_query(
    query_text: str,
    *,
    check: bool,
    lint: bool,
) -> None:
    """Format and pretty-print a Cypher query."""
    format_query_impl(query_text, check=check, lint=lint)
