"""Query subcommand for the nmetl CLI.

Extracted from ``nmetl_cli.py`` to reduce single-file cognitive load.
The ``register`` function adds the ``query`` command to the Click group.

All shared helpers (_cli_error, _parse_entity_arg, _parse_rel_arg, etc.)
remain in ``nmetl_cli`` and are imported from there.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    import pandas as pd


# ---------------------------------------------------------------------------
# Output formatting helpers
# ---------------------------------------------------------------------------


def _fmt_cell(value: object) -> str:
    """Format a cell value for display, showing NULL for None/NaN."""
    if value is None:
        return "NULL"
    if isinstance(value, float) and value != value:  # NaN fast-check
        return "NULL"
    return str(value)


def _truncate(text: str, width: int) -> str:
    """Truncate *text* to *width* chars, adding ellipsis if trimmed."""
    if len(text) <= width:
        return text
    return text[: width - 1] + "\u2026"


def _print_table(df: pd.DataFrame, *, no_header: bool = False) -> None:
    """Print *df* as a simple pipe-separated ASCII table to stdout.

    Column widths are capped so that the table fits within the terminal.
    Long cell values are truncated with an ellipsis character.
    """
    import shutil

    if df.empty:
        click.echo("(no rows returned)")
        return

    cols = list(df.columns)
    # Natural widths (uncapped)
    natural_widths = [
        max(len(str(c)), max((len(_fmt_cell(v)) for v in df[c]), default=0))
        for c in cols
    ]

    # Cap columns to fit terminal width.  Each column uses (width + 3)
    # chars for "| value " plus a final "|".
    term_width = shutil.get_terminal_size((120, 24)).columns
    overhead = len(cols) + 1  # pipe characters
    padding = len(cols) * 2  # one space each side per column
    available = term_width - overhead - padding
    n_cols = len(cols)

    if sum(natural_widths) > available and available > n_cols:
        # Distribute available space proportionally, minimum 4 chars/col
        min_col = 4
        col_widths = [
            max(min_col, int(w * available / sum(natural_widths)))
            for w in natural_widths
        ]
        # Redistribute any leftover to the widest columns
        leftover = available - sum(col_widths)
        if leftover > 0:
            ranked = sorted(
                range(n_cols), key=lambda i: natural_widths[i], reverse=True
            )
            for i in ranked[:leftover]:
                col_widths[i] += 1
    else:
        col_widths = natural_widths

    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    if not no_header:
        header = (
            "|"
            + "|".join(
                f" {_truncate(str(c), w):<{w}} "
                for c, w in zip(cols, col_widths, strict=False)
            )
            + "|"
        )
        click.echo(sep)
        click.echo(header)
    click.echo(sep)
    # Use vectorized approach instead of iterrows() for better performance
    for row_dict in df.to_dict("records"):
        line = (
            "|"
            + "|".join(
                f" {_truncate(_fmt_cell(row_dict[c]), w):<{w}} "
                for c, w in zip(cols, col_widths, strict=False)
            )
            + "|"
        )
        click.echo(line)
    click.echo(sep)


def _print_markdown_table(df: pd.DataFrame, output_path: Path | None) -> None:
    """Render *df* as a GitHub-flavoured Markdown table.

    If *output_path* is given, the table is written to that file;
    otherwise it is printed to stdout.
    """
    if df.empty:
        text = "(no rows returned)\n"
        if output_path is not None:
            output_path.write_text(text)
        else:
            click.echo(text, nl=False)
        return

    cols = list(df.columns)
    header = "| " + " | ".join(str(c) for c in cols) + " |"
    separator = "| " + " | ".join("---" for _ in cols) + " |"
    lines = [header, separator]
    for row_dict in df.to_dict("records"):
        row = "| " + " | ".join(_fmt_cell(row_dict[c]) for c in cols) + " |"
        lines.append(row)

    text = "\n".join(lines) + "\n"
    if output_path is not None:
        output_path.write_text(text)
        click.echo(f"Wrote {len(df)} row(s) \u2192 {output_path}")
    else:
        click.echo(text, nl=False)


def _print_dot_graph(df: pd.DataFrame, output_path: Path | None) -> None:
    """Render *df* as a Graphviz DOT digraph.

    Heuristic: if the DataFrame has exactly two columns whose names suggest
    source\u2192target (e.g. from a relationship query), render them as edges.
    Otherwise, render each row as a labelled node.

    If *output_path* is given, the DOT source is written to that file;
    otherwise it is printed to stdout.
    """
    lines: list[str] = [
        "digraph G {",
        "  rankdir=LR;",
        '  node [shape=box, style=filled, fillcolor="#e8e8e8"];',
    ]

    cols = list(df.columns)

    if len(cols) >= 2 and df.shape[0] > 0:
        # Try to detect source\u2192target pair: first two columns
        src_col, tgt_col = cols[0], cols[1]
        nodes: set[str] = set()
        for row_dict in df.to_dict("records"):
            src = _fmt_cell(row_dict[src_col])
            tgt = _fmt_cell(row_dict[tgt_col])
            nodes.add(src)
            nodes.add(tgt)
            # Build edge label from remaining columns
            extras = {
                c: _fmt_cell(row_dict[c])
                for c in cols[2:]
                if _fmt_cell(row_dict[c]) != "NULL"
            }
            label = ", ".join(f"{k}={v}" for k, v in extras.items())
            edge_attr = f' [label="{label}"]' if label else ""
            lines.append(f'  "{src}" -> "{tgt}"{edge_attr};')
        for node in sorted(nodes):
            lines.append(f'  "{node}";')
    elif df.shape[0] > 0:
        # Single-column or non-relational: render as labelled nodes
        for i, row_dict in enumerate(df.to_dict("records")):
            label = ", ".join(f"{c}: {_fmt_cell(row_dict[c])}" for c in cols)
            lines.append(f'  n{i} [label="{label}"];')
    else:
        lines.append("  // (no rows returned)")

    lines.append("}")
    text = "\n".join(lines) + "\n"

    if output_path is not None:
        output_path.write_text(text, encoding="utf-8")
        click.echo(f"Wrote DOT graph \u2192 {output_path}")
    else:
        click.echo(text, nl=False)


# ---------------------------------------------------------------------------
# Query command registration
# ---------------------------------------------------------------------------


def register(cli_group: click.Group) -> None:
    """Register the ``query`` subcommand on *cli_group*."""

    @cli_group.command("query")
    @click.argument("query_text")
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
        "--output",
        "output_path",
        type=click.Path(path_type=Path),
        default=None,
        help=(
            "Write results to this file.  Format is inferred from the extension "
            "(.csv, .parquet, .json).  Default: print a text table to stdout."
        ),
    )
    @click.option(
        "--id-col",
        "default_id_col",
        default=None,
        metavar="COL",
        help="Default ID column name for entity sources that do not specify one.",
    )
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(
            ["csv", "parquet", "json", "jsonl", "markdown", "table", "dot"],
            case_sensitive=False,
        ),
        default=None,
        help=(
            "Explicit output format.  Overrides the extension of --output when "
            "both are given.  'jsonl' emits one JSON object per line.  "
            "'markdown' produces a GitHub-flavoured Markdown table.  "
            "'dot' emits a Graphviz DOT digraph (pipe to `dot -Tpng`).  "
            "Use 'table' to force an ASCII table to stdout."
        ),
    )
    @click.option(
        "--no-header",
        "no_header",
        is_flag=True,
        default=False,
        help="Omit the header row from CSV and table output (useful for scripting).",
    )
    @click.option(
        "--timeout",
        "timeout_seconds",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            "Wall-clock timeout for query execution in seconds (e.g. --timeout 30).  "
            "If the query exceeds this limit it is cancelled and exits with an error.  "
            "Default: no timeout."
        ),
    )
    @click.option(
        "--verbose",
        "-v",
        is_flag=True,
        default=False,
        help="Show row count and timing after the query completes.",
    )
    @click.option(
        "--profile",
        is_flag=True,
        default=False,
        help=(
            "Print a detailed execution profile after the query completes.  "
            "Shows per-clause timing breakdown, hotspot identification, and "
            "optimization recommendations."
        ),
    )
    @click.option(
        "--explain",
        is_flag=True,
        default=False,
        help=(
            "Show the parsed AST and semantic validation result without "
            "executing the query.  Useful for debugging query structure."
        ),
    )
    def query(
        query_text: str,
        entity_specs: tuple[str, ...],
        rel_specs: tuple[str, ...],
        output_path: Path | None,
        default_id_col: str | None,
        fmt: str | None,
        no_header: bool,
        timeout_seconds: float | None,
        verbose: bool,
        profile: bool,
        explain: bool,
    ) -> None:
        r"""Execute a Cypher QUERY_TEXT against one or more data files.

        This command lets you run a single Cypher query ad-hoc — no YAML config
        required.  Supply entity sources with --entity and relationship sources
        with --rel.

        \b
        Examples:
          nmetl query "MATCH (p:Person) RETURN p.name" --entity Person=people.csv
          nmetl query "MATCH (p:Person) RETURN p.name" \
              --entity Person=people.csv:id \
              --output results.csv
          nmetl query "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p.name, q.name" \
              --entity Person=people.csv \
              --rel KNOWS=knows.csv:from_id:to_id
        """
        import time

        from pycypher.ingestion.context_builder import ContextBuilder
        from pycypher.nmetl_cli import (
            _ADHOC_QUERY_ERRORS,
            _cli_error,
            _get_adhoc_error_label,
            _load_data_source,
            _parse_entity_arg,
            _parse_rel_arg,
        )
        from pycypher.star import Star

        if not query_text or not query_text.strip():
            _cli_error("QUERY_TEXT must not be empty.")

        # ------------------------------------------------------------------
        # Explain mode: parse + validate without executing (no data needed)
        # ------------------------------------------------------------------
        if explain:
            _handle_explain(query_text)
            return

        if not entity_specs and not rel_specs:
            _cli_error("at least one --entity source is required.")

        # ------------------------------------------------------------------
        # Parse argument specs
        # ------------------------------------------------------------------
        parsed_entities: list[tuple[str, str, str | None]] = []
        for spec in entity_specs:
            try:
                parsed_entities.append(_parse_entity_arg(spec))
            except ValueError as exc:
                _cli_error(f"invalid --entity spec: {exc}")

        parsed_rels: list[tuple[str, str, str, str]] = []
        for spec in rel_specs:
            try:
                parsed_rels.append(_parse_rel_arg(spec))
            except ValueError as exc:
                _cli_error(f"invalid --rel spec: {exc}")

        # ------------------------------------------------------------------
        # Build execution context
        # ------------------------------------------------------------------
        n_sources = len(parsed_entities) + len(parsed_rels)
        click.echo(f"Loading {n_sources} data source(s) …")

        from pycypher.ingestion.security import mask_uri_credentials

        builder = ContextBuilder()
        for i, (label, path, id_col) in enumerate(parsed_entities, 1):
            click.echo(f"  [{i}/{n_sources}] entity {label} <- {mask_uri_credentials(path)}")
            effective_id_col = id_col or default_id_col
            _load_data_source(
                lambda _l=label, _p=path, _i=effective_id_col: (
                    builder.add_entity(
                        _l,
                        _p,
                        id_col=_i,
                    )
                ),
                "entity source",
                path,
            )

        for j, (rel_type, path, src_col, tgt_col) in enumerate(parsed_rels, 1):
            click.echo(
                f"  [{len(parsed_entities) + j}/{n_sources}]"
                f" relationship {rel_type} <- {mask_uri_credentials(path)}",
            )
            _load_data_source(
                lambda _r=rel_type, _p=path, _s=src_col, _t=tgt_col: (
                    builder.add_relationship(
                        _r,
                        _p,
                        source_col=_s,
                        target_col=_t,
                    )
                ),
                "relationship source",
                path,
            )

        try:
            context = builder.build()
        except ValueError as exc:
            _cli_error(f"invalid data configuration: {exc}")
        except RuntimeError as exc:
            _cli_error(f"could not build query context: {exc}")

        # ------------------------------------------------------------------
        # Execute the query
        # ------------------------------------------------------------------
        star = Star(context=context)
        profile_report = None
        click.echo("Executing query …")
        t0 = time.monotonic()
        try:
            result_df = star.execute_query(
                query_text,
                timeout_seconds=timeout_seconds,
            )
        except _ADHOC_QUERY_ERRORS as exc:
            from pycypher.exceptions import sanitize_error_message

            label = _get_adhoc_error_label(exc)
            _cli_error(f"{label}: {sanitize_error_message(exc)}")
        elapsed = time.monotonic() - t0

        # Build profile report from star's instrumentation data (no re-execution).
        if profile:
            profile_report = _build_profile_report(
                star,
                query_text,
                elapsed,
                result_df,
            )

        # ------------------------------------------------------------------
        # Emit results
        # ------------------------------------------------------------------
        _emit_results(
            result_df,
            output_path=output_path,
            fmt=fmt,
            no_header=no_header,
            verbose=verbose,
            elapsed=elapsed,
        )

        if profile_report is not None:
            click.echo(f"\n{profile_report}")


def _handle_explain(query_text: str) -> None:
    """Parse and validate a Cypher query without executing it."""
    import time as _time

    from pycypher.grammar_parser import GrammarParser
    from pycypher.nmetl_cli import _cli_error
    from pycypher.semantic_validator import SemanticValidator

    parser = GrammarParser()
    t0_parse = _time.monotonic()
    try:
        ast = parser.parse(query_text)
    except (SyntaxError, ValueError) as exc:
        _cli_error(f"parse error: {exc}")
    parse_ms = (_time.monotonic() - t0_parse) * 1000.0

    click.echo(f"Parse time: {parse_ms:.1f}ms")
    click.echo(f"AST type:   {type(ast).__name__}")
    click.echo(f"AST:        {ast!r}")

    validator = SemanticValidator()
    errors = validator.validate(ast)
    if errors:
        click.echo("\nValidation errors:")
        for err in errors:
            click.echo(f"  - {err}")
    else:
        click.echo("\nValidation: OK")


def _build_profile_report(
    star: object,
    query_text: str,
    elapsed: float,
    result_df: pd.DataFrame,
) -> object:
    """Build an execution profile report from Star instrumentation data."""
    from pycypher.query_profiler import (
        ProfileReport,
        _generate_recommendations,
    )

    clause_timings: dict[str, float] = dict(
        getattr(star, "_last_clause_timings", {}),
    )
    parse_ms = getattr(star, "_last_parse_time_ms", 0.0)
    plan_ms = getattr(star, "_last_plan_time_ms", 0.0)
    total_ms = elapsed * 1000.0
    row_count = len(result_df)
    hotspot = (
        max(clause_timings, key=lambda k: clause_timings[k])
        if clause_timings
        else None
    )
    recommendations = _generate_recommendations(
        query=query_text,
        total_ms=total_ms,
        parse_ms=parse_ms,
        plan_ms=plan_ms,
        clause_timings=clause_timings,
        row_count=row_count,
        hotspot=hotspot,
    )
    return ProfileReport(
        query=query_text,
        total_time_ms=total_ms,
        parse_time_ms=parse_ms,
        plan_time_ms=plan_ms,
        clause_timings=clause_timings,
        row_count=row_count,
        hotspot=hotspot,
        recommendations=recommendations,
    )


def _emit_results(
    result_df: pd.DataFrame,
    *,
    output_path: Path | None,
    fmt: str | None,
    no_header: bool,
    verbose: bool,
    elapsed: float,
) -> None:
    """Determine output format and write/print results."""
    from pycypher.nmetl_cli import _cli_error

    effective_fmt = fmt

    if output_path is not None and effective_fmt is None:
        ext = output_path.suffix.lower()
        _EXT_TO_FMT = {
            ".csv": "csv",
            ".parquet": "parquet",
            ".json": "json",
            ".md": "markdown",
            ".dot": "dot",
            ".gv": "dot",
        }
        effective_fmt = _EXT_TO_FMT.get(ext)
        if effective_fmt is not None and verbose:
            click.echo(f"Inferred output format: {effective_fmt} (from {ext})")
        if effective_fmt is None:
            _cli_error(
                f"cannot infer output format from {output_path}.  "
                "Use --format or give the file a .csv/.parquet/.json/.md/.dot extension.",
            )

    if effective_fmt is None:
        effective_fmt = "table"

    # Normalise jsonl -> json (same output, just an explicit alias)
    if effective_fmt == "jsonl":
        effective_fmt = "json"

    if output_path is not None and effective_fmt not in {
        "table",
        "markdown",
        "dot",
    }:
        from pycypher.ingestion.config import OutputFormat
        from pycypher.ingestion.security import SecurityError

        _FMT_MAP = {
            "csv": OutputFormat.CSV,
            "parquet": OutputFormat.PARQUET,
            "json": OutputFormat.JSON,
        }
        try:
            from pycypher.ingestion.output_writer import write_dataframe_to_uri

            write_dataframe_to_uri(
                result_df,
                str(output_path),
                _FMT_MAP[effective_fmt],
            )
        except SecurityError as exc:
            _cli_error(f"access denied writing to {output_path}: {exc}")
        except PermissionError:
            _cli_error(f"permission denied writing to: {output_path}")
        except FileNotFoundError:
            _cli_error(
                f"output directory does not exist: {output_path.parent}",
            )
        except ValueError as exc:
            _cli_error(f"invalid output format or data: {exc}")
        except OSError as exc:
            _cli_error(f"could not write output to {output_path}: {exc}")
        if verbose:
            click.echo(f"Wrote {len(result_df)} row(s) \u2192 {output_path}")
    elif effective_fmt == "csv":
        click.echo(
            result_df.to_csv(index=False, header=not no_header),
            nl=False,
        )
    elif effective_fmt == "json":
        click.echo(result_df.to_json(orient="records", lines=True), nl=False)
    elif effective_fmt == "markdown":
        _print_markdown_table(result_df, output_path)
    elif effective_fmt == "dot":
        _print_dot_graph(result_df, output_path)
    else:  # table (default)
        _print_table(result_df, no_header=no_header)

    if verbose and output_path is None:
        click.echo(f"\n{len(result_df)} row(s)  ({elapsed * 1000:.1f} ms)")
    elif verbose:
        click.echo(f"{len(result_df)} row(s)  ({elapsed * 1000:.1f} ms)")
