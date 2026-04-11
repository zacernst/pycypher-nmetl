"""Pipeline management commands for nmetl CLI.

Contains the ``run``, ``validate``, and ``list-queries`` command
implementations.  Shared helpers (error formatting, config loading) are
imported from :mod:`pycypher.cli.common`.
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import click

from pycypher.cli.common import (
    ErrorPolicyTracker,
    cli_error,
    get_pipeline_error_label,
    load_config,
)
from pycypher.ingestion.security import mask_uri_credentials

# Exception types caught during query execution
_QUERY_EXEC_ERRORS: tuple[type[BaseException], ...] = (
    ValueError,
    KeyError,
    RuntimeError,
    TypeError,
    AttributeError,
    ImportError,
    MemoryError,
    RecursionError,
    OSError,
)

# Exception types caught during output writing
_OUTPUT_ERRORS: tuple[type[BaseException], ...] = (
    PermissionError,
    FileNotFoundError,
    ValueError,
    OSError,
    TypeError,
    UnicodeError,
    ImportError,
    MemoryError,
    AttributeError,
)

# Labels for each output exception type
_OUTPUT_ERROR_LABELS: dict[type[BaseException], str] = {
    PermissionError: "permission denied",
    FileNotFoundError: "output directory not found",
    ValueError: "invalid output format",
    OSError: "file system error writing to",
    TypeError: "data serialization error writing to",
    UnicodeError: "data serialization error writing to",
    ImportError: "data serialization error writing to",
    MemoryError: "data serialization error writing to",
    AttributeError: "data serialization error writing to",
}


# ---------------------------------------------------------------------------
# Dry-run pre-flight validation
# ---------------------------------------------------------------------------


def dry_run_validate(
    cfg: Any,
    queries: list[Any],
    config_path: Path,
    *,
    verbose: bool = False,
) -> None:
    """Comprehensive pre-flight validation without loading data or executing.

    Checks:
    1. Source file paths exist and are readable.
    2. Query source files exist and contain parseable Cypher.
    3. Output directories are writable.
    4. Output query_id references point to valid queries.
    5. Unresolved environment variable placeholders.
    """
    config_dir = config_path.parent.resolve()
    errors: list[str] = []
    warnings: list[str] = []

    click.echo("Dry run — pre-flight validation:")

    # --- 1. Source file accessibility ---
    click.echo("\n  Sources:")
    _SQL_SCHEMES = frozenset(
        {"postgresql", "postgres", "mysql", "sqlite", "duckdb"}
    )
    all_sources = [
        *((s, "entity", s.entity_type) for s in cfg.sources.entities),
        *(
            (s, "relationship", s.relationship_type)
            for s in cfg.sources.relationships
        ),
    ]
    for src, kind, label in all_sources:
        uri = src.uri
        masked = mask_uri_credentials(uri)
        parsed = urlparse(uri)
        scheme = parsed.scheme.lower() if parsed.scheme else ""
        if scheme in _SQL_SCHEMES or scheme in (
            "http",
            "https",
            "s3",
            "gs",
            "abfss",
        ):
            click.echo(
                f"    [{src.id}] {kind} {label} ← {masked}  (remote, skipped)"
            )
            continue

        file_path = (
            Path(uri.replace("file://", "")) if scheme == "file" else Path(uri)
        )
        if not file_path.is_absolute():
            file_path = (config_dir / file_path).resolve()

        if file_path.exists():
            click.echo(f"    [{src.id}] {kind} {label} ← {masked}  OK")
        else:
            msg = f"Source [{src.id}] file not found: {masked}"
            errors.append(msg)
            click.echo(f"    [{src.id}] {kind} {label} ← {masked}  MISSING")

    # --- 2. Query parsing ---
    click.echo("\n  Queries:")
    query_ids_defined = {q.id for q in cfg.queries}
    for q in queries:
        try:
            if q.inline is not None:
                query_text = q.inline
            elif q.source is not None:
                query_path = (config_dir / q.source).resolve()
                if not query_path.exists():
                    msg = f"Query [{q.id}] source file not found: {q.source}"
                    errors.append(msg)
                    click.echo(f"    [{q.id}]  FILE MISSING: {q.source}")
                    continue
                query_text = query_path.read_text(encoding="utf-8")
            else:
                msg = f"Query [{q.id}] has neither 'inline' nor 'source'."
                errors.append(msg)
                click.echo(f"    [{q.id}]  NO SOURCE")
                continue

            from pycypher.grammar_parser import GrammarParser

            parser = GrammarParser()
            parser.parse(query_text)
            desc = f"  {q.description}" if q.description else ""
            src_label = f"file:{q.source}" if q.source else "inline"
            click.echo(f"    [{q.id}] ({src_label}){desc}  OK")

        except Exception as exc:  # noqa: BLE001 — CLI: display parse error to user
            from pycypher.exceptions import sanitize_error_message

            msg = f"Query [{q.id}] parse error: {sanitize_error_message(exc)}"
            errors.append(msg)
            click.echo(
                f"    [{q.id}]  PARSE ERROR: {sanitize_error_message(exc)}"
            )

    # --- 3. Output validation ---
    if cfg.output:
        click.echo("\n  Outputs:")
        for out in cfg.output:
            if out.query_id not in query_ids_defined:
                msg = f"Output references unknown query_id: {out.query_id!r}"
                errors.append(msg)
                click.echo(
                    f"    query:{out.query_id} → {out.uri}  INVALID QUERY REF"
                )
                continue

            out_uri = out.uri
            parsed_out = urlparse(out_uri)
            out_scheme = parsed_out.scheme.lower() if parsed_out.scheme else ""
            if out_scheme in ("s3", "gs", "abfss", "http", "https"):
                click.echo(
                    f"    query:{out.query_id} → {out.uri}  (remote, skipped)"
                )
                continue

            out_path = (
                Path(out_uri.replace("file://", ""))
                if out_scheme == "file"
                else Path(out_uri)
            )
            if not out_path.is_absolute():
                out_path = (config_dir / out_path).resolve()
            out_dir = out_path.parent
            if out_dir.exists() and os.access(out_dir, os.W_OK):
                click.echo(f"    query:{out.query_id} → {out.uri}  OK")
            elif not out_dir.exists():
                msg = f"Output directory does not exist: {out_dir}"
                warnings.append(msg)
                click.echo(
                    f"    query:{out.query_id} → {out.uri}  DIR MISSING (will be created)"
                )
            else:
                msg = f"Output directory not writable: {out_dir}"
                errors.append(msg)
                click.echo(
                    f"    query:{out.query_id} → {out.uri}  NOT WRITABLE"
                )

    # --- 4. Unresolved env vars ---
    _ENV_PATTERN = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")
    raw_yaml = config_path.read_text(encoding="utf-8")
    unresolved = sorted(set(_ENV_PATTERN.findall(raw_yaml)))
    if unresolved:
        click.echo(
            f"\n  Unresolved environment variables: {', '.join(unresolved)}"
        )
        for var in unresolved:
            warnings.append(f"Unresolved env var: {var}")

    # --- Summary ---
    click.echo("")
    if errors:
        click.echo(
            f"FAILED: {len(errors)} error(s), {len(warnings)} warning(s)"
        )
        for e in errors:
            click.echo(f"  ERROR: {e}", err=True)
        for w in warnings:
            click.echo(f"  WARN:  {w}", err=True)
        sys.exit(1)

    if warnings:
        click.echo(f"PASSED with {len(warnings)} warning(s)")
        for w in warnings:
            click.echo(f"  WARN:  {w}", err=True)
    else:
        click.echo("PASSED: all pre-flight checks OK")

    click.echo("No data loaded. No queries executed.")


# ---------------------------------------------------------------------------
# run sub-command (implementation)
# ---------------------------------------------------------------------------


def run_impl(
    config: Path,
    dry_run: bool,
    query_ids: tuple[str, ...],
    output_dir: Path | None,
    on_error: str | None,
    verbose: bool,
) -> None:
    """Implementation of the ``run`` command.

    Separated from the Click decorator so ``nmetl_cli.py`` can re-export it
    for backward compatibility.
    """
    from shared.logger import LOGGER

    if verbose:
        click.echo(f"Loading config: {config}")

    pipeline_config = load_config(config, verbose=verbose)

    if verbose:
        project_name = (
            pipeline_config.project.name
            if pipeline_config.project
            else "(unnamed)"
        )
        click.echo(f"Project: {project_name}")
        n_entity = len(pipeline_config.sources.entities)
        n_rel = len(pipeline_config.sources.relationships)
        click.echo(f"Sources: {n_entity} entity, {n_rel} relationship")
        click.echo(f"Queries: {len(pipeline_config.queries)}")

    # Filter to requested queries (if --query-id was used)
    queries = pipeline_config.queries
    if query_ids:
        requested = set(query_ids)
        queries = [q for q in queries if q.id in requested]
        unknown = requested - {q.id for q in pipeline_config.queries}
        if unknown:
            click.echo(
                f"Warning: unknown query IDs: {', '.join(sorted(unknown))}",
                err=True,
            )

    if dry_run:
        dry_run_validate(pipeline_config, queries, config, verbose=verbose)
        return

    # -------------------------------------------------------------------
    # Build execution context from config sources
    # -------------------------------------------------------------------
    from pycypher.ingestion.context_builder import ContextBuilder
    from pycypher.ingestion.output_writer import write_dataframe_to_uri
    from pycypher.star import Star

    n_entity = len(pipeline_config.sources.entities)
    n_rel = len(pipeline_config.sources.relationships)
    n_sources = n_entity + n_rel
    click.echo(f"Loading {n_sources} data source(s) …")

    try:
        builder = ContextBuilder()
        for i, entity_src in enumerate(pipeline_config.sources.entities, 1):
            click.echo(
                f"  [{i}/{n_sources}] entity {entity_src.entity_type}"
                f" <- {mask_uri_credentials(entity_src.uri)}",
            )
            builder.add_entity(
                entity_src.entity_type,
                entity_src.uri,
                id_col=entity_src.id_col,
                query=entity_src.query,
            )
        for j, rel_src in enumerate(
            pipeline_config.sources.relationships, 1
        ):
            click.echo(
                f"  [{n_entity + j}/{n_sources}] relationship"
                f" {rel_src.relationship_type}"
                f" <- {mask_uri_credentials(rel_src.uri)}",
            )
            builder.add_relationship(
                rel_src.relationship_type,
                rel_src.uri,
                source_col=rel_src.source_col,
                target_col=rel_src.target_col,
                id_col=rel_src.id_col,
                query=rel_src.query,
            )
        context = builder.build()
    except FileNotFoundError as exc:
        cli_error(f"data source file not found: {exc}")
    except PermissionError as exc:
        cli_error(f"permission denied accessing data source: {exc}")
    except ValueError as exc:
        cli_error(f"invalid data source format or configuration: {exc}")
    except OSError as exc:
        cli_error(f"file system error loading data sources: {exc}")

    star = Star(context=context)
    config_dir = config.parent

    # -------------------------------------------------------------------
    # Execute each query and write results to output sinks
    # -------------------------------------------------------------------
    tracker = ErrorPolicyTracker((on_error or "fail").lower())

    n_queries = len(queries)
    click.echo(f"Executing {n_queries} query/queries …")
    pipeline_start = time.monotonic()

    for qi, q in enumerate(queries, 1):
        click.echo(f"  [{qi}/{n_queries}] query [{q.id}] …")

        # --- Phase: load query text ---
        phase_start = time.monotonic()
        try:
            if q.inline is not None:
                query_text = q.inline
            elif q.source is not None:
                query_path = (config_dir / q.source).resolve()
                try:
                    query_path.relative_to(config_dir.resolve())
                except ValueError:
                    msg = (
                        f"Query source {q.source!r} escapes the config directory "
                        f"{config_dir}. Only paths within the config directory are "
                        "allowed."
                    )
                    raise ValueError(msg) from None
                query_text = query_path.read_text(encoding="utf-8")
            else:
                msg = f"Query {q.id!r} has neither 'inline' nor 'source'."
                raise ValueError(msg)
        except (
            ValueError,
            FileNotFoundError,
            PermissionError,
            UnicodeDecodeError,
            OSError,
        ) as exc:
            LOGGER.info(
                "query [%s] load failed after %.1fms: %s",
                q.id,
                (time.monotonic() - phase_start) * 1000,
                exc,
            )
            if tracker.handle(f"could not load query {q.id!r}: {exc}"):
                continue

        load_ms = (time.monotonic() - phase_start) * 1000
        LOGGER.info("query [%s] loaded in %.1fms", q.id, load_ms)

        # --- Phase: execute query ---
        phase_start = time.monotonic()
        try:
            result_df = star.execute_query(query_text)
        except _QUERY_EXEC_ERRORS as exc:
            exec_ms = (time.monotonic() - phase_start) * 1000
            label = get_pipeline_error_label(exc)
            LOGGER.info(
                "query [%s] execution failed after %.1fms (%s): %s",
                q.id,
                exec_ms,
                label,
                exc,
            )
            if tracker.handle(f"query [{q.id}] {label}: {exc}"):
                continue

        exec_ms = (time.monotonic() - phase_start) * 1000
        n_result_rows = len(result_df) if result_df is not None else 0
        LOGGER.info(
            "query [%s] executed in %.1fms (%d rows)",
            q.id,
            exec_ms,
            n_result_rows,
        )

        # --- Phase: write outputs ---
        sinks = [o for o in pipeline_config.output if o.query_id == q.id]
        for si, sink in enumerate(sinks, 1):
            sink_start = time.monotonic()
            try:
                write_dataframe_to_uri(result_df, sink.uri, sink.format)
                n_rows = len(result_df) if result_df is not None else 0
                sink_ms = (time.monotonic() - sink_start) * 1000
                click.echo(
                    f"    output [{si}/{len(sinks)}]"
                    f" {n_rows} row(s)"
                    f" -> {mask_uri_credentials(sink.uri)}",
                )
                LOGGER.info(
                    "query [%s] output %d/%d written in %.1fms (%d rows -> %s)",
                    q.id,
                    si,
                    len(sinks),
                    sink_ms,
                    n_rows,
                    mask_uri_credentials(sink.uri),
                )
            except _OUTPUT_ERRORS as exc:
                sink_ms = (time.monotonic() - sink_start) * 1000
                label = _OUTPUT_ERROR_LABELS.get(
                    type(exc), "error writing to"
                )
                LOGGER.info(
                    "query [%s] output %d/%d failed after %.1fms: %s",
                    q.id,
                    si,
                    len(sinks),
                    sink_ms,
                    exc,
                )
                tracker.handle(
                    f"query [{q.id}] {label} {mask_uri_credentials(sink.uri)!r}: {exc}",
                )

    pipeline_ms = (time.monotonic() - pipeline_start) * 1000
    if tracker.failed and tracker.policy == "warn":
        click.echo(
            "Pipeline completed with warnings.  "
            "One or more queries failed (see above).",
            err=True,
        )

    LOGGER.info("Pipeline completed in %.1fms", pipeline_ms)
    click.echo("Done.")


# ---------------------------------------------------------------------------
# validate sub-command (implementation)
# ---------------------------------------------------------------------------


def validate_impl(config: Path, verbose: bool) -> None:
    """Implementation of the ``validate`` command."""
    cfg = load_config(config, verbose=verbose)

    click.echo(f"Config is valid: {config}")

    if verbose:
        project_name = cfg.project.name if cfg.project else "(unnamed)"
        click.echo(f"\nProject:  {project_name}")
        if cfg.project and cfg.project.description:
            click.echo(f"          {cfg.project.description}")

        click.echo(f"\nVersion:  {cfg.version}")

        if cfg.sources.entities:
            click.echo(f"\nEntity sources ({len(cfg.sources.entities)}):")
            for src in cfg.sources.entities:
                click.echo(
                    f"  [{src.id}]  {src.entity_type}  ← {mask_uri_credentials(src.uri)}"
                )

        if cfg.sources.relationships:
            click.echo(
                f"\nRelationship sources ({len(cfg.sources.relationships)}):",
            )
            for src in cfg.sources.relationships:
                click.echo(
                    f"  [{src.id}]  {src.relationship_type}  ← {mask_uri_credentials(src.uri)}",
                )

        if cfg.queries:
            click.echo(f"\nQueries ({len(cfg.queries)}):")
            for q in cfg.queries:
                desc = f"  {q.description}" if q.description else ""
                src_label = f"file:{q.source}" if q.source else "inline"
                click.echo(f"  [{q.id}] ({src_label}){desc}")

        if cfg.output:
            click.echo(f"\nOutputs ({len(cfg.output)}):")
            for out in cfg.output:
                fmt = f" [{out.format}]" if out.format else ""
                click.echo(
                    f"  query:{out.query_id} → {out.uri}{fmt}",
                )

        if cfg.functions:
            click.echo(f"\nFunctions ({len(cfg.functions)}):")
            for fn in cfg.functions:
                if fn.callable:
                    click.echo(f"  callable: {fn.callable}")
                else:
                    names = fn.names if fn.names != "*" else "(all)"
                    click.echo(f"  module: {fn.module}  names: {names}")


# ---------------------------------------------------------------------------
# list-queries sub-command (implementation)
# ---------------------------------------------------------------------------


def list_queries_impl(config: Path, *, deps: bool) -> None:
    """Implementation of the ``list-queries`` command."""
    cfg = load_config(config)

    if not cfg.queries:
        click.echo("No queries defined in config.")
        return

    if not deps:
        for q in cfg.queries:
            desc = f"  — {q.description}" if q.description else ""
            src = f"(file: {q.source})" if q.source else "(inline)"
            click.echo(f"{q.id:30s} {src}{desc}")
        return

    # Dependency analysis mode
    from pycypher.multi_query_analyzer import QueryDependencyAnalyzer

    analyzer = QueryDependencyAnalyzer()
    query_pairs: list[tuple[str, str]] = []
    for q in cfg.queries:
        cypher = q.inline or ""
        if q.source and not cypher:
            try:
                cypher = Path(q.source).read_text()
            except OSError:
                cypher = ""
        if cypher:
            query_pairs.append((q.id, cypher))

    if not query_pairs:
        click.echo("No parseable queries found.")
        return

    try:
        graph = analyzer.analyze(query_pairs)
        sorted_nodes = graph.topological_sort()
    except Exception as exc:  # noqa: BLE001 — CLI: display analysis error to user
        from shared.logger import LOGGER as _logger

        _logger.warning("Dependency analysis failed: %s", exc, exc_info=True)
        click.echo(f"Dependency analysis failed: {exc}")
        return

    click.echo("\nQuery Dependency Analysis\n")
    for node in sorted_nodes:
        desc_q = next(
            (q for q in cfg.queries if q.id == node.query_id),
            None,
        )
        desc = (
            f"  — {desc_q.description}"
            if desc_q and desc_q.description
            else ""
        )
        produces = ", ".join(sorted(node.produces)) or "(none)"
        consumes = ", ".join(sorted(node.consumes)) or "(none)"
        dep_ids = ", ".join(sorted(node.dependencies)) or "(none)"
        click.echo(f"  {node.query_id}{desc}")
        click.echo(f"    produces: {produces}")
        click.echo(f"    consumes: {consumes}")
        click.echo(f"    depends on: {dep_ids}")
        click.echo()

    click.echo(
        f"Execution order: {' → '.join(n.query_id for n in sorted_nodes)}",
    )


# ---------------------------------------------------------------------------
# Click command wrappers
# ---------------------------------------------------------------------------


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
    r"""Run the ETL pipeline defined in CONFIG.

    CONFIG is the path to a YAML pipeline configuration file.  The file
    defines data sources, Cypher queries, and output sinks.  All Cypher
    queries are executed in dependency order (inferred from the entity types
    each query reads and writes).

    \b
    Examples:
      nmetl run pipeline.yaml                — run full pipeline
      nmetl run pipeline.yaml --dry-run      — preview execution plan
      nmetl run pipeline.yaml --query-id q1  — run only query "q1"
      nmetl run pipeline.yaml --on-error warn — continue on failures

    Exit codes:

    \b
      0  All queries completed successfully.
      1  One or more queries failed (with --on-error=fail, the default).
      2  Configuration error (invalid YAML or schema violation).
    """
    run_impl(config, dry_run, query_ids, output_dir, on_error, verbose)


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
    r"""Validate the pipeline config file at CONFIG.

    Loads and parses the YAML, then validates every field against the
    PipelineConfig schema.  Reports any errors found and exits with a
    non-zero status code if validation fails.

    This command does NOT load any data sources or execute any queries.

    \b
    Examples:
      nmetl validate pipeline.yaml             — check config is valid
      nmetl validate pipeline.yaml --verbose   — show full config summary

    Exit codes:

    \b
      0  Config is valid.
      1  Config contains schema or validation errors.
    """
    validate_impl(config, verbose)


@click.command("list-queries")
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--deps",
    is_flag=True,
    default=False,
    help="Show dependency analysis and execution order.",
)
def list_queries(config: Path, *, deps: bool) -> None:
    r"""List all queries defined in the CONFIG file.

    Prints one line per query with its ID and an optional description.

    \b
    Examples:
      nmetl list-queries pipeline.yaml
      nmetl list-queries pipeline.yaml --deps

    CONFIG is the path to a YAML pipeline configuration file.
    """
    list_queries_impl(config, deps=deps)
