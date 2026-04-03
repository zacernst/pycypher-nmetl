"""Command-line interface for the nmetl ETL pipeline runner.

nmetl is the command-line entry point for running PyCypher-based ETL
pipelines defined in a YAML configuration file.  It loads data sources,
executes Cypher queries, and writes results to configured output sinks.

Example usage::

    # Run a pipeline from a config file
    nmetl run pipeline.yaml

    # Validate a config file without running
    nmetl validate pipeline.yaml

    # List all queries defined in the config
    nmetl list-queries pipeline.yaml

    # Dry-run: show what would be executed
    nmetl run pipeline.yaml --dry-run
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import click
import yaml
from lark.exceptions import LarkError, ParseError, UnexpectedInput
from pydantic import ValidationError

from pycypher.exceptions import (
    CypherSyntaxError,
    FunctionArgumentError,
    GraphTypeNotFoundError,
    IncompatibleOperatorError,
    InvalidCastError,
    MissingParameterError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    UnsupportedFunctionError,
    VariableNotFoundError,
    VariableTypeMismatchError,
    WrongCypherTypeError,
)

# Import DuckDB exceptions for proper error handling
try:
    import duckdb  # noqa: F401  # required for _duckdb internal module
    from _duckdb import IOException as DuckDBIOException
except ImportError:
    # Handle case where DuckDB is not available
    DuckDBIOException = None  # type: ignore[assignment]  # fallback when duckdb unavailable

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Shared error-handling helpers
# ---------------------------------------------------------------------------


def _cli_error(message: str, *, exit_code: int = 1) -> NoReturn:
    """Print an error message to stderr and exit."""
    click.echo(f"Error: {message}", err=True)
    sys.exit(exit_code)


def _format_validation_errors(
    exc: ValidationError, *, verbose: bool = False
) -> str:
    """Format a Pydantic ValidationError into concise, actionable lines.

    Delegates to :func:`pycypher.cli.common.format_validation_errors`.
    """
    from pycypher.cli.common import format_validation_errors

    return format_validation_errors(exc, verbose=verbose)


def _translate_duckdb_error(exc: BaseException, kind: str, path: str) -> str:
    """Translate a DuckDB or generic data-loading exception into a user message.

    Delegates to :func:`pycypher.cli.common.translate_duckdb_error`.
    """
    from pycypher.cli.common import translate_duckdb_error

    return translate_duckdb_error(exc, kind, path)


def _load_data_source(
    action: Any,
    kind: str,
    path: str,
) -> None:
    """Call *action* and translate any data-loading exception into a CLI error.

    This centralises the exception handling for ``builder.add_entity`` /
    ``builder.add_relationship`` in the ``query`` sub-command.

    Args:
        action: A zero-argument callable that performs the data load.
        kind: Source kind for error messages (``"entity source"`` or
            ``"relationship source"``).
        path: File path string shown in error messages.

    """
    from pycypher.ingestion.security import mask_uri_credentials

    safe_path = mask_uri_credentials(path)
    desc = f"{kind} {safe_path!r}"
    try:
        action()
    except FileNotFoundError:
        _cli_error(f"{kind} file not found: {safe_path!r}")
    except PermissionError:
        _cli_error(f"permission denied accessing {kind}: {safe_path!r}")
    except KeyError as exc:
        hint = (
            "  Entity sources require an '__ID__' column.\n"
            "  Relationship sources require '__ID__', '__SOURCE__', "
            "and '__TARGET__' columns.\n"
            "  Specify the ID column with: Label=file.csv:my_id_col"
            if "relationship" not in kind
            else "  Relationship format: REL=file.csv:source_col:target_col"
        )
        _cli_error(
            f"missing required column in {desc}: {exc}\n{hint}",
        )
    except ValueError as exc:
        _cli_error(
            f"invalid data format in {desc}: {exc}\n"
            "  Check that the file is valid CSV/Parquet and column "
            "types match expected values.",
        )
    except OSError as exc:
        _cli_error(f"could not read {desc}: {exc}")
    except (RuntimeError, ImportError, TypeError, MemoryError) as exc:
        _cli_error(_translate_duckdb_error(exc, kind, safe_path))
    except Exception as exc:  # noqa: BLE001 — DuckDB type-dispatch; unknown exceptions re-raised
        if DuckDBIOException and isinstance(exc, DuckDBIOException):
            _cli_error(_translate_duckdb_error(exc, kind, safe_path))
        if "No files found that match the pattern" in str(exc):
            _cli_error(f"{kind} file not found: {safe_path!r}")
        # Re-raise unexpected exceptions for debugging
        raise


def _load_config(config: Path, *, verbose: bool = False) -> Any:
    """Load and validate a pipeline config, translating errors to CLI exits.

    Args:
        config: Path to the YAML config file.
        verbose: If True, show all validation errors instead of truncating.

    Returns:
        The parsed ``PipelineConfig`` object.

    """
    from pycypher.ingestion.config import load_pipeline_config

    try:
        return load_pipeline_config(config)
    except FileNotFoundError:
        _cli_error(f"config file not found: {config}", exit_code=1)
    except yaml.YAMLError as exc:
        _cli_error(f"invalid YAML syntax in config: {exc}", exit_code=2)
    except ValidationError as exc:
        _cli_error(
            _format_validation_errors(exc, verbose=verbose), exit_code=2
        )
    except (PermissionError, OSError) as exc:
        _cli_error(f"could not read config file: {exc}", exit_code=1)


class _ErrorPolicyTracker:
    """Tracks whether any query has failed under warn/skip policy.

    Used by the ``run`` command to apply the ``--on-error`` policy
    consistently across query loading, execution, and output phases.

    Args:
        policy: One of ``"fail"``, ``"warn"``, or ``"skip"``.

    """

    def __init__(self, policy: str) -> None:
        self.policy = policy
        self.failed = False

    def handle(self, message: str) -> bool:
        """Apply the error policy and return True if the caller should skip.

        Under ``fail`` policy this calls :func:`sys.exit`.  Under ``warn``
        it prints a warning and sets ``self.failed``.  Under ``skip`` it
        silently sets ``self.failed``.

        Args:
            message: The error message to display.

        Returns:
            ``True`` if the caller should ``continue`` to the next item.

        """
        if self.policy == "fail":
            click.echo(f"Error: {message}", err=True)
            sys.exit(1)
        if self.policy == "warn":
            click.echo(f"Warning: {message}", err=True)
        self.failed = True
        return True


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

# Shared PyCypher-specific exception labels.
# Order matters: more specific (subclass) entries must come before their
# base classes so that _match_error_label() finds the best match first.
_PYCYPHER_ERROR_LABELS: list[tuple[type[BaseException], str]] = [
    (VariableNotFoundError, "variable error"),
    (VariableTypeMismatchError, "type mismatch"),
    (UnsupportedFunctionError, "unknown function"),
    (FunctionArgumentError, "argument error"),
    (GraphTypeNotFoundError, "unknown graph type"),
    (MissingParameterError, "missing parameter"),
    (InvalidCastError, "cast error"),
    (IncompatibleOperatorError, "operator error"),
    (WrongCypherTypeError, "type error"),
    (QueryTimeoutError, "timeout"),
    (QueryMemoryBudgetError, "memory budget exceeded"),
    # Parse/syntax exceptions (Lark + PyCypher wrapper)
    (CypherSyntaxError, "Cypher syntax error"),
    (UnexpectedInput, "Cypher syntax error"),
    (ParseError, "Cypher syntax error"),
    (LarkError, "Cypher syntax error"),
]

# Built-in exception fallback labels for pipeline query execution.
_PIPELINE_BUILTIN_LABELS: list[tuple[type[BaseException], str]] = [
    (SyntaxError, "invalid Cypher syntax"),
    (ValueError, "validation error"),
    (KeyError, "reference error"),
    (RuntimeError, "execution error"),
    (TypeError, "type error"),
    (AttributeError, "system error"),
    (ImportError, "system error"),
    (MemoryError, "system error"),
    (RecursionError, "system error"),
    (OSError, "system error"),
]

# Built-in exception fallback labels for ad-hoc query execution.
_ADHOC_BUILTIN_LABELS: list[tuple[type[BaseException], str]] = [
    (SyntaxError, "invalid Cypher syntax"),
    (ValueError, "semantic error in query"),
    (TypeError, "type error in query execution"),
    (KeyError, "undefined variable or property"),
    (RuntimeError, "query execution failed"),
    (AttributeError, "query execution failed"),
    (ImportError, "query execution failed"),
    (MemoryError, "query execution failed"),
]

# Composed label lists used by _match_error_label().
_QUERY_ERROR_LABELS: list[tuple[type[BaseException], str]] = (
    _PYCYPHER_ERROR_LABELS + _PIPELINE_BUILTIN_LABELS
)
_ADHOC_QUERY_LABELS: list[tuple[type[BaseException], str]] = (
    _PYCYPHER_ERROR_LABELS + _ADHOC_BUILTIN_LABELS
)


def _match_error_label(
    exc: BaseException,
    labels: list[tuple[type[BaseException], str]],
    default: str = "error",
) -> str:
    """Return a user-friendly label for an exception.

    Walks *labels* in order, returning the label for the first entry whose
    type matches *exc* via ``isinstance``.  This ensures PyCypher custom
    exceptions (which subclass ValueError/TypeError) get their specific
    label rather than the generic base-class label.

    Args:
        exc: The caught exception.
        labels: Ordered list of (exception type, label) pairs.
        default: Fallback label when no entry matches.

    Returns:
        A short, user-facing label string.

    """
    for exc_type, label in labels:
        if isinstance(exc, exc_type):
            return label
    return default


def _get_error_label(exc: BaseException) -> str:
    """Return a user-friendly label for a pipeline query execution exception."""
    return _match_error_label(exc, _QUERY_ERROR_LABELS, "error")


def _get_adhoc_error_label(exc: BaseException) -> str:
    """Return a user-friendly label for an ad-hoc query execution exception."""
    return _match_error_label(
        exc, _ADHOC_QUERY_LABELS, "query execution failed"
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

# Exception types caught during ad-hoc query execution (query sub-command)
_ADHOC_QUERY_ERRORS: tuple[type[BaseException], ...] = (
    CypherSyntaxError,
    SyntaxError,
    ValueError,
    TypeError,
    KeyError,
    RuntimeError,
    LarkError,
    UnexpectedInput,
    ParseError,
    AttributeError,
    ImportError,
    MemoryError,
)


# ---------------------------------------------------------------------------
# Argument-parsing helpers for `nmetl query`
# ---------------------------------------------------------------------------


def _eager_validate_uri(path: str) -> None:
    """Validate a CLI-supplied URI at parse time to block SSRF early.

    Bare filesystem paths (no scheme, or a single-letter scheme like ``C:``
    on Windows) are allowed through without scheme validation since they
    cannot trigger network requests.

    Args:
        path: The path or URI string extracted from a CLI argument.

    Raises:
        SecurityError: If the URI has a dangerous scheme or targets a
            private/internal network.

    """
    from urllib.parse import urlparse

    from pycypher.ingestion.security import validate_uri_scheme

    parsed = urlparse(path)
    # Skip scheme validation for bare filesystem paths:
    # - Empty scheme (relative or absolute POSIX paths)
    # - Single-letter scheme (Windows drive letters like C:/)
    if parsed.scheme and len(parsed.scheme) > 1:
        validate_uri_scheme(path)


def _parse_entity_arg(spec: str) -> tuple[str, str, str | None]:
    """Parse an ``--entity`` spec of the form ``Label=path[[:id_col]]``.

    Returns:
        (label, path, id_col) where *id_col* is ``None`` when not specified.

    Raises:
        ValueError: If the spec is malformed.

    """
    if "=" not in spec:
        msg = (
            f"Expected 'Label=path[:id_col]', got {spec!r}.  "
            "Separate the entity label from the file path with '='."
        )
        raise ValueError(
            msg,
        )
    label_raw, rest = spec.split("=", 1)
    label = label_raw.strip()
    if not label:
        msg = (
            f"label must not be empty in spec {spec!r}.  "
            "Use 'Label=path/to/file.csv'."
        )
        raise ValueError(
            msg,
        )

    # The rest may be  "path/to/file.csv"  or  "path/to/file.csv:id_col".
    # URIs with schemes (http://, s3://) are treated as whole paths —
    # the "://" disambiguates them from "path:id_col" syntax.
    rest = rest.strip()

    # Detect URI schemes: if rest matches "scheme://..." with a multi-char
    # scheme, treat the entire rest as a path (no id_col extraction).
    _has_uri_scheme = "://" in rest and rest.index("://") > 1

    if _has_uri_scheme:
        # URI — do not split on colons; the entire string is the path.
        path = rest
        id_col: str | None = None
    elif (last_colon := rest.rfind(":")) == -1:
        path = rest
        id_col = None
    else:
        candidate_path = rest[:last_colon].strip()
        candidate_id = rest[last_colon + 1 :].strip()
        # Heuristic: if the candidate_path is empty, treat the whole thing as path
        if not candidate_path:
            path = rest
            id_col = None
        else:
            path = candidate_path
            id_col = candidate_id or None

    path = path.strip()
    if not path:
        msg = (
            f"path must not be empty in spec {spec!r}.  "
            "Use 'Label=path/to/file.csv'."
        )
        raise ValueError(
            msg,
        )

    # Eagerly validate the URI scheme to block SSRF attempts (e.g.
    # http://169.254.169.254/) *before* any network request is made.
    # Skip validation for bare filesystem paths (no scheme, or
    # single-letter scheme which is a Windows drive letter like C:/).
    _eager_validate_uri(path)

    return label, path, id_col


def _parse_rel_arg(spec: str) -> tuple[str, str, str, str]:
    """Parse a ``--rel`` spec of the form ``REL=path:source_col:target_col``.

    Returns:
        (rel_type, path, source_col, target_col)

    Raises:
        ValueError: If the spec is malformed.

    """
    if "=" not in spec:
        msg = (
            f"Expected 'REL=path:source_col:target_col', got {spec!r}.  "
            "Separate the relationship type from the file path with '='."
        )
        raise ValueError(
            msg,
        )
    rel_raw, rest = spec.split("=", 1)
    rel_type = rel_raw.strip()
    if not rel_type:
        msg = (
            f"rel_type must not be empty in spec {spec!r}.  "
            "Use 'REL=path/to/file.csv:src_col:tgt_col'."
        )
        raise ValueError(
            msg,
        )

    # Split on the last two colons to get path, source_col, target_col.
    # We need exactly 2 colons after the path portion.
    rest = rest.strip()
    parts = rest.rsplit(":", 2)
    if len(parts) < 3:
        msg = (
            f"source_col and target_col are required in spec {spec!r}.  "
            "Use 'REL=path/to/file.csv:source_col:target_col'."
        )
        raise ValueError(
            msg,
        )
    path, src_col, tgt_col = (
        parts[0].strip(),
        parts[1].strip(),
        parts[2].strip(),
    )
    if not src_col or not tgt_col:
        msg = f"source_col and target_col must not be empty in spec {spec!r}."
        raise ValueError(
            msg,
        )

    # Eagerly validate the URI scheme to block SSRF attempts *before*
    # any network request is made.
    _eager_validate_uri(path)

    return rel_type, path, src_col, tgt_col


# ---------------------------------------------------------------------------
# Top-level CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(package_name="pycypher")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable INFO-level logging.",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable DEBUG-level logging (implies --verbose).",
)
def cli(*, verbose: bool, debug: bool) -> None:
    r"""Nmetl — Node-to-node Managed ETL pipeline runner.

    Run PyCypher-based ETL pipelines defined in a YAML configuration file.
    Data flows from one or more source files or databases through Cypher
    queries, with results written to configured output sinks.

    Typical workflow:

    \b
    1. Write a pipeline config file (YAML)
    2. nmetl validate pipeline.yaml   — check for errors
    3. nmetl run pipeline.yaml        — execute the pipeline

    For ad-hoc queries without a config file:

    \b
    nmetl query 'MATCH (p:Person) RETURN p.name' --entity Person=people.csv

    Explore available Cypher functions and syntax:

    \b
    nmetl functions -v              — list all functions with descriptions
    nmetl parse --validate "..."    — check a query for errors
    """
    import logging

    from shared.logger import LOGGER

    if debug:
        LOGGER.setLevel(logging.DEBUG)
    elif verbose:
        LOGGER.setLevel(logging.INFO)


def _dry_run_validate(
    cfg: Any,
    queries: list[Any],
    config_path: Path,
    *,
    verbose: bool = False,
) -> None:
    """Backward-compatible wrapper — delegates to :mod:`pycypher.cli.pipeline`."""
    from pycypher.cli.pipeline import dry_run_validate

    dry_run_validate(cfg, queries, config_path, verbose=verbose)


# ---------------------------------------------------------------------------
# run sub-command
# ---------------------------------------------------------------------------


@cli.command()
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
    from pycypher.cli.pipeline import run_impl

    run_impl(config, dry_run, query_ids, output_dir, on_error, verbose)


# ---------------------------------------------------------------------------
# validate sub-command
# ---------------------------------------------------------------------------


@cli.command()
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
    from pycypher.cli.pipeline import validate_impl

    validate_impl(config, verbose)


# ---------------------------------------------------------------------------
# list-queries sub-command
# ---------------------------------------------------------------------------


@cli.command("list-queries")
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
    from pycypher.cli.pipeline import list_queries_impl

    list_queries_impl(config, deps=deps)


# ---------------------------------------------------------------------------
# functions sub-command
# ---------------------------------------------------------------------------


@cli.command("functions")
@click.option(
    "--agg",
    "show_agg",
    is_flag=True,
    default=False,
    help="Show aggregation functions only.",
)
@click.option(
    "--scalar",
    "show_scalar",
    is_flag=True,
    default=False,
    help="Show scalar functions only.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show descriptions and examples for each function.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output function list as JSON (for scripting and IDE integration).",
)
def functions(
    *,
    show_agg: bool,
    show_scalar: bool,
    verbose: bool,
    as_json: bool,
) -> None:
    r"""List all available Cypher functions.

    Shows registered scalar functions and aggregation functions that can
    be used in MATCH, WHERE, WITH, and RETURN clauses.

    \b
    Examples:
      nmetl functions              — list all functions
      nmetl functions --scalar     — scalar functions only
      nmetl functions --agg        — aggregation functions only
      nmetl functions -v           — include descriptions and examples
      nmetl functions --json       — machine-readable JSON output
    """
    from pycypher.cli.schema import functions_impl

    functions_impl(
        show_agg=show_agg,
        show_scalar=show_scalar,
        verbose=verbose,
        as_json=as_json,
    )


# ---------------------------------------------------------------------------
# parse sub-command
# ---------------------------------------------------------------------------


@cli.command("parse")
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
    r"""Parse a Cypher query and display its AST structure.

    Useful for debugging complex queries, inspecting how PyCypher interprets
    your Cypher syntax, and validating queries before execution.

    \b
    Examples:
      nmetl parse "MATCH (n:Person) RETURN n.name"
      nmetl parse --json "MATCH (n) WHERE n.age > 30 RETURN n"
      nmetl parse --validate "MATCH (n:Person) RETURN m"
    """
    from pycypher.cli.query import parse_impl

    parse_impl(cypher_query, as_json=as_json, run_validation=run_validation)


# ---------------------------------------------------------------------------
# query sub-command
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Query sub-command (extracted to _cli_query.py for maintainability)
# ---------------------------------------------------------------------------

from pycypher._cli_query import (
    register as _register_query_command,
)

_register_query_command(cli)


# ---------------------------------------------------------------------------
# schema sub-command
# ---------------------------------------------------------------------------


@cli.command("schema")
@click.option(
    "--entity",
    "entity_specs",
    multiple=True,
    metavar="LABEL=PATH[:ID_COL]",
    help=("Entity data source.  Same format as ``nmetl query --entity``."),
)
@click.option(
    "--rel",
    "rel_specs",
    multiple=True,
    metavar="TYPE=PATH:SRC_COL:TGT_COL",
    help=("Relationship data source.  Same format as ``nmetl query --rel``."),
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
    type=click.Choice(["table", "dot", "json"], case_sensitive=False),
    default="table",
    help="Output format: 'table' (default), 'dot' (Graphviz), or 'json'.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Write output to a file instead of stdout.",
)
def schema(
    entity_specs: tuple[str, ...],
    rel_specs: tuple[str, ...],
    default_id_col: str | None,
    fmt: str,
    output_path: Path | None,
) -> None:
    r"""Inspect the graph schema from data sources.

    Shows all entity types, relationship types, and their properties.
    Useful for understanding the shape of your data before writing queries.

    \b
    Examples:
      nmetl schema --entity Person=people.csv --rel KNOWS=knows.csv:from:to
      nmetl schema --entity Person=people.csv --format dot | dot -Tpng > schema.png
      nmetl schema --entity Person=people.csv --format json
    """
    from pycypher.cli.schema import schema_impl

    schema_impl(entity_specs, rel_specs, default_id_col, fmt, output_path)


# ---------------------------------------------------------------------------
# metrics sub-command
# ---------------------------------------------------------------------------


@cli.command("metrics")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output metrics as JSON for programmatic consumption.",
)
@click.option(
    "--diagnostic",
    is_flag=True,
    default=False,
    help="Show detailed diagnostic report with recommendations.",
)
def metrics(*, as_json: bool, diagnostic: bool) -> None:
    r"""Show current query execution metrics.

    Displays a snapshot of the in-process query metrics collector,
    including timing percentiles, error rates, cache statistics,
    and health status.

    \b
    Examples:
      nmetl metrics                — summary view
      nmetl metrics --diagnostic   — detailed report with recommendations
      nmetl metrics --json         — machine-readable output
    """
    from pycypher.cli.system import metrics_impl

    metrics_impl(as_json=as_json, diagnostic=diagnostic)


# ---------------------------------------------------------------------------
# config sub-command
# ---------------------------------------------------------------------------


@cli.command("config")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output configuration as JSON.",
)
def config_cmd(*, as_json: bool) -> None:
    r"""Show all configuration settings and their current values.

    Lists every environment variable that pycypher reads, its current
    value (or default), and a brief description.

    \b
    Examples:
      nmetl config              — table view
      nmetl config --json       — machine-readable output
      PYCYPHER_QUERY_TIMEOUT_S=30 nmetl config  — verify override
    """
    from pycypher.cli.system import config_impl

    config_impl(as_json=as_json)


# ---------------------------------------------------------------------------
# health sub-command
# ---------------------------------------------------------------------------


@cli.command("health")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output health report as JSON.",
)
@click.option(
    "--verbose",
    "-v",
    "verbose_health",
    is_flag=True,
    default=False,
    help="Include system resource details.",
)
def health(*, as_json: bool, verbose_health: bool) -> None:
    r"""Run health checks and report operational status.

    Combines metrics health classification with system resource checks
    (memory, CPU) into a single operational health report.  Exit code 0
    means healthy, 1 means degraded, 2 means unhealthy.

    \b
    Examples:
      nmetl health            — quick health check
      nmetl health --json     — machine-readable health report
      nmetl health -v         — include system resource details
    """
    from pycypher.cli.system import health_impl

    health_impl(as_json=as_json, verbose_health=verbose_health)


# ---------------------------------------------------------------------------
# health-server sub-command
# ---------------------------------------------------------------------------


@cli.command("health-server")
@click.option(
    "--port",
    default=8079,
    type=int,
    help="Port to listen on (default: 8079).",
)
@click.option(
    "--bind",
    default="127.0.0.1",
    help="Address to bind to (default: 127.0.0.1).",
)
def health_server(*, port: int, bind: str) -> None:
    r"""Start a lightweight HTTP health check endpoint.

    Serves health status at /health and /ready for container orchestrators
    (Docker, Kubernetes).  Uses only stdlib http.server — no external
    dependencies.

    \b
    Endpoints:
      GET /health   — liveness probe (returns 200/503)
      GET /ready    — readiness probe (returns 200/503)
      GET /metrics  — Prometheus text format (if queries recorded)

    \b
    Examples:
      nmetl health-server                — start on 127.0.0.1:8079
      nmetl health-server --port 9090    — custom port
      nmetl health-server --bind 0.0.0.0 — listen on all interfaces
    """
    from pycypher.cli.system import health_server_impl

    health_server_impl(port=port, bind=bind)


# ---------------------------------------------------------------------------
# format-query sub-command
# ---------------------------------------------------------------------------


@cli.command("format-query")
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
def format_query_cmd(
    query_text: str,
    *,
    check: bool,
    lint: bool,
) -> None:
    r"""Format a Cypher query with consistent style.

    Uppercases keywords, places clauses on separate lines, and normalizes
    whitespace.  Use --check in CI to verify formatting without changes.

    \b
    Examples:
      nmetl format-query "match (n:Person) where n.age > 30 return n.name"
      nmetl format-query --check "MATCH (n) RETURN n"
      nmetl format-query --lint "match (n) return n"
    """
    from pycypher.cli.query import format_query_impl

    format_query_impl(query_text, check=check, lint=lint)


# ---------------------------------------------------------------------------
# repl sub-command
# ---------------------------------------------------------------------------


@cli.command("repl")
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
    from pycypher.cli.interactive import repl_impl

    repl_impl(entity_specs, rel_specs, default_id_col)


# ---------------------------------------------------------------------------
# compat-check sub-command
# ---------------------------------------------------------------------------


@cli.command("compat-check")
@click.option(
    "--snapshot",
    type=click.Path(path_type=Path),
    default=None,
    help="Save API surface snapshot to a JSON file.",
)
@click.option(
    "--diff",
    "diff_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Compare current API against a saved snapshot and report changes.",
)
@click.option(
    "--neo4j",
    "neo4j_feature",
    type=str,
    default=None,
    help="Check PyCypher compatibility for a Neo4j Cypher feature.",
)
@click.option(
    "--neo4j-all",
    is_flag=True,
    default=False,
    help="List all Neo4j Cypher compatibility notes.",
)
def compat_check(
    *,
    snapshot: Path | None,
    diff_path: Path | None,
    neo4j_feature: str | None,
    neo4j_all: bool,
) -> None:
    r"""Check API compatibility and migration status.

    Capture the current PyCypher public API surface as a snapshot,
    compare against a previous snapshot to detect breaking changes,
    or check Neo4j Cypher feature compatibility.

    \b
    Examples:
      nmetl compat-check --snapshot api_v0.0.19.json
      nmetl compat-check --diff api_v0.0.18.json
      nmetl compat-check --neo4j "LOAD CSV"
      nmetl compat-check --neo4j-all
    """
    from pycypher.cli.utility import compat_check_impl

    compat_check_impl(
        snapshot=snapshot,
        diff_path=diff_path,
        neo4j_feature=neo4j_feature,
        neo4j_all=neo4j_all,
    )


# ---------------------------------------------------------------------------
# TUI subcommand
# ---------------------------------------------------------------------------


@cli.command("tui")
@click.argument("config", required=False, default=None, type=click.Path())
@click.option("--new", is_flag=True, help="Create a new pipeline config file.")
@click.option("--template", default=None, help="Template for --new creation.")
@click.option(
    "--list-templates", is_flag=True, help="List available templates and exit."
)
def tui_command(
    config: str | None,
    new: bool,
    template: str | None,
    list_templates: bool,
) -> None:
    """Launch the VIM-style TUI for pipeline configuration.

    Open an existing pipeline config for editing, or create a new one
    using --new. Templates provide pre-built configurations for common
    use cases.

    \b
    Examples:
        nmetl tui pipeline.yaml           # Edit existing config
        nmetl tui --new my_pipeline.yaml  # Create new config
        nmetl tui --template csv_analytics --new pipeline.yaml
        nmetl tui --list-templates        # Show templates
    """
    try:
        from pycypher_tui.cli import run_tui
    except ImportError:
        click.echo(
            "Error: pycypher-tui package is not installed.\n"
            "Install it with: pip install pycypher-tui",
            err=True,
        )
        raise SystemExit(1)

    if template and not new:
        raise click.UsageError("--template requires --new")
    if new and not config:
        raise click.UsageError("--new requires a config file path")

    run_tui(
        config_path=config,
        new=new,
        template=template,
        list_templates_flag=list_templates,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the nmetl command-line tool."""
    cli()


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# Modular CLI Alternative
# ---------------------------------------------------------------------------
#
# A modular version of this CLI is available at:
#   pycypher.cli.main
#
# This provides the same functionality with improved maintainability:
# - Common utilities extracted to pycypher.cli.common
# - Pipeline commands in pycypher.cli.pipeline
# - Query commands in pycypher.cli.query
# - Schema commands in pycypher.cli.schema
# - System commands in pycypher.cli.system
# - Interactive commands in pycypher.cli.interactive
# - Utility commands in pycypher.cli.utility
#
# Run with: python -m pycypher.cli.main
# ---------------------------------------------------------------------------
