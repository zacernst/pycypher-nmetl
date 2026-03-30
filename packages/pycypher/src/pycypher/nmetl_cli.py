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

import os
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
from pycypher.ingestion.security import mask_uri_credentials

# Import DuckDB exceptions for proper error handling
try:
    import duckdb
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


def _format_validation_errors(exc: ValidationError) -> str:
    """Format a Pydantic ValidationError into concise, actionable lines.

    Instead of dumping the full Pydantic error repr (often 50+ lines),
    extracts the first few field-level errors and formats each as a
    one-liner with the field path and a human-readable message.

    Args:
        exc: The Pydantic validation error.

    Returns:
        A compact multi-line error summary suitable for CLI output.

    """
    errors = exc.errors()
    max_shown = 5
    lines: list[str] = ["invalid config structure:"]
    for err in errors[:max_shown]:
        # Build dotted field path from Pydantic's loc tuple
        loc_parts = [str(p) for p in err.get("loc", ())]
        field = ".".join(loc_parts) if loc_parts else "(root)"
        msg = err.get("msg", "validation error")
        lines.append(f"  - {field}: {msg}")
    remaining = len(errors) - max_shown
    if remaining > 0:
        lines.append(f"  ... and {remaining} more error(s)")
    lines.append("  Run 'nmetl validate <config> --verbose' for full details.")
    return "\n".join(lines)


def _translate_duckdb_error(exc: BaseException, kind: str, path: str) -> str:
    """Translate a DuckDB or generic data-loading exception into a user message.

    Args:
        exc: The caught exception.
        kind: Source kind (e.g. ``"entity source"``).
        path: File path string.

    Returns:
        A user-friendly error message string.

    """
    exc_str = str(exc)
    lower = exc_str.lower()
    if "No files found that match the pattern" in exc_str:
        return f"{kind} file not found: {path!r}"
    if "permission" in lower or "denied" in lower:
        return f"permission denied accessing {kind}: {path!r}"
    if "encoding" in lower or "codec" in lower or "decode" in lower:
        return (
            f"encoding error reading {kind} {path!r}: {exc_str}\n"
            "  Try saving the file as UTF-8."
        )
    if "out of memory" in lower or "memory" in lower:
        return (
            f"insufficient memory loading {kind} {path!r}\n"
            "  Try reducing file size or increasing available memory."
        )
    if "empty" in lower and "file" in lower:
        return f"{kind} file is empty: {path!r}"
    return (
        f"could not load {kind} {path!r}: {exc_str}\n"
        "  Check that the file exists, is readable, and is valid CSV or Parquet."
    )


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
    desc = f"{kind} {path!r}"
    try:
        action()
    except FileNotFoundError:
        _cli_error(f"{kind} file not found: {path!r}")
    except PermissionError:
        _cli_error(f"permission denied accessing {kind}: {path!r}")
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
        _cli_error(_translate_duckdb_error(exc, kind, path))
    except Exception as exc:
        if DuckDBIOException and isinstance(exc, DuckDBIOException):
            _cli_error(_translate_duckdb_error(exc, kind, path))
        if "No files found that match the pattern" in str(exc):
            _cli_error(f"{kind} file not found: {path!r}")
        # Re-raise unexpected exceptions for debugging
        raise


def _load_config(config: Path) -> Any:
    """Load and validate a pipeline config, translating errors to CLI exits.

    Args:
        config: Path to the YAML config file.

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
        _cli_error(_format_validation_errors(exc), exit_code=2)
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


# ---------------------------------------------------------------------------
# Dry-run pre-flight validation
# ---------------------------------------------------------------------------


def _dry_run_validate(
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
    from pycypher.ingestion.security import mask_uri_credentials

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
        # Skip SQL and remote URIs — can't validate connectivity without loading.
        from urllib.parse import urlparse

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

        # Resolve local file path.
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

            # Attempt to parse the Cypher query.
            from pycypher.grammar_parser import GrammarParser

            parser = GrammarParser()
            parser.parse(query_text)
            desc = f"  {q.description}" if q.description else ""
            src_label = f"file:{q.source}" if q.source else "inline"
            click.echo(f"    [{q.id}] ({src_label}){desc}  OK")

        except Exception as exc:
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
            # Check that query_id references a defined query.
            if out.query_id not in query_ids_defined:
                msg = f"Output references unknown query_id: {out.query_id!r}"
                errors.append(msg)
                click.echo(
                    f"    query:{out.query_id} → {out.uri}  INVALID QUERY REF"
                )
                continue

            # Check output directory is writable for local paths.
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
    import re

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
    if verbose:
        click.echo(f"Loading config: {config}")

    pipeline_config = _load_config(config)

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
        _dry_run_validate(pipeline_config, queries, config, verbose=verbose)
        return

    # -----------------------------------------------------------------------
    # Build execution context from config sources
    # -----------------------------------------------------------------------
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
        for j, rel_src in enumerate(pipeline_config.sources.relationships, 1):
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
        _cli_error(f"data source file not found: {exc}")
    except PermissionError as exc:
        _cli_error(f"permission denied accessing data source: {exc}")
    except ValueError as exc:
        _cli_error(f"invalid data source format or configuration: {exc}")
    except OSError as exc:
        _cli_error(f"file system error loading data sources: {exc}")

    star = Star(context=context)
    config_dir = config.parent

    # -----------------------------------------------------------------------
    # Execute each query and write results to output sinks
    # -----------------------------------------------------------------------
    tracker = _ErrorPolicyTracker((on_error or "fail").lower())

    n_queries = len(queries)
    click.echo(f"Executing {n_queries} query/queries …")

    for qi, q in enumerate(queries, 1):
        click.echo(f"  [{qi}/{n_queries}] query [{q.id}] …")

        # Resolve query text (inline string or external .cypher file)
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
                    raise ValueError(
                        msg,
                    ) from None
                query_text = query_path.read_text(encoding="utf-8")
            else:
                msg = f"Query {q.id!r} has neither 'inline' nor 'source'."
                raise ValueError(
                    msg,
                )
        except (
            ValueError,
            FileNotFoundError,
            PermissionError,
            UnicodeDecodeError,
            OSError,
        ) as exc:
            if tracker.handle(f"could not load query {q.id!r}: {exc}"):
                continue

        # Execute the query
        try:
            result_df = star.execute_query(query_text)
        except _QUERY_EXEC_ERRORS as exc:
            label = _get_error_label(exc)
            if tracker.handle(f"query [{q.id}] {label}: {exc}"):
                continue

        # Write to all configured output sinks for this query
        sinks = [o for o in pipeline_config.output if o.query_id == q.id]
        for si, sink in enumerate(sinks, 1):
            try:
                write_dataframe_to_uri(result_df, sink.uri, sink.format)
                n_rows = len(result_df) if result_df is not None else 0
                click.echo(
                    f"    output [{si}/{len(sinks)}]"
                    f" {n_rows} row(s)"
                    f" -> {mask_uri_credentials(sink.uri)}",
                )
            except _OUTPUT_ERRORS as exc:
                label = _OUTPUT_ERROR_LABELS.get(type(exc), "error writing to")
                tracker.handle(
                    f"query [{q.id}] {label} {mask_uri_credentials(sink.uri)!r}: {exc}",
                )

    if tracker.failed and tracker.policy == "warn":
        click.echo(
            "Pipeline completed with warnings.  "
            "One or more queries failed (see above).",
            err=True,
        )

    click.echo("Done.")


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
    cfg = _load_config(config)

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
    cfg = _load_config(config)

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
    except Exception as exc:
        # CyclicDependencyError, ValueError from analysis, or parse
        # errors from query re-parsing.  Log at WARNING for visibility
        # in log output beyond the CLI echo.
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
    import json

    from pycypher.aggregation_evaluator import KNOWN_AGGREGATIONS
    from pycypher.scalar_functions import ScalarFunctionRegistry

    # Default: show both when neither flag is set
    if not show_agg and not show_scalar:
        show_agg = True
        show_scalar = True

    registry = ScalarFunctionRegistry.get_instance()

    if as_json:
        result: dict[str, list[dict[str, str]]] = {}
        if show_scalar:
            scalar_list: list[dict[str, str]] = []
            for name in registry.list_functions():
                entry: dict[str, str] = {
                    "name": name,
                    "category": "scalar",
                }
                meta = registry._functions.get(name)
                if meta and meta.description:
                    entry["description"] = meta.description
                if meta and meta.example:
                    entry["example"] = meta.example
                scalar_list.append(entry)
            result["scalar"] = scalar_list
        if show_agg:
            result["aggregation"] = [
                {"name": name, "category": "aggregation"}
                for name in sorted(KNOWN_AGGREGATIONS)
            ]
        click.echo(json.dumps(result, indent=2))
        return

    if show_scalar:
        func_names = registry.list_functions()
        click.echo(f"Scalar functions ({len(func_names)}):")
        for name in func_names:
            meta = registry._functions.get(name)
            if verbose and meta and meta.description:
                click.echo(f"  {name:30s} {meta.description}")
                if meta.example:
                    click.echo(f"  {'':30s} Example: {meta.example}")
            else:
                click.echo(f"  {name}")

    if show_agg:
        if show_scalar:
            click.echo()
        agg_names = sorted(KNOWN_AGGREGATIONS)
        click.echo(f"Aggregation functions ({len(agg_names)}):")
        for name in agg_names:
            click.echo(f"  {name}")


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
    import json as json_mod

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


def _context_to_schema_dot(
    entity_types: dict[str, list[str]],
    rel_types: dict[str, tuple[str, str, list[str]]],
) -> str:
    """Render a graph schema as a Graphviz DOT digraph.

    Args:
        entity_types: Maps entity label → list of property names.
        rel_types: Maps relationship label → (source_entity, target_entity, properties).

    Returns:
        DOT source string.

    """
    lines = [
        "digraph Schema {",
        "  rankdir=LR;",
        '  node [shape=record, style=filled, fillcolor="#d4edda"];',
        '  edge [fontsize=10, color="#666666"];',
    ]
    for label, props in sorted(entity_types.items()):
        prop_str = "\\l".join(props) + "\\l" if props else ""
        lines.append(f'  "{label}" [label="{{{label}|{prop_str}}}"];')
    for rel_label, (src, tgt, props) in sorted(rel_types.items()):
        label_parts = [rel_label]
        if props:
            label_parts.append("\\n" + ", ".join(props))
        lines.append(
            f'  "{src}" -> "{tgt}" [label="{" ".join(label_parts)}"];',
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


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
    from pycypher.ingestion.context_builder import ContextBuilder
    from pycypher.relational_models import ID_COLUMN

    if not entity_specs and not rel_specs:
        _cli_error("at least one --entity source is required.")

    # Parse specs (reuse existing parsers)
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

    # Build context
    builder = ContextBuilder()
    for label, path, id_col in parsed_entities:
        effective_id = id_col or default_id_col
        _load_data_source(
            lambda _l=label, _p=path, _i=effective_id: builder.add_entity(
                _l,
                _p,
                id_col=_i,
            ),
            "entity source",
            path,
        )

    for rel_type, path, src_col, tgt_col in parsed_rels:
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

    ctx = builder.build()

    # Extract schema info
    entity_types: dict[str, list[str]] = {}
    for label, table in sorted(ctx.entity_mapping.mapping.items()):
        props = [
            c for c in table.attribute_map if c not in {ID_COLUMN, "__ID__"}
        ]
        entity_types[label] = sorted(props)

    rel_types: dict[str, tuple[str, str, list[str]]] = {}
    for rel_label, table in sorted(ctx.relationship_mapping.mapping.items()):
        # Determine source/target entity types from data
        src_entity = "?"
        tgt_entity = "?"
        props = [
            c
            for c in table.attribute_map
            if c not in {ID_COLUMN, "__ID__", "__SOURCE__", "__TARGET__"}
        ]
        rel_types[rel_label] = (src_entity, tgt_entity, sorted(props))

    # Output
    if fmt == "dot":
        text = _context_to_schema_dot(entity_types, rel_types)
        if output_path is not None:
            output_path.write_text(text, encoding="utf-8")
            click.echo(f"Wrote schema DOT → {output_path}")
        else:
            click.echo(text, nl=False)
    elif fmt == "json":
        import json

        schema_dict = {
            "entities": {
                label: {"properties": props}
                for label, props in entity_types.items()
            },
            "relationships": {
                label: {"source": src, "target": tgt, "properties": props}
                for label, (src, tgt, props) in rel_types.items()
            },
        }
        text = json.dumps(schema_dict, indent=2) + "\n"
        if output_path is not None:
            output_path.write_text(text, encoding="utf-8")
            click.echo(f"Wrote schema JSON → {output_path}")
        else:
            click.echo(text, nl=False)
    else:  # table
        click.echo("Entity Types:")
        click.echo("-" * 50)
        for label, props in sorted(entity_types.items()):
            click.echo(f"  :{label}")
            for p in props:
                click.echo(f"    .{p}")
        if rel_types:
            click.echo("\nRelationship Types:")
            click.echo("-" * 50)
            for rel_label, (src, tgt, props) in sorted(rel_types.items()):
                arrow = f"(:{src})-[:{rel_label}]->(:{tgt})"
                click.echo(f"  {arrow}")
                for p in props:
                    click.echo(f"    .{p}")
        click.echo(
            f"\n{len(entity_types)} entity type(s), "
            f"{len(rel_types)} relationship type(s)",
        )


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
    import json

    from shared.metrics import QUERY_METRICS

    snap = QUERY_METRICS.snapshot()

    if as_json:
        click.echo(json.dumps(snap.to_dict(), indent=2, default=str))
    elif diagnostic:
        click.echo(snap.diagnostic_report())
    else:
        click.echo(f"Health: {snap.health_status()}")
        click.echo(snap.summary())


# ---------------------------------------------------------------------------
# config sub-command
# ---------------------------------------------------------------------------


# Registry of config knobs: (env_var, description, default_display)
_CONFIG_REGISTRY: list[tuple[str, str, str]] = [
    # --- Query execution ---
    ("PYCYPHER_QUERY_TIMEOUT_S", "Query timeout (seconds)", "None (no limit)"),
    ("PYCYPHER_MAX_CROSS_JOIN_ROWS", "Cross-join row ceiling", "1,000,000"),
    ("PYCYPHER_MAX_UNBOUNDED_PATH_HOPS", "Max BFS hops for [*] paths", "20"),
    ("PYCYPHER_MAX_COMPLEXITY_SCORE", "Complexity gate (0=disabled)", "0 (disabled)"),
    ("PYCYPHER_COMPLEXITY_WARN_THRESHOLD", "Complexity warning threshold", "0 (disabled)"),
    ("PYCYPHER_RATE_LIMIT_QPS", "Max queries/sec (0=disabled)", "0 (disabled)"),
    ("PYCYPHER_RATE_LIMIT_BURST", "Rate limit burst size", "10"),
    # --- Caching ---
    ("PYCYPHER_RESULT_CACHE_MAX_MB", "Result cache size (MB)", "100"),
    ("PYCYPHER_RESULT_CACHE_TTL_S", "Cache TTL (seconds, 0=no expiry)", "0"),
    ("PYCYPHER_AST_CACHE_MAX", "Parsed AST cache size (LRU)", "1024"),
    # --- Security limits ---
    ("PYCYPHER_MAX_QUERY_SIZE_BYTES", "Max query size (bytes)", "1,048,576"),
    ("PYCYPHER_MAX_QUERY_NESTING_DEPTH", "Max AST nesting depth", "200"),
    ("PYCYPHER_MAX_COLLECTION_SIZE", "Max collection/string size", "1,000,000"),
    # --- Logging and observability ---
    ("PYCYPHER_LOG_LEVEL", "Log level (DEBUG/INFO/WARNING/ERROR)", "WARNING"),
    ("PYCYPHER_LOG_FORMAT", "Log format (rich or json)", "rich"),
    ("PYCYPHER_AUDIT_LOG", "Audit logging (1/true/yes to enable)", "disabled"),
    ("PYCYPHER_METRICS_ENABLED", "In-process metrics (0/false to disable)", "1 (enabled)"),
    ("PYCYPHER_SLOW_QUERY_MS", "Slow query threshold (ms)", "1000"),
    ("PYCYPHER_OTEL_ENABLED", "OpenTelemetry tracing (1/true/yes)", "0 (disabled)"),
    # --- REPL ---
    ("PYCYPHER_REPL_MAX_ROWS", "REPL max displayed rows", "50"),
]


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
    import json
    import os

    entries = []
    for env_var, description, default_display in _CONFIG_REGISTRY:
        raw = os.environ.get(env_var)
        entries.append(
            {
                "variable": env_var,
                "value": raw if raw is not None else default_display,
                "source": "env" if raw is not None else "default",
                "description": description,
            },
        )

    if as_json:
        click.echo(json.dumps(entries, indent=2))
    else:
        click.echo("\nPyCypher Configuration\n")
        for entry in entries:
            marker = "*" if entry["source"] == "env" else " "
            click.echo(
                f"  {marker} {entry['variable']:<38} "
                f"{entry['value']:<18} {entry['description']}",
            )
        click.echo(
            "\n  * = set via environment variable\n"
            "  Set variables with: export PYCYPHER_<NAME>=<value>\n",
        )


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
    import json as json_mod

    from shared.metrics import QUERY_METRICS

    checks: dict[str, dict[str, Any]] = {}

    # 1. Metrics health
    snap = QUERY_METRICS.snapshot()
    metrics_status = snap.health_status()
    checks["metrics"] = {
        "status": metrics_status,
        "total_queries": snap.total_queries,
        "total_errors": snap.total_errors,
        "error_rate": round(snap.error_rate, 4),
    }

    # 2. System resources
    try:
        import resource as _resource

        rusage = _resource.getrusage(_resource.RUSAGE_SELF)
        mem_mb = rusage.ru_maxrss / (1024 * 1024)  # macOS returns bytes
        if sys.platform == "linux":
            mem_mb = rusage.ru_maxrss / 1024  # Linux returns KB

        # Heuristic: flag if RSS > 2GB
        mem_status = "healthy" if mem_mb < 2048 else "degraded"
        checks["memory"] = {
            "status": mem_status,
            "rss_mb": round(mem_mb, 1),
        }
    except Exception:
        from shared.logger import LOGGER as _logger

        _logger.debug("Memory health check failed", exc_info=True)
        checks["memory"] = {"status": "unknown", "rss_mb": None}

    # 3. Process uptime
    checks["uptime"] = {
        "status": "healthy",
        "uptime_s": round(snap.uptime_s, 1),
    }

    # 4. Cache efficiency (if queries have been run)
    if snap.total_queries > 0:
        total_cache_ops = snap.result_cache_hits + snap.result_cache_misses
        cache_hit_rate = (
            snap.result_cache_hits / total_cache_ops
            if total_cache_ops > 0
            else 0.0
        )
        checks["cache"] = {
            "status": "healthy",
            "hit_rate": round(cache_hit_rate, 4),
            "evictions": snap.result_cache_evictions,
        }

    # Overall status: worst of all checks
    status_order = {"healthy": 0, "degraded": 1, "unhealthy": 2, "unknown": 1}
    overall = max(
        (c.get("status", "unknown") for c in checks.values()),
        key=lambda s: status_order.get(s, 1),
    )

    report = {"status": overall, "checks": checks}

    if as_json:
        click.echo(json_mod.dumps(report, indent=2, default=str))
    else:
        status_icon = {"healthy": "+", "degraded": "~", "unhealthy": "!"}
        click.echo(
            f"[{status_icon.get(overall, '?')}] Overall: {overall.upper()}",
        )
        for name, check in checks.items():
            icon = status_icon.get(check.get("status", "?"), "?")
            detail_parts = [
                f"{k}={v}"
                for k, v in check.items()
                if k != "status" and (verbose_health or k not in {"rss_mb"})
            ]
            detail = f"  ({', '.join(detail_parts)})" if detail_parts else ""
            click.echo(
                f"  [{icon}] {name}: {check.get('status', 'unknown')}{detail}",
            )

    # Exit code reflects health status
    sys.exit(status_order.get(overall, 1))


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
    from pycypher.health_server import run_health_server

    click.echo(f"Starting health server on {bind}:{port}...")
    click.echo(f"  GET http://{bind}:{port}/health")
    click.echo(f"  GET http://{bind}:{port}/ready")
    click.echo(f"  GET http://{bind}:{port}/metrics")
    run_health_server(host=bind, port=port)


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
    from pycypher.repl import CypherRepl

    shell = CypherRepl(
        entity_specs=list(entity_specs),
        rel_specs=list(rel_specs),
        default_id_col=default_id_col,
    )
    shell.cmdloop()


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
    from shared.compat import (
        NEO4J_COMPAT_NOTES,
        check_neo4j_compat,
        diff_surfaces,
        load_snapshot,
        save_snapshot,
        snapshot_api_surface,
    )

    if snapshot is not None:
        surface = snapshot_api_surface("pycypher")
        save_snapshot(surface, snapshot)
        click.echo(
            f"Saved API snapshot: {len(surface.symbols)} symbols "
            f"(v{surface.version}) → {snapshot}",
        )
        return

    if diff_path is not None:
        old = load_snapshot(diff_path)
        current = snapshot_api_surface("pycypher")
        report = diff_surfaces(old, current)
        click.echo(report.summary())
        if report.has_breaking_changes:
            raise SystemExit(1)
        return

    if neo4j_feature is not None:
        result = check_neo4j_compat(neo4j_feature)
        if result is None:
            click.echo(f"No compatibility notes found for '{neo4j_feature}'.")
            raise SystemExit(1)
        status = "SUPPORTED" if result["supported"] else "NOT SUPPORTED"
        click.echo(f"{result['feature']}: {status}")
        click.echo(f"  {result['notes']}")
        if "workaround" in result:
            click.echo(f"  Workaround: {result['workaround']}")
        return

    if neo4j_all:
        for feature, info in NEO4J_COMPAT_NOTES.items():
            status = "+" if info["supported"] else "-"
            click.echo(f"  [{status}] {feature}")
            click.echo(f"      {info['notes']}")
            if "workaround" in info:
                click.echo(f"      Workaround: {info['workaround']}")
        return

    # Default: show current API surface summary
    surface = snapshot_api_surface("pycypher")
    click.echo(
        f"PyCypher v{surface.version} — {len(surface.symbols)} public symbols",
    )
    by_kind: dict[str, list[str]] = {}
    for sym in surface.symbols.values():
        by_kind.setdefault(sym.kind, []).append(sym.name)
    for kind in sorted(by_kind):
        names = sorted(by_kind[kind])
        click.echo(f"\n  {kind}s ({len(names)}):")
        for name in names:
            click.echo(f"    {name}")


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
