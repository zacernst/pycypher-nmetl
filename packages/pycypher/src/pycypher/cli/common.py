"""Common utilities and error handling for CLI commands."""

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


def cli_error(message: str, *, exit_code: int = 1) -> NoReturn:
    """Print an error message to stderr and exit."""
    click.echo(f"Error: {message}", err=True)
    sys.exit(exit_code)


def format_validation_errors(
    exc: ValidationError, *, verbose: bool = False
) -> str:
    """Format a Pydantic ValidationError into concise, actionable lines.

    Instead of dumping the full Pydantic error repr (often 50+ lines),
    extracts field-level errors and formats each as a one-liner with
    the field path and a human-readable message.

    Args:
        exc: The Pydantic validation error.
        verbose: If True, show all errors instead of truncating to 5.

    Returns:
        A compact multi-line error summary suitable for CLI output.

    """
    errors = exc.errors()
    max_shown = len(errors) if verbose else 5
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
        lines.append(
            "  Run 'nmetl validate <config> --verbose' for full details."
        )
    return "\n".join(lines)


def translate_duckdb_error(exc: BaseException, kind: str, path: str) -> str:
    """Translate a DuckDB or generic data-loading exception into a user message.

    Args:
        exc: The caught exception.
        kind: Source kind (e.g. ``"entity source"``).
        path: File path string (credentials are masked before display).

    Returns:
        A user-friendly error message string with credentials masked.

    """
    from pycypher.ingestion.security import mask_uri_credentials

    safe_path = mask_uri_credentials(path)
    exc_str = str(exc)
    lower = exc_str.lower()
    if "No files found that match the pattern" in exc_str:
        return f"{kind} file not found: {safe_path!r}"
    if "permission" in lower or "denied" in lower:
        return f"permission denied accessing {kind}: {safe_path!r}"
    if "encoding" in lower or "codec" in lower or "decode" in lower:
        return (
            f"encoding error reading {kind} {safe_path!r}: {exc_str}\n"
            "  Try saving the file as UTF-8."
        )
    if "out of memory" in lower or "memory" in lower:
        return (
            f"insufficient memory loading {kind} {safe_path!r}\n"
            "  Try reducing file size or increasing available memory."
        )
    if "empty" in lower and "file" in lower:
        return f"{kind} file is empty: {safe_path!r}"
    from pycypher.exceptions import sanitize_error_message

    safe_msg = sanitize_error_message(exc)
    return (
        f"could not load {kind} {safe_path!r}: {safe_msg}\n"
        "  Check that the file exists, is readable, and is valid CSV or Parquet."
    )


def load_data_source(
    action: Any,
    kind: str,
    path: str,
) -> None:
    """Call *action* and translate any data-loading exception into a CLI error.

    This centralises the exception handling for ``builder.add_entity`` /
    ``builder.add_relationship`` in the ``query`` sub-command.

    Args:
        action: A zero-argument callable that performs the data load.
        kind: Source kind (``"entity source"`` or ``"relationship source"``).
        path: File path to log on failure.

    Raises:
        SystemExit: On validation or data-loading failure.

    """
    from pycypher.ingestion.security import mask_uri_credentials

    safe_path = mask_uri_credentials(path)
    try:
        action()
    except ValidationError as exc:
        cli_error(format_validation_errors(exc), exit_code=2)
    except Exception as exc:  # noqa: BLE001 — CLI top-level: format any error for user display
        if DuckDBIOException is not None and isinstance(
            exc, DuckDBIOException
        ):
            cli_error(translate_duckdb_error(exc, kind, safe_path), exit_code=1)
        # Generic fallback for unexpected errors
        exc_name = type(exc).__name__
        cli_error(
            f"unexpected {exc_name} loading {kind} {safe_path!r}: {exc}",
            exit_code=1,
        )


def load_config(config: Path, *, verbose: bool = False) -> Any:
    """Load and validate a pipeline configuration file.

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
        cli_error(f"config file not found: {config}", exit_code=1)
    except yaml.YAMLError as exc:
        cli_error(f"invalid YAML syntax in config: {exc}", exit_code=2)
    except ValidationError as exc:
        cli_error(format_validation_errors(exc, verbose=verbose), exit_code=2)
    except (PermissionError, OSError) as exc:
        cli_error(f"could not read config file: {exc}", exit_code=1)


class ErrorPolicyTracker:
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
QUERY_EXEC_ERRORS: tuple[type[BaseException], ...] = (
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
PYCYPHER_ERROR_LABELS: list[tuple[type[BaseException], str]] = [
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
PIPELINE_BUILTIN_LABELS: list[tuple[type[BaseException], str]] = [
    (SyntaxError, "invalid Cypher syntax"),
    (ValueError, "validation error"),
    (KeyError, "reference error"),
    (RuntimeError, "execution error"),
    (TypeError, "type error"),
    (AttributeError, "system error"),
    (ImportError, "system error"),
    (MemoryError, "system error"),
    (RecursionError, "system error"),
    (OSError, "file system error"),
]

# Built-in exception fallback labels for ad-hoc query execution.
ADHOC_BUILTIN_LABELS: list[tuple[type[BaseException], str]] = [
    (SyntaxError, "invalid Cypher syntax"),
    (ValueError, "invalid query"),
    (KeyError, "variable not found"),
    (RuntimeError, "query failed"),
    (TypeError, "type error"),
    (AttributeError, "system error"),
    (ImportError, "system error"),
    (MemoryError, "out of memory"),
    (RecursionError, "query too complex"),
    (OSError, "data source error"),
]


def match_error_label(
    exc: BaseException,
    builtin_labels: list[tuple[type[BaseException], str]],
) -> str:
    """Find a user-friendly error label for an exception.

    Args:
        exc: The exception to classify.
        builtin_labels: Fallback labels for built-in exception types.

    Returns:
        A short error category string.

    """
    # Try PyCypher-specific errors first
    for exc_type, label in PYCYPHER_ERROR_LABELS:
        if isinstance(exc, exc_type):
            return label

    # Fall back to context-specific built-in labels
    for exc_type, label in builtin_labels:
        if isinstance(exc, exc_type):
            return label

    # Last resort
    return "system error"


def get_pipeline_error_label(exc: BaseException) -> str:
    """Get error label for pipeline query execution."""
    return match_error_label(exc, PIPELINE_BUILTIN_LABELS)


def get_adhoc_error_label(exc: BaseException) -> str:
    """Get error label for ad-hoc query execution."""
    return match_error_label(exc, ADHOC_BUILTIN_LABELS)
