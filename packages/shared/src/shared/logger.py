"""Shared logging configuration module.

This module provides a centralized logger configuration using Rich for
enhanced console output formatting. The logger level defaults to WARNING
but can be overridden via the ``PYCYPHER_LOG_LEVEL`` environment variable
(e.g. ``PYCYPHER_LOG_LEVEL=DEBUG``).

Set ``PYCYPHER_LOG_FORMAT=json`` for machine-readable JSON-lines output
suitable for log aggregation pipelines (ELK, Datadog, Splunk).

Query correlation
-----------------

Use :func:`set_query_id` / :func:`get_query_id` to propagate a query
correlation ID through the call stack via :mod:`contextvars`.  The JSON
formatter automatically includes the active ``query_id`` in every log
line, eliminating the need to pass ``extra={"query_id": ...}`` manually.

::

    from shared.logger import set_query_id, reset_query_id

    token = set_query_id("abc123")
    try:
        LOGGER.info("processing")  # JSON output includes "query_id": "abc123"
    finally:
        reset_query_id(token)

Attributes:
    LOGGING_LEVEL: Effective logging level string (from env or default).
    LOGGER: Configured logger instance.

"""

import contextvars
import json
import logging
import os
import sys
import threading
from datetime import UTC, datetime

# ---------------------------------------------------------------------------
# Query-ID context variable for automatic log correlation
# ---------------------------------------------------------------------------

_query_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "query_id",
    default=None,
)


def set_query_id(query_id: str) -> contextvars.Token[str | None]:
    """Set the active query correlation ID for the current context.

    Returns a token that can be passed to :func:`reset_query_id` to
    restore the previous value.

    Args:
        query_id: The correlation ID to propagate through log output.

    Returns:
        A :class:`contextvars.Token` for restoring the previous value.

    """
    return _query_id_var.set(query_id)


def get_query_id() -> str | None:
    """Return the active query correlation ID, or ``None`` if unset."""
    return _query_id_var.get()


def reset_query_id(token: contextvars.Token[str | None]) -> None:
    """Restore the query ID to its previous value.

    Args:
        token: The token returned by :func:`set_query_id`.

    """
    _query_id_var.reset(token)


# ---------------------------------------------------------------------------
# Filter that injects the contextvar query_id onto every LogRecord
# ---------------------------------------------------------------------------


class _QueryIdFilter(logging.Filter):
    """Inject the active ``query_id`` contextvar into every log record.

    This ensures *all* handlers (Rich, caplog, third-party) can see
    ``record.query_id`` without requiring ``extra={"query_id": ...}``
    on every log call.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "query_id"):
            record.query_id = _query_id_var.get()
        return True


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class _JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a single JSON line with query_id correlation.

        The ``query_id`` is resolved in order:
        1. Explicit ``extra={"query_id": ...}`` on the log call (backwards compat)
        2. The active :func:`set_query_id` contextvar value

        """
        entry = {
            "ts": datetime.fromtimestamp(
                record.created,
                tz=UTC,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        # query_id is injected by _QueryIdFilter; fall back to contextvar
        # for records that bypass the filter (e.g. different logger).
        query_id = getattr(record, "query_id", None) or _query_id_var.get()
        if query_id is not None:
            entry["query_id"] = query_id
        return json.dumps(entry, default=str)


_VALID_LOG_LEVELS: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


# ---------------------------------------------------------------------------
# Thread-safe Rich handler
# ---------------------------------------------------------------------------


class _ThreadSafeRichHandler(logging.Handler):
    """A thread-safe wrapper around Rich's ``RichHandler``.

    Rich's ``Console`` is not fully thread-safe and may raise
    ``OSError [Errno 9] Bad file descriptor`` when background threads
    (e.g. AST cache warmup) emit log records while the underlying
    ``sys.stderr`` file descriptor is closed or being recycled.

    This handler serialises ``emit()`` calls with a lock and catches
    ``OSError`` so that logging from daemon threads never crashes the
    application.
    """

    def __init__(self) -> None:
        super().__init__()
        from rich.console import Console
        from rich.logging import RichHandler

        # Create a dedicated Console with an explicit stderr reference so
        # Rich doesn't re-resolve sys.stderr on every write.
        self._console = Console(stderr=True)
        self._inner = RichHandler(console=self._console)
        self._lock_obj = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        with self._lock_obj:
            try:
                # Guard against closed/invalid stderr (daemon thread at
                # interpreter shutdown or after fd recycling).
                if sys.stderr is None or sys.stderr.closed:
                    return
                self._inner.emit(record)
            except OSError:
                # Silently drop the record — the fd is gone and there is
                # nothing useful we can do.
                pass


LOGGING_LEVEL = os.environ.get("PYCYPHER_LOG_LEVEL", "WARNING").upper()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(_VALID_LOG_LEVELS.get(LOGGING_LEVEL, logging.WARNING))

_log_format = os.environ.get("PYCYPHER_LOG_FORMAT", "rich").lower()

if _log_format == "json":
    _handler: logging.Handler = logging.StreamHandler()
    _handler.setFormatter(_JSONFormatter())
else:
    _handler = _ThreadSafeRichHandler()

LOGGER.addFilter(_QueryIdFilter())
LOGGER.addHandler(_handler)
