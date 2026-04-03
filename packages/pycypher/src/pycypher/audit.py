"""Opt-in structured query audit logging.

Provides a dedicated ``pycypher.audit`` logger that emits one JSON record
per query execution (success or failure).  The audit log is **off by
default** and activated by setting the ``PYCYPHER_AUDIT_LOG`` environment
variable to ``1``, ``true``, or ``yes``.

When enabled, a :class:`logging.StreamHandler` writing to *stderr* is
attached automatically.  For production use, configure the
``pycypher.audit`` logger via :mod:`logging.config` or ``dictConfig`` to
route records to a file, syslog, or log aggregator.

Each record contains:

* ``query_id`` — unique correlation ID (hex string).
* ``timestamp`` — ISO-8601 UTC timestamp.
* ``query`` — the query text (truncated to ``max_query_length``).
* ``status`` — ``"ok"`` or ``"error"``.
* ``elapsed_ms`` — wall-clock execution time in milliseconds.
* ``rows`` — number of result rows (success only).
* ``error_type`` — exception class name (failure only).
* ``parameter_keys`` — list of parameter names (never values).

Security note: parameter *values* and full result data are **never**
logged.  Query text is truncated to prevent unbounded log growth.

Usage::

    export PYCYPHER_AUDIT_LOG=1
    # Queries are now logged to stderr via pycypher.audit logger.

Programmatic activation::

    from pycypher.audit import enable_audit_log
    enable_audit_log()
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

__all__ = [
    "AUDIT_LOGGER",
    "audit_query_success",
    "audit_query_error",
    "audit_mutation",
    "enable_audit_log",
    "is_audit_enabled",
]

#: Dedicated audit logger — separate from the application logger so that
#: users can route audit records independently.
AUDIT_LOGGER = logging.getLogger("pycypher.audit")

#: Maximum number of characters of the query text included in each record.
_MAX_QUERY_LENGTH = 2048

_TRUE_VALUES = frozenset({"1", "true", "yes"})


def is_audit_enabled() -> bool:
    """Return whether the audit logger has any active handlers."""
    return AUDIT_LOGGER.isEnabledFor(logging.INFO) and bool(
        AUDIT_LOGGER.handlers
        or (AUDIT_LOGGER.parent and AUDIT_LOGGER.parent.handlers),
    )


def enable_audit_log(*, level: int = logging.INFO) -> None:
    """Programmatically enable audit logging to *stderr*.

    Safe to call multiple times — a handler is added only once.

    Args:
        level: Logging level for the audit logger (default ``INFO``).

    """
    if any(
        isinstance(h, logging.StreamHandler) for h in AUDIT_LOGGER.handlers
    ):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    AUDIT_LOGGER.addHandler(handler)
    AUDIT_LOGGER.setLevel(level)
    # Prevent duplicate propagation to root logger.
    AUDIT_LOGGER.propagate = False


def _truncate(query: str) -> str:
    if len(query) <= _MAX_QUERY_LENGTH:
        return query
    return query[:_MAX_QUERY_LENGTH] + "..."


def _emit(record: dict[str, Any]) -> None:
    """Serialise *record* as compact JSON and emit via the audit logger."""
    AUDIT_LOGGER.info(json.dumps(record, separators=(",", ":")))


def audit_query_success(
    *,
    query_id: str,
    query: str,
    elapsed_s: float,
    rows: int,
    parameter_keys: list[str] | None = None,
    cached: bool = False,
) -> None:
    """Log a successful query execution.

    Args:
        query_id: Unique correlation ID.
        query: The Cypher query text.
        elapsed_s: Wall-clock execution time in seconds.
        rows: Number of result rows.
        parameter_keys: Parameter names (values are never logged).
        cached: Whether the result was served from cache.

    """
    if not is_audit_enabled():
        return
    _emit(
        {
            "event": "query",
            "query_id": query_id,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "status": "ok",
            "query": _truncate(query),
            "elapsed_ms": round(elapsed_s * 1000.0, 2),
            "rows": rows,
            "cached": cached,
            "parameter_keys": parameter_keys or [],
        },
    )


def audit_mutation(
    *,
    query_id: str,
    operation: str,
    entity_type: str,
    affected_count: int,
    elapsed_s: float,
    details: dict[str, Any] | None = None,
) -> None:
    """Log a mutation operation (CREATE, SET, DELETE, MERGE, REMOVE).

    Emits a structured JSON record for each mutation clause execution,
    enabling audit trails for all write operations.

    Args:
        query_id: Unique correlation ID linking to the parent query.
        operation: Mutation type (``"CREATE"``, ``"SET"``, ``"DELETE"``,
            ``"DETACH_DELETE"``, ``"MERGE"``, ``"REMOVE"``).
        entity_type: The entity or relationship type affected.
        affected_count: Number of rows/entities affected.
        elapsed_s: Wall-clock execution time in seconds.
        details: Optional extra context (e.g. property keys modified,
            detached relationship count).  Values must be JSON-serializable.

    Security note: entity *data* (property values, IDs) is **never** logged.
    Only structural metadata (counts, types, property names) is recorded.

    """
    if not is_audit_enabled():
        return
    record: dict[str, Any] = {
        "event": "mutation",
        "query_id": query_id,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "operation": operation,
        "entity_type": entity_type,
        "affected_count": affected_count,
        "elapsed_ms": round(elapsed_s * 1000.0, 2),
    }
    if details:
        record["details"] = details
    _emit(record)


def audit_query_error(
    *,
    query_id: str,
    query: str,
    elapsed_s: float,
    error_type: str,
    parameter_keys: list[str] | None = None,
) -> None:
    """Log a failed query execution.

    Args:
        query_id: Unique correlation ID.
        query: The Cypher query text.
        elapsed_s: Wall-clock execution time before failure.
        error_type: Exception class name.
        parameter_keys: Parameter names (values are never logged).

    """
    if not is_audit_enabled():
        return
    _emit(
        {
            "event": "query",
            "query_id": query_id,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "status": "error",
            "query": _truncate(query),
            "elapsed_ms": round(elapsed_s * 1000.0, 2),
            "error_type": error_type,
            "parameter_keys": parameter_keys or [],
        },
    )


# Auto-enable when the environment variable is set.
if os.environ.get("PYCYPHER_AUDIT_LOG", "").strip().lower() in _TRUE_VALUES:
    enable_audit_log()
