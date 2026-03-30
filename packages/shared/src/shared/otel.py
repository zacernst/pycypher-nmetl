"""OpenTelemetry integration for PyCypher distributed tracing.

Wraps query execution with OpenTelemetry spans for end-to-end
distributed tracing.  All configuration is via standard ``OTEL_*``
environment variables.

**Zero-config design**: if ``opentelemetry-api`` is not installed, every
public function is a silent no-op — no import errors, no runtime cost.

Enable via:

- ``PYCYPHER_OTEL_ENABLED=1`` (default: ``0``)
- Standard OTEL env vars (``OTEL_SERVICE_NAME``, ``OTEL_EXPORTER_*``, etc.)

Usage::

    from shared.otel import trace_query, get_tracer

    # Context-manager style
    with trace_query("MATCH (p:Person) RETURN p.name", query_id="abc") as span:
        result = star.execute_query(query)
        span.set_attribute("result.rows", len(result))

    # Manual tracer access
    tracer = get_tracer()
    with tracer.start_as_current_span("custom-operation") as span:
        ...

Attributes:
    OTEL_ENABLED: Whether OpenTelemetry tracing is active.

"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

_logger = logging.getLogger(__name__)

OTEL_ENABLED: bool = os.environ.get(
    "PYCYPHER_OTEL_ENABLED",
    "0",
).lower() in ("1", "true", "yes")

_SERVICE_NAME: str = os.environ.get("OTEL_SERVICE_NAME", "pycypher")

# ---------------------------------------------------------------------------
# Conditional import — graceful degradation when OTel is not installed
# ---------------------------------------------------------------------------

_tracer: Any = None
_trace_mod: Any = None
_StatusCode: Any = None

if OTEL_ENABLED:
    try:
        from opentelemetry import trace as _trace_mod
        from opentelemetry.trace import (
            StatusCode as _StatusCode,
        )

        _tracer = _trace_mod.get_tracer(
            "pycypher",
            schema_url="https://opentelemetry.io/schemas/1.21.0",
        )
        _logger.info(
            "OpenTelemetry tracing enabled  service=%s",
            _SERVICE_NAME,
        )
    except ImportError:
        _logger.debug(
            "opentelemetry-api not installed — tracing disabled",
        )
        OTEL_ENABLED = False
    except Exception:
        _logger.warning(
            "OpenTelemetry configuration failed — tracing disabled",
            exc_info=True,
        )
        OTEL_ENABLED = False


# ---------------------------------------------------------------------------
# Null-object tracer for when OTel is disabled
# ---------------------------------------------------------------------------


class _NullSpan:
    """No-op span that silently discards all attribute/event/status calls."""

    def set_attribute(self, key: str, value: Any) -> None:
        """No-op."""

    def set_status(self, status: Any, description: str | None = None) -> None:
        """No-op."""

    def add_event(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """No-op."""

    def record_exception(
        self,
        exception: BaseException,
        *,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """No-op."""

    def __enter__(self) -> _NullSpan:
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _NullTracer:
    """No-op tracer that returns :class:`_NullSpan` instances."""

    def start_as_current_span(
        self,
        name: str,
        **kwargs: Any,
    ) -> _NullSpan:
        """Return a no-op span context manager."""
        return _NullSpan()


_NULL_TRACER = _NullTracer()
_NULL_SPAN = _NullSpan()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_tracer() -> Any:
    """Return the active OpenTelemetry tracer, or a no-op tracer.

    Returns:
        An OpenTelemetry ``Tracer`` if enabled and installed,
        otherwise a :class:`_NullTracer` that silently discards all calls.

    """
    if _tracer is not None:
        return _tracer
    return _NULL_TRACER


@contextmanager
def trace_query(
    query: str,
    *,
    query_id: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> Generator[Any]:
    """Wrap a query execution in an OpenTelemetry span.

    Sets standard attributes on the span:

    - ``db.system`` = ``"pycypher"``
    - ``db.statement`` = query text (truncated to 500 chars)
    - ``db.operation`` = first Cypher keyword (MATCH, CREATE, etc.)
    - ``pycypher.query_id`` = correlation ID (if provided)

    On exception, the span is marked ERROR and the exception is recorded.

    Args:
        query: The Cypher query string.
        query_id: Optional correlation ID for cross-system tracing.
        parameters: Optional query parameters (keys logged, values omitted
            for security).

    Yields:
        The active span (real or :class:`_NullSpan`).

    Example::

        with trace_query("MATCH (n) RETURN n", query_id="q-123") as span:
            result = star.execute_query(query)
            span.set_attribute("result.rows", len(result))

    """
    if not OTEL_ENABLED or _tracer is None:
        yield _NULL_SPAN
        return

    operation = _extract_operation(query)
    span_name = f"pycypher.{operation}" if operation else "pycypher.query"

    with _tracer.start_as_current_span(span_name) as span:
        span.set_attribute("db.system", "pycypher")
        span.set_attribute("db.statement", query[:500])
        if operation:
            span.set_attribute("db.operation", operation)
        if query_id:
            span.set_attribute("pycypher.query_id", query_id)
        if parameters:
            span.set_attribute(
                "pycypher.parameter_keys",
                list(parameters.keys()),
            )
        try:
            yield span
        except Exception as exc:
            span.set_status(_StatusCode.ERROR, str(exc)[:200])  # type: ignore[union-attr]  # _StatusCode is set when OTEL_ENABLED
            span.record_exception(exc)
            raise


@contextmanager
def trace_phase(
    phase: str,
    *,
    query_id: str | None = None,
) -> Generator[Any]:
    """Wrap a query execution phase (parse, plan, execute) in a child span.

    Args:
        phase: Phase name (e.g. ``"parse"``, ``"plan"``, ``"execute"``).
        query_id: Optional correlation ID.

    Yields:
        The active span (real or :class:`_NullSpan`).

    """
    if not OTEL_ENABLED or _tracer is None:
        yield _NULL_SPAN
        return

    with _tracer.start_as_current_span(f"pycypher.{phase}") as span:
        if query_id:
            span.set_attribute("pycypher.query_id", query_id)
        span.set_attribute("pycypher.phase", phase)
        try:
            yield span
        except Exception as exc:
            span.set_status(_StatusCode.ERROR, str(exc)[:200])  # type: ignore[union-attr]  # _StatusCode is set when OTEL_ENABLED
            span.record_exception(exc)
            raise


def record_metrics_to_span(
    span: Any,
    snapshot: Any,
) -> None:
    """Attach MetricsSnapshot summary data to an existing span.

    Useful for periodic health-check spans or diagnostic traces.

    Args:
        span: An OpenTelemetry span (or :class:`_NullSpan`).
        snapshot: A :class:`~shared.metrics.MetricsSnapshot` instance.

    """
    span.set_attribute("pycypher.total_queries", snapshot.total_queries)
    span.set_attribute("pycypher.total_errors", snapshot.total_errors)
    span.set_attribute("pycypher.error_rate", snapshot.error_rate)
    span.set_attribute("pycypher.timing_p50_ms", snapshot.timing_p50_ms)
    span.set_attribute("pycypher.timing_p99_ms", snapshot.timing_p99_ms)
    span.set_attribute("pycypher.health_status", snapshot.health_status())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_operation(query: str) -> str | None:
    """Extract the primary Cypher operation from a query string.

    Returns the first keyword from {MATCH, CREATE, MERGE, DELETE, SET,
    REMOVE, UNWIND, WITH, RETURN, CALL}, or ``None``.
    """
    _OPERATIONS = frozenset(
        {
            "MATCH",
            "CREATE",
            "MERGE",
            "DELETE",
            "SET",
            "REMOVE",
            "UNWIND",
            "WITH",
            "RETURN",
            "CALL",
        },
    )
    for token in query.split():
        upper = token.upper()
        if upper in _OPERATIONS:
            return upper
    return None
