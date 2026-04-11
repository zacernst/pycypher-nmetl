"""Backend abstraction layer for pluggable DataFrame engines.

Provides a ``BackendEngine`` protocol that allows the query engine to work
with different DataFrame backends (pandas, Dask, DuckDB, Polars) without
coupling to any specific implementation.

The protocol surface is derived from an audit of the actual pandas API calls
in the PyCypher execution path (688 import references, 918 API calls across
18 source files).  Rather than abstracting all 918 calls, the protocol
captures the ~15 primitive operations that ``BindingFrame``, ``star.py``,
and ``MutationEngine`` depend on.

Architecture
------------

::

    BackendEngine (Protocol)
    ├── PandasBackend     — current default, zero-cost wrapper
    ├── DuckDBBackend     — analytical queries, SQL-based operations
    ├── PolarsBackend     — Arrow-native, lazy-evaluated scaling
    └── (future) DaskBackend

Concrete implementations live in :mod:`pycypher.backends`:

- :mod:`pycypher.backends.pandas_backend`
- :mod:`pycypher.backends.duckdb_backend`
- :mod:`pycypher.backends.polars_backend`

Usage::

    engine = select_backend(hint="auto", estimated_rows=500_000)
    ids = engine.scan_entity(source_obj, "Person")
    filtered = engine.filter(ids, mask)
    joined = engine.join(left, right, on="__ID__")
    result = engine.to_pandas(joined)

Integration Status
------------------

.. note:: **Partially integrated into the execution path.**

   The ``BackendEngine`` protocol is wired into the core execution path via
   ``BindingFrame`` delegation.  When ``context.backend`` is available, the
   following operations route through the backend:

   - :meth:`BindingFrame.join` — inner joins with strategy hints
   - :meth:`BindingFrame.left_join` — left outer joins (OPTIONAL MATCH)
   - :meth:`BindingFrame.cross_join` — Cartesian products
   - :meth:`BindingFrame.filter` — boolean mask filtering
   - :meth:`BindingFrame.rename` — column renaming
   - :mod:`~pycypher.pattern_matcher` — concat and distinct for multi-type
     scans and variable-length path expansion

   Selecting ``backend="duckdb"`` at ``Context`` construction now routes
   these operations through the DuckDB engine.

   - :mod:`~pycypher.mutation_engine` — CREATE (concat), DELETE/DETACH DELETE
     (filter) route through backend when available

   Not yet delegated (future work):

   - Property resolution in :meth:`BindingFrame.get_property`
   - Aggregation operations in ``AggregationEvaluator``
"""

from __future__ import annotations

import enum
import time
from typing import Protocol, runtime_checkable

import pandas as pd
from shared.logger import LOGGER
from shared.otel import get_tracer, trace_phase

from pycypher.backends._helpers import (
    IDENTIFIER_RE,
    validate_identifier,
)
from pycypher.backends.duckdb_backend import DuckDBBackend
from pycypher.backends.pandas_backend import PandasBackend
from pycypher.backends.polars_backend import PolarsBackend
from pycypher.constants import ID_COLUMN
from pycypher.cypher_types import (
    BackendFrame,
    BackendMask,
    ColumnValues,
    SourceObject,
)

# ---------------------------------------------------------------------------
# BackendEngine protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class BackendEngine(Protocol):
    """Protocol for pluggable DataFrame computation backends.

    All methods operate on "frames" — the backend-specific DataFrame type.
    The pandas backend uses ``pd.DataFrame``; other backends use their native
    types and convert to pandas only at materialisation boundaries.

    The protocol covers five categories of operation, mapped from the actual
    usage patterns in the PyCypher execution path:

    1. **Scan** — ``scan_entity`` loads entity IDs from source data.
    2. **Transform** — ``filter``, ``join``, ``rename``, ``concat``,
       ``distinct``, ``assign_column``, ``drop_columns`` reshape frames.
    3. **Aggregate** — ``aggregate`` performs grouped or full-table aggregation.
    4. **Order** — ``sort``, ``limit``, ``skip`` control output ordering.
    5. **Materialise** — ``to_pandas``, ``row_count``, ``is_empty``,
       ``memory_estimate_bytes`` convert or inspect frames.
    """

    @property
    def name(self) -> str:
        """Human-readable backend name (e.g. ``'pandas'``, ``'duckdb'``)."""
        ...

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_entity(
        self,
        source_obj: SourceObject,
        entity_type: str,
    ) -> BackendFrame:
        """Load all entity IDs from *source_obj* as a single-column frame.

        Args:
            source_obj: The raw data (pd.DataFrame, pa.Table, etc.).
            entity_type: The entity type label.

        Returns:
            A frame with at least an ``__ID__`` column.

        """
        ...

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def filter(self, frame: BackendFrame, mask: BackendMask) -> BackendFrame:
        """Apply a boolean mask to *frame*, returning matching rows.

        Args:
            frame: Backend-specific DataFrame.
            mask: Boolean array/Series aligned with *frame*.

        Returns:
            Filtered frame with reset index.

        """
        ...

    def join(
        self,
        left: BackendFrame,
        right: BackendFrame,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> BackendFrame:
        """Join two frames on the given column(s).

        Args:
            left: Left frame.
            right: Right frame.
            on: Column name(s) to join on.  Empty list for cross joins.
            how: Join type (``'inner'``, ``'left'``, ``'cross'``).
            strategy: Join algorithm hint — ``'auto'``, ``'broadcast'``,
                ``'hash'``, or ``'merge'``.  Backends may ignore this if they
                perform their own optimisation (e.g. DuckDB).

        Returns:
            Joined frame.

        """
        ...

    def rename(self, frame: BackendFrame, columns: dict[str, str]) -> BackendFrame:
        """Rename columns in *frame*.

        Used by ``BindingFrame.rename()`` to promote structural join-key
        columns (e.g. ``_tgt_r``) to named Cypher variables (e.g. ``q``).

        Args:
            frame: Input frame.
            columns: Mapping from old name to new name.

        Returns:
            Frame with renamed columns.

        """
        ...

    def concat(self, frames: list[BackendFrame], *, ignore_index: bool = True) -> BackendFrame:
        """Concatenate multiple frames vertically.

        Used by ``MutationEngine.shadow_create_entity()`` and union queries.

        Args:
            frames: List of frames to concatenate.
            ignore_index: If True, reset the index after concatenation.

        Returns:
            Combined frame.

        """
        ...

    def distinct(self, frame: BackendFrame) -> BackendFrame:
        """Remove duplicate rows from *frame*.

        Used by WITH DISTINCT and RETURN DISTINCT.

        Args:
            frame: Input frame.

        Returns:
            Deduplicated frame.

        """
        ...

    def assign_column(self, frame: BackendFrame, name: str, values: ColumnValues) -> BackendFrame:
        """Add or replace a column in *frame*.

        Used by ``star.py`` during pattern traversal to add new variable
        columns to the binding frame.

        Args:
            frame: Input frame.
            name: Column name.
            values: Column values (Series, list, or scalar).

        Returns:
            Frame with the new/replaced column.

        """
        ...

    def drop_columns(self, frame: BackendFrame, columns: list[str]) -> BackendFrame:
        """Remove columns from *frame*.

        Used for post-join cleanup (removing ``_right`` suffixed duplicates
        and structural join keys).

        Args:
            frame: Input frame.
            columns: Column names to drop.  Missing names are silently ignored.

        Returns:
            Frame without the specified columns.

        """
        ...

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------

    def aggregate(
        self,
        frame: BackendFrame,
        group_cols: list[str],
        agg_specs: dict[str, tuple[str, str]],
    ) -> BackendFrame:
        """Grouped aggregation.

        Args:
            frame: Input frame.
            group_cols: Columns to group by (empty for full-table agg).
            agg_specs: Mapping from output column name to
                ``(source_column, agg_func)`` tuples.

        Returns:
            Aggregated frame.

        """
        ...

    # ------------------------------------------------------------------
    # Order
    # ------------------------------------------------------------------

    def sort(
        self,
        frame: BackendFrame,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> BackendFrame:
        """Sort *frame* by the given columns.

        Args:
            frame: Input frame.
            by: Column names to sort on.
            ascending: Per-column sort direction. Defaults to all ascending.

        Returns:
            Sorted frame.

        """
        ...

    def limit(self, frame: BackendFrame, n: int) -> BackendFrame:
        """Return the first *n* rows of *frame*.

        Args:
            frame: Input frame.
            n: Number of rows.

        Returns:
            Truncated frame.

        """
        ...

    def skip(self, frame: BackendFrame, n: int) -> BackendFrame:
        """Skip the first *n* rows of *frame*.

        Used by the SKIP clause in WITH/RETURN.

        Args:
            frame: Input frame.
            n: Number of rows to skip.

        Returns:
            Frame without the first *n* rows.

        """
        ...

    # ------------------------------------------------------------------
    # Materialise / inspect
    # ------------------------------------------------------------------

    def to_pandas(self, frame: BackendFrame) -> pd.DataFrame:
        """Materialise *frame* as a pandas DataFrame.

        This is the universal escape hatch — every backend must support this
        for interop with existing code that expects pandas.

        Args:
            frame: Backend-specific frame.

        Returns:
            A pandas DataFrame.

        """
        ...

    def row_count(self, frame: BackendFrame) -> int:
        """Return the number of rows in *frame* without full materialisation.

        Args:
            frame: Backend-specific frame.

        Returns:
            Row count.

        """
        ...

    def is_empty(self, frame: BackendFrame) -> bool:
        """Return True if *frame* has zero rows.

        Equivalent to ``row_count(frame) == 0`` but may be faster for lazy
        backends that can short-circuit after checking the first partition.

        Args:
            frame: Backend-specific frame.

        Returns:
            True if the frame has no rows.

        """
        ...

    def memory_estimate_bytes(self, frame: BackendFrame) -> int:
        """Estimate memory consumption of *frame* in bytes.

        Used by the query planner for join strategy selection and memory
        budgeting.  Does not need to be exact — order of magnitude is fine.

        Args:
            frame: Backend-specific frame.

        Returns:
            Estimated size in bytes.

        """
        ...


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------


def check_backend_health(backend: BackendEngine) -> bool:
    """Run a lightweight health probe against *backend*.

    Creates a tiny DataFrame, performs a scan + filter + join cycle, and
    verifies the result.  Returns ``True`` if the backend is operational,
    ``False`` on any exception.

    Args:
        backend: The backend instance to probe.

    Returns:
        ``True`` if all probe operations succeed with correct results.

    """
    try:
        probe = pd.DataFrame({ID_COLUMN: [1, 2], "_v": ["a", "b"]})
        scanned = backend.scan_entity(probe, "_health")
        filtered = backend.filter(scanned, pd.Series([True, False]))
        joined = backend.join(
            filtered,
            pd.DataFrame({ID_COLUMN: [1]}),
            on=ID_COLUMN,
            how="inner",
        )
        result = backend.to_pandas(joined)
        return len(result) == 1 and result[ID_COLUMN].iloc[0] == 1
    except (
        RuntimeError,
        TypeError,
        ValueError,
        KeyError,
        AttributeError,
    ) as exc:
        LOGGER.error(
            "Health check failed for backend %s: %s",
            backend.name,
            type(exc).__name__,
            exc_info=True,
        )
        return False


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class CircuitState(enum.Enum):
    """Three-state circuit breaker model."""

    CLOSED = "closed"  # Normal operation — requests flow through
    OPEN = "open"  # Failing — requests are blocked
    HALF_OPEN = "half_open"  # Probing — one test request allowed


class CircuitBreaker:
    """Per-backend circuit breaker with configurable thresholds.

    Tracks consecutive failures for a backend.  When failures exceed
    *failure_threshold*, the circuit opens and the backend is bypassed for
    *recovery_timeout* seconds.  After the timeout, one probe request is
    allowed; if it succeeds the circuit closes, otherwise it reopens.

    Args:
        failure_threshold: Consecutive failures before opening the circuit.
        recovery_timeout: Seconds to wait before probing a failed backend.

    """

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
    ) -> None:
        """Initialise the circuit breaker.

        Args:
            failure_threshold: Number of consecutive failures to trigger
                circuit open.
            recovery_timeout: Seconds before attempting a probe after open.

        """
        self._threshold = failure_threshold
        self._timeout = recovery_timeout
        self._failures: dict[str, int] = {}
        self._states: dict[str, CircuitState] = {}
        self._last_failure_time: dict[str, float] = {}

    @property
    def failure_threshold(self) -> int:
        """The configured failure threshold."""
        return self._threshold

    @property
    def recovery_timeout(self) -> float:
        """The configured recovery timeout in seconds."""
        return self._timeout

    def record_success(self, backend_name: str) -> None:
        """Record a successful operation, resetting the failure counter."""
        self._failures[backend_name] = 0
        self._states[backend_name] = CircuitState.CLOSED

    def record_failure(self, backend_name: str) -> None:
        """Record a failure, potentially opening the circuit."""
        self._failures[backend_name] = self._failures.get(backend_name, 0) + 1
        self._last_failure_time[backend_name] = time.monotonic()
        if self._failures[backend_name] >= self._threshold:
            self._states[backend_name] = CircuitState.OPEN
            LOGGER.warning(
                "Circuit breaker OPEN for backend %s after %d failures",
                backend_name,
                self._failures[backend_name],
            )

    def is_available(self, backend_name: str) -> bool:
        """Check if a backend is available (circuit closed or half-open)."""
        state = self._states.get(backend_name, CircuitState.CLOSED)
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time.get(
                backend_name,
                0,
            )
            if elapsed >= self._timeout:
                self._states[backend_name] = CircuitState.HALF_OPEN
                return True
            return False
        # HALF_OPEN — allow one probe
        return True

    def state(self, backend_name: str) -> CircuitState:
        """Return the current circuit state for a backend.

        Handles the OPEN → HALF_OPEN transition when the recovery timeout
        has elapsed, consistent with :meth:`is_available`.

        Args:
            backend_name: The backend to query.

        Returns:
            The current :class:`CircuitState`.

        """
        current = self._states.get(backend_name, CircuitState.CLOSED)
        if current == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time.get(
                backend_name,
                0,
            )
            if elapsed >= self._timeout:
                self._states[backend_name] = CircuitState.HALF_OPEN
                return CircuitState.HALF_OPEN
        return current

    def reset(self, backend_name: str | None = None) -> None:
        """Reset circuit breaker state.

        If *backend_name* is given, resets only that backend.  Otherwise
        resets all backends to CLOSED.

        Args:
            backend_name: Optional backend to reset.  ``None`` resets all.

        """
        if backend_name is not None:
            self._failures.pop(backend_name, None)
            self._states.pop(backend_name, None)
            self._last_failure_time.pop(backend_name, None)
        else:
            self._failures.clear()
            self._states.clear()
            self._last_failure_time.clear()

    def get_state(self, backend_name: str) -> CircuitState:
        """Return the current circuit state for a backend.

        .. deprecated::
            Use :meth:`state` instead, which handles OPEN → HALF_OPEN transitions.

        """
        return self.state(backend_name)


#: Module-level circuit breaker shared across ``select_backend`` calls.
_circuit_breaker = CircuitBreaker()


def get_circuit_breaker() -> CircuitBreaker:
    """Return the module-level circuit breaker instance."""
    return _circuit_breaker


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------


_BACKEND_FACTORIES: dict[str, type] = {
    "polars": PolarsBackend,
    "duckdb": DuckDBBackend,
    "pandas": PandasBackend,
}

#: Fallback chain for auto selection (preferred → last resort).
_FALLBACK_CHAIN: list[str] = ["polars", "duckdb", "pandas"]


# ---------------------------------------------------------------------------
# Instrumented Backend — observability wrapper
# ---------------------------------------------------------------------------


class InstrumentedBackend:
    """Transparent wrapper that records per-operation timing and counts.

    Delegates all ``BackendEngine`` operations to an inner backend while
    recording timing, operation counts, and emitting DEBUG-level log
    messages for each operation.

    Attributes:
        operation_timings: Dict mapping operation names to lists of elapsed
            times in milliseconds.
        operation_counts: Dict mapping operation names to invocation counts.

    """

    def __init__(self, inner: BackendEngine) -> None:
        """Wrap *inner* with per-operation timing and count instrumentation.

        Args:
            inner: The :class:`BackendEngine` to delegate operations to.
                All method calls are forwarded transparently; timing and
                counts are recorded in :attr:`operation_timings` and
                :attr:`operation_counts`.

        """
        self._inner = inner
        self.operation_timings: dict[str, list[float]] = {}
        self.operation_counts: dict[str, int] = {}
        self._tracer = get_tracer()

    @property
    def name(self) -> str:
        """Return the inner backend's name."""
        return self._inner.name

    def _record(self, op: str, elapsed_ms: float, span: object = None) -> None:
        """Record timing for an operation and annotate the OTel span."""
        self.operation_timings.setdefault(op, []).append(elapsed_ms)
        self.operation_counts[op] = self.operation_counts.get(op, 0) + 1
        if span is not None:
            span.set_attribute("backend.name", self._inner.name)
            span.set_attribute("backend.operation", op)
            span.set_attribute("backend.elapsed_ms", round(elapsed_ms, 2))
        LOGGER.debug(
            "backend %s: %s completed in %.2fms",
            self._inner.name,
            op,
            elapsed_ms,
        )

    def timing_summary(self) -> dict[str, dict[str, float]]:
        """Return a summary of per-operation timing statistics.

        Returns:
            Dict mapping operation names to dicts with ``count`` and
            ``total_ms`` keys.

        """
        result: dict[str, dict[str, float]] = {}
        for op, timings in self.operation_timings.items():
            result[op] = {
                "count": float(len(timings)),
                "total_ms": sum(timings),
            }
        return result

    # -- Scan --

    def scan_entity(self, source_obj: SourceObject, entity_type: str) -> BackendFrame:
        """Instrumented scan_entity with OTel tracing."""
        with trace_phase("backend.scan_entity") as span:
            span.set_attribute("backend.entity_type", entity_type)
            t0 = time.perf_counter()
            result = self._inner.scan_entity(source_obj, entity_type)
            self._record(
                "scan_entity",
                (time.perf_counter() - t0) * 1000.0,
                span,
            )
            return result

    # -- Transform --

    def filter(self, frame: BackendFrame, mask: BackendMask) -> BackendFrame:
        """Instrumented filter with OTel tracing."""
        with trace_phase("backend.filter") as span:
            t0 = time.perf_counter()
            result = self._inner.filter(frame, mask)
            self._record("filter", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def join(
        self,
        left: BackendFrame,
        right: BackendFrame,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> BackendFrame:
        """Instrumented join with OTel tracing."""
        with trace_phase("backend.join") as span:
            span.set_attribute("backend.join_how", how)
            span.set_attribute("backend.join_strategy", strategy)
            t0 = time.perf_counter()
            result = self._inner.join(
                left,
                right,
                on=on,
                how=how,
                strategy=strategy,
            )
            self._record("join", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def rename(self, frame: BackendFrame, columns: dict[str, str]) -> BackendFrame:
        """Instrumented rename with OTel tracing."""
        with trace_phase("backend.rename") as span:
            t0 = time.perf_counter()
            result = self._inner.rename(frame, columns)
            self._record("rename", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def concat(self, frames: list[BackendFrame], *, ignore_index: bool = True) -> BackendFrame:
        """Instrumented concat with OTel tracing."""
        with trace_phase("backend.concat") as span:
            span.set_attribute("backend.frame_count", len(frames))
            t0 = time.perf_counter()
            result = self._inner.concat(frames, ignore_index=ignore_index)
            self._record("concat", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def distinct(self, frame: BackendFrame) -> BackendFrame:
        """Instrumented distinct with OTel tracing."""
        with trace_phase("backend.distinct") as span:
            t0 = time.perf_counter()
            result = self._inner.distinct(frame)
            self._record("distinct", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def assign_column(self, frame: BackendFrame, name: str, values: ColumnValues) -> BackendFrame:
        """Instrumented assign_column with OTel tracing."""
        with trace_phase("backend.assign_column") as span:
            span.set_attribute("backend.column_name", name)
            t0 = time.perf_counter()
            result = self._inner.assign_column(frame, name, values)
            self._record(
                "assign_column",
                (time.perf_counter() - t0) * 1000.0,
                span,
            )
            return result

    def drop_columns(self, frame: BackendFrame, columns: list[str]) -> BackendFrame:
        """Instrumented drop_columns with OTel tracing."""
        with trace_phase("backend.drop_columns") as span:
            span.set_attribute("backend.drop_count", len(columns))
            t0 = time.perf_counter()
            result = self._inner.drop_columns(frame, columns)
            self._record(
                "drop_columns",
                (time.perf_counter() - t0) * 1000.0,
                span,
            )
            return result

    # -- Aggregate --

    def aggregate(
        self,
        frame: BackendFrame,
        group_cols: list[str],
        agg_specs: dict[str, tuple[str, str]],
    ) -> BackendFrame:
        """Instrumented aggregate with OTel tracing."""
        with trace_phase("backend.aggregate") as span:
            span.set_attribute("backend.group_col_count", len(group_cols))
            span.set_attribute("backend.agg_count", len(agg_specs))
            t0 = time.perf_counter()
            result = self._inner.aggregate(frame, group_cols, agg_specs)
            self._record(
                "aggregate",
                (time.perf_counter() - t0) * 1000.0,
                span,
            )
            return result

    # -- Order --

    def sort(
        self,
        frame: BackendFrame,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> BackendFrame:
        """Instrumented sort with OTel tracing."""
        with trace_phase("backend.sort") as span:
            span.set_attribute("backend.sort_col_count", len(by))
            t0 = time.perf_counter()
            result = self._inner.sort(frame, by=by, ascending=ascending)
            self._record("sort", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def limit(self, frame: BackendFrame, n: int) -> BackendFrame:
        """Instrumented limit with OTel tracing."""
        with trace_phase("backend.limit") as span:
            span.set_attribute("backend.limit_n", n)
            t0 = time.perf_counter()
            result = self._inner.limit(frame, n)
            self._record("limit", (time.perf_counter() - t0) * 1000.0, span)
            return result

    def skip(self, frame: BackendFrame, n: int) -> BackendFrame:
        """Instrumented skip with OTel tracing."""
        with trace_phase("backend.skip") as span:
            span.set_attribute("backend.skip_n", n)
            t0 = time.perf_counter()
            result = self._inner.skip(frame, n)
            self._record("skip", (time.perf_counter() - t0) * 1000.0, span)
            return result

    # -- Materialise --

    def to_pandas(self, frame: BackendFrame) -> pd.DataFrame:
        """Instrumented to_pandas with OTel tracing."""
        with trace_phase("backend.to_pandas") as span:
            t0 = time.perf_counter()
            result = self._inner.to_pandas(frame)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            span.set_attribute("backend.result_rows", len(result))
            self._record("to_pandas", elapsed_ms, span)
            return result

    def row_count(self, frame: BackendFrame) -> int:
        """Delegate row_count without instrumentation (trivial op)."""
        return self._inner.row_count(frame)

    def is_empty(self, frame: BackendFrame) -> bool:
        """Delegate is_empty without instrumentation (trivial op)."""
        return self._inner.is_empty(frame)

    def memory_estimate_bytes(self, frame: BackendFrame) -> int:
        """Delegate memory_estimate_bytes."""
        return self._inner.memory_estimate_bytes(frame)


def select_backend_for_query(
    *,
    current_backend: BackendEngine,
    optimization_hints: dict[str, object],
    estimated_rows: int = 0,
    instrument: bool = False,
) -> BackendEngine | None:
    """Re-evaluate backend selection using post-optimization query hints.

    Called after the query optimizer produces cardinality estimates and
    join strategy hints.  Returns a replacement backend if the hints
    suggest the current backend is suboptimal, or ``None`` if no change
    is needed.

    Args:
        current_backend: The backend currently in use.
        optimization_hints: Merged hints from :class:`OptimizationPlan`.
        estimated_rows: Row estimate from the query planner (overrides
            the Context-level entity count if available).
        instrument: Wrap the replacement with :class:`InstrumentedBackend`.

    Returns:
        A replacement :class:`BackendEngine`, or ``None`` if the current
        backend is already optimal.

    """
    cb = _circuit_breaker
    current_name = current_backend.name

    # Extract cardinality estimates from optimizer hints
    cardinality_estimates = optimization_hints.get("cardinality_estimates", {})
    if cardinality_estimates:
        max_cardinality = max(cardinality_estimates.values(), default=0)
        estimated_rows = max(estimated_rows, max_cardinality)

    # Heuristic: if estimated rows exceed threshold and we're on pandas,
    # suggest switching to a more scalable backend
    threshold = 100_000
    if estimated_rows >= threshold and current_name == "pandas":
        for name in _FALLBACK_CHAIN:
            if name == "pandas":
                continue
            if not cb.is_available(name):
                continue
            backend = _try_create(name)
            if backend is not None:
                LOGGER.info(
                    "Backend re-evaluation: switching from %s to %s "
                    "(estimated %s rows exceeds %s threshold)",
                    current_name,
                    name,
                    f"{estimated_rows:,}",
                    f"{threshold:,}",
                )
                if instrument:
                    return InstrumentedBackend(backend)
                return backend

    # Heuristic: heavy multi-join queries with many filters benefit from
    # analytical backends even at lower row counts
    match_count = optimization_hints.get("match_clause_count", 0)
    filter_count = optimization_hints.get("filter_pushdown_count", 0)
    if (
        match_count > 1
        and filter_count >= 2
        and current_name == "pandas"
        and estimated_rows >= 50_000
    ):
        if cb.is_available("duckdb"):
            backend = _try_create("duckdb")
            if backend is not None:
                LOGGER.info(
                    "Backend re-evaluation: switching to duckdb for "
                    "complex query (%d joins, %d filters, %s rows)",
                    match_count,
                    filter_count,
                    f"{estimated_rows:,}",
                )
                if instrument:
                    return InstrumentedBackend(backend)
                return backend

    return None


def select_backend(
    *,
    hint: str = "auto",
    estimated_rows: int = 0,
    run_health_check: bool = False,
    instrument: bool = False,
) -> BackendEngine:
    """Select the optimal backend engine for the workload.

    Args:
        hint: Backend preference — ``'auto'``, ``'pandas'``, ``'duckdb'``,
            ``'polars'``.
        estimated_rows: Estimated total rows to process.  Used by ``'auto'``
            to decide between backends.
        run_health_check: If ``True``, run a lightweight health probe on the
            selected backend before returning it.  On failure the circuit
            breaker is tripped and the next backend in the fallback chain
            is tried.
        instrument: If ``True``, wrap the selected backend with
            :class:`InstrumentedBackend` for timing and count recording.

    Returns:
        A ``BackendEngine`` instance.

    Raises:
        RuntimeError: If all backends fail health checks (only possible when
            *run_health_check* is ``True``).
        ValueError: If *hint* is not a recognised backend name.

    The auto-selection heuristic:

    - < 100K rows → PandasBackend (low overhead, no startup cost)
    - >= 100K rows → fallback chain with circuit breaker awareness

    """
    cb = _circuit_breaker

    def _maybe_instrument(b: BackendEngine) -> BackendEngine:
        """Wrap *b* with :class:`InstrumentedBackend` when instrumentation is enabled."""
        return InstrumentedBackend(b) if instrument else b

    if hint not in {"auto", "pandas", "duckdb", "polars"}:
        msg = f"Unknown backend hint: {hint!r}. Use 'auto', 'pandas', 'polars', or 'duckdb'."
        raise ValueError(msg)

    # --- Explicit backend requested ---
    if hint != "auto":
        backend = _try_create(hint)
        if backend is None:
            msg = f"Backend {hint!r} is not available (import failed)"
            raise ValueError(msg)
        if run_health_check:
            return _maybe_instrument(_health_checked(backend, cb))
        return _maybe_instrument(backend)

    # --- Auto selection ---
    threshold = 100_000
    if estimated_rows < threshold:
        candidates = ["pandas"]
    else:
        candidates = [
            name for name in _FALLBACK_CHAIN if cb.is_available(name)
        ]
        if not candidates:
            # All circuits open — fall back to pandas (always available)
            candidates = ["pandas"]

    for name in candidates:
        backend = _try_create(name)
        if backend is None:
            continue
        if run_health_check:
            try:
                return _maybe_instrument(_health_checked(backend, cb))
            except RuntimeError:
                continue
        return _maybe_instrument(backend)

    # Last resort — pandas never fails to import
    return _maybe_instrument(PandasBackend())


def _try_create(name: str) -> BackendEngine | None:
    """Attempt to instantiate a backend by name, returning None on failure."""
    factory = _BACKEND_FACTORIES.get(name)
    if factory is None:
        return None
    try:
        return factory()
    except (RuntimeError, ImportError, OSError, TypeError, ValueError) as exc:
        LOGGER.error(
            "Failed to create backend %s: %s",
            name,
            type(exc).__name__,
            exc_info=True,
        )
        return None


def _health_checked(
    backend: BackendEngine,
    cb: CircuitBreaker,
) -> BackendEngine:
    """Run a health check and update the circuit breaker.

    Returns the backend on success; raises ``RuntimeError`` on failure.
    """
    if check_backend_health(backend):
        cb.record_success(backend.name)
        return backend
    cb.record_failure(backend.name)
    msg = f"Health check failed for backend {backend.name!r}"
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Backward-compatible re-exports
# ---------------------------------------------------------------------------
# These ensure that ``from pycypher.backend_engine import PandasBackend``
# (and similar) continue to work without changes to existing call sites.

__all__ = [
    "BackendEngine",
    "CircuitBreaker",
    "CircuitState",
    "DuckDBBackend",
    "IDENTIFIER_RE",
    "InstrumentedBackend",
    "PandasBackend",
    "PolarsBackend",
    "check_backend_health",
    "get_circuit_breaker",
    "select_backend",
    "select_backend_for_query",
    "validate_identifier",
]
