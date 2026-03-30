"""Query performance metrics collection.

:class:`MetricsCollector` captures per-query execution metrics including
timing breakdowns, row counts, and error states. Collected metrics are
stored in a bounded ring buffer for memory-safe historical analysis.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class QueryStatus(Enum):
    """Outcome of a query execution."""

    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class QueryMetric:
    """Performance metrics for a single query execution.

    Attributes
    ----------
    query_id : str
        Unique identifier for this execution.
    query_text : str
        The Cypher query that was executed.
    status : QueryStatus
        Outcome of the execution.
    total_ms : float
        Total wall-clock time in milliseconds.
    parse_ms : float
        Time spent parsing the query.
    plan_ms : float
        Time spent generating the execution plan.
    exec_ms : float
        Time spent executing the plan.
    row_count : int
        Number of result rows returned.
    timestamp : float
        Epoch timestamp when the query started.
    error_message : str | None
        Error description if status is not SUCCESS.
    metadata : dict[str, Any]
        Additional context (query parameters, plan info, etc.).

    """

    query_id: str
    query_text: str
    status: QueryStatus
    total_ms: float
    parse_ms: float = 0.0
    plan_ms: float = 0.0
    exec_ms: float = 0.0
    row_count: int = 0
    timestamp: float = field(default_factory=time.time)
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dictionary."""
        return {
            "query_id": self.query_id,
            "query_text": self.query_text,
            "status": self.status.value,
            "total_ms": round(self.total_ms, 3),
            "parse_ms": round(self.parse_ms, 3),
            "plan_ms": round(self.plan_ms, 3),
            "exec_ms": round(self.exec_ms, 3),
            "row_count": self.row_count,
            "timestamp": self.timestamp,
            "error_message": self.error_message,
        }


class MetricsCollector:
    """Thread-safe collector for query performance metrics.

    Stores up to ``max_history`` metrics in a ring buffer. Provides
    fast access to recent metrics and aggregate counters.

    Parameters
    ----------
    max_history : int
        Maximum number of query metrics to retain.

    """

    def __init__(self, max_history: int = 10_000) -> None:
        self._history: deque[QueryMetric] = deque(maxlen=max_history)
        self._lock = threading.Lock()
        self._total_queries = 0
        self._total_errors = 0
        self._total_ms = 0.0
        self._start_time = time.time()

    @property
    def total_queries(self) -> int:
        """Total queries recorded since collector creation."""
        return self._total_queries

    @property
    def total_errors(self) -> int:
        """Total failed queries since collector creation."""
        return self._total_errors

    @property
    def error_rate(self) -> float:
        """Fraction of queries that resulted in errors (0.0–1.0)."""
        if self._total_queries == 0:
            return 0.0
        return self._total_errors / self._total_queries

    @property
    def uptime_seconds(self) -> float:
        """Seconds since the collector was created."""
        return time.time() - self._start_time

    @property
    def queries_per_second(self) -> float:
        """Average query throughput over collector lifetime."""
        elapsed = self.uptime_seconds
        if elapsed <= 0:
            return 0.0
        return self._total_queries / elapsed

    def record(self, metric: QueryMetric) -> None:
        """Record a query metric.

        Parameters
        ----------
        metric : QueryMetric
            The metric to record.

        """
        with self._lock:
            self._history.append(metric)
            self._total_queries += 1
            self._total_ms += metric.total_ms
            if metric.status != QueryStatus.SUCCESS:
                self._total_errors += 1

    def record_success(
        self,
        query_text: str,
        total_ms: float,
        row_count: int = 0,
        *,
        parse_ms: float = 0.0,
        plan_ms: float = 0.0,
        exec_ms: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> QueryMetric:
        """Convenience method to record a successful query.

        Returns the created :class:`QueryMetric`.
        """
        metric = QueryMetric(
            query_id=uuid.uuid4().hex[:12],
            query_text=query_text,
            status=QueryStatus.SUCCESS,
            total_ms=total_ms,
            parse_ms=parse_ms,
            plan_ms=plan_ms,
            exec_ms=exec_ms,
            row_count=row_count,
            metadata=metadata or {},
        )
        self.record(metric)
        return metric

    def record_error(
        self,
        query_text: str,
        total_ms: float,
        error_message: str,
        *,
        status: QueryStatus = QueryStatus.ERROR,
        metadata: dict[str, Any] | None = None,
    ) -> QueryMetric:
        """Convenience method to record a failed query.

        Returns the created :class:`QueryMetric`.
        """
        metric = QueryMetric(
            query_id=uuid.uuid4().hex[:12],
            query_text=query_text,
            status=status,
            total_ms=total_ms,
            error_message=error_message,
            metadata=metadata or {},
        )
        self.record(metric)
        return metric

    def recent(self, n: int = 100) -> list[QueryMetric]:
        """Return the *n* most recent metrics, newest first.

        Parameters
        ----------
        n : int
            Maximum number of metrics to return.

        """
        with self._lock:
            items = list(self._history)
        return list(reversed(items[-n:]))

    def all_metrics(self) -> list[QueryMetric]:
        """Return all stored metrics in chronological order."""
        with self._lock:
            return list(self._history)

    def clear(self) -> None:
        """Remove all stored metrics and reset counters."""
        with self._lock:
            self._history.clear()
            self._total_queries = 0
            self._total_errors = 0
            self._total_ms = 0.0
            self._start_time = time.time()

    @staticmethod
    def new_query_id() -> str:
        """Generate a unique query identifier."""
        return uuid.uuid4().hex[:12]
