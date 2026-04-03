"""Distributed execution scaffolding for cluster deployment.

Defines the coordination protocols, worker node registration, query
distribution strategies, and fault tolerance patterns needed for
enterprise-scale distributed query execution.

This module provides **interfaces and local implementations** — the
actual network transport and distributed scheduling are deferred to
Phase 3.  The local implementations allow testing the coordination
logic without a real cluster.

Architecture
~~~~~~~~~~~~

::

    ┌──────────────────────────────────┐
    │         ClusterCoordinator       │
    │  (registers workers, routes      │
    │   queries, aggregates metrics)   │
    └──────┬───────────┬───────────┬───┘
           │           │           │
     ┌─────▼──┐  ┌─────▼──┐  ┌────▼───┐
     │Worker 1│  │Worker 2│  │Worker N│
     │(Star)  │  │(Star)  │  │(Star)  │
     └────────┘  └────────┘  └────────┘

Each worker wraps a :class:`~pycypher.star.Star` instance.  The
coordinator selects a worker via a pluggable :class:`QueryRouter`
strategy (round-robin, least-loaded, hash-based).

Usage::

    from pycypher.cluster import ClusterCoordinator, LocalWorker

    coord = ClusterCoordinator()
    coord.register_worker(LocalWorker("w1"))
    coord.register_worker(LocalWorker("w2"))

    result = coord.execute_query("MATCH (p:Person) RETURN p.name")
    health = coord.cluster_health()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

import pandas as pd
from shared.logger import LOGGER

from pycypher.exceptions import WorkerExecutionError

# ---------------------------------------------------------------------------
# Worker status
# ---------------------------------------------------------------------------


class WorkerStatus(Enum):
    """Health state of a cluster worker."""

    ACTIVE = "active"
    DRAINING = "draining"
    UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# Worker protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Worker(Protocol):
    """Protocol that every cluster worker must satisfy.

    A worker wraps a query execution engine and exposes health
    and metrics information for the coordinator.
    """

    @property
    def worker_id(self) -> str:
        """Unique identifier for this worker."""
        ...

    @property
    def status(self) -> WorkerStatus:
        """Current health status."""
        ...

    def execute_query(
        self,
        query: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a Cypher query on this worker.

        Args:
            query: Cypher query string.
            parameters: Optional named parameters.

        Returns:
            DataFrame with result rows.

        Raises:
            Exception: On query failure.

        """
        ...

    def health_check(self) -> WorkerHealth:
        """Return current health snapshot for this worker."""
        ...


# ---------------------------------------------------------------------------
# Worker health snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerHealth:
    """Point-in-time health snapshot for a single worker.

    Attributes:
        worker_id: Unique worker identifier.
        status: Current worker status.
        queries_executed: Total queries handled by this worker.
        errors: Total errors on this worker.
        avg_latency_ms: Rolling average query latency in ms.
        last_heartbeat: Monotonic timestamp of last successful heartbeat.
        active_queries: Number of queries currently executing.

    """

    worker_id: str
    status: WorkerStatus
    queries_executed: int
    errors: int
    avg_latency_ms: float
    last_heartbeat: float
    active_queries: int


# ---------------------------------------------------------------------------
# Cluster health
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClusterHealth:
    """Aggregate health snapshot for the entire cluster.

    Attributes:
        total_workers: Number of registered workers.
        active_workers: Workers in ACTIVE status.
        unavailable_workers: Workers in UNAVAILABLE status.
        total_queries: Sum of queries across all workers.
        total_errors: Sum of errors across all workers.
        cluster_error_rate: Aggregate error rate (0.0–1.0).
        avg_latency_ms: Weighted average latency across workers.
        worker_health: Per-worker health snapshots.

    """

    total_workers: int
    active_workers: int
    unavailable_workers: int
    total_queries: int
    total_errors: int
    cluster_error_rate: float
    avg_latency_ms: float
    worker_health: list[WorkerHealth]


# ---------------------------------------------------------------------------
# Query routing strategy
# ---------------------------------------------------------------------------


@runtime_checkable
class QueryRouter(Protocol):
    """Strategy for selecting which worker handles a query."""

    def select_worker(
        self,
        workers: list[Worker],
        query: str,
    ) -> Worker:
        """Choose a worker for the given query.

        Args:
            workers: List of active workers to choose from.
            query: The Cypher query string (for hash-based routing).

        Returns:
            The selected worker.

        Raises:
            RuntimeError: If no suitable worker is available.

        """
        ...


# ---------------------------------------------------------------------------
# Built-in routing strategies
# ---------------------------------------------------------------------------


class RoundRobinRouter:
    """Distributes queries evenly across workers in order.

    Thread-safe via atomic counter increment.
    """

    def __init__(self) -> None:
        """Initialise the router with a zero counter and a threading lock."""
        self._counter = 0
        self._lock = threading.Lock()

    def select_worker(
        self,
        workers: list[Worker],
        query: str,
    ) -> Worker:
        """Select next worker in round-robin order.

        Args:
            workers: Active workers.
            query: Query string (unused for round-robin).

        Returns:
            The next worker in rotation.

        Raises:
            RuntimeError: If workers list is empty.

        """
        if not workers:
            msg = "No active workers available"
            raise RuntimeError(msg)
        with self._lock:
            idx = self._counter % len(workers)
            self._counter += 1
        return workers[idx]


class LeastLoadedRouter:
    """Routes queries to the worker with fewest active queries.

    Falls back to round-robin when all workers have equal load.
    """

    def select_worker(
        self,
        workers: list[Worker],
        query: str,
    ) -> Worker:
        """Select the least-loaded worker.

        Args:
            workers: Active workers.
            query: Query string (unused).

        Returns:
            The worker with lowest active_queries count.

        Raises:
            RuntimeError: If workers list is empty.

        """
        if not workers:
            msg = "No active workers available"
            raise RuntimeError(msg)
        return min(workers, key=lambda w: w.health_check().active_queries)


# ---------------------------------------------------------------------------
# Local worker implementation (for testing without a cluster)
# ---------------------------------------------------------------------------


class LocalWorker:
    """In-process worker that wraps a Star instance.

    Provides the :class:`Worker` protocol for local testing and
    single-node deployment.

    Args:
        worker_id: Unique identifier for this worker.
        star: Optional pre-configured Star instance.  If ``None``,
            a default Star is created.

    """

    def __init__(self, worker_id: str, star: Any | None = None) -> None:
        """Create a local in-process worker.

        Args:
            worker_id: Unique identifier for this worker.
            star: Optional pre-configured :class:`~pycypher.star.Star`
                instance.  If ``None``, a default Star is created lazily.

        """
        self._worker_id = worker_id
        self._status = WorkerStatus.ACTIVE
        self._queries_executed = 0
        self._errors = 0
        self._total_latency_ms = 0.0
        self._active_queries = 0
        self._last_heartbeat = time.monotonic()
        self._lock = threading.Lock()

        if star is not None:
            self._star = star
        else:
            from pycypher.star import Star

            self._star = Star()

    @property
    def worker_id(self) -> str:
        """Unique identifier for this worker."""
        return self._worker_id

    @property
    def status(self) -> WorkerStatus:
        """Current health status."""
        return self._status

    def execute_query(
        self,
        query: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a query on the wrapped Star instance.

        Args:
            query: Cypher query string.
            parameters: Optional named parameters.

        Returns:
            DataFrame with result rows.

        """
        with self._lock:
            self._active_queries += 1

        t0 = time.perf_counter()
        try:
            result = self._star.execute_query(query, parameters=parameters)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            with self._lock:
                self._queries_executed += 1
                self._total_latency_ms += elapsed_ms
                self._last_heartbeat = time.monotonic()
            return result
        except Exception as exc:  # noqa: BLE001 — wraps any error with worker context; re-raised as WorkerExecutionError
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            snippet = query[:80]
            with self._lock:
                self._errors += 1
            LOGGER.debug(
                "Worker %s query failed after %.1fms: %s",
                self._worker_id,
                elapsed_ms,
                snippet,
                exc_info=True,
            )
            raise WorkerExecutionError(
                worker_id=self._worker_id,
                query_snippet=snippet,
                elapsed_ms=elapsed_ms,
            ) from exc
        finally:
            with self._lock:
                self._active_queries -= 1

    def health_check(self) -> WorkerHealth:
        """Return current health snapshot.

        Returns:
            WorkerHealth with current counters and latency.

        """
        with self._lock:
            total = self._queries_executed
            avg_latency = self._total_latency_ms / total if total > 0 else 0.0
            return WorkerHealth(
                worker_id=self._worker_id,
                status=self._status,
                queries_executed=total,
                errors=self._errors,
                avg_latency_ms=avg_latency,
                last_heartbeat=self._last_heartbeat,
                active_queries=self._active_queries,
            )


# ---------------------------------------------------------------------------
# Cluster coordinator
# ---------------------------------------------------------------------------


@dataclass
class ClusterCoordinator:
    """Central coordinator for distributed query execution.

    Manages worker registration, query routing, and aggregate health
    monitoring.  Thread-safe for concurrent query submission.

    Args:
        router: Query routing strategy.  Defaults to
            :class:`RoundRobinRouter`.
        heartbeat_timeout_s: Seconds after which a worker with no
            heartbeat is marked unavailable.

    """

    router: QueryRouter = field(default_factory=RoundRobinRouter)
    heartbeat_timeout_s: float = 30.0
    _workers: dict[str, Worker] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def register_worker(self, worker: Worker) -> None:
        """Register a worker with the cluster.

        Args:
            worker: A Worker instance to add.

        Raises:
            ValueError: If a worker with the same ID is already registered.

        """
        with self._lock:
            if worker.worker_id in self._workers:
                msg = f"Worker '{worker.worker_id}' already registered"
                raise ValueError(msg)
            self._workers[worker.worker_id] = worker

    def deregister_worker(self, worker_id: str) -> None:
        """Remove a worker from the cluster.

        Args:
            worker_id: ID of the worker to remove.

        Raises:
            ValueError: If worker_id is not registered.

        """
        with self._lock:
            if worker_id not in self._workers:
                registered = list(self._workers.keys())
                msg = (
                    f"Worker '{worker_id}' not registered. "
                    f"Registered workers: {registered}"
                )
                raise ValueError(msg)
            del self._workers[worker_id]

    def _active_workers(self) -> list[Worker]:
        """Return list of workers in ACTIVE status."""
        return [
            w
            for w in self._workers.values()
            if w.status == WorkerStatus.ACTIVE
        ]

    def execute_query(
        self,
        query: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Route and execute a query on a selected worker.

        Args:
            query: Cypher query string.
            parameters: Optional named parameters.

        Returns:
            DataFrame with result rows.

        Raises:
            RuntimeError: If no active workers are available.

        """
        with self._lock:
            active = self._active_workers()
        worker = self.router.select_worker(active, query)
        return worker.execute_query(query, parameters=parameters)

    def cluster_health(self) -> ClusterHealth:
        """Return aggregate health snapshot for the cluster.

        Returns:
            ClusterHealth with per-worker and aggregate metrics.

        """
        with self._lock:
            workers = list(self._workers.values())

        worker_healths = [w.health_check() for w in workers]

        total_queries = sum(wh.queries_executed for wh in worker_healths)
        total_errors = sum(wh.errors for wh in worker_healths)
        total_ops = total_queries + total_errors

        # Weighted average latency by query count.
        weighted_latency = sum(
            wh.avg_latency_ms * wh.queries_executed for wh in worker_healths
        )
        avg_latency = (
            weighted_latency / total_queries if total_queries > 0 else 0.0
        )

        return ClusterHealth(
            total_workers=len(worker_healths),
            active_workers=sum(
                1 for wh in worker_healths if wh.status == WorkerStatus.ACTIVE
            ),
            unavailable_workers=sum(
                1
                for wh in worker_healths
                if wh.status == WorkerStatus.UNAVAILABLE
            ),
            total_queries=total_queries,
            total_errors=total_errors,
            cluster_error_rate=total_errors / total_ops
            if total_ops > 0
            else 0.0,
            avg_latency_ms=avg_latency,
            worker_health=worker_healths,
        )

    @property
    def worker_count(self) -> int:
        """Number of registered workers."""
        with self._lock:
            return len(self._workers)
