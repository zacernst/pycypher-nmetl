"""Dynamic node health monitoring.

:class:`NodeMonitor` tracks the real-time health of database nodes
in the cluster, providing health status, load metrics, and failure
detection used by the load balancer for routing decisions.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class HealthStatus(Enum):
    """Health state of a monitored node."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNREACHABLE = "unreachable"


@dataclass
class NodeHealth:
    """Health snapshot for a single node.

    Attributes
    ----------
    node_id : str
        Identifier of the monitored node.
    status : HealthStatus
        Current health classification.
    load : float
        Current load fraction (0.0–1.0).
    latency_ms : float
        Recent average query latency in ms.
    error_rate : float
        Recent error rate (0.0–1.0).
    active_queries : int
        Number of currently executing queries.
    last_check : float
        Epoch timestamp of last health check.
    consecutive_failures : int
        Number of consecutive failed health checks.
    available : bool
        Whether the node is accepting queries.

    """

    node_id: str
    status: HealthStatus = HealthStatus.HEALTHY
    load: float = 0.0
    latency_ms: float = 0.0
    error_rate: float = 0.0
    active_queries: int = 0
    last_check: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    available: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "load": round(self.load, 4),
            "latency_ms": round(self.latency_ms, 2),
            "error_rate": round(self.error_rate, 4),
            "active_queries": self.active_queries,
            "last_check": self.last_check,
            "consecutive_failures": self.consecutive_failures,
            "available": self.available,
        }


class NodeMonitor:
    """Monitors health of database nodes in the cluster.

    Maintains per-node health state with EMA-smoothed latency and
    error rate. Detects node degradation and failure through
    configurable thresholds.

    Parameters
    ----------
    degraded_latency_ms : float
        Latency above which a node is classified as degraded.
    unhealthy_error_rate : float
        Error rate above which a node is classified as unhealthy.
    unreachable_failures : int
        Consecutive failures before marking a node unreachable.
    ema_alpha : float
        Smoothing factor for EMA metrics (0 < alpha <= 1).

    """

    def __init__(
        self,
        *,
        degraded_latency_ms: float = 500.0,
        unhealthy_error_rate: float = 0.2,
        unreachable_failures: int = 3,
        ema_alpha: float = 0.3,
    ) -> None:
        self._degraded_latency_ms = degraded_latency_ms
        self._unhealthy_error_rate = unhealthy_error_rate
        self._unreachable_failures = unreachable_failures
        self._ema_alpha = ema_alpha
        self._health: dict[str, NodeHealth] = {}
        self._lock = threading.Lock()

    def register_node(self, node_id: str) -> NodeHealth:
        """Register a new node for monitoring.

        Parameters
        ----------
        node_id : str
            The node identifier.

        Returns
        -------
        NodeHealth
            Initial health record.

        """
        with self._lock:
            health = NodeHealth(node_id=node_id)
            self._health[node_id] = health
            return health

    def get_health(self, node_id: str) -> NodeHealth | None:
        """Return current health for a node, or None if unregistered."""
        with self._lock:
            return self._health.get(node_id)

    def all_health(self) -> list[NodeHealth]:
        """Return health snapshots for all monitored nodes."""
        with self._lock:
            return list(self._health.values())

    def available_nodes(self) -> list[NodeHealth]:
        """Return only nodes that are available for queries."""
        with self._lock:
            return [h for h in self._health.values() if h.available]

    def record_success(
        self,
        node_id: str,
        latency_ms: float,
        *,
        active_queries: int | None = None,
    ) -> None:
        """Record a successful query execution on a node.

        Updates EMA-smoothed latency and resets failure counter.

        Parameters
        ----------
        node_id : str
            The node that executed the query.
        latency_ms : float
            Observed query latency.
        active_queries : int | None
            Current active query count, if known.

        """
        with self._lock:
            health = self._health.get(node_id)
            if health is None:
                return
            alpha = self._ema_alpha
            health.latency_ms = alpha * latency_ms + (1 - alpha) * health.latency_ms
            health.error_rate = (1 - alpha) * health.error_rate
            health.consecutive_failures = 0
            health.last_check = time.time()
            if active_queries is not None:
                health.active_queries = active_queries
            self._classify(health)

    def record_failure(self, node_id: str) -> None:
        """Record a failed query or health check on a node.

        Increments error rate and failure counter.

        Parameters
        ----------
        node_id : str
            The node that failed.

        """
        with self._lock:
            health = self._health.get(node_id)
            if health is None:
                return
            alpha = self._ema_alpha
            health.error_rate = alpha * 1.0 + (1 - alpha) * health.error_rate
            health.consecutive_failures += 1
            health.last_check = time.time()
            self._classify(health)

    def update_load(self, node_id: str, load: float) -> None:
        """Update the load metric for a node.

        Parameters
        ----------
        node_id : str
            The node.
        load : float
            Current load fraction (0.0–1.0).

        """
        with self._lock:
            health = self._health.get(node_id)
            if health is None:
                return
            health.load = max(0.0, min(1.0, load))
            self._classify(health)

    def remove_node(self, node_id: str) -> None:
        """Remove a node from monitoring."""
        with self._lock:
            self._health.pop(node_id, None)

    def _classify(self, health: NodeHealth) -> None:
        """Classify node health based on current metrics (caller holds lock)."""
        if health.consecutive_failures >= self._unreachable_failures:
            health.status = HealthStatus.UNREACHABLE
            health.available = False
        elif health.error_rate > self._unhealthy_error_rate:
            health.status = HealthStatus.UNHEALTHY
            health.available = False
        elif health.latency_ms > self._degraded_latency_ms:
            health.status = HealthStatus.DEGRADED
            health.available = True  # Still usable, but prefer others
        else:
            health.status = HealthStatus.HEALTHY
            health.available = True
