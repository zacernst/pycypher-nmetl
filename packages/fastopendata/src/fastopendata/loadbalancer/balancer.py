"""Intelligent query load balancer.

:class:`LoadBalancer` combines query complexity analysis, node health
monitoring, and adaptive routing to dispatch queries to the optimal
node in a distributed cluster. Supports multiple routing strategies
with automatic failover.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from fastopendata.loadbalancer.analyzer import (
    QueryComplexity,
    QueryComplexityAnalyzer,
)
from fastopendata.loadbalancer.monitor import (
    HealthStatus,
    NodeHealth,
    NodeMonitor,
)

_logger = logging.getLogger(__name__)


class RoutingStrategy(Enum):
    """Strategy for selecting a target node."""

    LEAST_LOADED = "least_loaded"
    ROUND_ROBIN = "round_robin"
    LATENCY_AWARE = "latency_aware"
    CAPABILITY_MATCH = "capability_match"
    WEIGHTED_SCORE = "weighted_score"


@dataclass(frozen=True)
class RoutingDecision:
    """Result of the load balancer's routing decision.

    Attributes
    ----------
    target_node_id : str
        Selected node to receive the query.
    strategy_used : RoutingStrategy
        Which strategy produced this decision.
    complexity : QueryComplexity
        Analyzed complexity of the query.
    node_health : NodeHealth
        Health snapshot of the selected node at decision time.
    score : float
        Routing score (lower = better fit).
    alternatives : list[str]
        Fallback node IDs ordered by preference.
    decision_time_ms : float
        Time taken to make the routing decision.

    """

    target_node_id: str
    strategy_used: RoutingStrategy
    complexity: QueryComplexity
    node_health: NodeHealth
    score: float
    alternatives: list[str] = field(default_factory=list)
    decision_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "target_node_id": self.target_node_id,
            "strategy_used": self.strategy_used.value,
            "complexity": self.complexity.to_dict(),
            "node_health": self.node_health.to_dict(),
            "score": round(self.score, 4),
            "alternatives": self.alternatives,
            "decision_time_ms": round(self.decision_time_ms, 3),
        }


class LoadBalancer:
    """Intelligent query load balancer for distributed execution.

    Analyzes incoming queries, evaluates node health, and selects
    the optimal target node using configurable routing strategies.
    Supports automatic failover when the primary target is unavailable.

    Parameters
    ----------
    monitor : NodeMonitor
        Health monitor providing node status.
    analyzer : QueryComplexityAnalyzer | None
        Query complexity analyzer. Created with defaults if not provided.
    strategy : RoutingStrategy
        Default routing strategy.
    load_weight : float
        Weight for load factor in weighted scoring (0.0–1.0).
    latency_weight : float
        Weight for latency factor in weighted scoring (0.0–1.0).
    error_weight : float
        Weight for error rate factor in weighted scoring (0.0–1.0).

    """

    def __init__(
        self,
        monitor: NodeMonitor,
        analyzer: QueryComplexityAnalyzer | None = None,
        *,
        strategy: RoutingStrategy = RoutingStrategy.WEIGHTED_SCORE,
        load_weight: float = 0.4,
        latency_weight: float = 0.35,
        error_weight: float = 0.25,
    ) -> None:
        self._monitor = monitor
        self._analyzer = analyzer or QueryComplexityAnalyzer()
        self._strategy = strategy
        self._load_weight = load_weight
        self._latency_weight = latency_weight
        self._error_weight = error_weight
        self._round_robin_idx = 0
        self._total_routed = 0
        self._total_failovers = 0

    @property
    def monitor(self) -> NodeMonitor:
        """The underlying node health monitor."""
        return self._monitor

    @property
    def total_routed(self) -> int:
        """Total queries routed since creation."""
        return self._total_routed

    @property
    def total_failovers(self) -> int:
        """Total failover events since creation."""
        return self._total_failovers

    def route(
        self,
        query: str,
        *,
        strategy: RoutingStrategy | None = None,
        preferred_node: str | None = None,
    ) -> RoutingDecision:
        """Select the best node for a query.

        Parameters
        ----------
        query : str
            The Cypher query text.
        strategy : RoutingStrategy | None
            Override the default routing strategy for this query.
        preferred_node : str | None
            If set, prefer this node if it's healthy and available.

        Returns
        -------
        RoutingDecision
            The routing decision including target, alternatives, and metadata.

        Raises
        ------
        RuntimeError
            If no available nodes exist.

        """
        t_start = time.monotonic()
        strategy = strategy or self._strategy
        complexity = self._analyzer.analyze(query)

        available = self._monitor.available_nodes()
        if not available:
            msg = "No available nodes for query routing"
            raise RuntimeError(msg)

        # Check preferred node first
        if preferred_node is not None:
            preferred_health = self._monitor.get_health(preferred_node)
            if preferred_health is not None and preferred_health.available:
                ranked = self._rank_nodes(available, complexity, strategy)
                alternatives = [
                    n.node_id for n in ranked if n.node_id != preferred_node
                ][:3]
                self._total_routed += 1
                return RoutingDecision(
                    target_node_id=preferred_node,
                    strategy_used=strategy,
                    complexity=complexity,
                    node_health=preferred_health,
                    score=0.0,
                    alternatives=alternatives,
                    decision_time_ms=(time.monotonic() - t_start) * 1000,
                )

        # Rank nodes by strategy
        ranked = self._rank_nodes(available, complexity, strategy)
        target = ranked[0]
        alternatives = [n.node_id for n in ranked[1:4]]

        self._total_routed += 1
        return RoutingDecision(
            target_node_id=target.node_id,
            strategy_used=strategy,
            complexity=complexity,
            node_health=target,
            score=self._node_score(target, complexity),
            alternatives=alternatives,
            decision_time_ms=(time.monotonic() - t_start) * 1000,
        )

    def failover(self, decision: RoutingDecision) -> RoutingDecision | None:
        """Select an alternative node after the primary target fails.

        Parameters
        ----------
        decision : RoutingDecision
            The original routing decision that failed.

        Returns
        -------
        RoutingDecision | None
            A new routing decision targeting an alternative, or None
            if no alternatives are available.

        """
        self._total_failovers += 1
        for alt_id in decision.alternatives:
            health = self._monitor.get_health(alt_id)
            if health is not None and health.available:
                remaining_alts = [
                    a for a in decision.alternatives if a != alt_id
                ]
                return RoutingDecision(
                    target_node_id=alt_id,
                    strategy_used=decision.strategy_used,
                    complexity=decision.complexity,
                    node_health=health,
                    score=self._node_score(health, decision.complexity),
                    alternatives=remaining_alts,
                    decision_time_ms=0.0,
                )
        return None

    def _rank_nodes(
        self,
        nodes: list[NodeHealth],
        complexity: QueryComplexity,
        strategy: RoutingStrategy,
    ) -> list[NodeHealth]:
        """Rank available nodes according to the routing strategy."""
        if strategy == RoutingStrategy.LEAST_LOADED:
            return sorted(nodes, key=lambda n: n.load)

        if strategy == RoutingStrategy.ROUND_ROBIN:
            # Rotate through available nodes
            self._round_robin_idx = (self._round_robin_idx + 1) % len(nodes)
            rotated = (
                nodes[self._round_robin_idx :] + nodes[: self._round_robin_idx]
            )
            return rotated

        if strategy == RoutingStrategy.LATENCY_AWARE:
            return sorted(nodes, key=lambda n: n.latency_ms)

        # Default: WEIGHTED_SCORE or CAPABILITY_MATCH
        return sorted(
            nodes,
            key=lambda n: self._node_score(n, complexity),
        )

    def _node_score(
        self, node: NodeHealth, complexity: QueryComplexity
    ) -> float:
        """Compute a composite score for a node (lower = better).

        Combines load, latency, and error rate with configurable weights.
        """
        # Normalize latency to 0-1 range (cap at 5000ms)
        norm_latency = min(node.latency_ms / 5000.0, 1.0)
        score = (
            self._load_weight * node.load
            + self._latency_weight * norm_latency
            + self._error_weight * node.error_rate
        )
        # Penalty for degraded nodes
        if node.status == HealthStatus.DEGRADED:
            score += 0.3
        return score
