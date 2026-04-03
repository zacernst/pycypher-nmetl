"""Tests for fastopendata.loadbalancer — query routing and load balancing."""

from __future__ import annotations

import pytest
from fastopendata.loadbalancer.analyzer import (
    ComplexityTier,
    QueryComplexityAnalyzer,
)
from fastopendata.loadbalancer.balancer import (
    LoadBalancer,
    RoutingStrategy,
)
from fastopendata.loadbalancer.monitor import (
    HealthStatus,
    NodeHealth,
    NodeMonitor,
)

# ── QueryComplexityAnalyzer ──────────────────────────────────────────


class TestQueryComplexityAnalyzer:
    def setup_method(self) -> None:
        self.analyzer = QueryComplexityAnalyzer()

    def test_simple_match_return(self) -> None:
        c = self.analyzer.analyze("MATCH (n:Person) RETURN n")
        assert c.pattern_count >= 1
        assert c.join_count == 0
        assert "scan" in c.capabilities_needed

    def test_match_with_relationship(self) -> None:
        c = self.analyzer.analyze(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
        )
        assert c.join_count >= 1
        assert "join" in c.capabilities_needed

    def test_aggregation_detected(self) -> None:
        c = self.analyzer.analyze("MATCH (n:Person) RETURN COUNT(n)")
        assert c.has_aggregation
        assert "aggregate" in c.capabilities_needed

    def test_order_by_detected(self) -> None:
        c = self.analyzer.analyze(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name",
        )
        assert c.has_ordering
        assert "sort" in c.capabilities_needed

    def test_limit_detected(self) -> None:
        c = self.analyzer.analyze("MATCH (n:Person) RETURN n LIMIT 10")
        assert c.has_limit
        assert c.estimated_rows == 10

    def test_limit_caps_row_estimate(self) -> None:
        c = self.analyzer.analyze("MATCH (n) RETURN n LIMIT 5")
        assert c.estimated_rows == 5

    def test_where_detected(self) -> None:
        c = self.analyzer.analyze(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n",
        )
        assert c.filter_count >= 1
        assert "filter" in c.capabilities_needed

    def test_complex_query_high_score(self) -> None:
        c = self.analyzer.analyze(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City) "
            "WHERE a.age > 25 "
            "RETURN a.name, COUNT(b) ORDER BY a.name",
        )
        assert c.score > 20
        assert c.tier in (
            ComplexityTier.MODERATE,
            ComplexityTier.HEAVY,
            ComplexityTier.EXTREME,
        )

    def test_trivial_query_low_score(self) -> None:
        c = self.analyzer.analyze("MATCH (n) RETURN n LIMIT 1")
        assert c.tier in (ComplexityTier.TRIVIAL, ComplexityTier.LIGHT)

    def test_optional_match_increases_complexity(self) -> None:
        c1 = self.analyzer.analyze("MATCH (n:Person) RETURN n")
        c2 = self.analyzer.analyze(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m",
        )
        assert c2.score > c1.score

    def test_multiple_match_clauses(self) -> None:
        c = self.analyzer.analyze(
            "MATCH (a:Person) MATCH (b:City) RETURN a, b",
        )
        assert c.pattern_count >= 2

    def test_to_dict(self) -> None:
        c = self.analyzer.analyze("MATCH (n) RETURN n")
        d = c.to_dict()
        assert "tier" in d
        assert "score" in d
        assert "capabilities_needed" in d
        assert isinstance(d["capabilities_needed"], list)

    def test_all_aggregation_functions(self) -> None:
        for func in ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"]:
            c = self.analyzer.analyze(f"MATCH (n) RETURN {func}(n.val)")
            assert c.has_aggregation, f"{func} not detected"

    def test_score_minimum_is_one(self) -> None:
        # Even with heavy filtering, score should not go below 1.0
        c = self.analyzer.analyze(
            "MATCH (n) WHERE n.a > 1 WHERE n.b > 2 WHERE n.c > 3 RETURN n LIMIT 1",
        )
        assert c.score >= 1.0

    def test_custom_base_row_estimate(self) -> None:
        analyzer = QueryComplexityAnalyzer(base_row_estimate=500)
        c = analyzer.analyze("MATCH (n) RETURN n")
        assert c.estimated_rows == 500

    def test_limit_does_not_exceed_base(self) -> None:
        c = self.analyzer.analyze("MATCH (n) RETURN n LIMIT 50000")
        assert c.estimated_rows == 1000  # Capped by base estimate


# ── ComplexityTier scoring ───────────────────────────────────────────


class TestComplexityTier:
    def test_score_tiers(self) -> None:
        assert (
            QueryComplexityAnalyzer._score_to_tier(3.0)
            == ComplexityTier.TRIVIAL
        )
        assert (
            QueryComplexityAnalyzer._score_to_tier(10.0)
            == ComplexityTier.LIGHT
        )
        assert (
            QueryComplexityAnalyzer._score_to_tier(25.0)
            == ComplexityTier.MODERATE
        )
        assert (
            QueryComplexityAnalyzer._score_to_tier(50.0)
            == ComplexityTier.HEAVY
        )
        assert (
            QueryComplexityAnalyzer._score_to_tier(100.0)
            == ComplexityTier.EXTREME
        )


# ── NodeMonitor ──────────────────────────────────────────────────────


class TestNodeMonitor:
    def test_register_node(self) -> None:
        mon = NodeMonitor()
        h = mon.register_node("node-1")
        assert h.node_id == "node-1"
        assert h.status == HealthStatus.HEALTHY
        assert h.available

    def test_get_health(self) -> None:
        mon = NodeMonitor()
        mon.register_node("node-1")
        h = mon.get_health("node-1")
        assert h is not None
        assert h.node_id == "node-1"

    def test_get_health_unknown(self) -> None:
        mon = NodeMonitor()
        assert mon.get_health("unknown") is None

    def test_record_success(self) -> None:
        mon = NodeMonitor()
        mon.register_node("node-1")
        mon.record_success("node-1", latency_ms=50.0)
        h = mon.get_health("node-1")
        assert h is not None
        assert h.latency_ms > 0
        assert h.status == HealthStatus.HEALTHY

    def test_record_failure_degrades(self) -> None:
        mon = NodeMonitor(unreachable_failures=3)
        mon.register_node("node-1")
        mon.record_failure("node-1")
        mon.record_failure("node-1")
        h = mon.get_health("node-1")
        assert h is not None
        assert h.consecutive_failures == 2

    def test_unreachable_after_failures(self) -> None:
        mon = NodeMonitor(unreachable_failures=3)
        mon.register_node("node-1")
        for _ in range(3):
            mon.record_failure("node-1")
        h = mon.get_health("node-1")
        assert h is not None
        assert h.status == HealthStatus.UNREACHABLE
        assert not h.available

    def test_success_resets_failures(self) -> None:
        mon = NodeMonitor(unreachable_failures=5)
        mon.register_node("node-1")
        mon.record_failure("node-1")
        mon.record_failure("node-1")
        mon.record_success("node-1", latency_ms=10.0)
        h = mon.get_health("node-1")
        assert h is not None
        assert h.consecutive_failures == 0

    def test_high_error_rate_unhealthy(self) -> None:
        mon = NodeMonitor(unhealthy_error_rate=0.2)
        mon.register_node("node-1")
        # Repeated failures push error rate above threshold
        for _ in range(5):
            mon.record_failure("node-1")
        h = mon.get_health("node-1")
        assert h is not None
        assert h.status in (HealthStatus.UNHEALTHY, HealthStatus.UNREACHABLE)

    def test_high_latency_degraded(self) -> None:
        mon = NodeMonitor(degraded_latency_ms=100.0)
        mon.register_node("node-1")
        # Push latency above threshold with EMA
        for _ in range(20):
            mon.record_success("node-1", latency_ms=500.0)
        h = mon.get_health("node-1")
        assert h is not None
        assert h.status == HealthStatus.DEGRADED
        assert h.available  # Degraded is still usable

    def test_available_nodes(self) -> None:
        mon = NodeMonitor(unreachable_failures=2)
        mon.register_node("node-1")
        mon.register_node("node-2")
        # Make node-2 unreachable
        mon.record_failure("node-2")
        mon.record_failure("node-2")
        available = mon.available_nodes()
        assert len(available) == 1
        assert available[0].node_id == "node-1"

    def test_all_health(self) -> None:
        mon = NodeMonitor()
        mon.register_node("a")
        mon.register_node("b")
        assert len(mon.all_health()) == 2

    def test_remove_node(self) -> None:
        mon = NodeMonitor()
        mon.register_node("node-1")
        mon.remove_node("node-1")
        assert mon.get_health("node-1") is None

    def test_update_load(self) -> None:
        mon = NodeMonitor()
        mon.register_node("node-1")
        mon.update_load("node-1", 0.75)
        h = mon.get_health("node-1")
        assert h is not None
        assert h.load == 0.75

    def test_load_clamped(self) -> None:
        mon = NodeMonitor()
        mon.register_node("node-1")
        mon.update_load("node-1", 1.5)
        h = mon.get_health("node-1")
        assert h is not None
        assert h.load == 1.0

    def test_record_success_with_active_queries(self) -> None:
        mon = NodeMonitor()
        mon.register_node("node-1")
        mon.record_success("node-1", latency_ms=10.0, active_queries=3)
        h = mon.get_health("node-1")
        assert h is not None
        assert h.active_queries == 3

    def test_to_dict(self) -> None:
        h = NodeHealth(node_id="test", status=HealthStatus.HEALTHY)
        d = h.to_dict()
        assert d["node_id"] == "test"
        assert d["status"] == "healthy"
        assert "load" in d


# ── LoadBalancer ─────────────────────────────────────────────────────


def _setup_cluster(
    n_nodes: int = 3,
) -> tuple[NodeMonitor, LoadBalancer]:
    """Helper to create a monitor with registered nodes and a balancer."""
    mon = NodeMonitor()
    for i in range(n_nodes):
        mon.register_node(f"node-{i}")
    balancer = LoadBalancer(mon)
    return mon, balancer


class TestLoadBalancer:
    def test_route_basic(self) -> None:
        mon, lb = _setup_cluster(3)
        decision = lb.route("MATCH (n) RETURN n")
        assert decision.target_node_id.startswith("node-")
        assert decision.complexity.pattern_count >= 1
        assert decision.decision_time_ms >= 0

    def test_route_no_available_nodes(self) -> None:
        mon = NodeMonitor(unreachable_failures=1)
        mon.register_node("node-0")
        mon.record_failure("node-0")
        lb = LoadBalancer(mon)
        with pytest.raises(RuntimeError, match="No available nodes"):
            lb.route("MATCH (n) RETURN n")

    def test_route_least_loaded(self) -> None:
        mon, lb = _setup_cluster(3)
        mon.update_load("node-0", 0.8)
        mon.update_load("node-1", 0.2)
        mon.update_load("node-2", 0.5)
        decision = lb.route(
            "MATCH (n) RETURN n",
            strategy=RoutingStrategy.LEAST_LOADED,
        )
        assert decision.target_node_id == "node-1"

    def test_route_latency_aware(self) -> None:
        mon, lb = _setup_cluster(3)
        for _ in range(10):
            mon.record_success("node-0", latency_ms=200.0)
            mon.record_success("node-1", latency_ms=10.0)
            mon.record_success("node-2", latency_ms=100.0)
        decision = lb.route(
            "MATCH (n) RETURN n",
            strategy=RoutingStrategy.LATENCY_AWARE,
        )
        assert decision.target_node_id == "node-1"

    def test_route_round_robin(self) -> None:
        mon, lb = _setup_cluster(3)
        targets = set()
        for _ in range(6):
            d = lb.route(
                "MATCH (n) RETURN n", strategy=RoutingStrategy.ROUND_ROBIN
            )
            targets.add(d.target_node_id)
        # Should have hit all 3 nodes
        assert len(targets) == 3

    def test_route_preferred_node(self) -> None:
        mon, lb = _setup_cluster(3)
        decision = lb.route("MATCH (n) RETURN n", preferred_node="node-2")
        assert decision.target_node_id == "node-2"

    def test_preferred_node_unavailable_falls_back(self) -> None:
        mon = NodeMonitor(unreachable_failures=1)
        for i in range(3):
            mon.register_node(f"node-{i}")
        mon.record_failure("node-2")
        lb = LoadBalancer(mon)
        decision = lb.route("MATCH (n) RETURN n", preferred_node="node-2")
        assert decision.target_node_id != "node-2"

    def test_alternatives_provided(self) -> None:
        mon, lb = _setup_cluster(5)
        decision = lb.route("MATCH (n) RETURN n")
        assert len(decision.alternatives) >= 1
        assert decision.target_node_id not in decision.alternatives

    def test_failover(self) -> None:
        mon, lb = _setup_cluster(3)
        decision = lb.route("MATCH (n) RETURN n")
        # Simulate primary failure
        mon.record_failure(decision.target_node_id)
        new_decision = lb.failover(decision)
        assert new_decision is not None
        assert new_decision.target_node_id != decision.target_node_id

    def test_failover_no_alternatives(self) -> None:
        mon = NodeMonitor(unreachable_failures=1)
        mon.register_node("node-0")
        lb = LoadBalancer(mon)
        decision = lb.route("MATCH (n) RETURN n")
        assert decision.alternatives == []
        result = lb.failover(decision)
        assert result is None

    def test_failover_increments_counter(self) -> None:
        mon, lb = _setup_cluster(3)
        decision = lb.route("MATCH (n) RETURN n")
        assert lb.total_failovers == 0
        lb.failover(decision)
        assert lb.total_failovers == 1

    def test_total_routed_increments(self) -> None:
        mon, lb = _setup_cluster(2)
        assert lb.total_routed == 0
        lb.route("MATCH (n) RETURN n")
        lb.route("MATCH (n) RETURN n")
        assert lb.total_routed == 2

    def test_weighted_score_prefers_healthy(self) -> None:
        mon, lb = _setup_cluster(3)
        # Make node-0 degraded
        for _ in range(20):
            mon.record_success("node-0", latency_ms=2000.0)
        mon.record_success("node-1", latency_ms=10.0)
        mon.record_success("node-2", latency_ms=10.0)
        decision = lb.route(
            "MATCH (n) RETURN n",
            strategy=RoutingStrategy.WEIGHTED_SCORE,
        )
        # Should not pick the degraded node
        assert decision.target_node_id != "node-0"

    def test_to_dict(self) -> None:
        mon, lb = _setup_cluster(2)
        decision = lb.route("MATCH (n) RETURN n")
        d = decision.to_dict()
        assert "target_node_id" in d
        assert "strategy_used" in d
        assert "complexity" in d
        assert "node_health" in d
        assert "alternatives" in d

    def test_complex_query_routing(self) -> None:
        mon, lb = _setup_cluster(3)
        decision = lb.route(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.age > 25 "
            "RETURN a.name, COUNT(b) ORDER BY a.name",
        )
        assert decision.complexity.join_count >= 1
        assert decision.complexity.has_aggregation
        assert decision.complexity.has_ordering

    def test_capability_match_strategy(self) -> None:
        mon, lb = _setup_cluster(3)
        decision = lb.route(
            "MATCH (n) RETURN n",
            strategy=RoutingStrategy.CAPABILITY_MATCH,
        )
        assert decision.target_node_id.startswith("node-")
