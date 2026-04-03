"""Tests for the swarm intelligence query processing module.

Tests cover:
- Core data structures (SwarmNode, SwarmTopology, EmergentMetrics)
- Ant Colony Optimization for query routing
- Artificial Bee Colony for load balancing
- Particle Swarm Optimization for join ordering
- Emergent behavior properties (convergence, self-organization)
"""

from __future__ import annotations

import pytest
from fastopendata.swarm.ant_colony import (
    AntColonyOptimizer,
    AntColonyResult,
    Pheromone,
    QueryAnt,
)
from fastopendata.swarm.bee_colony import (
    BeeColonyOptimizer,
    BeeColonyResult,
    FoodSource,
    WorkerBee,
)
from fastopendata.swarm.core import (
    EmergentMetrics,
    NodeCapability,
    SwarmConfig,
    SwarmNode,
    SwarmTopology,
)
from fastopendata.swarm.particle_swarm import (
    JoinParticle,
    ParticleSwarmOptimizer,
    ParticleSwarmResult,
    SwarmVelocity,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def basic_topology() -> SwarmTopology:
    """Create a small ring topology with 5 heterogeneous nodes."""
    nodes = [
        SwarmNode(
            node_id=f"node_{i}",
            capabilities={NodeCapability.SCAN, NodeCapability.FILTER}
            if i % 2 == 0
            else {NodeCapability.JOIN, NodeCapability.AGGREGATE},
            latency_ms=5.0 + i * 2.0,
            max_concurrent=4,
        )
        for i in range(5)
    ]
    topo = SwarmTopology()
    topo.build_ring(nodes)
    return topo


@pytest.fixture
def mesh_topology() -> SwarmTopology:
    """Create a mesh topology with 8 nodes."""
    nodes = [
        SwarmNode(
            node_id=f"mesh_{i}",
            capabilities=set(NodeCapability),
            latency_ms=3.0 + i,
            max_concurrent=6,
        )
        for i in range(8)
    ]
    topo = SwarmTopology()
    topo.build_mesh(nodes, k=3)
    return topo


@pytest.fixture
def config() -> SwarmConfig:
    """Small config for fast tests."""
    return SwarmConfig(
        ant_colony_size=10,
        ant_iterations=15,
        evaporation_rate=0.2,
        bee_employed_count=8,
        bee_onlooker_count=8,
        bee_scout_limit=5,
        particle_count=10,
        pso_iterations=20,
    )


# ---------------------------------------------------------------------------
# Core: SwarmNode
# ---------------------------------------------------------------------------


class TestSwarmNode:
    def test_default_creation(self) -> None:
        node = SwarmNode()
        assert node.node_id
        assert node.load == 0.0
        assert not node.is_saturated

    def test_utilization(self) -> None:
        node = SwarmNode(active_queries=4, max_concurrent=8)
        assert node.utilization == pytest.approx(0.5)

    def test_accept_and_release(self) -> None:
        node = SwarmNode(max_concurrent=2)
        assert node.accept_query()
        assert node.active_queries == 1
        assert node.accept_query()
        assert node.active_queries == 2
        assert not node.accept_query()  # saturated
        node.release_query(10.0)
        assert node.active_queries == 1
        assert not node.is_saturated

    def test_latency_ema(self) -> None:
        node = SwarmNode(latency_ms=10.0, max_concurrent=4)
        node.accept_query()
        node.release_query(40.0)
        # EMA: 0.3 * 40 + 0.7 * 10 = 19.0
        assert node.latency_ms == pytest.approx(19.0)


# ---------------------------------------------------------------------------
# Core: SwarmTopology
# ---------------------------------------------------------------------------


class TestSwarmTopology:
    def test_ring_topology(self, basic_topology: SwarmTopology) -> None:
        assert basic_topology.size == 5
        assert len(basic_topology.edges) == 5
        # Each node should have exactly 2 neighbors in a ring
        for node in basic_topology.nodes.values():
            assert len(node.neighbor_ids) == 2

    def test_mesh_topology(self, mesh_topology: SwarmTopology) -> None:
        assert mesh_topology.size == 8
        for node in mesh_topology.nodes.values():
            assert len(node.neighbor_ids) >= 3

    def test_neighbors_of(self, basic_topology: SwarmTopology) -> None:
        neighbors = basic_topology.neighbors_of("node_0")
        assert len(neighbors) == 2
        neighbor_ids = {n.node_id for n in neighbors}
        assert "node_1" in neighbor_ids
        assert "node_4" in neighbor_ids

    def test_connectivity(self, basic_topology: SwarmTopology) -> None:
        assert 0.0 < basic_topology.connectivity <= 1.0

    def test_self_connect_ignored(self) -> None:
        topo = SwarmTopology()
        node = SwarmNode(node_id="solo")
        topo.add_node(node)
        topo.connect("solo", "solo")
        assert len(topo.edges) == 0


# ---------------------------------------------------------------------------
# Core: EmergentMetrics
# ---------------------------------------------------------------------------


class TestEmergentMetrics:
    def test_load_entropy_uniform(self) -> None:
        metrics = EmergentMetrics()
        # Uniform distribution: entropy = log2(4) = 2.0
        entropy = metrics.compute_load_entropy([0.25, 0.25, 0.25, 0.25])
        assert entropy == pytest.approx(2.0)

    def test_load_entropy_skewed(self) -> None:
        metrics = EmergentMetrics()
        # All load on one node: entropy = 0
        entropy = metrics.compute_load_entropy([1.0, 0.0, 0.0, 0.0])
        assert entropy == pytest.approx(0.0)

    def test_route_convergence(self) -> None:
        metrics = EmergentMetrics()
        conv = metrics.compute_route_convergence([10.0, 1.0, 1.0])
        assert conv == pytest.approx(10.0 / 12.0)

    def test_snapshot(self) -> None:
        metrics = EmergentMetrics()
        metrics.compute_load_entropy([0.5, 0.5])
        snap = metrics.snapshot()
        assert "load_balance_entropy" in snap
        assert len(metrics.history) == 1


# ---------------------------------------------------------------------------
# Pheromone
# ---------------------------------------------------------------------------


class TestPheromone:
    def test_deposit_and_cap(self) -> None:
        p = Pheromone(max_intensity=5.0, intensity=4.0)
        p.deposit(3.0)
        assert p.intensity == 5.0  # capped
        assert p.deposit_count == 1

    def test_evaporate_with_floor(self) -> None:
        p = Pheromone(intensity=0.2, min_intensity=0.1)
        p.evaporate(0.8)
        assert p.intensity == pytest.approx(0.1)  # floored

    def test_edge_key(self) -> None:
        p = Pheromone(source_id="a", target_id="b")
        assert p.edge_key == ("a", "b")


# ---------------------------------------------------------------------------
# QueryAnt
# ---------------------------------------------------------------------------


class TestQueryAnt:
    def test_fitness_when_found(self) -> None:
        ant = QueryAnt(path=["a", "b", "c"], path_cost=10.0, found_target=True)
        assert ant.fitness == pytest.approx(0.1)
        assert ant.path_length == 2

    def test_fitness_when_not_found(self) -> None:
        ant = QueryAnt(path=["a", "b"], path_cost=5.0, found_target=False)
        assert ant.fitness == 0.0


# ---------------------------------------------------------------------------
# Ant Colony Optimizer
# ---------------------------------------------------------------------------


class TestAntColonyOptimizer:
    def test_optimize_finds_route(
        self,
        basic_topology: SwarmTopology,
        config: SwarmConfig,
    ) -> None:
        optimizer = AntColonyOptimizer(basic_topology, config)
        result = optimizer.optimize("node_0", NodeCapability.JOIN)
        assert isinstance(result, AntColonyResult)
        assert result.best_route[0] == "node_0"
        assert result.elapsed_ms > 0

    def test_optimize_convergence(
        self,
        mesh_topology: SwarmTopology,
        config: SwarmConfig,
    ) -> None:
        optimizer = AntColonyOptimizer(mesh_topology, config)
        result = optimizer.optimize("mesh_0", NodeCapability.AGGREGATE)
        assert result.found_target
        assert result.route_convergence > 0

    def test_pheromone_initialization(
        self,
        basic_topology: SwarmTopology,
    ) -> None:
        optimizer = AntColonyOptimizer(basic_topology)
        # Should have pheromone on both directions of each edge
        assert len(optimizer.pheromone_matrix) == 2 * len(basic_topology.edges)


# ---------------------------------------------------------------------------
# Food Source and Worker Bee
# ---------------------------------------------------------------------------


class TestFoodSource:
    def test_stagnation_detection(self) -> None:
        source = FoodSource(fitness=1.0, best_fitness=2.0, trial_count=5)
        assert source.is_stagnant


class TestWorkerBee:
    def test_default_role(self) -> None:
        bee = WorkerBee()
        assert bee.role == "employed"
        assert bee.discoveries == 0


# ---------------------------------------------------------------------------
# Bee Colony Optimizer
# ---------------------------------------------------------------------------


class TestBeeColonyOptimizer:
    def test_optimize_load_balance(
        self,
        mesh_topology: SwarmTopology,
        config: SwarmConfig,
    ) -> None:
        optimizer = BeeColonyOptimizer(mesh_topology, config)
        partitions = [f"part_{i}" for i in range(12)]
        costs = {p: 1.0 + (i % 3) for i, p in enumerate(partitions)}
        result = optimizer.optimize(partitions, costs, iterations=10)

        assert isinstance(result, BeeColonyResult)
        assert result.best_fitness > 0
        assert len(result.best_allocation) == len(partitions)
        assert result.elapsed_ms > 0

    def test_single_node_allocation(self, config: SwarmConfig) -> None:
        topo = SwarmTopology()
        topo.add_node(SwarmNode(node_id="only"))
        optimizer = BeeColonyOptimizer(topo, config)
        result = optimizer.optimize(
            ["p1", "p2"], {"p1": 1.0, "p2": 1.0}, iterations=5
        )
        # All partitions must go to the only node
        assert all(v == "only" for v in result.best_allocation.values())


# ---------------------------------------------------------------------------
# SwarmVelocity
# ---------------------------------------------------------------------------


class TestSwarmVelocity:
    def test_apply_swaps(self) -> None:
        v = SwarmVelocity(swaps=[(0, 2)])
        result = v.apply_to(["a", "b", "c"])
        assert result == ["c", "b", "a"]

    def test_difference_identity(self) -> None:
        perm = ["a", "b", "c", "d"]
        v = SwarmVelocity.difference(perm, perm)
        assert v.magnitude == 0

    def test_difference_reversal(self) -> None:
        a = ["a", "b", "c"]
        b = ["c", "b", "a"]
        v = SwarmVelocity.difference(a, b)
        result = v.apply_to(b)
        assert result == a

    def test_scale_zero(self) -> None:
        v = SwarmVelocity(swaps=[(0, 1), (1, 2), (2, 3)])
        scaled = v.scale(0.0)
        assert scaled.magnitude == 0

    def test_combine(self) -> None:
        v1 = SwarmVelocity(swaps=[(0, 1)])
        v2 = SwarmVelocity(swaps=[(2, 3)])
        combined = v1.combine(v2)
        assert combined.magnitude == 2


# ---------------------------------------------------------------------------
# JoinParticle
# ---------------------------------------------------------------------------


class TestJoinParticle:
    def test_has_improved(self) -> None:
        p = JoinParticle(stagnation_count=0)
        assert p.has_improved
        p.stagnation_count = 3
        assert not p.has_improved


# ---------------------------------------------------------------------------
# Particle Swarm Optimizer
# ---------------------------------------------------------------------------


class TestParticleSwarmOptimizer:
    def test_optimize_trivial(self) -> None:
        optimizer = ParticleSwarmOptimizer()
        result = optimizer.optimize(["A"])
        assert result.best_join_order == ["A"]
        assert result.iterations_run == 0

    def test_optimize_small(self, config: SwarmConfig) -> None:
        optimizer = ParticleSwarmOptimizer(config=config)
        keys = ["Person", "KNOWS", "Movie", "ACTED_IN"]
        cards = {
            "Person": 1000.0,
            "KNOWS": 5000.0,
            "Movie": 500.0,
            "ACTED_IN": 3000.0,
        }
        result = optimizer.optimize(keys, cards, iterations=15)

        assert isinstance(result, ParticleSwarmResult)
        assert set(result.best_join_order) == set(keys)
        assert result.best_fitness > 0
        assert result.elapsed_ms > 0

    def test_prefers_small_tables_first(self) -> None:
        """PSO should tend to place small cardinality tables first."""
        config = SwarmConfig(particle_count=20, pso_iterations=30)
        optimizer = ParticleSwarmOptimizer(config=config)
        keys = ["Huge", "Tiny", "Medium"]
        cards = {"Huge": 10000.0, "Tiny": 10.0, "Medium": 500.0}
        result = optimizer.optimize(keys, cards)

        # The optimal order places Tiny first to minimize intermediates.
        # Not guaranteed due to stochastic nature, but likely.
        assert result.best_join_order[0] in ("Tiny", "Medium")

    def test_convergence_metrics(self, config: SwarmConfig) -> None:
        optimizer = ParticleSwarmOptimizer(config=config)
        result = optimizer.optimize(
            ["A", "B", "C"],
            {"A": 100.0, "B": 200.0, "C": 50.0},
        )
        assert 0.0 <= result.swarm_coherence <= 1.0
        assert result.iterations_run > 0


# ---------------------------------------------------------------------------
# Integration: Swarm module imports
# ---------------------------------------------------------------------------


class TestSwarmImports:
    def test_all_public_symbols_importable(self) -> None:
        from fastopendata import swarm

        for name in swarm.__all__:
            assert hasattr(swarm, name), f"Missing export: {name}"
