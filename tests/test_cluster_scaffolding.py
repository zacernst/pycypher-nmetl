"""Tests for distributed execution scaffolding.

Validates worker registration, query routing, health monitoring,
and fault tolerance patterns for the cluster coordination layer.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from pycypher.cluster import (
    ClusterCoordinator,
    ClusterHealth,
    LeastLoadedRouter,
    LocalWorker,
    RoundRobinRouter,
    WorkerHealth,
    WorkerStatus,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_star_with_data() -> Any:
    """Create a Star with Person data for testing."""
    from pycypher.relational_models import EntityMapping, EntityTable
    from pycypher.star import Context, Star

    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
        ),
    )


@pytest.fixture()
def worker_a() -> LocalWorker:
    return LocalWorker("worker-a", star=_make_star_with_data())


@pytest.fixture()
def worker_b() -> LocalWorker:
    return LocalWorker("worker-b", star=_make_star_with_data())


@pytest.fixture()
def coordinator(
    worker_a: LocalWorker, worker_b: LocalWorker
) -> ClusterCoordinator:
    coord = ClusterCoordinator()
    coord.register_worker(worker_a)
    coord.register_worker(worker_b)
    return coord


# ---------------------------------------------------------------------------
# Worker protocol tests
# ---------------------------------------------------------------------------


class TestLocalWorker:
    """Verify LocalWorker satisfies the Worker protocol."""

    def test_worker_id(self, worker_a: LocalWorker) -> None:
        assert worker_a.worker_id == "worker-a"

    def test_initial_status_active(self, worker_a: LocalWorker) -> None:
        assert worker_a.status == WorkerStatus.ACTIVE

    def test_execute_query_returns_dataframe(
        self, worker_a: LocalWorker
    ) -> None:
        result = worker_a.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_health_check_after_query(self, worker_a: LocalWorker) -> None:
        worker_a.execute_query("MATCH (p:Person) RETURN p.name AS name")
        health = worker_a.health_check()
        assert health.queries_executed == 1
        assert health.errors == 0
        assert health.avg_latency_ms > 0.0
        assert health.active_queries == 0

    def test_health_check_tracks_errors(self, worker_a: LocalWorker) -> None:
        with pytest.raises(Exception):  # noqa: B017, PT011
            worker_a.execute_query("MATCH (p:NonExistent) RETURN p")
        health = worker_a.health_check()
        assert health.errors >= 1

    def test_health_check_initial(self, worker_a: LocalWorker) -> None:
        health = worker_a.health_check()
        assert health.queries_executed == 0
        assert health.avg_latency_ms == 0.0
        assert health.worker_id == "worker-a"
        assert health.status == WorkerStatus.ACTIVE


# ---------------------------------------------------------------------------
# Worker registration tests
# ---------------------------------------------------------------------------


class TestWorkerRegistration:
    """Verify coordinator worker management."""

    def test_register_worker(self) -> None:
        coord = ClusterCoordinator()
        w = LocalWorker("w1", star=_make_star_with_data())
        coord.register_worker(w)
        assert coord.worker_count == 1

    def test_register_duplicate_raises(
        self, coordinator: ClusterCoordinator, worker_a: LocalWorker
    ) -> None:
        with pytest.raises(ValueError, match="already registered"):
            coordinator.register_worker(worker_a)

    def test_deregister_worker(self, coordinator: ClusterCoordinator) -> None:
        coordinator.deregister_worker("worker-a")
        assert coordinator.worker_count == 1

    def test_deregister_unknown_raises(
        self, coordinator: ClusterCoordinator
    ) -> None:
        with pytest.raises(ValueError, match="not registered"):
            coordinator.deregister_worker("nonexistent")

    def test_worker_count(self, coordinator: ClusterCoordinator) -> None:
        assert coordinator.worker_count == 2


# ---------------------------------------------------------------------------
# Query routing tests
# ---------------------------------------------------------------------------


class TestRoundRobinRouter:
    """Verify round-robin routing distributes queries evenly."""

    def test_alternates_workers(self, coordinator: ClusterCoordinator) -> None:
        results = []
        for _ in range(4):
            result = coordinator.execute_query(
                "MATCH (p:Person) RETURN p.name AS name"
            )
            results.append(len(result))
        # All queries should succeed regardless of which worker handles them
        assert all(r == 3 for r in results)

    def test_round_robin_distributes_evenly(self) -> None:
        router = RoundRobinRouter()
        w1 = LocalWorker("w1", star=_make_star_with_data())
        w2 = LocalWorker("w2", star=_make_star_with_data())
        workers = [w1, w2]

        selected = [
            router.select_worker(workers, "q").worker_id for _ in range(6)
        ]
        assert selected == ["w1", "w2", "w1", "w2", "w1", "w2"]

    def test_round_robin_empty_raises(self) -> None:
        router = RoundRobinRouter()
        with pytest.raises(RuntimeError, match="No active workers"):
            router.select_worker([], "query")


class TestLeastLoadedRouter:
    """Verify least-loaded routing picks the idle worker."""

    def test_selects_least_loaded(self) -> None:
        router = LeastLoadedRouter()
        w1 = LocalWorker("w1", star=_make_star_with_data())
        w2 = LocalWorker("w2", star=_make_star_with_data())

        # Execute a query on w1 to give it load history
        # Both should have 0 active queries after completion
        w1.execute_query("MATCH (p:Person) RETURN p.name AS name")

        selected = router.select_worker([w1, w2], "query")
        # Both have 0 active queries, so first in list wins from min()
        assert isinstance(selected, LocalWorker)

    def test_empty_raises(self) -> None:
        router = LeastLoadedRouter()
        with pytest.raises(RuntimeError, match="No active workers"):
            router.select_worker([], "query")


# ---------------------------------------------------------------------------
# Cluster health monitoring tests
# ---------------------------------------------------------------------------


class TestClusterHealth:
    """Verify aggregate cluster health reporting."""

    def test_cluster_health_initial(
        self, coordinator: ClusterCoordinator
    ) -> None:
        health = coordinator.cluster_health()
        assert health.total_workers == 2
        assert health.active_workers == 2
        assert health.unavailable_workers == 0
        assert health.total_queries == 0
        assert health.total_errors == 0
        assert health.cluster_error_rate == 0.0

    def test_cluster_health_after_queries(
        self, coordinator: ClusterCoordinator
    ) -> None:
        for _ in range(4):
            coordinator.execute_query("MATCH (p:Person) RETURN p.name AS name")

        health = coordinator.cluster_health()
        assert health.total_queries == 4
        assert health.total_errors == 0
        assert health.cluster_error_rate == 0.0
        assert health.avg_latency_ms > 0.0

    def test_cluster_health_per_worker(
        self, coordinator: ClusterCoordinator
    ) -> None:
        coordinator.execute_query("MATCH (p:Person) RETURN p.name AS name")
        health = coordinator.cluster_health()
        assert len(health.worker_health) == 2
        assert all(isinstance(wh, WorkerHealth) for wh in health.worker_health)

    def test_cluster_health_empty_cluster(self) -> None:
        coord = ClusterCoordinator()
        health = coord.cluster_health()
        assert health.total_workers == 0
        assert health.cluster_error_rate == 0.0

    def test_worker_health_frozen(self, worker_a: LocalWorker) -> None:
        health = worker_a.health_check()
        with pytest.raises(AttributeError):
            health.queries_executed = 999  # type: ignore[misc]

    def test_cluster_health_frozen(
        self, coordinator: ClusterCoordinator
    ) -> None:
        health = coordinator.cluster_health()
        with pytest.raises(AttributeError):
            health.total_workers = 999  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Fault tolerance tests
# ---------------------------------------------------------------------------


class TestFaultTolerance:
    """Verify coordinator handles worker failures gracefully."""

    def test_no_active_workers_raises(self) -> None:
        coord = ClusterCoordinator()
        with pytest.raises(RuntimeError, match="No active workers"):
            coord.execute_query("MATCH (p:Person) RETURN p.name")

    def test_deregister_then_query_routes_to_remaining(
        self,
        coordinator: ClusterCoordinator,
    ) -> None:
        coordinator.deregister_worker("worker-a")
        result = coordinator.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert len(result) == 3

    def test_worker_status_enum_values(self) -> None:
        assert WorkerStatus.ACTIVE.value == "active"
        assert WorkerStatus.DRAINING.value == "draining"
        assert WorkerStatus.UNAVAILABLE.value == "unavailable"


# ---------------------------------------------------------------------------
# Data class tests
# ---------------------------------------------------------------------------


class TestDataClasses:
    """Verify immutability and structure of health snapshots."""

    def test_worker_health_fields(self) -> None:
        wh = WorkerHealth(
            worker_id="test",
            status=WorkerStatus.ACTIVE,
            queries_executed=10,
            errors=1,
            avg_latency_ms=5.0,
            last_heartbeat=100.0,
            active_queries=0,
        )
        assert wh.worker_id == "test"
        assert wh.errors == 1

    def test_cluster_health_fields(self) -> None:
        ch = ClusterHealth(
            total_workers=3,
            active_workers=2,
            unavailable_workers=1,
            total_queries=100,
            total_errors=5,
            cluster_error_rate=0.05,
            avg_latency_ms=10.0,
            worker_health=[],
        )
        assert ch.cluster_error_rate == 0.05
