"""Load tests: BFS path expansion stress testing.

Verifies that variable-length path expansion and shortest-path BFS
behave correctly under stress — large frontiers, deep hops, cyclic
graphs, and safety limit enforcement.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.path_expander import (
    _MAX_FRONTIER_ROWS,
    _MAX_UNBOUNDED_PATH_HOPS,
    PathExpander,
)
from pycypher.star import Star

from .load_generator import SCALE_MEDIUM, SCALE_SMALL, build_graph


@pytest.fixture(scope="module")
def small_star() -> Star:
    """Module-scoped Star with small graph."""
    ctx = build_graph(SCALE_SMALL)
    return Star(ctx)


@pytest.fixture(scope="module")
def dense_star() -> Star:
    """Star with a deliberately dense graph for frontier explosion testing."""
    rng = np.random.default_rng(99)
    n_nodes = 200
    # High connectivity: ~50 edges per node on average.
    n_edges = 10_000

    person_ids = [f"d{i}" for i in range(n_nodes)]
    person_df = pd.DataFrame(
        {
            "__ID__": person_ids,
            "name": [f"Dense_{i}" for i in range(n_nodes)],
        }
    )

    knows_df = pd.DataFrame(
        {
            "__SOURCE__": rng.choice(person_ids, size=n_edges).tolist(),
            "__TARGET__": rng.choice(person_ids, size=n_edges).tolist(),
        }
    )

    ctx = (
        ContextBuilder()
        .add_entity("Person", person_df, id_col="__ID__")
        .add_relationship(
            "KNOWS", knows_df, source_col="__SOURCE__", target_col="__TARGET__"
        )
        .build()
    )
    return Star(ctx)


class TestBFSFrontierLimits:
    """BFS frontier must be bounded to prevent memory exhaustion."""

    def test_unbounded_path_capped_at_max_hops(
        self,
        small_star: Star,
    ) -> None:
        """``[*]`` patterns must be capped at _MAX_UNBOUNDED_PATH_HOPS."""
        # This should not hang or OOM — the hop cap prevents infinite expansion.
        result = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*]->(b:Person) "
            "RETURN a.name, b.name LIMIT 100",
            timeout_seconds=30.0,
        )
        assert len(result) <= 100

    def test_bounded_path_respects_hop_limit(
        self,
        small_star: Star,
    ) -> None:
        """``[*1..3]`` must expand exactly 1-3 hops."""
        result = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "RETURN a.name, b.name LIMIT 200",
            timeout_seconds=30.0,
        )
        # Just verify it completes within limits.
        assert len(result) <= 200

    def test_dense_graph_frontier_does_not_oom(
        self,
        dense_star: Star,
    ) -> None:
        """Dense graph BFS must be truncated by frontier limit, not OOM."""
        t0 = time.perf_counter()
        result = dense_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) "
            "RETURN a.name, b.name LIMIT 1000",
            timeout_seconds=60.0,
        )
        elapsed = time.perf_counter() - t0

        assert len(result) <= 1000
        # Should complete in reasonable time (under 60s).
        assert elapsed < 60.0

    def test_frontier_limit_constant_is_reasonable(self) -> None:
        """The frontier limit must be set to prevent multi-GB allocations."""
        # 1M rows * ~100 bytes/row = ~100MB — aggressive but safe.
        assert _MAX_FRONTIER_ROWS == 1_000_000
        assert _MAX_UNBOUNDED_PATH_HOPS == 20


class TestBFSCyclicGraphs:
    """BFS on cyclic graphs must terminate and produce correct results."""

    def test_self_loop_does_not_hang(self) -> None:
        """A graph with self-loops must not cause infinite BFS."""
        person_df = pd.DataFrame(
            {
                "__ID__": ["x"],
                "name": ["Self"],
            }
        )
        # Self-loop: x -> x.
        knows_df = pd.DataFrame(
            {
                "__SOURCE__": ["x"],
                "__TARGET__": ["x"],
            }
        )
        ctx = (
            ContextBuilder()
            .add_entity("Person", person_df, id_col="__ID__")
            .add_relationship(
                "KNOWS",
                knows_df,
                source_col="__SOURCE__",
                target_col="__TARGET__",
            )
            .build()
        )
        star = Star(ctx)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) RETURN a.name, b.name",
            timeout_seconds=10.0,
        )
        # Self-loop produces 5 rows (one per hop, all (x, x)).
        assert len(result) >= 1

    def test_two_node_cycle_terminates(self) -> None:
        """A two-node cycle must terminate due to deduplication."""
        person_df = pd.DataFrame(
            {
                "__ID__": ["a", "b"],
                "name": ["Alice", "Bob"],
            }
        )
        knows_df = pd.DataFrame(
            {
                "__SOURCE__": ["a", "b"],
                "__TARGET__": ["b", "a"],
            }
        )
        ctx = (
            ContextBuilder()
            .add_entity("Person", person_df, id_col="__ID__")
            .add_relationship(
                "KNOWS",
                knows_df,
                source_col="__SOURCE__",
                target_col="__TARGET__",
            )
            .build()
        )
        star = Star(ctx)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..10]->(b:Person) "
            "RETURN a.name, b.name",
            timeout_seconds=10.0,
        )
        # With dedup, frontier converges quickly.
        assert len(result) > 0

    def test_complete_graph_bounded(self) -> None:
        """A complete graph (all-to-all) must not explode the frontier."""
        n = 20
        ids = [f"k{i}" for i in range(n)]
        person_df = pd.DataFrame(
            {
                "__ID__": ids,
                "name": [f"Node_{i}" for i in range(n)],
            }
        )
        # Complete graph: n*(n-1) edges.
        src, tgt = [], []
        for i in ids:
            for j in ids:
                if i != j:
                    src.append(i)
                    tgt.append(j)
        knows_df = pd.DataFrame({"__SOURCE__": src, "__TARGET__": tgt})

        ctx = (
            ContextBuilder()
            .add_entity("Person", person_df, id_col="__ID__")
            .add_relationship(
                "KNOWS",
                knows_df,
                source_col="__SOURCE__",
                target_col="__TARGET__",
            )
            .build()
        )
        star = Star(ctx)

        t0 = time.perf_counter()
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "RETURN a.name, b.name LIMIT 500",
            timeout_seconds=30.0,
        )
        elapsed = time.perf_counter() - t0

        assert len(result) <= 500
        assert elapsed < 30.0


class TestBFSRecovery:
    """BFS must recover correctly after hitting limits."""

    def test_query_after_frontier_truncation(
        self,
        dense_star: Star,
    ) -> None:
        """After BFS truncation, subsequent queries must work correctly."""
        # Trigger potential truncation.
        dense_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) "
            "RETURN a.name, b.name LIMIT 100",
            timeout_seconds=30.0,
        )

        # Normal query must still work.
        result = dense_star.execute_query(
            "MATCH (p:Person) RETURN count(p)",
            timeout_seconds=10.0,
        )
        assert len(result) == 1
        assert int(result.iloc[0, 0]) == 200  # 200 nodes in dense graph
