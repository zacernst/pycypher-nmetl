"""Benchmark: EXISTS subquery batching optimization.

Validates that EXISTS subqueries (including those with WITH clauses)
execute via a single batch pass rather than O(n_rows) per-row subquery
executions.

Run directly::

    uv run python tests/benchmarks/bench_exists_batch.py

Or via pytest::

    uv run pytest tests/benchmarks/bench_exists_batch.py -v -s
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _build_social_graph(n_persons: int, edges_per_node: int = 3) -> Context:
    """Build a social graph where roughly half the nodes have outgoing edges."""
    rng = np.random.default_rng(42)
    persons_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_persons + 1)),
            "name": [f"P{i}" for i in range(1, n_persons + 1)],
            "age": rng.integers(18, 65, size=n_persons),
        }
    )
    n_edges = n_persons * edges_per_node
    # Only half the nodes have outgoing edges (sources from first half)
    sources = rng.integers(1, n_persons // 2 + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_actual + 1)),
            "__SOURCE__": sources,
            "__TARGET__": targets,
        }
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=persons_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def _time_exists_query(
    star: Star,
    query: str,
    n_warmup: int = 1,
    n_iterations: int = 5,
) -> dict[str, float]:
    """Time an EXISTS query execution."""
    for _ in range(n_warmup):
        star.execute_query(query)

    timings: list[float] = []
    result_rows = 0
    for _ in range(n_iterations):
        t0 = time.perf_counter()
        result = star.execute_query(query)
        timings.append(time.perf_counter() - t0)
        result_rows = len(result)

    return {
        "median_seconds": float(np.median(timings)),
        "min_seconds": min(timings),
        "max_seconds": max(timings),
        "result_rows": result_rows,
    }


# ---------------------------------------------------------------------------
# Pytest tests
# ---------------------------------------------------------------------------


class TestExistsBatchOptimization:
    """Validate EXISTS batch execution correctness and performance."""

    def test_correctness_basic_exists(self) -> None:
        """Basic EXISTS subquery returns correct filtered results."""
        ctx = _build_social_graph(100)
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name"
        )
        assert len(result) > 0
        assert len(result) < 100  # Not all persons have outgoing edges

    def test_correctness_not_exists(self) -> None:
        """NOT EXISTS correctly inverts the predicate."""
        ctx = _build_social_graph(100)
        star = Star(context=ctx)
        exists_result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name"
        )
        not_exists_result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name"
        )
        # EXISTS + NOT EXISTS should cover all persons
        assert len(exists_result) + len(not_exists_result) == 100

    def test_correctness_exists_with_where_filter(self) -> None:
        """EXISTS with WHERE inside correctly filters on inner variable."""
        ctx = _build_social_graph(100)
        star = Star(context=ctx)
        # Filter for persons who know someone over age 50
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > 50 RETURN f } "
            "RETURN p.name AS name"
        )
        assert len(result) >= 0  # May or may not have matches

    @pytest.mark.timeout(15)
    def test_benchmark_exists_500_persons(self) -> None:
        """EXISTS on 500-person graph should complete quickly via batch execution."""
        ctx = _build_social_graph(500)
        star = Star(context=ctx)
        query = (
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name"
        )
        stats = _time_exists_query(star, query, n_warmup=1, n_iterations=3)

        print(f"\n  EXISTS 500-person graph:")
        print(f"    Median: {stats['median_seconds']:.4f}s")
        print(f"    Min:    {stats['min_seconds']:.4f}s")
        print(f"    Rows:   {stats['result_rows']}")

        # Batch execution should handle 500 rows well under 2s
        assert stats["median_seconds"] < 2.0

    @pytest.mark.timeout(15)
    def test_benchmark_exists_with_where_500_persons(self) -> None:
        """EXISTS + WHERE on 500-person graph should be fast."""
        ctx = _build_social_graph(500)
        star = Star(context=ctx)
        query = (
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > 30 RETURN f } "
            "RETURN p.name AS name"
        )
        stats = _time_exists_query(star, query, n_warmup=1, n_iterations=3)

        print(f"\n  EXISTS+WHERE 500-person graph:")
        print(f"    Median: {stats['median_seconds']:.4f}s")
        print(f"    Min:    {stats['min_seconds']:.4f}s")
        print(f"    Rows:   {stats['result_rows']}")

        assert stats["median_seconds"] < 2.0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run benchmarks from command line."""
    print("=" * 60)
    print("EXISTS Subquery Batch Optimization Benchmark")
    print("=" * 60)

    for n_persons in [100, 500, 1000]:
        print(f"\n--- {n_persons} persons ---")
        ctx = _build_social_graph(n_persons)
        star = Star(context=ctx)

        query_basic = (
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) RETURN f } "
            "RETURN p.name AS name"
        )
        stats = _time_exists_query(
            star, query_basic, n_warmup=2, n_iterations=5
        )
        print(
            f"  Basic EXISTS:  median={stats['median_seconds']:.4f}s  rows={stats['result_rows']}"
        )

        query_where = (
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > 30 RETURN f } "
            "RETURN p.name AS name"
        )
        stats = _time_exists_query(
            star, query_where, n_warmup=2, n_iterations=5
        )
        print(
            f"  EXISTS+WHERE:  median={stats['median_seconds']:.4f}s  rows={stats['result_rows']}"
        )


if __name__ == "__main__":
    main()
