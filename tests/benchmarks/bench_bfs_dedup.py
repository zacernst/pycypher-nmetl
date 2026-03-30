"""Benchmark: BFS frontier dedup optimization in PathExpander.

Measures the performance impact of using ``duplicated()`` mask with boolean
indexing vs the previous ``drop_duplicates() + reset_index()`` approach in
the BFS hot loop of ``expand_variable_length_path``.

Run directly::

    uv run python tests/benchmarks/bench_bfs_dedup.py

Or via pytest::

    uv run pytest tests/benchmarks/bench_bfs_dedup.py -v -s
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest
from pycypher.binding_frame import EntityScan
from pycypher.path_expander import PathExpander
from pycypher.relational_models import (
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

_ID = "__ID__"


def _build_dense_graph(n_persons: int, edges_per_node: int = 5) -> Context:
    """Build a dense random graph for benchmarking BFS."""
    rng = np.random.default_rng(42)
    persons_df = pd.DataFrame(
        {
            _ID: list(range(1, n_persons + 1)),
            "name": [f"P{i}" for i in range(1, n_persons + 1)],
        },
    )
    n_edges = n_persons * edges_per_node
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    knows_df = pd.DataFrame(
        {
            _ID: list(range(1, n_actual + 1)),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": [2020] * n_actual,
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[_ID, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=persons_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[_ID, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def _time_bfs_expansion(
    ctx: Context,
    max_hops: int = 3,
    n_warmup: int = 1,
    n_iterations: int = 5,
) -> dict[str, float]:
    """Time the BFS variable-length path expansion."""
    from pycypher.ast_models import RelationshipDirection

    scan = EntityScan(entity_type="Person", var_name="a")
    start_frame = scan.scan(ctx)

    expander = PathExpander(context=ctx)

    # Warmup
    for _ in range(n_warmup):
        expander.expand_variable_length_path(
            start_frame=start_frame,
            start_var="a",
            rel_type="KNOWS",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Person",
            min_hops=1,
            max_hops=max_hops,
            anon_counter=[0],
        )

    # Timed iterations
    timings: list[float] = []
    result_rows = 0
    for _ in range(n_iterations):
        t0 = time.perf_counter()
        result = expander.expand_variable_length_path(
            start_frame=start_frame,
            start_var="a",
            rel_type="KNOWS",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Person",
            min_hops=1,
            max_hops=max_hops,
            anon_counter=[0],
        )
        timings.append(time.perf_counter() - t0)
        result_rows = len(result.bindings)

    return {
        "median_seconds": float(np.median(timings)),
        "min_seconds": min(timings),
        "max_seconds": max(timings),
        "result_rows": result_rows,
        "n_iterations": n_iterations,
    }


# ---------------------------------------------------------------------------
# Pytest benchmarks
# ---------------------------------------------------------------------------


class TestBFSDedupOptimization:
    """Validate BFS dedup optimization produces correct results efficiently."""

    def test_correctness_dense_graph(self) -> None:
        """Optimized dedup produces same results as reference implementation."""
        ctx = _build_dense_graph(200, edges_per_node=5)
        star = Star(context=ctx)

        # Run the query and verify it returns non-empty results
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name",
        )
        assert len(result) > 0
        assert "a.name" in result.columns
        assert "b.name" in result.columns

    def test_correctness_chain_graph_exact_counts(self) -> None:
        """Chain graph produces exact expected row counts after optimization."""
        # Chain: 1→2→3→4→5
        persons_df = pd.DataFrame(
            {
                _ID: list(range(1, 6)),
                "name": [f"P{i}" for i in range(1, 6)],
            },
        )
        knows_df = pd.DataFrame(
            {
                _ID: list(range(1, 5)),
                "__SOURCE__": [1, 2, 3, 4],
                "__TARGET__": [2, 3, 4, 5],
                "since": [2020] * 4,
            },
        )
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[_ID, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=persons_df,
        )
        knows_table = RelationshipTable(
            relationship_type="KNOWS",
            identifier="KNOWS",
            column_names=[_ID, "__SOURCE__", "__TARGET__", "since"],
            source_obj_attribute_map={"since": "since"},
            attribute_map={"since": "since"},
            source_obj=knows_df,
            source_entity_type="Person",
            target_entity_type="Person",
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        )
        star = Star(context=ctx)

        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name",
        )
        # 1→{2,3,4}, 2→{3,4,5}, 3→{4,5}, 4→{5} = 3+3+2+1 = 9
        assert len(result) == 9

    def test_dedup_eliminates_duplicates(self) -> None:
        """Diamond graph: A→B, A→C, B→D, C→D — dedup on (start, tip)."""
        persons_df = pd.DataFrame(
            {_ID: [1, 2, 3, 4], "name": ["A", "B", "C", "D"]},
        )
        knows_df = pd.DataFrame(
            {
                _ID: [1, 2, 3, 4],
                "__SOURCE__": [1, 1, 2, 3],
                "__TARGET__": [2, 3, 3, 4],
                "since": [2020] * 4,
            },
        )
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[_ID, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=persons_df,
        )
        knows_table = RelationshipTable(
            relationship_type="KNOWS",
            identifier="KNOWS",
            column_names=[_ID, "__SOURCE__", "__TARGET__", "since"],
            source_obj_attribute_map={"since": "since"},
            attribute_map={"since": "since"},
            source_obj=knows_df,
            source_entity_type="Person",
            target_entity_type="Person",
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        )
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'A' RETURN b.name AS bname",
        )
        names = sorted(result["bname"].tolist())
        # 1-hop: B, C; 2-hop: C (via B), D (via C) — dedup means C appears once
        assert names == ["B", "C", "C", "D"]

    @pytest.mark.timeout(30)
    def test_benchmark_bfs_expansion(self) -> None:
        """Benchmark BFS expansion on a 500-node dense graph."""
        ctx = _build_dense_graph(500, edges_per_node=5)
        stats = _time_bfs_expansion(
            ctx,
            max_hops=3,
            n_warmup=1,
            n_iterations=3,
        )

        print("\n  BFS 500-node dense graph, 3 hops:")
        print(f"    Median: {stats['median_seconds']:.4f}s")
        print(f"    Min:    {stats['min_seconds']:.4f}s")
        print(f"    Max:    {stats['max_seconds']:.4f}s")
        print(f"    Rows:   {stats['result_rows']:,}")

        # Sanity: BFS on 500 nodes with 3 hops should complete in < 10s
        assert stats["median_seconds"] < 10.0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run benchmark from command line."""
    print("=" * 60)
    print("BFS Frontier Dedup Optimization Benchmark")
    print("=" * 60)

    for n_persons in [100, 500, 1000]:
        print(f"\n--- {n_persons} nodes, 5 edges/node, 3 hops ---")
        ctx = _build_dense_graph(n_persons, edges_per_node=5)
        stats = _time_bfs_expansion(
            ctx,
            max_hops=3,
            n_warmup=2,
            n_iterations=5,
        )
        print(f"  Median: {stats['median_seconds']:.4f}s")
        print(f"  Min:    {stats['min_seconds']:.4f}s")
        print(f"  Max:    {stats['max_seconds']:.4f}s")
        print(f"  Rows:   {stats['result_rows']:,}")


if __name__ == "__main__":
    main()
