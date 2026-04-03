"""Benchmark: Query optimizer pass performance.

Measures the overhead and impact of each optimization rule
(FilterPushdown, LimitPushdown, JoinReordering, PredicateSimplification,
IndexScan) on queries of varying complexity.

Run via pytest-benchmark::

    uv run pytest tests/benchmarks/bench_optimizer.py -v --benchmark-only

Or directly::

    uv run python tests/benchmarks/bench_optimizer.py
"""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.query_optimizer import (
    FilterPushdownRule,
    IndexScanRule,
    JoinReorderingRule,
    LimitPushdownRule,
    PredicateSimplificationRule,
    QueryOptimizer,
)
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
# Test queries at varying complexity
# ---------------------------------------------------------------------------

OPTIMIZER_QUERIES: dict[str, str] = {
    "simple_scan": "MATCH (n:Person) RETURN n.name",
    "filter_pushdown_candidate": (
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) "
        "WHERE n.age > 30 RETURN n.name, m.name"
    ),
    "multi_filter": (
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) "
        "WHERE n.age > 25 AND m.salary > 100000 AND r.since > 2010 "
        "RETURN n.name, m.name, r.since"
    ),
    "limit_pushdown_candidate": (
        "MATCH (n:Person) WHERE n.age > 30 RETURN n.name LIMIT 10"
    ),
    "join_reorder_candidate": (
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
        "WHERE c.salary > 150000 RETURN a.name, c.name"
    ),
    "predicate_simplification": (
        "MATCH (n:Person) WHERE n.age > 30 AND n.age > 30 RETURN n.name"
    ),
    "complex_multi_clause": (
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
        "WHERE a.age > 25 AND c.salary > 100000 AND b.dept = 'eng' "
        "RETURN a.name, b.name, c.name, c.salary"
    ),
    "aggregation_with_filter": (
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) "
        "WHERE n.age > 30 "
        "RETURN n.dept, count(m) AS friends, avg(m.salary) AS avg_friend_sal"
    ),
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_context(n_persons: int) -> Context:
    """Build a Context for optimizer benchmarking."""
    rng = np.random.default_rng(42)
    depts = ["eng", "mktg", "sales", "ops", "hr"]
    persons_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_persons + 1),
            "name": [f"Person_{i}" for i in range(1, n_persons + 1)],
            "age": rng.integers(18, 65, size=n_persons),
            "dept": rng.choice(depts, size=n_persons),
            "salary": rng.integers(40_000, 200_000, size=n_persons),
        },
    )
    n_edges = n_persons * 5
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_actual + 1),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": rng.integers(2000, 2026, size=n_actual),
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(persons_df.columns),
        source_obj_attribute_map={
            c: c for c in persons_df.columns if c != ID_COLUMN
        },
        attribute_map={c: c for c in persons_df.columns if c != ID_COLUMN},
        source_obj=persons_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows_df.columns),
        source_obj_attribute_map={
            c: c
            for c in knows_df.columns
            if c not in {ID_COLUMN, "__SOURCE__", "__TARGET__"}
        },
        attribute_map={
            c: c
            for c in knows_df.columns
            if c not in {ID_COLUMN, "__SOURCE__", "__TARGET__"}
        },
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


@pytest.fixture(scope="module")
def parser() -> GrammarParser:
    """Shared parser instance."""
    return GrammarParser()


@pytest.fixture(scope="module")
def optimizer() -> QueryOptimizer:
    """Default optimizer with all rules."""
    return QueryOptimizer.default()


@pytest.fixture(scope="module")
def ctx_1k() -> Context:
    """1K-row context for optimizer benchmarks."""
    return _build_context(1_000)


# ---------------------------------------------------------------------------
# Group 1: Optimizer overhead benchmarks — parse + optimize vs parse only
# ---------------------------------------------------------------------------


class TestOptimizerOverhead:
    """Measure the overhead of running optimizer passes on parsed ASTs."""

    def test_optimize_simple_scan(
        self,
        benchmark: Any,
        parser: GrammarParser,
        optimizer: QueryOptimizer,
        ctx_1k: Context,
    ) -> None:
        """Optimizer overhead on a simple scan query."""
        ast = parser.parse(OPTIMIZER_QUERIES["simple_scan"])
        benchmark(optimizer.optimize, ast, ctx_1k)

    def test_optimize_filter_pushdown(
        self,
        benchmark: Any,
        parser: GrammarParser,
        optimizer: QueryOptimizer,
        ctx_1k: Context,
    ) -> None:
        """Optimizer on a filter pushdown candidate."""
        ast = parser.parse(OPTIMIZER_QUERIES["filter_pushdown_candidate"])
        benchmark(optimizer.optimize, ast, ctx_1k)

    def test_optimize_multi_filter(
        self,
        benchmark: Any,
        parser: GrammarParser,
        optimizer: QueryOptimizer,
        ctx_1k: Context,
    ) -> None:
        """Optimizer on a multi-filter query."""
        ast = parser.parse(OPTIMIZER_QUERIES["multi_filter"])
        benchmark(optimizer.optimize, ast, ctx_1k)

    def test_optimize_join_reorder(
        self,
        benchmark: Any,
        parser: GrammarParser,
        optimizer: QueryOptimizer,
        ctx_1k: Context,
    ) -> None:
        """Optimizer on a join reordering candidate."""
        ast = parser.parse(OPTIMIZER_QUERIES["join_reorder_candidate"])
        benchmark(optimizer.optimize, ast, ctx_1k)

    def test_optimize_complex_multi_clause(
        self,
        benchmark: Any,
        parser: GrammarParser,
        optimizer: QueryOptimizer,
        ctx_1k: Context,
    ) -> None:
        """Optimizer on a complex multi-clause query."""
        ast = parser.parse(OPTIMIZER_QUERIES["complex_multi_clause"])
        benchmark(optimizer.optimize, ast, ctx_1k)


# ---------------------------------------------------------------------------
# Group 2: Individual rule benchmarks
# ---------------------------------------------------------------------------


class TestIndividualRuleBenchmarks:
    """Benchmark each optimization rule in isolation."""

    def test_filter_pushdown_rule(
        self,
        benchmark: Any,
        parser: GrammarParser,
        ctx_1k: Context,
    ) -> None:
        """FilterPushdownRule on a filter-heavy query."""
        rule = FilterPushdownRule()
        ast = parser.parse(OPTIMIZER_QUERIES["multi_filter"])
        benchmark(rule.analyze, ast, ctx_1k)

    def test_limit_pushdown_rule(
        self,
        benchmark: Any,
        parser: GrammarParser,
        ctx_1k: Context,
    ) -> None:
        """LimitPushdownRule on a LIMIT query."""
        rule = LimitPushdownRule()
        ast = parser.parse(OPTIMIZER_QUERIES["limit_pushdown_candidate"])
        benchmark(rule.analyze, ast, ctx_1k)

    def test_join_reordering_rule(
        self,
        benchmark: Any,
        parser: GrammarParser,
        ctx_1k: Context,
    ) -> None:
        """JoinReorderingRule on a multi-hop join."""
        rule = JoinReorderingRule()
        ast = parser.parse(OPTIMIZER_QUERIES["join_reorder_candidate"])
        benchmark(rule.analyze, ast, ctx_1k)

    def test_predicate_simplification_rule(
        self,
        benchmark: Any,
        parser: GrammarParser,
        ctx_1k: Context,
    ) -> None:
        """PredicateSimplificationRule on a redundant predicate query."""
        rule = PredicateSimplificationRule()
        ast = parser.parse(OPTIMIZER_QUERIES["predicate_simplification"])
        benchmark(rule.analyze, ast, ctx_1k)

    def test_index_scan_rule(
        self,
        benchmark: Any,
        parser: GrammarParser,
        ctx_1k: Context,
    ) -> None:
        """IndexScanRule on a filtered scan."""
        rule = IndexScanRule()
        ast = parser.parse(OPTIMIZER_QUERIES["filter_pushdown_candidate"])
        benchmark(rule.analyze, ast, ctx_1k)


# ---------------------------------------------------------------------------
# Group 3: Optimized vs unoptimized execution comparison
# ---------------------------------------------------------------------------


class TestOptimizedExecution:
    """Compare query execution with and without optimization."""

    def test_filter_pushdown_execution_optimized(
        self,
        benchmark: Any,
        ctx_1k: Context,
    ) -> None:
        """Execute filter-heavy query with optimizer enabled (default)."""
        star = Star(context=ctx_1k)
        result = benchmark(
            star.execute_query,
            OPTIMIZER_QUERIES["filter_pushdown_candidate"],
        )
        assert len(result) > 0

    def test_aggregation_filter_execution(
        self,
        benchmark: Any,
        ctx_1k: Context,
    ) -> None:
        """Execute aggregation+filter query with optimizer."""
        star = Star(context=ctx_1k)
        result = benchmark(
            star.execute_query,
            OPTIMIZER_QUERIES["aggregation_with_filter"],
        )
        assert len(result) > 0

    def test_limit_pushdown_execution(
        self,
        benchmark: Any,
        ctx_1k: Context,
    ) -> None:
        """Execute LIMIT query with optimizer."""
        star = Star(context=ctx_1k)
        result = benchmark(
            star.execute_query,
            OPTIMIZER_QUERIES["limit_pushdown_candidate"],
        )
        assert len(result) <= 10


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run optimizer benchmarks from command line."""
    parser = GrammarParser()
    optimizer = QueryOptimizer.default()
    ctx = _build_context(1_000)

    print("=" * 70)
    print("Query Optimizer Benchmark Suite")
    print("=" * 70)

    # Benchmark each query through the full optimizer pipeline
    for qname, qtext in OPTIMIZER_QUERIES.items():
        ast = parser.parse(qtext)

        timings: list[float] = []
        for _ in range(50):
            t0 = time.perf_counter()
            plan = optimizer.optimize(ast, ctx)
            timings.append(time.perf_counter() - t0)

        median_us = float(np.median(timings)) * 1e6
        rules_applied = sum(1 for r in plan.results if r.applied)
        speedup = plan.total_estimated_speedup

        print(
            f"  {qname:40s}  "
            f"median={median_us:>8.1f}us  "
            f"rules_applied={rules_applied}  "
            f"est_speedup={speedup:.2f}x",
        )

    # Benchmark individual rules
    print(f"\n{'=' * 70}")
    print("Individual Rule Performance")
    print(f"{'=' * 70}")

    rules = [
        FilterPushdownRule(),
        LimitPushdownRule(),
        JoinReorderingRule(),
        PredicateSimplificationRule(),
        IndexScanRule(),
    ]
    test_ast = parser.parse(OPTIMIZER_QUERIES["complex_multi_clause"])

    for rule in rules:
        timings = []
        for _ in range(100):
            t0 = time.perf_counter()
            rule.analyze(test_ast, ctx)
            timings.append(time.perf_counter() - t0)

        median_us = float(np.median(timings)) * 1e6
        print(f"  {rule.name:40s}  median={median_us:>8.1f}us")


if __name__ == "__main__":
    main()
