"""Comprehensive pytest-benchmark suite for PyCypher core operations.

Covers micro-benchmarks (scalar functions, joins, property lookups),
integration benchmarks (MATCH, WHERE, aggregations), and scale testing
(1K, 10K, 100K rows).

Run benchmarks::

    make bench                    # Run all benchmarks
    make bench-save               # Run and save baseline
    make bench-compare            # Compare against saved baseline

Or directly::

    uv run pytest tests/benchmarks/bench_core_operations.py -v --benchmark-only
    uv run pytest tests/benchmarks/bench_core_operations.py --benchmark-save=baseline
    uv run pytest tests/benchmarks/bench_core_operations.py --benchmark-compare=0001_baseline
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures: synthetic graph data at multiple scales
# ---------------------------------------------------------------------------


def _build_persons(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
    """Generate Person entity DataFrame."""
    depts = ["eng", "mktg", "sales", "ops", "hr"]
    return pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n + 1),
            "name": [f"Person_{i}" for i in range(1, n + 1)],
            "age": rng.integers(18, 65, size=n),
            "dept": rng.choice(depts, size=n),
            "salary": rng.integers(40_000, 200_000, size=n),
        },
    )


def _build_knows(
    n_persons: int,
    *,
    avg_degree: int = 5,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate KNOWS relationship DataFrame."""
    n_edges = n_persons * avg_degree
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    return pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_actual + 1),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": rng.integers(2000, 2026, size=n_actual),
        },
    )


def _build_context(n_persons: int) -> Context:
    """Build a Context with n_persons Person entities and KNOWS relationships."""
    rng = np.random.default_rng(42)
    persons_df = _build_persons(n_persons, rng=rng)
    knows_df = _build_knows(n_persons, rng=rng)

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


# Pre-built contexts at different scales (session-scoped for speed)
@pytest.fixture(scope="module")
def ctx_1k() -> Context:
    """1,000 person graph."""
    return _build_context(1_000)


@pytest.fixture(scope="module")
def ctx_10k() -> Context:
    """10,000 person graph."""
    return _build_context(10_000)


@pytest.fixture(scope="module")
def ctx_100k() -> Context:
    """100,000 person graph."""
    return _build_context(100_000)


@pytest.fixture(scope="module")
def star_1k(ctx_1k: Context) -> Star:
    """Star for 1K graph."""
    return Star(context=ctx_1k)


@pytest.fixture(scope="module")
def star_10k(ctx_10k: Context) -> Star:
    """Star for 10K graph."""
    return Star(context=ctx_10k)


@pytest.fixture(scope="module")
def star_100k(ctx_100k: Context) -> Star:
    """Star for 100K graph."""
    return Star(context=ctx_100k)


# ---------------------------------------------------------------------------
# Group 1: Micro-benchmarks — Parser
# ---------------------------------------------------------------------------


class TestParserMicrobenchmarks:
    """Benchmark Cypher query parsing speed."""

    @pytest.fixture(autouse=True)
    def _parser(self) -> None:
        self.parser = GrammarParser()

    def test_parse_simple_match(self, benchmark: Any) -> None:
        """Parse a simple MATCH...RETURN query."""
        benchmark(self.parser.parse, "MATCH (n:Person) RETURN n.name")

    def test_parse_filtered_match(self, benchmark: Any) -> None:
        """Parse a MATCH with WHERE clause."""
        benchmark(
            self.parser.parse,
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
        )

    def test_parse_relationship_pattern(self, benchmark: Any) -> None:
        """Parse a relationship pattern query."""
        benchmark(
            self.parser.parse,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name",
        )

    def test_parse_complex_query(self, benchmark: Any) -> None:
        """Parse a multi-clause query."""
        benchmark(
            self.parser.parse,
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.age > 25 AND c.salary > 100000 "
            "RETURN a.name, c.name, c.salary",
        )

    def test_parse_aggregation(self, benchmark: Any) -> None:
        """Parse an aggregation query."""
        benchmark(
            self.parser.parse,
            "MATCH (n:Person) RETURN n.dept, count(n) AS cnt, avg(n.salary) AS avg_sal",
        )


# ---------------------------------------------------------------------------
# Group 2: Micro-benchmarks — Scalar Functions
# ---------------------------------------------------------------------------


class TestScalarFunctionMicrobenchmarks:
    """Benchmark scalar function registry lookups and execution."""

    def test_registry_lookup(self, benchmark: Any) -> None:
        """Benchmark function registry lookup speed."""
        registry = ScalarFunctionRegistry.get_instance()

        def lookup() -> None:
            for name in ["toUpper", "toLower", "trim", "size", "abs"]:
                # Access the internal dict to benchmark lookup only
                _ = name.lower() in registry._functions

        benchmark(lookup)

    def test_toupper_execution(self, benchmark: Any) -> None:
        """Benchmark toUpper on a Series."""
        series = pd.Series([f"name_{i}" for i in range(1000)])
        registry = ScalarFunctionRegistry.get_instance()
        benchmark(registry.execute, "toUpper", [series])

    def test_abs_execution(self, benchmark: Any) -> None:
        """Benchmark abs() on a numeric Series."""
        rng = np.random.default_rng(42)
        series = pd.Series(rng.integers(-1000, 1000, size=10_000))
        registry = ScalarFunctionRegistry.get_instance()
        benchmark(registry.execute, "abs", [series])


# ---------------------------------------------------------------------------
# Group 3: Integration benchmarks — Query execution at 1K scale
# ---------------------------------------------------------------------------


class TestQueryBenchmarks1K:
    """Integration benchmarks on 1,000-row dataset."""

    def test_simple_scan(self, benchmark: Any, star_1k: Star) -> None:
        """MATCH (n:Person) RETURN n.name on 1K rows."""
        result = benchmark(
            star_1k.execute_query,
            "MATCH (n:Person) RETURN n.name",
        )
        assert len(result) == 1000

    def test_filtered_scan(self, benchmark: Any, star_1k: Star) -> None:
        """MATCH with WHERE filter on 1K rows."""
        result = benchmark(
            star_1k.execute_query,
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
        )
        assert len(result) > 0

    def test_single_hop(self, benchmark: Any, star_1k: Star) -> None:
        """Single-hop relationship traversal on 1K rows."""
        result = benchmark(
            star_1k.execute_query,
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name, m.name",
        )
        assert len(result) > 0

    def test_filtered_hop(self, benchmark: Any, star_1k: Star) -> None:
        """Filtered relationship traversal on 1K rows."""
        result = benchmark(
            star_1k.execute_query,
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) "
            "WHERE n.age > 25 RETURN n.name, m.name, r.since",
        )
        assert len(result) > 0

    def test_aggregation_count(self, benchmark: Any, star_1k: Star) -> None:
        """COUNT aggregation on 1K rows."""
        result = benchmark(
            star_1k.execute_query,
            "MATCH (n:Person) RETURN n.dept, count(n) AS cnt",
        )
        assert len(result) > 0

    def test_aggregation_avg(self, benchmark: Any, star_1k: Star) -> None:
        """AVG aggregation on 1K rows."""
        result = benchmark(
            star_1k.execute_query,
            "MATCH (n:Person) RETURN n.dept, avg(n.salary) AS avg_sal",
        )
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Group 4: Scale testing — 10K rows
# ---------------------------------------------------------------------------


class TestQueryBenchmarks10K:
    """Integration benchmarks on 10,000-row dataset."""

    def test_simple_scan(self, benchmark: Any, star_10k: Star) -> None:
        """MATCH (n:Person) RETURN n.name on 10K rows."""
        result = benchmark(
            star_10k.execute_query,
            "MATCH (n:Person) RETURN n.name",
        )
        assert len(result) == 10_000

    def test_filtered_scan(self, benchmark: Any, star_10k: Star) -> None:
        """Filtered scan on 10K rows."""
        result = benchmark(
            star_10k.execute_query,
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
        )
        assert len(result) > 0

    def test_single_hop(self, benchmark: Any, star_10k: Star) -> None:
        """Single-hop on 10K rows."""
        result = benchmark(
            star_10k.execute_query,
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name, m.name",
        )
        assert len(result) > 0

    def test_aggregation_count(self, benchmark: Any, star_10k: Star) -> None:
        """COUNT aggregation on 10K rows."""
        result = benchmark(
            star_10k.execute_query,
            "MATCH (n:Person) RETURN n.dept, count(n) AS cnt",
        )
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Group 5: Scale testing — 100K rows
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestQueryBenchmarks100K:
    """Integration benchmarks on 100,000-row dataset.

    Marked slow — excluded from default test runs.
    """

    def test_simple_scan(self, benchmark: Any, star_100k: Star) -> None:
        """MATCH scan on 100K rows."""
        result = benchmark(
            star_100k.execute_query,
            "MATCH (n:Person) RETURN n.name",
        )
        assert len(result) == 100_000

    def test_filtered_scan(self, benchmark: Any, star_100k: Star) -> None:
        """Filtered scan on 100K rows."""
        result = benchmark(
            star_100k.execute_query,
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
        )
        assert len(result) > 0

    def test_single_hop(self, benchmark: Any, star_100k: Star) -> None:
        """Single-hop on 100K rows."""
        benchmark.pedantic(
            star_100k.execute_query,
            args=(
                "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name, m.name",
            ),
            iterations=1,
            rounds=3,
        )

    def test_aggregation_count(self, benchmark: Any, star_100k: Star) -> None:
        """COUNT aggregation on 100K rows."""
        result = benchmark(
            star_100k.execute_query,
            "MATCH (n:Person) RETURN n.dept, count(n) AS cnt",
        )
        assert len(result) > 0
