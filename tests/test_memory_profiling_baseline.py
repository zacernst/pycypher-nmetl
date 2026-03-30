"""Memory profiling baseline for large dataset optimization.

Measures current memory footprint and execution time of key operations
at various dataset sizes to establish performance baselines before
streaming/optimization work begins.

Targets from the large dataset implementation plan:
- 1GB dataset: < 2GB memory, < 30s query time
- 10GB dataset: < 8GB memory, < 2min query time
- 100GB dataset: < 16GB memory, < 10min query time
- 1TB dataset: < 32GB memory, < 30min query time
"""

from __future__ import annotations

import gc
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import psutil
import pytest

pytestmark = [pytest.mark.slow, pytest.mark.performance]
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class MemorySnapshot:
    """Captures process memory at a point in time."""

    rss_mb: float
    vms_mb: float
    timestamp: float = field(default_factory=time.monotonic)

    @classmethod
    def take(cls) -> MemorySnapshot:
        """Take a snapshot of current process memory."""
        proc = psutil.Process()
        info = proc.memory_info()
        return cls(
            rss_mb=info.rss / (1024 * 1024),
            vms_mb=info.vms / (1024 * 1024),
        )


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    operation: str
    dataset_size: int
    duration_s: float
    memory_before_mb: float
    memory_after_mb: float
    memory_delta_mb: float
    peak_memory_mb: float
    extra: dict[str, Any] = field(default_factory=dict)


def generate_entity_df(n: int, n_attrs: int = 5) -> pd.DataFrame:
    """Generate a DataFrame with *n* entities and *n_attrs* string attributes."""
    import numpy as np

    rng = np.random.default_rng(42)
    data: dict[str, Any] = {ID_COLUMN: list(range(1, n + 1))}
    for i in range(n_attrs):
        data[f"attr_{i}"] = [f"value_{rng.integers(0, 1000)}" for _ in range(n)]
    return pd.DataFrame(data)


def generate_relationship_df(
    n_rels: int,
    n_entities: int,
) -> pd.DataFrame:
    """Generate a relationship DataFrame connecting random entity pairs."""
    import numpy as np

    rng = np.random.default_rng(42)
    sources = rng.integers(1, n_entities + 1, size=n_rels).tolist()
    targets = rng.integers(1, n_entities + 1, size=n_rels).tolist()
    return pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_rels + 1)),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "weight": [float(rng.random()) for _ in range(n_rels)],
        },
    )


def build_context(
    n_entities: int,
    n_relationships: int,
    n_attrs: int = 5,
) -> Context:
    """Build a Context with generated entity and relationship data."""
    entity_df = generate_entity_df(n_entities, n_attrs)
    rel_df = generate_relationship_df(n_relationships, n_entities)

    entity_table = EntityTable(
        entity_type="Node",
        identifier="Node",
        column_names=list(entity_df.columns),
        source_obj_attribute_map={c: c for c in entity_df.columns if c != ID_COLUMN},
        attribute_map={c: c for c in entity_df.columns if c != ID_COLUMN},
        source_obj=entity_df,
    )

    rel_table = RelationshipTable(
        relationship_type="EDGE",
        identifier="EDGE",
        column_names=list(rel_df.columns),
        source_obj_attribute_map={"weight": "weight"},
        attribute_map={"weight": "weight"},
        source_obj=rel_df,
        source_entity_type="Node",
        target_entity_type="Node",
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Node": entity_table}),
        relationship_mapping=RelationshipMapping(mapping={"EDGE": rel_table}),
    )


def run_benchmark(
    operation: str,
    fn: Any,
    dataset_size: int,
) -> BenchmarkResult:
    """Run a benchmark, measuring time and memory."""
    gc.collect()
    before = MemorySnapshot.take()
    start = time.monotonic()
    result = fn()
    duration = time.monotonic() - start
    after = MemorySnapshot.take()
    gc.collect()

    return BenchmarkResult(
        operation=operation,
        dataset_size=dataset_size,
        duration_s=duration,
        memory_before_mb=before.rss_mb,
        memory_after_mb=after.rss_mb,
        memory_delta_mb=after.rss_mb - before.rss_mb,
        peak_memory_mb=after.rss_mb,
        extra={"result_type": type(result).__name__} if result is not None else {},
    )


# ---------------------------------------------------------------------------
# Baseline tests — small scale (CI-safe)
# ---------------------------------------------------------------------------


SMALL_ENTITIES = 1_000
SMALL_RELS = 5_000


@pytest.fixture
def small_context() -> Context:
    """Small context for baseline measurements."""
    return build_context(SMALL_ENTITIES, SMALL_RELS)


@pytest.fixture
def small_star(small_context: Context) -> Star:
    """Star with small dataset."""
    return Star(context=small_context)


class TestBaselineEntityScan:
    """Baseline: entity scan (MATCH (n:Node) RETURN n)."""

    def test_entity_scan_memory(self, small_star: Star) -> None:
        """Measure memory for simple entity scan."""
        result = run_benchmark(
            "entity_scan",
            lambda: small_star.execute_query(
                "MATCH (n:Node) RETURN n.attr_0",
            ),
            SMALL_ENTITIES,
        )
        # Baseline: just record, don't assert strict limits yet
        assert result.duration_s < 30, f"Entity scan took {result.duration_s:.2f}s"
        assert result.memory_delta_mb < 500, (
            f"Memory delta: {result.memory_delta_mb:.1f}MB"
        )

    def test_entity_scan_with_filter(self, small_star: Star) -> None:
        """Measure memory for entity scan with WHERE filter."""
        result = run_benchmark(
            "entity_scan_filtered",
            lambda: small_star.execute_query(
                "MATCH (n:Node) WHERE n.attr_0 STARTS WITH 'value_1' RETURN n.attr_0",
            ),
            SMALL_ENTITIES,
        )
        assert result.duration_s < 30


class TestBaselineJoin:
    """Baseline: join operations (MATCH (a)-[:EDGE]->(b))."""

    def test_single_hop_join(self, small_star: Star) -> None:
        """Measure memory for single-hop relationship traversal."""
        result = run_benchmark(
            "single_hop_join",
            lambda: small_star.execute_query(
                "MATCH (a:Node)-[:EDGE]->(b:Node) RETURN a.attr_0, b.attr_0",
            ),
            SMALL_RELS,
        )
        assert result.duration_s < 30, f"Single hop took {result.duration_s:.2f}s"

    def test_two_hop_join(self, small_star: Star) -> None:
        """Measure memory for two-hop relationship traversal."""
        result = run_benchmark(
            "two_hop_join",
            lambda: small_star.execute_query(
                "MATCH (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node) "
                "RETURN a.attr_0, c.attr_0",
            ),
            SMALL_RELS,
        )
        assert result.duration_s < 60, f"Two hop took {result.duration_s:.2f}s"


class TestBaselineAggregation:
    """Baseline: aggregation operations."""

    def test_count_aggregation(self, small_star: Star) -> None:
        """Measure memory for COUNT aggregation."""
        result = run_benchmark(
            "count_agg",
            lambda: small_star.execute_query(
                "MATCH (n:Node) RETURN count(n)",
            ),
            SMALL_ENTITIES,
        )
        assert result.duration_s < 30

    def test_grouped_aggregation(self, small_star: Star) -> None:
        """Measure memory for grouped aggregation."""
        result = run_benchmark(
            "grouped_agg",
            lambda: small_star.execute_query(
                "MATCH (n:Node) RETURN n.attr_0, count(n)",
            ),
            SMALL_ENTITIES,
        )
        assert result.duration_s < 30


class TestBaselineOrderBy:
    """Baseline: ORDER BY operations."""

    def test_order_by(self, small_star: Star) -> None:
        """Measure memory for ORDER BY."""
        result = run_benchmark(
            "order_by",
            lambda: small_star.execute_query(
                "MATCH (n:Node) RETURN n.attr_0 ORDER BY n.attr_0",
            ),
            SMALL_ENTITIES,
        )
        assert result.duration_s < 30


# ---------------------------------------------------------------------------
# Medium scale tests (marked for optional CI)
# ---------------------------------------------------------------------------


MEDIUM_ENTITIES = 100_000
MEDIUM_RELS = 500_000


@pytest.fixture
def medium_context() -> Context:
    """Medium context for scaling measurements."""
    return build_context(MEDIUM_ENTITIES, MEDIUM_RELS, n_attrs=10)


@pytest.fixture
def medium_star(medium_context: Context) -> Star:
    """Star with medium dataset."""
    return Star(context=medium_context)


@pytest.mark.performance
class TestMediumScaleBaseline:
    """Medium-scale baselines for tracking optimization progress."""

    def test_entity_scan_100k(self, medium_star: Star) -> None:
        """100K entity scan baseline."""
        result = run_benchmark(
            "entity_scan_100k",
            lambda: medium_star.execute_query(
                "MATCH (n:Node) RETURN n.attr_0",
            ),
            MEDIUM_ENTITIES,
        )
        # Record baseline — these will become stricter as we optimize
        assert result.duration_s < 60, f"100K scan: {result.duration_s:.2f}s"
        assert result.memory_delta_mb < 2000, (
            f"100K scan memory: {result.memory_delta_mb:.1f}MB"
        )

    def test_single_hop_500k(self, medium_star: Star) -> None:
        """500K relationship single-hop baseline."""
        result = run_benchmark(
            "single_hop_500k",
            lambda: medium_star.execute_query(
                "MATCH (a:Node)-[:EDGE]->(b:Node) RETURN a.attr_0, b.attr_0",
            ),
            MEDIUM_RELS,
        )
        assert result.duration_s < 120, f"500K join: {result.duration_s:.2f}s"

    def test_aggregation_100k(self, medium_star: Star) -> None:
        """100K grouped aggregation baseline."""
        result = run_benchmark(
            "grouped_agg_100k",
            lambda: medium_star.execute_query(
                "MATCH (n:Node) RETURN n.attr_0, count(n)",
            ),
            MEDIUM_ENTITIES,
        )
        assert result.duration_s < 60, f"100K agg: {result.duration_s:.2f}s"


# ---------------------------------------------------------------------------
# Scaling analysis (manual execution)
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestScalingAnalysis:
    """Measure how operations scale with dataset size.

    Run manually to get scaling curves:
        uv run pytest tests/test_memory_profiling_baseline.py::TestScalingAnalysis -v -s
    """

    @pytest.mark.parametrize("n_entities", [100, 1_000, 10_000, 50_000])
    def test_entity_scan_scaling(self, n_entities: int) -> None:
        """Measure entity scan scaling across sizes."""
        ctx = build_context(n_entities, n_entities * 5)
        star = Star(context=ctx)
        result = run_benchmark(
            f"entity_scan_{n_entities}",
            lambda: star.execute_query("MATCH (n:Node) RETURN n.attr_0"),
            n_entities,
        )
        # Print for manual analysis
        print(
            f"\n  entities={n_entities:>7d}  "
            f"time={result.duration_s:>6.2f}s  "
            f"mem_delta={result.memory_delta_mb:>+8.1f}MB",
        )

    @pytest.mark.parametrize("n_rels", [100, 1_000, 10_000, 50_000])
    def test_join_scaling(self, n_rels: int) -> None:
        """Measure join scaling across relationship counts."""
        n_entities = max(n_rels // 5, 100)
        ctx = build_context(n_entities, n_rels)
        star = Star(context=ctx)
        result = run_benchmark(
            f"join_{n_rels}",
            lambda: star.execute_query(
                "MATCH (a:Node)-[:EDGE]->(b:Node) RETURN a.attr_0, b.attr_0",
            ),
            n_rels,
        )
        print(
            f"\n  rels={n_rels:>7d}  "
            f"time={result.duration_s:>6.2f}s  "
            f"mem_delta={result.memory_delta_mb:>+8.1f}MB",
        )
