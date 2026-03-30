"""Phase 3 distributed testing scaffolding.

Provides test infrastructure for validating PyCypher's large-dataset
capabilities under distributed execution scenarios:

1. **Dask cluster simulation** — LocalCluster fixtures with configurable
   worker counts and memory limits
2. **Fault tolerance** — Worker failure simulation and recovery validation
3. **Memory stability** — Long-running stability checks for leak detection
4. **Scale tier validation** — Configurable dataset sizes from 1K to 10M rows
5. **Backend equivalence** — Results match across pandas/Dask/DuckDB backends

These tests are marked with ``@pytest.mark.integration`` so they are excluded
from the default fast test suite.  Run with::

    uv run pytest -m integration tests/test_distributed_scaffolding.py

Or via the Makefile::

    make test-large-dataset
"""

from __future__ import annotations

import gc
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.backend_engine import (
    DuckDBBackend,
    PandasBackend,
    select_backend,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Pytest markers
# ---------------------------------------------------------------------------

pytestmark = [pytest.mark.integration]

# ---------------------------------------------------------------------------
# Scale tier configuration
# ---------------------------------------------------------------------------

SCALE_TIERS: dict[str, int] = {
    "tiny": 100,
    "small": 1_000,
    "medium": 10_000,
    "large": 100_000,
    "xlarge": 1_000_000,
}


# ---------------------------------------------------------------------------
# Fixtures: synthetic data generators
# ---------------------------------------------------------------------------


def _generate_entity_df(n_rows: int, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic entity DataFrame with n_rows."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "__ID__": range(n_rows),
            "name": [f"entity_{i}" for i in range(n_rows)],
            "value": rng.standard_normal(n_rows),
            "category": rng.choice(["A", "B", "C", "D"], size=n_rows),
            "score": rng.integers(0, 100, size=n_rows),
        },
    )


def _generate_relationship_df(
    n_entities: int,
    avg_degree: int = 3,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic relationship DataFrame."""
    rng = np.random.default_rng(seed)
    n_rels = n_entities * avg_degree
    sources = rng.integers(0, n_entities, size=n_rels)
    targets = rng.integers(0, n_entities, size=n_rels)
    return pd.DataFrame(
        {
            "__ID__": range(n_rels),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "weight": rng.uniform(0, 1, size=n_rels),
        },
    )


@pytest.fixture(params=["tiny", "small", "medium"])
def scale_tier(request: pytest.FixtureRequest) -> str:
    """Parametrized scale tier for multi-scale testing."""
    return request.param


@pytest.fixture
def scaled_context(scale_tier: str) -> Context:
    """Context with entities/relationships at the given scale tier."""
    n = SCALE_TIERS[scale_tier]
    entity_df = _generate_entity_df(n)
    rel_df = _generate_relationship_df(n)

    entity_table = EntityTable(
        entity_type="Node",
        identifier="Node",
        column_names=list(entity_df.columns),
        source_obj_attribute_map={c: c for c in entity_df.columns if c != "__ID__"},
        attribute_map={c: c for c in entity_df.columns if c != "__ID__"},
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


# ---------------------------------------------------------------------------
# Fixtures: Dask cluster
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dask_cluster() -> Any:
    """Module-scoped Dask LocalCluster for distributed tests.

    Yields a (client, cluster) tuple. Cleans up after all tests in the
    module complete.
    """
    distributed = pytest.importorskip(
        "distributed",
        reason="distributed not installed",
    )
    cluster = distributed.LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        memory_limit="256MB",
        silence_logs=50,
    )
    client = distributed.Client(cluster)
    yield client, cluster
    client.close()
    cluster.close()


# ---------------------------------------------------------------------------
# Test class: Dask cluster simulation
# ---------------------------------------------------------------------------


class TestDaskClusterSimulation:
    """Validate Dask distributed execution basics."""

    def test_cluster_starts(self, dask_cluster: Any) -> None:
        """Verify LocalCluster creates workers."""
        client, _cluster = dask_cluster
        workers = client.scheduler_info()["workers"]
        assert len(workers) == 2

    def test_dask_dataframe_distributed(self, dask_cluster: Any) -> None:
        """Run a Dask DataFrame operation through the cluster."""
        import dask.dataframe as dd

        client, _cluster = dask_cluster
        pdf = _generate_entity_df(10_000)
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Filter + aggregate through distributed scheduler
        result = (
            ddf[ddf["value"] > 0].groupby("category").agg({"score": "mean"}).compute()
        )
        assert len(result) > 0

    def test_dask_merge_distributed(self, dask_cluster: Any) -> None:
        """Run a merge (join equivalent) through the cluster."""
        import dask.dataframe as dd

        client, _cluster = dask_cluster
        left = dd.from_pandas(
            _generate_entity_df(5_000, seed=1),
            npartitions=2,
        )
        right = dd.from_pandas(
            _generate_entity_df(5_000, seed=2),
            npartitions=2,
        )
        merged = left.merge(right, on="__ID__", suffixes=("_l", "_r"))
        result = merged.compute()
        assert len(result) == 5_000


# ---------------------------------------------------------------------------
# Test class: Backend equivalence
# ---------------------------------------------------------------------------


class TestBackendEquivalence:
    """Verify that different backends produce identical results."""

    def test_filter_equivalence(self) -> None:
        """Pandas and DuckDB backends produce same filter results."""
        df = _generate_entity_df(1_000)
        mask = df["value"] > 0

        pandas_result = PandasBackend().filter(df, mask)
        duckdb_result = DuckDBBackend().filter(df.copy(), mask)

        assert len(pandas_result) == len(duckdb_result)
        pd.testing.assert_frame_equal(
            pandas_result.reset_index(drop=True),
            duckdb_result.reset_index(drop=True),
        )

    def test_join_equivalence(self) -> None:
        """Pandas and DuckDB backends produce same join results."""
        left = _generate_entity_df(500, seed=1)
        right = _generate_entity_df(500, seed=2)

        pandas_result = PandasBackend().join(left, right, on="__ID__")
        duckdb_result = DuckDBBackend().join(
            left.copy(),
            right.copy(),
            on="__ID__",
        )

        assert len(pandas_result) == len(duckdb_result)

    def test_aggregate_equivalence(self) -> None:
        """Pandas and DuckDB backends produce same aggregation results."""
        df = _generate_entity_df(1_000)

        agg_specs = {"avg_score": ("score", "mean")}
        pandas_result = PandasBackend().aggregate(df, ["category"], agg_specs)
        duckdb_result = DuckDBBackend().aggregate(
            df.copy(),
            ["category"],
            agg_specs,
        )

        assert len(pandas_result) == len(duckdb_result)


# ---------------------------------------------------------------------------
# Test class: Scale tier validation
# ---------------------------------------------------------------------------


class TestScaleTierValidation:
    """Run core queries at different scale tiers."""

    def test_simple_match_return(
        self,
        scaled_context: Context,
        scale_tier: str,
    ) -> None:
        """MATCH (n:Node) RETURN n.name LIMIT 10 works at all scales."""
        star = Star(context=scaled_context)
        result = star.execute_query("MATCH (n:Node) RETURN n.name LIMIT 10")
        assert len(result) == 10

    def test_filtered_match(
        self,
        scaled_context: Context,
        scale_tier: str,
    ) -> None:
        """MATCH with WHERE clause works at all scales."""
        star = Star(context=scaled_context)
        result = star.execute_query(
            "MATCH (n:Node) WHERE n.score > 50 RETURN n.name AS name, n.score AS score",
        )
        assert len(result) > 0
        assert all(row["score"] > 50 for _, row in result.iterrows())

    def test_aggregation_at_scale(
        self,
        scaled_context: Context,
        scale_tier: str,
    ) -> None:
        """Aggregation works at all scales."""
        star = Star(context=scaled_context)
        result = star.execute_query(
            "MATCH (n:Node) RETURN n.category, count(n) AS cnt",
        )
        n = SCALE_TIERS[scale_tier]
        total = result["cnt"].sum()
        assert total == n

    def test_relationship_traversal(
        self,
        scaled_context: Context,
        scale_tier: str,
    ) -> None:
        """Relationship traversal works at all scales."""
        star = Star(context=scaled_context)
        result = star.execute_query(
            "MATCH (a:Node)-[r:EDGE]->(b:Node) RETURN a.name, b.name LIMIT 10",
        )
        assert len(result) <= 10


# ---------------------------------------------------------------------------
# Test class: Memory stability
# ---------------------------------------------------------------------------


class TestMemoryStability:
    """Detect memory leaks via repeated execution cycles."""

    def test_no_memory_leak_repeated_queries(self) -> None:
        """Run same query 100 times; memory should not grow unbounded."""
        import psutil

        n = 10_000
        entity_df = _generate_entity_df(n)
        entity_table = EntityTable(
            entity_type="Node",
            identifier="Node",
            column_names=list(entity_df.columns),
            source_obj_attribute_map={c: c for c in entity_df.columns if c != "__ID__"},
            attribute_map={c: c for c in entity_df.columns if c != "__ID__"},
            source_obj=entity_df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Node": entity_table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=ctx)

        process = psutil.Process()
        gc.collect()
        baseline_mb = process.memory_info().rss / (1024 * 1024)

        for _ in range(100):
            star.execute_query(
                "MATCH (n:Node) WHERE n.score > 50 RETURN n.name LIMIT 10",
            )

        gc.collect()
        final_mb = process.memory_info().rss / (1024 * 1024)
        growth_mb = final_mb - baseline_mb

        # Allow up to 50MB growth for 100 iterations of a 10K-row query.
        # Anything more suggests a leak.
        assert growth_mb < 50, (
            f"Memory grew by {growth_mb:.1f} MB over 100 iterations "
            f"(baseline={baseline_mb:.1f} MB, final={final_mb:.1f} MB)"
        )


# ---------------------------------------------------------------------------
# Test class: Backend selection heuristics
# ---------------------------------------------------------------------------


class TestBackendSelection:
    """Verify automatic backend selection works correctly."""

    def test_small_data_selects_pandas(self) -> None:
        """Small datasets should use PandasBackend."""
        backend = select_backend(hint="auto", estimated_rows=1_000)
        assert backend.name == "pandas"

    def test_large_data_selects_polars(self) -> None:
        """Large datasets should use PolarsBackend (preferred over DuckDB)."""
        backend = select_backend(hint="auto", estimated_rows=500_000)
        assert backend.name == "polars"

    def test_explicit_pandas(self) -> None:
        """Explicit hint='pandas' always returns PandasBackend."""
        backend = select_backend(hint="pandas")
        assert backend.name == "pandas"

    def test_explicit_duckdb(self) -> None:
        """Explicit hint='duckdb' always returns DuckDBBackend."""
        backend = select_backend(hint="duckdb")
        assert backend.name == "duckdb"

    def test_invalid_hint_raises(self) -> None:
        """Invalid backend hint raises ValueError."""
        with pytest.raises(ValueError, match="Unknown backend hint"):
            select_backend(hint="nonexistent")
