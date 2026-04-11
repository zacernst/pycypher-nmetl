"""Tests for the data preview infrastructure (Task #3: R4).

Covers:
- DataSampler: pluggable sampling strategies (head, tail, random, stratified)
- Schema introspection without full table load
- Column statistics computation
- Preview caching with LRU eviction
- QueryTester: execute Cypher queries against sampled data
- Query result caching and timing
"""

from __future__ import annotations

import csv
import tempfile
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from pycypher.ingestion.data_preview import (
    ColumnStats,
    DataSampler,
    PreviewCache,
    QueryTester,
    SamplingStrategy,
    SchemaInfo,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    """Create a CSV with 100 rows for testing."""
    path = tmp_path / "people.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "city"])
        for i in range(100):
            writer.writerow([i, f"Person_{i}", 20 + (i % 50), ["NYC", "LA", "CHI", "SF", None][i % 5]])
    return path


@pytest.fixture()
def sample_parquet(tmp_path: Path) -> Path:
    """Create a Parquet file with 200 rows for testing."""
    path = tmp_path / "data.parquet"
    table = pa.table({
        "id": list(range(200)),
        "value": [float(i * 1.5) for i in range(200)],
        "category": [["A", "B", "C"][i % 3] for i in range(200)],
    })
    import pyarrow.parquet as pq

    pq.write_table(table, path)
    return path


@pytest.fixture()
def sample_json(tmp_path: Path) -> Path:
    """Create a JSON file with 50 rows for testing."""
    import json

    path = tmp_path / "records.json"
    records = [{"id": i, "label": f"item_{i}", "score": i * 0.1} for i in range(50)]
    with open(path, "w") as f:
        json.dump(records, f)
    return path


@pytest.fixture()
def arrow_table() -> pa.Table:
    """In-memory Arrow table for testing."""
    return pa.table({
        "id": list(range(500)),
        "name": [f"entity_{i}" for i in range(500)],
        "weight": [float(i) / 10 for i in range(500)],
        "flag": [i % 2 == 0 for i in range(500)],
        "nullable_col": [None if i % 7 == 0 else i for i in range(500)],
    })


# ---------------------------------------------------------------------------
# TestSamplingStrategy
# ---------------------------------------------------------------------------


class TestSamplingStrategy:
    """Tests for SamplingStrategy enum."""

    def test_all_strategies_defined(self) -> None:
        assert SamplingStrategy.HEAD in SamplingStrategy
        assert SamplingStrategy.TAIL in SamplingStrategy
        assert SamplingStrategy.RANDOM in SamplingStrategy


# ---------------------------------------------------------------------------
# TestDataSampler — CSV
# ---------------------------------------------------------------------------


class TestDataSamplerCSV:
    """DataSampler tests against CSV files."""

    def test_head_sample(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        result = sampler.sample(n=10, strategy=SamplingStrategy.HEAD)
        assert isinstance(result, pa.Table)
        assert len(result) == 10
        # HEAD should return first 10 rows (id 0..9)
        assert result.column("id").to_pylist() == list(range(10))

    def test_tail_sample(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        result = sampler.sample(n=10, strategy=SamplingStrategy.TAIL)
        assert isinstance(result, pa.Table)
        assert len(result) == 10
        # TAIL should return last 10 rows (id 90..99)
        assert result.column("id").to_pylist() == list(range(90, 100))

    def test_random_sample(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        result = sampler.sample(n=20, strategy=SamplingStrategy.RANDOM)
        assert isinstance(result, pa.Table)
        assert len(result) == 20
        # Random should return different rows than pure head
        ids = result.column("id").to_pylist()
        assert len(set(ids)) == 20  # All unique

    def test_sample_larger_than_dataset(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        result = sampler.sample(n=500, strategy=SamplingStrategy.HEAD)
        assert len(result) == 100  # Only 100 rows in file

    def test_default_strategy_is_head(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        result = sampler.sample(n=5)
        assert result.column("id").to_pylist() == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# TestDataSampler — Parquet
# ---------------------------------------------------------------------------


class TestDataSamplerParquet:
    """DataSampler tests against Parquet files."""

    def test_head_sample(self, sample_parquet: Path) -> None:
        sampler = DataSampler(str(sample_parquet))
        result = sampler.sample(n=15, strategy=SamplingStrategy.HEAD)
        assert len(result) == 15
        assert result.column("id").to_pylist() == list(range(15))

    def test_tail_sample(self, sample_parquet: Path) -> None:
        sampler = DataSampler(str(sample_parquet))
        result = sampler.sample(n=10, strategy=SamplingStrategy.TAIL)
        assert len(result) == 10
        assert result.column("id").to_pylist() == list(range(190, 200))


# ---------------------------------------------------------------------------
# TestDataSampler — JSON
# ---------------------------------------------------------------------------


class TestDataSamplerJSON:
    """DataSampler tests against JSON files."""

    def test_head_sample(self, sample_json: Path) -> None:
        sampler = DataSampler(str(sample_json))
        result = sampler.sample(n=5, strategy=SamplingStrategy.HEAD)
        assert len(result) == 5
        assert result.column("id").to_pylist() == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# TestDataSampler — Arrow table
# ---------------------------------------------------------------------------


class TestDataSamplerArrow:
    """DataSampler tests with in-memory Arrow tables."""

    def test_head_sample(self, arrow_table: pa.Table) -> None:
        sampler = DataSampler(arrow_table)
        result = sampler.sample(n=10, strategy=SamplingStrategy.HEAD)
        assert len(result) == 10

    def test_tail_sample(self, arrow_table: pa.Table) -> None:
        sampler = DataSampler(arrow_table)
        result = sampler.sample(n=10, strategy=SamplingStrategy.TAIL)
        assert len(result) == 10
        assert result.column("id").to_pylist() == list(range(490, 500))

    def test_random_sample(self, arrow_table: pa.Table) -> None:
        sampler = DataSampler(arrow_table)
        result = sampler.sample(n=50, strategy=SamplingStrategy.RANDOM)
        assert len(result) == 50


# ---------------------------------------------------------------------------
# TestSchemaIntrospection
# ---------------------------------------------------------------------------


class TestSchemaIntrospection:
    """Schema introspection without full table load."""

    def test_schema_from_csv(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        schema = sampler.schema()
        assert isinstance(schema, SchemaInfo)
        assert set(schema.column_names) >= {"id", "name", "age", "city"}
        assert len(schema.column_types) == len(schema.column_names)

    def test_schema_from_parquet(self, sample_parquet: Path) -> None:
        sampler = DataSampler(str(sample_parquet))
        schema = sampler.schema()
        assert "id" in schema.column_names
        assert "value" in schema.column_names
        assert "category" in schema.column_names

    def test_row_count(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        schema = sampler.schema()
        assert schema.row_count == 100

    def test_row_count_parquet(self, sample_parquet: Path) -> None:
        sampler = DataSampler(str(sample_parquet))
        schema = sampler.schema()
        assert schema.row_count == 200

    def test_schema_from_arrow(self, arrow_table: pa.Table) -> None:
        sampler = DataSampler(arrow_table)
        schema = sampler.schema()
        assert schema.row_count == 500
        assert "id" in schema.column_names


# ---------------------------------------------------------------------------
# TestColumnStatistics
# ---------------------------------------------------------------------------


class TestColumnStatistics:
    """Column-level statistics computation."""

    def test_stats_from_csv(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        stats = sampler.column_stats("age")
        assert isinstance(stats, ColumnStats)
        assert stats.null_count >= 0
        assert stats.unique_count > 0
        assert stats.min_value is not None
        assert stats.max_value is not None

    def test_stats_null_column(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        stats = sampler.column_stats("city")
        # city has None values (every 5th row)
        assert stats.null_count == 20  # 100 rows, every 5th is None

    def test_stats_from_arrow(self, arrow_table: pa.Table) -> None:
        sampler = DataSampler(arrow_table)
        stats = sampler.column_stats("nullable_col")
        # Every 7th row is None: 0,7,14,...,497 => ceil(500/7) = 72 nulls
        assert stats.null_count == 72

    def test_stats_nonexistent_column(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        with pytest.raises(ValueError, match="not found"):
            sampler.column_stats("nonexistent")

    def test_all_column_stats(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))
        all_stats = sampler.all_column_stats()
        assert isinstance(all_stats, dict)
        assert "id" in all_stats
        assert "name" in all_stats
        assert "age" in all_stats


# ---------------------------------------------------------------------------
# TestPreviewCache
# ---------------------------------------------------------------------------


class TestPreviewCache:
    """LRU cache for preview results."""

    def test_cache_hit(self, sample_csv: Path) -> None:
        cache = PreviewCache(max_size=8)
        sampler = DataSampler(str(sample_csv), cache=cache)
        result1 = sampler.sample(n=10, strategy=SamplingStrategy.HEAD)
        result2 = sampler.sample(n=10, strategy=SamplingStrategy.HEAD)
        assert result1.equals(result2)
        assert cache.hits == 1
        assert cache.misses == 1

    def test_cache_miss_different_params(self, sample_csv: Path) -> None:
        cache = PreviewCache(max_size=8)
        sampler = DataSampler(str(sample_csv), cache=cache)
        sampler.sample(n=10, strategy=SamplingStrategy.HEAD)
        sampler.sample(n=20, strategy=SamplingStrategy.HEAD)
        assert cache.hits == 0
        assert cache.misses == 2

    def test_cache_eviction(self) -> None:
        cache = PreviewCache(max_size=2)
        cache.put("key1", pa.table({"a": [1]}))
        cache.put("key2", pa.table({"a": [2]}))
        cache.put("key3", pa.table({"a": [3]}))
        assert cache.get("key1") is None  # Evicted
        assert cache.get("key3") is not None

    def test_cache_disabled(self, sample_csv: Path) -> None:
        sampler = DataSampler(str(sample_csv))  # No cache
        result1 = sampler.sample(n=10)
        result2 = sampler.sample(n=10)
        assert result1.equals(result2)


# ---------------------------------------------------------------------------
# TestQueryTester
# ---------------------------------------------------------------------------


class TestQueryTester:
    """Execute Cypher queries against sample data."""

    def test_basic_query(self, sample_csv: Path) -> None:
        tester = QueryTester()
        tester.add_entity("Person", str(sample_csv), id_col="id")
        result = tester.run("MATCH (p:Person) RETURN p.name LIMIT 5")
        assert result.table is not None
        assert len(result.table) == 5

    def test_query_timing(self, sample_csv: Path) -> None:
        tester = QueryTester()
        tester.add_entity("Person", str(sample_csv), id_col="id")
        result = tester.run("MATCH (p:Person) RETURN p.name LIMIT 3")
        assert result.elapsed_ms >= 0

    def test_query_with_sample_limit(self, sample_csv: Path) -> None:
        tester = QueryTester(sample_size=20)
        tester.add_entity("Person", str(sample_csv), id_col="id")
        result = tester.run("MATCH (p:Person) RETURN p.name")
        # Should run against sampled data (20 rows), not all 100
        assert result.table is not None
        assert len(result.table) <= 20

    def test_query_error_handling(self, sample_csv: Path) -> None:
        tester = QueryTester()
        tester.add_entity("Person", str(sample_csv), id_col="id")
        result = tester.run("MATCH (x:NonExistent) RETURN x")
        # Should not crash — returns empty or error info
        assert result.table is not None or result.error is not None

    def test_multiple_entities(self, tmp_path: Path) -> None:
        people_csv = tmp_path / "people.csv"
        with open(people_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Alice"])
            writer.writerow([2, "Bob"])

        orders_csv = tmp_path / "orders.csv"
        with open(orders_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "product", "amount"])
            writer.writerow([101, "Widget", 9.99])
            writer.writerow([102, "Gadget", 19.99])

        tester = QueryTester()
        tester.add_entity("Person", str(people_csv), id_col="id")
        tester.add_entity("Order", str(orders_csv), id_col="id")
        result = tester.run("MATCH (p:Person) RETURN p.name")
        assert result.table is not None
        assert len(result.table) == 2


# ---------------------------------------------------------------------------
# TestDataSamplerPandasIntegration
# ---------------------------------------------------------------------------


class TestDataSamplerPandasIntegration:
    """DataSampler with pandas DataFrame input."""

    def test_sample_from_dataframe(self) -> None:
        df = pd.DataFrame({"x": range(50), "y": [f"val_{i}" for i in range(50)]})
        sampler = DataSampler(df)
        result = sampler.sample(n=10, strategy=SamplingStrategy.HEAD)
        assert len(result) == 10

    def test_schema_from_dataframe(self) -> None:
        df = pd.DataFrame({"x": range(50), "y": [f"val_{i}" for i in range(50)]})
        sampler = DataSampler(df)
        schema = sampler.schema()
        assert schema.row_count == 50
        assert "x" in schema.column_names
