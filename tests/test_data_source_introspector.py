"""Tests for DataSourceIntrospector — schema detection, sampling, and column stats.

Phase 2 of Task #2 (R2: Context Builder Enhancement for TUI).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from pycypher.ingestion.introspector import (
    ColumnStats,
    DataSourceIntrospector,
    SchemaInfo,
)


# ===========================================================================
# Fixtures — create temporary CSV/Parquet files for testing
# ===========================================================================


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a sample CSV file with mixed types and nulls."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", None, "Eve"],
            "age": [30, 25, 35, 28, None],
            "score": [95.5, 87.3, 92.1, 95.5, 87.3],
        }
    )
    path = tmp_path / "sample.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """Create a sample Parquet file."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "city": ["NYC", "LA", "SF"],
            "population": [8_000_000, 4_000_000, 900_000],
        }
    )
    path = tmp_path / "sample.parquet"
    df.to_parquet(path, index=False)
    return path


@pytest.fixture
def sample_json(tmp_path: Path) -> Path:
    """Create a sample JSON file."""
    path = tmp_path / "sample.json"
    df = pd.DataFrame(
        {
            "product": ["Widget", "Gadget"],
            "price": [9.99, 49.99],
        }
    )
    df.to_json(path, orient="records", lines=True)
    return path


@pytest.fixture
def sample_arrow_table() -> pa.Table:
    """An in-memory Arrow table for introspection."""
    return pa.table(
        {
            "x": pa.array([1, 2, 3, None, 5], type=pa.int64()),
            "y": pa.array(["a", "b", "a", "c", None], type=pa.string()),
        }
    )


# ===========================================================================
# SchemaInfo — schema detection
# ===========================================================================


class TestSchemaDetection:
    def test_csv_schema(self, sample_csv: Path) -> None:
        """Detect schema from a CSV file without reading all data."""
        introspector = DataSourceIntrospector(str(sample_csv))
        schema = introspector.get_schema()
        assert isinstance(schema, SchemaInfo)
        col_names = [c["name"] for c in schema.columns]
        assert "id" in col_names
        assert "name" in col_names
        assert "age" in col_names
        assert "score" in col_names

    def test_parquet_schema(self, sample_parquet: Path) -> None:
        """Detect schema from a Parquet file."""
        introspector = DataSourceIntrospector(str(sample_parquet))
        schema = introspector.get_schema()
        col_names = [c["name"] for c in schema.columns]
        assert "id" in col_names
        assert "city" in col_names
        assert "population" in col_names

    def test_json_schema(self, sample_json: Path) -> None:
        """Detect schema from a JSON file."""
        introspector = DataSourceIntrospector(str(sample_json))
        schema = introspector.get_schema()
        col_names = [c["name"] for c in schema.columns]
        assert "product" in col_names
        assert "price" in col_names

    def test_arrow_table_schema(self, sample_arrow_table: pa.Table) -> None:
        """Detect schema from an in-memory Arrow table."""
        introspector = DataSourceIntrospector(sample_arrow_table)
        schema = introspector.get_schema()
        col_names = [c["name"] for c in schema.columns]
        assert col_names == ["x", "y"]
        types = {c["name"]: c["type"] for c in schema.columns}
        assert "int64" in types["x"]
        assert "string" in types["y"]

    def test_schema_includes_types(self, sample_csv: Path) -> None:
        """Schema columns include type information."""
        introspector = DataSourceIntrospector(str(sample_csv))
        schema = introspector.get_schema()
        for col in schema.columns:
            assert "name" in col
            assert "type" in col
            assert isinstance(col["type"], str)

    def test_schema_row_count(self, sample_csv: Path) -> None:
        """Schema includes total row count."""
        introspector = DataSourceIntrospector(str(sample_csv))
        schema = introspector.get_schema()
        assert schema.row_count == 5


# ===========================================================================
# Data sampling
# ===========================================================================


class TestDataSampling:
    def test_sample_default(self, sample_csv: Path) -> None:
        """sample() returns a DataFrame with default sample size."""
        introspector = DataSourceIntrospector(str(sample_csv))
        df = introspector.sample()
        assert isinstance(df, pd.DataFrame)
        assert len(df) <= 5  # file only has 5 rows

    def test_sample_with_limit(self, sample_csv: Path) -> None:
        """sample(n=2) returns at most 2 rows."""
        introspector = DataSourceIntrospector(str(sample_csv))
        df = introspector.sample(n=3)
        assert len(df) == 3

    def test_sample_larger_than_data(self, sample_csv: Path) -> None:
        """sample(n=100) returns all rows when data is smaller."""
        introspector = DataSourceIntrospector(str(sample_csv))
        df = introspector.sample(n=100)
        assert len(df) == 5

    def test_sample_from_arrow(self, sample_arrow_table: pa.Table) -> None:
        """Sampling works from Arrow tables."""
        introspector = DataSourceIntrospector(sample_arrow_table)
        df = introspector.sample(n=2)
        assert len(df) == 2

    def test_sample_parquet(self, sample_parquet: Path) -> None:
        """Sampling works from Parquet files."""
        introspector = DataSourceIntrospector(str(sample_parquet))
        df = introspector.sample(n=2)
        assert len(df) == 2


# ===========================================================================
# Column statistics
# ===========================================================================


class TestColumnStats:
    def test_column_stats_csv(self, sample_csv: Path) -> None:
        """get_column_stats() returns stats for all columns."""
        introspector = DataSourceIntrospector(str(sample_csv))
        stats = introspector.get_column_stats()
        assert isinstance(stats, dict)
        assert "id" in stats
        assert "name" in stats

    def test_null_count(self, sample_csv: Path) -> None:
        """Stats include null counts."""
        introspector = DataSourceIntrospector(str(sample_csv))
        stats = introspector.get_column_stats()
        assert stats["name"].null_count >= 1  # one null in name
        assert stats["age"].null_count >= 1  # one null in age

    def test_unique_count(self, sample_csv: Path) -> None:
        """Stats include unique value counts."""
        introspector = DataSourceIntrospector(str(sample_csv))
        stats = introspector.get_column_stats()
        assert stats["id"].unique_count == 5  # all unique
        assert stats["score"].unique_count < 5  # has duplicates

    def test_stats_from_arrow(self, sample_arrow_table: pa.Table) -> None:
        """Column stats work from Arrow tables."""
        introspector = DataSourceIntrospector(sample_arrow_table)
        stats = introspector.get_column_stats()
        assert "x" in stats
        assert stats["x"].null_count == 1
        assert stats["y"].null_count == 1

    def test_stats_include_type(self, sample_csv: Path) -> None:
        """Column stats include the data type."""
        introspector = DataSourceIntrospector(str(sample_csv))
        stats = introspector.get_column_stats()
        assert isinstance(stats["id"].dtype, str)

    def test_stats_min_max_numeric(self, sample_csv: Path) -> None:
        """Numeric columns have min/max values."""
        introspector = DataSourceIntrospector(str(sample_csv))
        stats = introspector.get_column_stats()
        assert stats["id"].min_value is not None
        assert stats["id"].max_value is not None
