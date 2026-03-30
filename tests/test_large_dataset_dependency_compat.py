"""Dependency compatibility tests for large-dataset backend stack.

Validates that all optional backend dependencies can be imported and
perform basic operations on the current Python version. Prevents
regressions when dependency versions change.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Small DataFrame for backend interop testing."""
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "id": range(100),
            "label": ["Person"] * 50 + ["Company"] * 50,
            "value": rng.standard_normal(100),
        },
    )


dask = pytest.importorskip("dask", reason="dask not installed")


class TestDaskCompatibility:
    """Verify Dask backend works with current pandas/Python."""

    def test_dask_import(self) -> None:
        import dask
        import dask.dataframe

        assert dask.__version__

    def test_distributed_import(self) -> None:
        distributed = pytest.importorskip("distributed", reason="distributed not installed")

        assert distributed.__version__

    def test_dask_from_pandas(self, sample_dataframe: pd.DataFrame) -> None:
        import dask.dataframe as dd

        ddf = dd.from_pandas(sample_dataframe, npartitions=2)
        result = ddf.compute()
        assert len(result) == len(sample_dataframe)

    def test_dask_filter(self, sample_dataframe: pd.DataFrame) -> None:
        import dask.dataframe as dd

        ddf = dd.from_pandas(sample_dataframe, npartitions=2)
        filtered = ddf[ddf["value"] > 0].compute()
        assert 0 < len(filtered) < len(sample_dataframe)

    def test_dask_merge(self, sample_dataframe: pd.DataFrame) -> None:
        import dask.dataframe as dd

        ddf = dd.from_pandas(sample_dataframe, npartitions=2)
        merged = ddf.merge(ddf, on="id", suffixes=("_l", "_r"))
        result = merged.compute()
        assert len(result) == len(sample_dataframe)

    def test_dask_groupby_agg(self, sample_dataframe: pd.DataFrame) -> None:
        import dask.dataframe as dd

        ddf = dd.from_pandas(sample_dataframe, npartitions=2)
        agg = ddf.groupby("label").agg({"value": "sum"}).compute()
        assert len(agg) == 2

    @pytest.mark.integration
    def test_dask_local_cluster(self) -> None:
        from distributed import Client, LocalCluster

        cluster = LocalCluster(
            n_workers=1,
            threads_per_worker=1,
            memory_limit="128MB",
            silence_logs=50,
        )
        client = Client(cluster)
        try:
            assert len(client.scheduler_info()["workers"]) == 1
        finally:
            client.close()
            cluster.close()


class TestPolarsCompatibility:
    """Verify Polars backend works with current Python."""

    def test_polars_import(self) -> None:
        import polars as pl

        assert pl.__version__

    def test_polars_from_pandas(self, sample_dataframe: pd.DataFrame) -> None:
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        assert pldf.height == len(sample_dataframe)

    def test_polars_to_pandas(self, sample_dataframe: pd.DataFrame) -> None:
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        roundtrip = pldf.to_pandas()
        assert len(roundtrip) == len(sample_dataframe)

    def test_polars_filter(self, sample_dataframe: pd.DataFrame) -> None:
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        filtered = pldf.filter(pl.col("value") > 0)
        assert 0 < filtered.height < pldf.height

    def test_polars_join(self, sample_dataframe: pd.DataFrame) -> None:
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        joined = pldf.join(pldf, on="id", suffix="_r")
        assert joined.height == pldf.height

    def test_polars_groupby_agg(self, sample_dataframe: pd.DataFrame) -> None:
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        agg = pldf.group_by("label").agg(pl.col("value").sum())
        assert agg.height == 2

    def test_polars_lazy(self, sample_dataframe: pd.DataFrame) -> None:
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        result = (
            pldf.lazy()
            .filter(pl.col("value") > 0)
            .group_by("label")
            .agg(pl.col("value").mean())
            .collect()
        )
        assert result.height == 2


class TestDuckDBLargeDatasetPatterns:
    """Verify DuckDB works for large-dataset query patterns."""

    def test_duckdb_sql_pushdown(self, sample_dataframe: pd.DataFrame) -> None:
        import duckdb

        conn = duckdb.connect()
        conn.register("nodes", sample_dataframe)
        result = conn.execute(
            "SELECT * FROM nodes WHERE value > 0 AND label = ?",
            ["Person"],
        ).df()
        assert 0 < len(result) < len(sample_dataframe)
        conn.close()

    def test_duckdb_join(self, sample_dataframe: pd.DataFrame) -> None:
        import duckdb

        conn = duckdb.connect()
        conn.register("t", sample_dataframe)
        result = conn.execute("SELECT * FROM t a JOIN t b ON a.id = b.id").df()
        assert len(result) == len(sample_dataframe)
        conn.close()

    def test_duckdb_aggregation(self, sample_dataframe: pd.DataFrame) -> None:
        import duckdb

        conn = duckdb.connect()
        conn.register("t", sample_dataframe)
        result = conn.execute(
            "SELECT label, COUNT(*), AVG(value) FROM t GROUP BY label",
        ).df()
        assert len(result) == 2
        conn.close()

    def test_duckdb_parquet_roundtrip(
        self,
        sample_dataframe: pd.DataFrame,
        tmp_path: object,
    ) -> None:
        from pathlib import Path

        import duckdb

        parquet_path = Path(str(tmp_path)) / "test.parquet"
        conn = duckdb.connect()
        conn.register("t", sample_dataframe)
        conn.execute(f"COPY t TO '{parquet_path}' (FORMAT PARQUET)")
        count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')",
        ).fetchone()
        assert count is not None
        assert count[0] == len(sample_dataframe)
        conn.close()


deltalake = pytest.importorskip("deltalake", reason="deltalake not installed")


class TestDeltaLakeCompatibility:
    """Verify Delta Lake storage layer works."""

    def test_deltalake_import(self) -> None:
        import deltalake

        assert deltalake.__version__

    def test_deltalake_write_read(
        self,
        sample_dataframe: pd.DataFrame,
        tmp_path: object,
    ) -> None:
        from pathlib import Path

        import deltalake

        delta_path = str(Path(str(tmp_path)) / "delta_table")
        deltalake.write_deltalake(delta_path, sample_dataframe)
        dt = deltalake.DeltaTable(delta_path)
        result = dt.to_pandas()
        assert len(result) == len(sample_dataframe)

    def test_deltalake_duckdb_cross_read(
        self,
        sample_dataframe: pd.DataFrame,
        tmp_path: object,
    ) -> None:
        from pathlib import Path

        import deltalake
        import duckdb

        delta_path = str(Path(str(tmp_path)) / "delta_table")
        deltalake.write_deltalake(delta_path, sample_dataframe)

        conn = duckdb.connect()
        count = conn.execute(
            f"SELECT COUNT(*) FROM delta_scan('{delta_path}')",
        ).fetchone()
        assert count is not None
        assert count[0] == len(sample_dataframe)
        conn.close()


class TestCrossBackendInterop:
    """Verify backends can exchange data via Arrow/pandas."""

    def test_dask_to_duckdb(self, sample_dataframe: pd.DataFrame) -> None:
        pytest.importorskip("dask", reason="dask not installed")
        import dask.dataframe as dd
        import duckdb

        ddf = dd.from_pandas(sample_dataframe, npartitions=2)
        materialized = ddf[ddf["value"] > 0].compute()

        conn = duckdb.connect()
        conn.register("filtered", materialized)
        count = conn.execute("SELECT COUNT(*) FROM filtered").fetchone()
        assert count is not None
        assert count[0] == len(materialized)
        conn.close()

    def test_polars_to_duckdb(self, sample_dataframe: pd.DataFrame) -> None:
        import duckdb
        import polars as pl

        pldf = pl.from_pandas(sample_dataframe)
        pdf = pldf.filter(pl.col("value") > 0).to_pandas()

        conn = duckdb.connect()
        conn.register("filtered", pdf)
        count = conn.execute("SELECT COUNT(*) FROM filtered").fetchone()
        assert count is not None
        assert count[0] == len(pdf)
        conn.close()
