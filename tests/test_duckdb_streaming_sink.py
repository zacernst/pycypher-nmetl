"""Phase 3 (out-of-core DuckDB) — streaming sink via COPY ... TO.

Verifies write_relation_to_uri streams a DuckDBLazyFrame to csv/parquet/json
without a pandas frame, produces output equivalent to the pandas writer, honours
format inference and explicit format, rejects cloud URIs and non-relation input,
and round-trips with Phase 2's read_relation.

See docs/duckdb_out_of_core_design.md, Phase 3.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest
from pycypher.backends.duckdb_backend import create_duckdb_connection
from pycypher.ingestion.config import OutputFormat
from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.ingestion.output_writer import (
    write_dataframe_to_uri,
    write_relation_to_uri,
)


@pytest.fixture
def con():
    c = create_duckdb_connection()
    yield c
    c.close()


@pytest.fixture
def src_parquet(tmp_path):
    path = tmp_path / "src.parquet"
    pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    ).to_parquet(path)
    return path


def _relation(con, src_parquet):
    return data_source_from_uri(str(src_parquet)).read_relation(con)


class TestStreamingWrite:
    def test_parquet(self, con, src_parquet, tmp_path) -> None:
        out = tmp_path / "out.parquet"
        write_relation_to_uri(_relation(con, src_parquet), str(out))
        assert out.exists()
        got = pd.read_parquet(out).sort_values("id").reset_index(drop=True)
        assert got["name"].tolist() == ["Alice", "Bob", "Carol"]

    def test_csv(self, con, src_parquet, tmp_path) -> None:
        out = tmp_path / "out.csv"
        write_relation_to_uri(_relation(con, src_parquet), str(out))
        got = pd.read_csv(out).sort_values("id").reset_index(drop=True)
        assert got["age"].tolist() == [30, 25, 35]
        assert list(got.columns) == ["id", "name", "age"]  # header written

    def test_json_ndjson(self, con, src_parquet, tmp_path) -> None:
        out = tmp_path / "out.json"
        write_relation_to_uri(_relation(con, src_parquet), str(out))
        lines = [json.loads(line) for line in out.read_text().splitlines() if line.strip()]
        assert len(lines) == 3
        assert {row["name"] for row in lines} == {"Alice", "Bob", "Carol"}


class TestParityWithPandasWriter:
    @pytest.mark.parametrize("ext", ["parquet", "csv", "json"])
    def test_relation_output_matches_dataframe_output(
        self, con, src_parquet, tmp_path, ext,
    ) -> None:
        rel = _relation(con, src_parquet)
        df = rel.to_pandas()

        rel_out = tmp_path / f"rel.{ext}"
        df_out = tmp_path / f"df.{ext}"
        write_relation_to_uri(_relation(con, src_parquet), str(rel_out))
        write_dataframe_to_uri(df, str(df_out))

        if ext == "parquet":
            a = pd.read_parquet(rel_out)
            b = pd.read_parquet(df_out)
        elif ext == "csv":
            a = pd.read_csv(rel_out)
            b = pd.read_csv(df_out)
        else:
            a = pd.read_json(rel_out, lines=True)
            b = pd.read_json(df_out, lines=True)

        a = a.sort_values("id").reset_index(drop=True)
        b = b.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(a, b, check_dtype=False)


class TestFormatResolution:
    def test_explicit_format_overrides_extension(self, con, src_parquet, tmp_path) -> None:
        out = tmp_path / "out.dat"  # unknown extension
        write_relation_to_uri(_relation(con, src_parquet), str(out), OutputFormat.CSV)
        got = pd.read_csv(out)
        assert len(got) == 3

    def test_unknown_extension_raises(self, con, src_parquet, tmp_path) -> None:
        out = tmp_path / "out.dat"
        with pytest.raises(ValueError, match="Cannot infer output format"):
            write_relation_to_uri(_relation(con, src_parquet), str(out))


class TestGuards:
    def test_cloud_uri_rejected(self, con, src_parquet) -> None:
        with pytest.raises(NotImplementedError, match="Cloud output"):
            write_relation_to_uri(_relation(con, src_parquet), "s3://bucket/out.parquet")

    def test_non_relation_rejected(self, tmp_path) -> None:
        with pytest.raises(TypeError, match="DuckDBLazyFrame"):
            write_relation_to_uri(pd.DataFrame({"a": [1]}), str(tmp_path / "x.csv"))


class TestRoundTrip:
    def test_read_relation_then_write_relation(self, con, src_parquet, tmp_path) -> None:
        # Full streaming path: file -> read_relation -> write_relation -> file.
        out = tmp_path / "roundtrip.parquet"
        write_relation_to_uri(_relation(con, src_parquet), str(out))
        original = pd.read_parquet(src_parquet).sort_values("id").reset_index(drop=True)
        result = pd.read_parquet(out).sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(original, result, check_dtype=False)
