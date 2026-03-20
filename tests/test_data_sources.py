"""Tests for pycypher.ingestion.data_sources.

Covers:
- Format ABC and concrete subclasses (unit, no I/O)
- FileDataSource with Format strategy (real I/O)
- Remote / cloud source mocking via unittest.mock
- Fixture-based integration tests (all formats × URI forms)
- DataFrameDataSource, ArrowDataSource, SqlDataSource
- data_source_from_uri factory dispatch
- URI scheme / extension handling
- Error cases
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest
from pycypher.ingestion.data_sources import (
    ArrowDataSource,
    CsvFormat,
    DataFrameDataSource,
    DataSource,
    FileDataSource,
    Format,
    JsonFormat,
    ParquetFormat,
    SqlDataSource,
    data_source_from_uri,
)

# Path to static fixture files
FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"


# ===========================================================================
# Helpers
# ===========================================================================


def _make_csv(tmp_path: Path, rows: list[dict]) -> Path:
    """Write *rows* to a CSV file and return its path."""
    p = tmp_path / "data.csv"
    if rows:
        with p.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    return p


def _make_parquet(tmp_path: Path, data: dict) -> Path:
    """Write an Arrow table built from *data* to a Parquet file."""
    import pyarrow.parquet as pq

    p = tmp_path / "data.parquet"
    table = pa.table(data)
    pq.write_table(table, str(p))
    return p


def _make_json(tmp_path: Path, rows: list[dict]) -> Path:
    """Write *rows* as newline-delimited JSON and return the path."""
    p = tmp_path / "data.json"
    with p.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return p


_SAMPLE_DATA = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
]


# ===========================================================================
# TestDataSourceABC — contract
# ===========================================================================


class TestDataSourceABC:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            DataSource()  # type: ignore[abstract]

    def test_concrete_subclass_is_instance(self) -> None:
        src = ArrowDataSource(pa.table({"x": [1, 2]}))
        assert isinstance(src, DataSource)

    def test_all_concrete_subclasses_have_uri(self) -> None:
        arrow_src = ArrowDataSource(pa.table({"x": [1]}))
        df_src = DataFrameDataSource(pd.DataFrame({"x": [1]}))
        assert isinstance(arrow_src.uri, str)
        assert isinstance(df_src.uri, str)


# ===========================================================================
# TestFormat — unit tests, no I/O
# ===========================================================================


class TestFormat:
    def test_format_cannot_be_instantiated(self) -> None:
        with pytest.raises(TypeError):
            Format()  # type: ignore[abstract]

    def test_csv_format_name(self) -> None:
        assert CsvFormat().name == "csv"

    def test_parquet_format_name(self) -> None:
        assert ParquetFormat().name == "parquet"

    def test_json_format_name(self) -> None:
        assert JsonFormat().name == "json"

    def test_csv_view_sql_default(self) -> None:
        assert CsvFormat().view_sql("/f.csv") == "read_csv_auto('/f.csv')"

    def test_parquet_view_sql(self) -> None:
        assert (
            ParquetFormat().view_sql("/f.parquet")
            == "read_parquet('/f.parquet')"
        )

    def test_json_view_sql_default(self) -> None:
        assert JsonFormat().view_sql("/f.json") == "read_json_auto('/f.json')"

    def test_csv_custom_delimiter(self) -> None:
        sql = CsvFormat(delimiter="|").view_sql("/f.csv")
        assert "delim='|'" in sql

    def test_csv_no_header(self) -> None:
        sql = CsvFormat(header=False).view_sql("/f.csv")
        assert "header=false" in sql

    def test_csv_null_padding(self) -> None:
        sql = CsvFormat(null_padding=True).view_sql("/f.csv")
        assert "null_padding=true" in sql

    def test_csv_default_delimiter_not_in_sql(self) -> None:
        sql = CsvFormat().view_sql("/f.csv")
        assert "delim" not in sql

    def test_json_newline_delimited(self) -> None:
        sql = JsonFormat(records="newline_delimited").view_sql("/f.json")
        assert "format=" in sql
        assert "newline_delimited" in sql

    def test_json_auto_records_no_format_arg(self) -> None:
        sql = JsonFormat(records="auto").view_sql("/f.json")
        assert "format=" not in sql


# ===========================================================================
# TestFileDataSource — real I/O
# ===========================================================================


class TestFileDataSource:
    def test_csv_read_returns_arrow(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        result = FileDataSource(str(p), CsvFormat()).read()
        assert isinstance(result, pa.Table)

    def test_csv_row_count(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        assert FileDataSource(str(p), CsvFormat()).read().num_rows == 2

    def test_csv_columns(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        result = FileDataSource(str(p), CsvFormat()).read()
        assert set(result.column_names) == {"id", "name"}

    def test_parquet_read_returns_arrow(self, tmp_path: Path) -> None:
        p = _make_parquet(tmp_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        result = FileDataSource(str(p), ParquetFormat()).read()
        assert isinstance(result, pa.Table)

    def test_parquet_row_count(self, tmp_path: Path) -> None:
        p = _make_parquet(tmp_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        assert FileDataSource(str(p), ParquetFormat()).read().num_rows == 2

    def test_parquet_columns(self, tmp_path: Path) -> None:
        p = _make_parquet(tmp_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        result = FileDataSource(str(p), ParquetFormat()).read()
        assert set(result.column_names) == {"id", "name"}

    def test_json_read_returns_arrow(self, tmp_path: Path) -> None:
        p = _make_json(tmp_path, _SAMPLE_DATA)
        result = FileDataSource(str(p), JsonFormat()).read()
        assert isinstance(result, pa.Table)

    def test_json_row_count(self, tmp_path: Path) -> None:
        p = _make_json(tmp_path, _SAMPLE_DATA)
        assert FileDataSource(str(p), JsonFormat()).read().num_rows == 2

    def test_json_columns(self, tmp_path: Path) -> None:
        p = _make_json(tmp_path, _SAMPLE_DATA)
        result = FileDataSource(str(p), JsonFormat()).read()
        assert set(result.column_names) == {"id", "name"}

    def test_format_property(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        fmt = CsvFormat()
        src = FileDataSource(str(p), fmt)
        assert src.format is fmt

    def test_uri_property(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        src = FileDataSource(str(p), CsvFormat())
        assert src.uri == str(p)

    def test_query_property_none_by_default(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        src = FileDataSource(str(p), CsvFormat())
        assert src.query is None

    def test_csv_query_override(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        src = FileDataSource(
            str(p), CsvFormat(), query="SELECT name FROM source WHERE id = 1"
        )
        result = src.read()
        assert result.num_rows == 1
        assert result.column_names == ["name"]

    def test_parquet_query_override(self, tmp_path: Path) -> None:
        p = _make_parquet(tmp_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        src = FileDataSource(
            str(p), ParquetFormat(), query="SELECT name FROM source"
        )
        result = src.read()
        assert result.column_names == ["name"]

    def test_json_query_override(self, tmp_path: Path) -> None:
        p = _make_json(tmp_path, _SAMPLE_DATA)
        src = FileDataSource(
            str(p), JsonFormat(), query="SELECT name FROM source WHERE id = 1"
        )
        result = src.read()
        assert result.num_rows == 1

    def test_file_uri_scheme_stripped(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        src = FileDataSource(f"file://{p}", CsvFormat())
        result = src.read()
        assert result.num_rows == 2


# ===========================================================================
# TestRemoteSourceMocking — verify URI pass-through without real network
# ===========================================================================


def _mock_duckdb_connect(preset_table: pa.Table) -> MagicMock:
    """Build a mock duckdb.connect() that returns *preset_table* on .to_arrow_table().

    The mock connection supports the context-manager protocol so that
    ``with duckdb.connect() as con:`` works correctly in tests.
    """
    mock_result = MagicMock()
    mock_result.to_arrow_table.return_value = preset_table

    mock_con = MagicMock()
    mock_con.execute.return_value = mock_result
    # Context manager: __enter__ returns the connection itself
    mock_con.__enter__ = MagicMock(return_value=mock_con)
    mock_con.__exit__ = MagicMock(return_value=False)

    mock_connect = MagicMock(return_value=mock_con)
    return mock_connect


_PRESET_TABLE = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})


class TestRemoteSourceMocking:
    def _assert_view_sql_contains(
        self, mock_connect: MagicMock, fragment: str
    ) -> None:
        """Assert that the first execute() call on the connection contains *fragment*."""
        con = mock_connect.return_value
        first_call_sql: str = con.execute.call_args_list[0][0][0]
        assert fragment in first_call_sql

    def test_s3_csv_uri_passes_through(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        with patch("duckdb.connect", mock_connect):
            result = FileDataSource("s3://bucket/data.csv", CsvFormat()).read()
        self._assert_view_sql_contains(
            mock_connect, "read_csv_auto('s3://bucket/data.csv')"
        )
        assert result is _PRESET_TABLE
        mock_connect.assert_called_once()

    def test_s3_parquet_uri_passes_through(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        with patch("duckdb.connect", mock_connect):
            result = FileDataSource(
                "s3://bucket/data.parquet", ParquetFormat()
            ).read()
        self._assert_view_sql_contains(
            mock_connect, "read_parquet('s3://bucket/data.parquet')"
        )
        assert result is _PRESET_TABLE
        mock_connect.assert_called_once()

    def test_gcs_parquet_uri_passes_through(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        with patch("duckdb.connect", mock_connect):
            FileDataSource("gs://bucket/data.parquet", ParquetFormat()).read()
        self._assert_view_sql_contains(
            mock_connect, "read_parquet('gs://bucket/data.parquet')"
        )

    def test_https_csv_uri_passes_through(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        # Mock DNS resolution so the SSRF check doesn't reject unresolvable test hostname
        fake_addrinfo = [(2, 1, 6, "", ("93.184.216.34", 0))]
        with (
            patch("duckdb.connect", mock_connect),
            patch("socket.getaddrinfo", return_value=fake_addrinfo),
        ):
            FileDataSource("https://host/data.csv", CsvFormat()).read()
        self._assert_view_sql_contains(
            mock_connect, "read_csv_auto('https://host/data.csv')"
        )

    def test_abfss_json_uri_passes_through(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        uri = "abfss://container@account/data.json"
        with patch("duckdb.connect", mock_connect):
            FileDataSource(uri, JsonFormat()).read()
        self._assert_view_sql_contains(
            mock_connect, f"read_json_auto('{uri}')"
        )

    def test_file_scheme_stripped_before_duckdb(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        with patch("duckdb.connect", mock_connect):
            FileDataSource("file:///abs/path.csv", CsvFormat()).read()
        # scheme must be stripped; bare path passed
        self._assert_view_sql_contains(
            mock_connect, "read_csv_auto('/abs/path.csv')"
        )

    def test_custom_query_passed_as_second_execute(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        q = "SELECT id FROM source WHERE id = 1"
        with patch("duckdb.connect", mock_connect):
            FileDataSource("s3://bucket/data.csv", CsvFormat(), query=q).read()
        con = mock_connect.return_value
        second_call_sql: str = con.execute.call_args_list[1][0][0]
        assert second_call_sql == q

    def test_dataframe_source_uses_select_from_df(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        with patch("duckdb.connect", mock_connect):
            result = DataFrameDataSource(df).read()
        con = mock_connect.return_value
        call_sql: str = con.execute.call_args_list[0][0][0]
        assert "SELECT * FROM df" in call_sql
        assert result is _PRESET_TABLE

    def test_sql_source_connects_with_uri(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        uri = "duckdb:///path/to/db.duckdb"
        with patch("duckdb.connect", mock_connect):
            SqlDataSource(uri, "SELECT 1").read()
        mock_connect.assert_called_once_with(uri)

    def test_mock_returns_preset_table_unchanged(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        with patch("duckdb.connect", mock_connect):
            result = FileDataSource("s3://b/f.parquet", ParquetFormat()).read()
        assert result is _PRESET_TABLE

    def test_duckdb_connect_called_once(self) -> None:
        mock_connect = _mock_duckdb_connect(_PRESET_TABLE)
        with patch("duckdb.connect", mock_connect):
            FileDataSource("s3://b/f.csv", CsvFormat()).read()
        mock_connect.assert_called_once()


# ===========================================================================
# TestFileDataSourceFixtures — exhaustive format × URI-scheme integration
# ===========================================================================


class TestFileDataSourceFixtures:
    """Integration tests using static fixture files in tests/fixtures/data/."""

    def test_csv_bare_path_row_count(self) -> None:
        src = FileDataSource(str(FIXTURES_DATA / "sample.csv"), CsvFormat())
        assert src.read().num_rows == 2

    def test_csv_bare_path_columns(self) -> None:
        src = FileDataSource(str(FIXTURES_DATA / "sample.csv"), CsvFormat())
        assert set(src.read().column_names) == {"id", "name", "value"}

    def test_csv_file_uri_row_count(self) -> None:
        uri = (FIXTURES_DATA / "sample.csv").as_uri()
        src = FileDataSource(uri, CsvFormat())
        assert src.read().num_rows == 2

    def test_csv_query_override_filters(self) -> None:
        src = FileDataSource(
            str(FIXTURES_DATA / "sample.csv"),
            CsvFormat(),
            query="SELECT * FROM source WHERE id = 1",
        )
        assert src.read().num_rows == 1

    def test_csv_pipe_delimiter(self) -> None:
        src = FileDataSource(
            str(FIXTURES_DATA / "sample_pipe.csv"), CsvFormat(delimiter="|")
        )
        result = src.read()
        assert result.num_rows == 2
        assert set(result.column_names) == {"id", "name", "value"}

    def test_parquet_bare_path_row_count(
        self, sample_parquet_path: Path
    ) -> None:
        src = FileDataSource(str(sample_parquet_path), ParquetFormat())
        assert src.read().num_rows == 2

    def test_parquet_bare_path_columns(
        self, sample_parquet_path: Path
    ) -> None:
        src = FileDataSource(str(sample_parquet_path), ParquetFormat())
        assert set(src.read().column_names) == {"id", "name", "value"}

    def test_parquet_file_uri_row_count(
        self, sample_parquet_path: Path
    ) -> None:
        uri = sample_parquet_path.as_uri()
        src = FileDataSource(uri, ParquetFormat())
        assert src.read().num_rows == 2

    def test_parquet_query_override_filters(
        self, sample_parquet_path: Path
    ) -> None:
        src = FileDataSource(
            str(sample_parquet_path),
            ParquetFormat(),
            query="SELECT * FROM source WHERE id = 1",
        )
        assert src.read().num_rows == 1

    def test_json_bare_path_row_count(self) -> None:
        src = FileDataSource(str(FIXTURES_DATA / "sample.json"), JsonFormat())
        assert src.read().num_rows == 2

    def test_json_bare_path_columns(self) -> None:
        src = FileDataSource(str(FIXTURES_DATA / "sample.json"), JsonFormat())
        assert set(src.read().column_names) == {"id", "name", "value"}

    def test_json_file_uri_row_count(self) -> None:
        uri = (FIXTURES_DATA / "sample.json").as_uri()
        src = FileDataSource(uri, JsonFormat())
        assert src.read().num_rows == 2


# ===========================================================================
# TestDataFrameDataSource
# ===========================================================================


class TestDataFrameDataSource:
    def test_read_returns_arrow_table(self) -> None:
        df = pd.DataFrame(_SAMPLE_DATA)
        result = DataFrameDataSource(df).read()
        assert isinstance(result, pa.Table)

    def test_row_count(self) -> None:
        df = pd.DataFrame(_SAMPLE_DATA)
        result = DataFrameDataSource(df).read()
        assert result.num_rows == 2

    def test_columns(self) -> None:
        df = pd.DataFrame(_SAMPLE_DATA)
        result = DataFrameDataSource(df).read()
        assert set(result.column_names) == {"id", "name"}

    def test_uri_placeholder(self) -> None:
        src = DataFrameDataSource(pd.DataFrame({"x": [1]}))
        assert src.uri == "<dataframe>"

    def test_dataframe_property(self) -> None:
        df = pd.DataFrame({"x": [1]})
        src = DataFrameDataSource(df)
        assert src.dataframe is df


# ===========================================================================
# TestArrowDataSource
# ===========================================================================


class TestArrowDataSource:
    def test_read_returns_same_table(self) -> None:
        table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        src = ArrowDataSource(table)
        result = src.read()
        assert result is table  # exact same object, no copy

    def test_uri_placeholder(self) -> None:
        src = ArrowDataSource(pa.table({"x": [1]}))
        assert src.uri == "<arrow>"

    def test_table_property(self) -> None:
        table = pa.table({"x": [1, 2]})
        src = ArrowDataSource(table)
        assert src.table is table

    def test_row_count(self) -> None:
        table = pa.table({"id": [1, 2, 3]})
        result = ArrowDataSource(table).read()
        assert result.num_rows == 3


# ===========================================================================
# TestSqlDataSource — uses in-process DuckDB file
# ===========================================================================


class TestSqlDataSource:
    def test_read_from_duckdb(self, tmp_path: Path) -> None:
        import duckdb

        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")
        con.execute("INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')")
        con.close()

        src = SqlDataSource(str(db_path), query="SELECT * FROM people")
        result = src.read()
        assert isinstance(result, pa.Table)
        assert result.num_rows == 2

    def test_uri_property(self, tmp_path: Path) -> None:
        uri = str(tmp_path / "x.duckdb")
        src = SqlDataSource(uri, query="SELECT 1")
        assert src.uri == uri

    def test_query_property(self) -> None:
        q = "SELECT * FROM foo"
        src = SqlDataSource("duckdb:///path.duckdb", query=q)
        assert src.query == q


# ===========================================================================
# TestDataSourceFromUri — factory dispatch
# ===========================================================================


class TestDataSourceFromUri:
    # --- in-memory types ---

    def test_arrow_table_dispatch(self) -> None:
        table = pa.table({"x": [1]})
        src = data_source_from_uri(table)
        assert isinstance(src, ArrowDataSource)

    def test_dataframe_dispatch(self) -> None:
        df = pd.DataFrame({"x": [1]})
        src = data_source_from_uri(df)
        assert isinstance(src, DataFrameDataSource)

    # --- file extension dispatch → FileDataSource ---

    def test_csv_bare_path_returns_file_source(self) -> None:
        src = data_source_from_uri("/data/people.csv")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, CsvFormat)

    def test_parquet_bare_path_returns_file_source(self) -> None:
        src = data_source_from_uri("/data/people.parquet")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, ParquetFormat)

    def test_json_bare_path_returns_file_source(self) -> None:
        src = data_source_from_uri("/data/people.json")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, JsonFormat)

    def test_csv_file_uri(self) -> None:
        src = data_source_from_uri("file:///data/people.csv")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, CsvFormat)

    def test_parquet_file_uri(self) -> None:
        src = data_source_from_uri("file:///data/people.parquet")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, ParquetFormat)

    def test_json_file_uri(self) -> None:
        src = data_source_from_uri("file:///data/people.json")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, JsonFormat)

    def test_csv_s3_uri(self) -> None:
        src = data_source_from_uri("s3://bucket/prefix/people.csv")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, CsvFormat)

    def test_parquet_s3_uri(self) -> None:
        src = data_source_from_uri("s3://bucket/prefix/data.parquet")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, ParquetFormat)

    def test_json_https_uri(self) -> None:
        src = data_source_from_uri("https://example.com/data.json")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, JsonFormat)

    def test_csv_https_uri(self) -> None:
        src = data_source_from_uri("https://example.com/export.csv")
        assert isinstance(src, FileDataSource)
        assert isinstance(src.format, CsvFormat)

    # --- SQL scheme dispatch ---

    def test_postgresql_scheme(self) -> None:
        src = data_source_from_uri(
            "postgresql://user:pass@host/db",
            query="SELECT * FROM t",
        )
        assert isinstance(src, SqlDataSource)

    def test_postgres_scheme_alias(self) -> None:
        src = data_source_from_uri(
            "postgres://user:pass@host/db",
            query="SELECT 1",
        )
        assert isinstance(src, SqlDataSource)

    def test_mysql_scheme(self) -> None:
        src = data_source_from_uri(
            "mysql://user:pass@host/db",
            query="SELECT 1",
        )
        assert isinstance(src, SqlDataSource)

    def test_sqlite_scheme(self) -> None:
        src = data_source_from_uri(
            "sqlite:///path/to/db.sqlite",
            query="SELECT * FROM t",
        )
        assert isinstance(src, SqlDataSource)

    def test_duckdb_scheme(self) -> None:
        src = data_source_from_uri(
            "duckdb:///path/to/db.duckdb",
            query="SELECT * FROM t",
        )
        assert isinstance(src, SqlDataSource)

    # --- query propagation ---

    def test_query_forwarded_to_csv(self) -> None:
        q = "SELECT id FROM source WHERE active"
        src = data_source_from_uri("/data/people.csv", query=q)
        assert isinstance(src, FileDataSource)
        assert src.query == q

    def test_query_forwarded_to_parquet(self) -> None:
        q = "SELECT name FROM source"
        src = data_source_from_uri("/data/people.parquet", query=q)
        assert src.query == q

    def test_query_stored_on_sql_source(self) -> None:
        q = "SELECT * FROM people"
        src = data_source_from_uri("postgresql://host/db", query=q)
        assert src.query == q  # type: ignore[union-attr]

    # --- error cases ---

    def test_sql_without_query_raises(self) -> None:
        with pytest.raises(ValueError, match="requires a 'query'"):
            data_source_from_uri("postgresql://host/db")

    def test_unrecognised_extension_raises(self) -> None:
        with pytest.raises(ValueError, match="recognised extension"):
            data_source_from_uri("/data/file.xlsx")

    def test_bad_type_raises(self) -> None:
        with pytest.raises(TypeError, match="str, pd.DataFrame, or pa.Table"):
            data_source_from_uri(42)  # type: ignore[arg-type]

    def test_no_extension_raises(self) -> None:
        with pytest.raises(ValueError, match="recognised extension"):
            data_source_from_uri("/data/file")

    # --- end-to-end: factory → read ---

    def test_factory_csv_round_trip(self, tmp_path: Path) -> None:
        p = _make_csv(tmp_path, _SAMPLE_DATA)
        src = data_source_from_uri(str(p))
        result = src.read()
        assert result.num_rows == 2

    def test_factory_parquet_round_trip(self, tmp_path: Path) -> None:
        p = _make_parquet(tmp_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        src = data_source_from_uri(str(p))
        result = src.read()
        assert result.num_rows == 2

    def test_factory_json_round_trip(self, tmp_path: Path) -> None:
        p = _make_json(tmp_path, _SAMPLE_DATA)
        src = data_source_from_uri(str(p))
        result = src.read()
        assert result.num_rows == 2

    def test_factory_dataframe_round_trip(self) -> None:
        df = pd.DataFrame(_SAMPLE_DATA)
        src = data_source_from_uri(df)
        result = src.read()
        assert result.num_rows == 2

    def test_factory_arrow_round_trip(self) -> None:
        table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        src = data_source_from_uri(table)
        result = src.read()
        assert result is table


# ---------------------------------------------------------------------------
# DuckDB connection cleanup
# ---------------------------------------------------------------------------


class TestDuckDBConnectionCleanup:
    """Every read() that opens a DuckDB connection must close it afterward.

    Connections are verified via unittest.mock so no real files are needed.
    The mock chain: patch('duckdb.connect') → mock_con; after read(), assert
    mock_con.close() was called exactly once (or __exit__ was called, which
    the with-statement triggers automatically).
    """

    def _build_mock_con(self) -> MagicMock:
        """Return a MagicMock that satisfies the duckdb connection protocol."""
        mock_con = MagicMock()
        # Support 'with duckdb.connect() as con:' context manager protocol
        mock_con.__enter__ = MagicMock(return_value=mock_con)
        mock_con.__exit__ = MagicMock(return_value=False)
        # con.execute(sql).to_arrow_table() chain
        mock_con.execute.return_value.to_arrow_table.return_value = pa.table(
            {"x": [1]}
        )
        return mock_con

    def test_file_data_source_closes_connection(self, tmp_path: Path) -> None:
        """FileDataSource.read() must close the DuckDB connection."""
        src = FileDataSource(str(tmp_path / "f.csv"), CsvFormat())
        mock_con = self._build_mock_con()

        with patch("duckdb.connect", return_value=mock_con):
            src.read()

        # Context manager protocol: __exit__ called once → connection closed
        mock_con.__exit__.assert_called_once()

    def test_dataframe_data_source_closes_connection(self) -> None:
        """DataFrameDataSource.read() must close the DuckDB connection."""
        df = pd.DataFrame({"x": [1, 2]})
        src = DataFrameDataSource(df)
        mock_con = self._build_mock_con()

        with patch("duckdb.connect", return_value=mock_con):
            src.read()

        mock_con.__exit__.assert_called_once()

    def test_sql_data_source_closes_connection(self) -> None:
        """SqlDataSource.read() must close the DuckDB connection."""
        src = SqlDataSource("sqlite:///:memory:", "SELECT 1 AS x")
        mock_con = self._build_mock_con()

        with patch("duckdb.connect", return_value=mock_con):
            src.read()

        mock_con.__exit__.assert_called_once()

    def test_connection_closed_even_if_execute_raises(
        self, tmp_path: Path
    ) -> None:
        """DuckDB connection must be closed even when execute() raises."""
        src = FileDataSource(str(tmp_path / "f.csv"), CsvFormat())
        mock_con = self._build_mock_con()
        mock_con.__exit__ = MagicMock(return_value=False)
        mock_con.execute.side_effect = RuntimeError("boom")

        with patch("duckdb.connect", return_value=mock_con):
            with pytest.raises(RuntimeError):
                src.read()

        # __exit__ must still be called (cleanup on exception)
        mock_con.__exit__.assert_called_once()
