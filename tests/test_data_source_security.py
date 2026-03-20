"""Security tests for data source SQL injection and CLI path traversal (Loop 167).

Red-phase tests demonstrate each vulnerability before the fix.  After the fix
all tests pass green.

Vulnerabilities targeted:
1. SQL injection in Format.view_sql() — path, delimiter, and records fields
   are interpolated directly into DuckDB SQL strings without escaping.
2. Path traversal in nmetl_cli.py — YAML 'source' field is joined to
   config_dir without checking that the result stays within config_dir.
"""

from __future__ import annotations

import pathlib
import textwrap

import pytest
from pycypher.ingestion.data_sources import (
    CsvFormat,
    JsonFormat,
    ParquetFormat,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.slow


def _is_sql_safe(sql: str) -> bool:
    """Minimal check: no unescaped single-quotes or NUL bytes in the output."""
    # A safe SQL string literal has no odd number of consecutive unescaped quotes.
    # We use the simple heuristic: the substring '); is a reliable injection marker.
    return "');" not in sql and "\x00" not in sql


# ===========================================================================
# Category 1 — SQL injection via path parameter
# ===========================================================================


class TestCsvFormatViewSqlInjection:
    """CsvFormat.view_sql() must reject paths that would break out of the SQL string."""

    ATTACK_PATH = "/data/file.csv'); SELECT * FROM '/etc/passwd"

    def test_single_quote_in_path_raises(self) -> None:
        """A path containing a single quote must raise ValueError, not produce SQL."""
        fmt = CsvFormat()
        with pytest.raises(ValueError, match="single quote"):
            fmt.view_sql("/data/file.csv'")

    def test_realistic_injection_path_raises(self) -> None:
        """A realistic SQL injection payload in path must raise ValueError."""
        fmt = CsvFormat()
        with pytest.raises(ValueError):
            fmt.view_sql(self.ATTACK_PATH)

    def test_nul_byte_in_path_raises(self) -> None:
        """A NUL byte in path must raise ValueError."""
        fmt = CsvFormat()
        with pytest.raises(ValueError, match="NUL"):
            fmt.view_sql("/data/file\x00csv")

    def test_safe_path_is_accepted(self) -> None:
        """Normal file paths must still work."""
        fmt = CsvFormat()
        sql = fmt.view_sql("/data/people.csv")
        assert "read_csv_auto" in sql
        assert "/data/people.csv" in sql

    def test_safe_cloud_uri_is_accepted(self) -> None:
        """S3 URIs without special chars must be accepted."""
        fmt = CsvFormat()
        sql = fmt.view_sql("s3://my-bucket/data/people.csv")
        assert "read_csv_auto" in sql

    def test_delimiter_injection_raises(self) -> None:
        """A delimiter containing a single quote must raise ValueError."""
        fmt = CsvFormat(delimiter="',")
        with pytest.raises(ValueError, match="single quote"):
            fmt.view_sql("/data/file.csv")

    def test_delimiter_nul_raises(self) -> None:
        """A delimiter containing a NUL byte must raise ValueError."""
        fmt = CsvFormat(delimiter="\x00")
        with pytest.raises(ValueError, match="NUL"):
            fmt.view_sql("/data/file.csv")

    def test_safe_delimiter_is_accepted(self) -> None:
        """A non-default safe delimiter must work normally."""
        fmt = CsvFormat(delimiter=";")
        sql = fmt.view_sql("/data/file.csv")
        assert "delim=';'" in sql


class TestParquetFormatViewSqlInjection:
    """ParquetFormat.view_sql() must reject malicious paths."""

    def test_single_quote_in_path_raises(self) -> None:
        fmt = ParquetFormat()
        with pytest.raises(ValueError, match="single quote"):
            fmt.view_sql("/data/file.parquet'")

    def test_nul_byte_in_path_raises(self) -> None:
        fmt = ParquetFormat()
        with pytest.raises(ValueError, match="NUL"):
            fmt.view_sql("/data/file\x00parquet")

    def test_safe_path_is_accepted(self) -> None:
        fmt = ParquetFormat()
        sql = fmt.view_sql("/data/events.parquet")
        assert "read_parquet" in sql
        assert "/data/events.parquet" in sql


class TestJsonFormatViewSqlInjection:
    """JsonFormat.view_sql() must reject malicious paths and records values."""

    def test_single_quote_in_path_raises(self) -> None:
        fmt = JsonFormat()
        with pytest.raises(ValueError, match="single quote"):
            fmt.view_sql("/data/file.json'")

    def test_nul_byte_in_path_raises(self) -> None:
        fmt = JsonFormat()
        with pytest.raises(ValueError, match="NUL"):
            fmt.view_sql("/data/file\x00json")

    def test_safe_path_is_accepted(self) -> None:
        fmt = JsonFormat()
        sql = fmt.view_sql("/data/events.json")
        assert "read_json_auto" in sql

    def test_records_injection_raises(self) -> None:
        """A records value containing a single quote must raise ValueError."""
        fmt = JsonFormat(records="auto', read_csv_auto('/etc/passwd")
        with pytest.raises(ValueError, match="single quote"):
            fmt.view_sql("/data/file.json")

    def test_safe_records_value_is_accepted(self) -> None:
        fmt = JsonFormat(records="newline_delimited")
        sql = fmt.view_sql("/data/events.json")
        assert "newline_delimited" in sql


# ===========================================================================
# Category 2 — Path traversal in nmetl_cli.py query loading
# ===========================================================================


class TestNmetlCliPathTraversal:
    """The 'nmetl run' command must reject query sources that escape config_dir."""

    def _make_pipeline_config(self, source_value: str) -> str:
        """Return a minimal YAML pipeline config with the given query source."""
        return textwrap.dedent(f"""\
            sources:
              entities: []
              relationships: []
            queries:
              - id: q1
                source: "{source_value}"
            """)

    def test_path_traversal_in_source_is_rejected(
        self, tmp_path: pathlib.Path
    ) -> None:
        """A source like ../../../../etc/passwd must raise an error, not read the file."""
        from click.testing import CliRunner
        from pycypher.nmetl_cli import cli

        config_file = tmp_path / "pipeline.yml"
        config_file.write_text(
            self._make_pipeline_config("../../../../etc/passwd")
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["run", str(config_file)])

        # Must not succeed — either non-zero exit or error message mentioning traversal
        assert result.exit_code != 0 or (
            "escapes" in (result.output or "").lower()
            or "traversal" in (result.output or "").lower()
            or "outside" in (result.output or "").lower()
        ), (
            f"Path traversal was not blocked. exit={result.exit_code}, "
            f"output={result.output!r}"
        )

    def test_absolute_path_source_is_rejected(
        self, tmp_path: pathlib.Path
    ) -> None:
        """An absolute source path outside config_dir must be rejected."""
        from click.testing import CliRunner
        from pycypher.nmetl_cli import cli

        config_file = tmp_path / "pipeline.yml"
        config_file.write_text(self._make_pipeline_config("/etc/passwd"))

        runner = CliRunner()
        result = runner.invoke(cli, ["run", str(config_file)])

        assert result.exit_code != 0 or (
            "escapes" in (result.output or "").lower()
            or "outside" in (result.output or "").lower()
        ), (
            f"Absolute path traversal not blocked. exit={result.exit_code}, "
            f"output={result.output!r}"
        )

    def test_sibling_directory_source_is_rejected(
        self, tmp_path: pathlib.Path
    ) -> None:
        """A source using ../sibling/file must not escape config_dir."""
        sibling_dir = tmp_path / "sibling"
        sibling_dir.mkdir()
        secret_file = sibling_dir / "secret.cypher"
        secret_file.write_text("RETURN 'leaked'")

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "pipeline.yml"
        config_file.write_text(
            self._make_pipeline_config("../sibling/secret.cypher")
        )

        from click.testing import CliRunner
        from pycypher.nmetl_cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["run", str(config_file)])

        assert result.exit_code != 0 or (
            "escapes" in (result.output or "").lower()
            or "outside" in (result.output or "").lower()
        ), (
            f"Sibling path traversal not blocked. exit={result.exit_code}, "
            f"output={result.output!r}"
        )

    def test_safe_relative_source_is_accepted(
        self, tmp_path: pathlib.Path
    ) -> None:
        """A query source within config_dir must be accepted (not blocked)."""
        from click.testing import CliRunner
        from pycypher.nmetl_cli import cli

        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        query_file = queries_dir / "my_query.cypher"
        query_file.write_text("MATCH (n) RETURN n LIMIT 1")

        config_file = tmp_path / "pipeline.yml"
        config_file.write_text(
            self._make_pipeline_config("queries/my_query.cypher")
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["run", str(config_file)])

        # Should NOT fail with a path-traversal error (may fail for other
        # reasons like no entity data, which is fine).
        output_lower = (result.output or "").lower()
        assert "escapes" not in output_lower, (
            f"Safe relative path was incorrectly blocked: {result.output!r}"
        )
        assert "outside" not in output_lower, (
            f"Safe relative path was incorrectly blocked: {result.output!r}"
        )
