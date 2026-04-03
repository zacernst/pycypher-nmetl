"""Tests for the `nmetl query` ad-hoc query subcommand.

Tests are organised into:
  1. Unit tests for argument-parsing helpers (_parse_entity_arg, _parse_rel_arg)
  2. Click CliRunner integration tests for the `query` subcommand itself
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner
from pycypher.nmetl_cli import _parse_entity_arg, _parse_rel_arg, cli

# ---------------------------------------------------------------------------
# Shared fixture: sample CSV data files
# ---------------------------------------------------------------------------

_FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"
_SAMPLE_CSV = _FIXTURES_DATA / "sample.csv"  # id,name,value  (2 rows)


# ===========================================================================
# Unit tests — _parse_entity_arg
# ===========================================================================


class TestParseEntityArg:
    """Unit tests for the ``_parse_entity_arg(spec)`` helper."""

    def test_basic_label_equals_path(self) -> None:
        label, path, id_col = _parse_entity_arg("Person=data/people.csv")
        assert label == "Person"
        assert path == "data/people.csv"
        assert id_col is None

    def test_label_equals_path_with_id_col(self) -> None:
        label, path, id_col = _parse_entity_arg(
            "Person=data/people.csv:person_id",
        )
        assert label == "Person"
        assert path == "data/people.csv"
        assert id_col == "person_id"

    def test_path_with_multiple_colons_only_last_is_id_col(self) -> None:
        # Windows-style paths could contain colons; only the LAST colon is id_col
        label, path, id_col = _parse_entity_arg(
            "Item=C:/Users/data/items.csv:item_id",
        )
        assert label == "Item"
        assert id_col == "item_id"
        assert path == "C:/Users/data/items.csv"

    def test_missing_equals_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Expected 'Label=path"):
            _parse_entity_arg("PersonData/people.csv")

    def test_empty_label_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="label must not be empty"):
            _parse_entity_arg("=data/people.csv")

    def test_empty_path_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="path must not be empty"):
            _parse_entity_arg("Person=")

    def test_whitespace_stripped_from_label_and_path(self) -> None:
        label, path, id_col = _parse_entity_arg(
            "  Person  =  data/people.csv  ",
        )
        assert label == "Person"
        assert path == "data/people.csv"


# ===========================================================================
# Unit tests — _parse_rel_arg
# ===========================================================================


class TestParseRelArg:
    """Unit tests for the ``_parse_rel_arg(spec)`` helper."""

    def test_basic_rel_spec(self) -> None:
        rel_type, path, src_col, tgt_col = _parse_rel_arg(
            "KNOWS=data/knows.csv:from_id:to_id",
        )
        assert rel_type == "KNOWS"
        assert path == "data/knows.csv"
        assert src_col == "from_id"
        assert tgt_col == "to_id"

    def test_missing_equals_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Expected 'REL=path"):
            _parse_rel_arg("KNOWSdata/knows.csv:from_id:to_id")

    def test_missing_source_col_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="source_col and target_col"):
            _parse_rel_arg("KNOWS=data/knows.csv")

    def test_missing_target_col_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="source_col and target_col"):
            _parse_rel_arg("KNOWS=data/knows.csv:from_id")

    def test_empty_rel_type_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="rel_type must not be empty"):
            _parse_rel_arg("=data/knows.csv:from_id:to_id")

    def test_whitespace_stripped(self) -> None:
        rel_type, path, src_col, tgt_col = _parse_rel_arg(
            "  KNOWS  =  data/knows.csv  :  from_id  :  to_id  ",
        )
        assert rel_type == "KNOWS"
        assert path == "data/knows.csv"
        assert src_col == "from_id"
        assert tgt_col == "to_id"


# ===========================================================================
# CliRunner integration tests — `nmetl query`
# ===========================================================================


class TestQueryCommand:
    """CliRunner integration tests for `nmetl query`."""

    runner = CliRunner()

    # -----------------------------------------------------------------------
    # Error paths
    # -----------------------------------------------------------------------

    def test_empty_query_exits_nonzero(self) -> None:
        result = self.runner.invoke(cli, ["query", ""])
        assert result.exit_code != 0

    def test_no_entity_exits_nonzero(self) -> None:
        """A query with no --entity sources should exit non-zero."""
        result = self.runner.invoke(
            cli,
            ["query", "MATCH (p:Person) RETURN p.name AS name"],
        )
        assert result.exit_code != 0

    def test_missing_entity_file_exits_nonzero(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (p:Person) RETURN p.name AS name",
                "--entity",
                "Person=/no/such/file.csv",
            ],
        )
        assert result.exit_code != 0

    def test_malformed_entity_spec_exits_nonzero(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (p:Person) RETURN p.name AS name",
                "--entity",
                "not-valid",
            ],
        )
        assert result.exit_code != 0

    def test_malformed_rel_spec_exits_nonzero(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (p:Person)-[r:KNOWS]->(q:Person) RETURN p.name",
                "--entity",
                f"Person={_SAMPLE_CSV}:id",
                "--rel",
                "KNOWS=/no/such/file.csv",  # missing source/target cols
            ],
        )
        assert result.exit_code != 0

    # -----------------------------------------------------------------------
    # Basic stdout output
    # -----------------------------------------------------------------------

    def test_basic_entity_query_returns_rows(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Alice" in result.output
        assert "Bob" in result.output

    def test_output_contains_column_header(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        assert result.exit_code == 0, result.output
        # Default table output should show the column name
        assert "name" in result.output

    def test_where_clause_filters_rows(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) WHERE s.name = 'Alice' RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Alice" in result.output
        assert "Bob" not in result.output

    def test_default_id_col_option(self) -> None:
        """--id-col should set the default ID column for all entity sources."""
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}",
                "--id-col",
                "id",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Alice" in result.output

    # -----------------------------------------------------------------------
    # --output: file writing
    # -----------------------------------------------------------------------

    def test_output_csv_creates_file(self, tmp_path: Path) -> None:
        out_file = tmp_path / "result.csv"
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_file.exists()
        rows = list(csv.DictReader(out_file.read_text().splitlines()))
        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert names == {"Alice", "Bob"}

    def test_output_json_creates_file(self, tmp_path: Path) -> None:
        out_file = tmp_path / "result.json"
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_file.exists()
        records = [
            json.loads(line) for line in out_file.read_text().splitlines()
        ]
        assert len(records) == 2

    def test_output_parquet_creates_file(self, tmp_path: Path) -> None:
        out_file = tmp_path / "result.parquet"
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out_file.exists()
        df = pd.read_parquet(str(out_file))
        assert len(df) == 2
        assert set(df["name"]) == {"Alice", "Bob"}

    def test_output_to_stdout_when_no_output_flag(self) -> None:
        """Without --output, results go to stdout (not to a file)."""
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        assert result.exit_code == 0, result.output
        # stdout should have content
        assert len(result.output.strip()) > 0

    # -----------------------------------------------------------------------
    # --format override
    # -----------------------------------------------------------------------

    def test_format_table_prints_ascii_table(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--format",
                "table",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Alice" in result.output
        assert "Bob" in result.output

    def test_format_csv_prints_csv_to_stdout(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--format",
                "csv",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "name" in result.output  # header
        assert "Alice" in result.output

    def test_format_json_prints_json_to_stdout(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--format",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        # Filter to only lines that look like JSON objects (logging may also appear)
        json_lines = [
            line
            for line in result.output.splitlines()
            if line.strip().startswith("{")
        ]
        records = [json.loads(line) for line in json_lines]
        assert len(records) == 2

    # -----------------------------------------------------------------------
    # --verbose
    # -----------------------------------------------------------------------

    def test_verbose_shows_row_count(self) -> None:
        result = self.runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        # Should mention the row count (2 rows)
        assert "2" in result.output
