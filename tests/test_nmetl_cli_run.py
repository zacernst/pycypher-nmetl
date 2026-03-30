"""Integration tests for the `nmetl run` command.

Tests the full pipeline: config load → context build → query execution →
output writing.  Uses Click's CliRunner and real fixture data files.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner
from pycypher.nmetl_cli import cli

# Path to shared fixture data
_FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"
_SAMPLE_CSV = _FIXTURES_DATA / "sample.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    tmp_path: Path,
    *,
    source_uri: str = "",
    query_text: str = "MATCH (s:Sample) RETURN s.name AS name",
    extra_queries: str = "",
    output_section: str = "",
    project_name: str | None = None,
    source_id_col: str = "id",
) -> Path:
    """Write a minimal pipeline YAML to ``tmp_path/pipeline.yaml``."""
    if not source_uri:
        source_uri = str(_SAMPLE_CSV)
    project_block = f"project:\n  name: {project_name}\n" if project_name else ""
    text = f"""\
version: "1.0"
{project_block}
sources:
  entities:
    - id: samples
      uri: "{source_uri}"
      entity_type: Sample
      id_col: {source_id_col}
queries:
  - id: q1
    inline: "{query_text}"
{extra_queries}
{output_section}
"""
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(text)
    return config_file


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


# ===========================================================================
# Dry-run behaviour
# ===========================================================================


class TestDryRun:
    def test_dry_run_exits_zero(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
        assert result.exit_code == 0, result.output

    def test_dry_run_shows_query_id(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
        assert "q1" in result.output

    def test_dry_run_no_data_loaded(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
        assert "No data loaded" in result.output

    def test_dry_run_does_not_create_output_file(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out_path = tmp_path / "out.csv"
        output_section = f'output:\n  - query_id: q1\n    uri: "{out_path}"\n'
        cfg = _make_config(tmp_path, output_section=output_section)
        runner.invoke(cli, ["run", str(cfg), "--dry-run"])
        assert not out_path.exists()


# ===========================================================================
# Error handling for missing / invalid config
# ===========================================================================


class TestConfigErrors:
    def test_missing_config_exits_nonzero(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        result = runner.invoke(
            cli,
            ["run", str(tmp_path / "nonexistent.yaml")],
        )
        assert result.exit_code != 0

    def test_invalid_yaml_exits_2(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(": invalid: [yaml")
        result = runner.invoke(cli, ["run", str(bad)])
        assert result.exit_code == 2


# ===========================================================================
# Successful query execution and output writing
# ===========================================================================


class TestRunExecutesAndWrites:
    def test_run_exits_zero_with_no_output(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output

    def test_run_writes_csv_output(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out_path = tmp_path / "output.csv"
        output_section = f'output:\n  - query_id: q1\n    uri: "{out_path}"\n'
        cfg = _make_config(tmp_path, output_section=output_section)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert out_path.exists()
        df = pd.read_csv(out_path)
        assert "name" in df.columns
        assert set(df["name"]) == {"Alice", "Bob"}

    def test_run_writes_parquet_output(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out_path = tmp_path / "output.parquet"
        output_section = f'output:\n  - query_id: q1\n    uri: "{out_path}"\n'
        cfg = _make_config(tmp_path, output_section=output_section)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert out_path.exists()
        df = pd.read_parquet(out_path)
        assert len(df) == 2

    def test_run_writes_json_output(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out_path = tmp_path / "output.json"
        output_section = f'output:\n  - query_id: q1\n    uri: "{out_path}"\n'
        cfg = _make_config(tmp_path, output_section=output_section)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert out_path.exists()
        df = pd.read_json(out_path, lines=True)
        assert len(df) == 2

    def test_run_multiple_outputs_for_same_query(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out_csv = tmp_path / "output.csv"
        out_parquet = tmp_path / "output.parquet"
        output_section = (
            f"output:\n"
            f'  - query_id: q1\n    uri: "{out_csv}"\n'
            f'  - query_id: q1\n    uri: "{out_parquet}"\n'
        )
        cfg = _make_config(tmp_path, output_section=output_section)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert out_csv.exists()
        assert out_parquet.exists()

    def test_run_external_query_file(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Query text loaded from a .cypher file referenced by source:."""
        cypher_dir = tmp_path / "queries"
        cypher_dir.mkdir()
        cypher_file = cypher_dir / "q1.cypher"
        cypher_file.write_text("MATCH (s:Sample) RETURN s.name AS name\n")
        out_path = tmp_path / "output.csv"
        config_text = f"""\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
queries:
  - id: q1
    source: "queries/q1.cypher"
output:
  - query_id: q1
    uri: "{out_path}"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert out_path.exists()
        df = pd.read_csv(out_path)
        assert set(df["name"]) == {"Alice", "Bob"}


# ===========================================================================
# --query-id filtering
# ===========================================================================


class TestQueryIdFilter:
    def test_query_id_runs_only_selected(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out1 = tmp_path / "out1.csv"
        out2 = tmp_path / "out2.csv"
        config_text = f"""\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
queries:
  - id: q1
    inline: "MATCH (s:Sample) RETURN s.name AS name"
  - id: q2
    inline: "MATCH (s:Sample) RETURN s.value AS value"
output:
  - query_id: q1
    uri: "{out1}"
  - query_id: q2
    uri: "{out2}"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg), "--query-id", "q1"])
        assert result.exit_code == 0, result.output
        assert out1.exists()
        assert not out2.exists()

    def test_unknown_query_id_warns(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path)
        result = runner.invoke(
            cli,
            ["run", str(cfg), "--query-id", "nonexistent"],
        )
        # Should not hard-fail; just warn
        full_output = (result.output or "") + (result.stderr or "")
        assert "nonexistent" in full_output or "unknown" in full_output.lower()


# ===========================================================================
# --on-error policy
# ===========================================================================


class TestOnErrorPolicy:
    def test_on_error_warn_continues_after_failure(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """--on-error=warn: a failing query logs a warning but exits 0."""
        out_good = tmp_path / "out_good.csv"
        config_text = f"""\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
queries:
  - id: q_bad
    inline: "MATCH (x:NoSuchType) RETURN x.name AS name"
  - id: q_good
    inline: "MATCH (s:Sample) RETURN s.name AS name"
output:
  - query_id: q_good
    uri: "{out_good}"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg), "--on-error", "warn"])
        assert result.exit_code == 0, result.output
        assert out_good.exists()

    def test_on_error_skip_continues_silently(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out_good = tmp_path / "out_good.csv"
        config_text = f"""\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
queries:
  - id: q_bad
    inline: "MATCH (x:NoSuchType) RETURN x.name AS name"
  - id: q_good
    inline: "MATCH (s:Sample) RETURN s.name AS name"
output:
  - query_id: q_good
    uri: "{out_good}"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg), "--on-error", "skip"])
        assert result.exit_code == 0, result.output
        assert out_good.exists()

    def test_on_error_fail_aborts_on_failure(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Default (fail) policy: exits non-zero if any query fails."""
        config_text = f"""\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
queries:
  - id: q_bad
    inline: "MATCH (x:NoSuchType) RETURN x.name AS name"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg), "--on-error", "fail"])
        assert result.exit_code != 0


# ===========================================================================
# --verbose flag
# ===========================================================================


class TestVerboseFlag:
    def test_verbose_shows_project_name(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path, project_name="my-pipeline")
        result = runner.invoke(cli, ["run", str(cfg), "--verbose"])
        assert result.exit_code == 0, result.output
        assert "my-pipeline" in result.output

    def test_verbose_shows_source_count(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg), "--verbose"])
        assert result.exit_code == 0, result.output
        # At least one entity source
        assert "1" in result.output
