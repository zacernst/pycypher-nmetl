"""Tests for the enhanced --dry-run pipeline validation."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def cli():
    from pycypher.nmetl_cli import cli as _cli

    return _cli


def _write_config(tmp_path: Path, yaml_text: str) -> Path:
    """Write a YAML config to a temp directory and return its path."""
    cfg = tmp_path / "pipeline.yaml"
    cfg.write_text(textwrap.dedent(yaml_text), encoding="utf-8")
    return cfg


# ---------------------------------------------------------------------------
# Happy path: all checks pass
# ---------------------------------------------------------------------------


def test_dry_run_all_ok(tmp_path, runner, cli):
    """Dry run passes when sources exist and queries parse."""
    # Create a source file.
    src = tmp_path / "people.csv"
    src.write_text("__ID__,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    cfg = _write_config(
        tmp_path,
        f"""\
        version: "1.0"
        sources:
          entities:
            - id: people
              uri: "{src}"
              entity_type: Person
        queries:
          - id: q1
            description: "Get people"
            inline: "MATCH (n:Person) RETURN n.name"
        output:
          - query_id: q1
            uri: "{tmp_path / "out.csv"}"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "PASSED" in result.output
    assert "OK" in result.output
    assert "No data loaded" in result.output


# ---------------------------------------------------------------------------
# Source file missing
# ---------------------------------------------------------------------------


def test_dry_run_missing_source(tmp_path, runner, cli):
    """Dry run fails when a source file doesn't exist."""
    cfg = _write_config(
        tmp_path,
        """\
        version: "1.0"
        sources:
          entities:
            - id: ghosts
              uri: "nonexistent.csv"
              entity_type: Ghost
        queries:
          - id: q1
            inline: "MATCH (n:Ghost) RETURN n"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 1, result.output
    assert "MISSING" in result.output
    assert "FAILED" in result.output


# ---------------------------------------------------------------------------
# Query parse error
# ---------------------------------------------------------------------------


def test_dry_run_query_parse_error(tmp_path, runner, cli):
    """Dry run catches Cypher syntax errors."""
    src = tmp_path / "data.csv"
    src.write_text("__ID__\n1\n", encoding="utf-8")

    cfg = _write_config(
        tmp_path,
        f"""\
        version: "1.0"
        sources:
          entities:
            - id: data
              uri: "{src}"
              entity_type: Thing
        queries:
          - id: bad_q
            inline: "MATCH (n:Thing) RETRUN n"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 1, result.output
    assert "PARSE ERROR" in result.output
    assert "FAILED" in result.output


# ---------------------------------------------------------------------------
# Missing query source file
# ---------------------------------------------------------------------------


def test_dry_run_missing_query_file(tmp_path, runner, cli):
    """Dry run catches missing .cypher source files."""
    src = tmp_path / "data.csv"
    src.write_text("__ID__\n1\n", encoding="utf-8")

    cfg = _write_config(
        tmp_path,
        f"""\
        version: "1.0"
        sources:
          entities:
            - id: data
              uri: "{src}"
              entity_type: Thing
        queries:
          - id: q1
            source: "missing_query.cypher"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 1, result.output
    assert "FILE MISSING" in result.output


# ---------------------------------------------------------------------------
# Output references unknown query
# ---------------------------------------------------------------------------


def test_dry_run_invalid_output_ref(tmp_path, runner, cli):
    """Dry run catches output referencing non-existent query ID."""
    src = tmp_path / "data.csv"
    src.write_text("__ID__\n1\n", encoding="utf-8")

    cfg = _write_config(
        tmp_path,
        f"""\
        version: "1.0"
        sources:
          entities:
            - id: data
              uri: "{src}"
              entity_type: Thing
        queries:
          - id: q1
            inline: "MATCH (n:Thing) RETURN n"
        output:
          - query_id: nonexistent_query
            uri: "{tmp_path / "out.csv"}"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 1, result.output
    assert "INVALID QUERY REF" in result.output


# ---------------------------------------------------------------------------
# Remote sources are skipped gracefully
# ---------------------------------------------------------------------------


def test_dry_run_remote_sources_skipped(tmp_path, runner, cli):
    """Remote sources (S3, HTTP) are reported but not validated."""
    cfg = _write_config(
        tmp_path,
        """\
        version: "1.0"
        sources:
          entities:
            - id: remote_data
              uri: "s3://bucket/data.parquet"
              entity_type: RemoteThing
        queries:
          - id: q1
            inline: "MATCH (n:RemoteThing) RETURN n"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "remote, skipped" in result.output
    assert "PASSED" in result.output


# ---------------------------------------------------------------------------
# Query from .cypher file parses OK
# ---------------------------------------------------------------------------


def test_dry_run_query_from_file(tmp_path, runner, cli):
    """Dry run parses queries from external .cypher files."""
    src = tmp_path / "data.csv"
    src.write_text("__ID__,name\n1,Alice\n", encoding="utf-8")

    cypher_file = tmp_path / "q1.cypher"
    cypher_file.write_text("MATCH (n:Thing) RETURN n.name", encoding="utf-8")

    cfg = _write_config(
        tmp_path,
        f"""\
        version: "1.0"
        sources:
          entities:
            - id: data
              uri: "{src}"
              entity_type: Thing
        queries:
          - id: q1
            source: "q1.cypher"
        """,
    )

    result = runner.invoke(cli, ["run", str(cfg), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "OK" in result.output
    assert "PASSED" in result.output
