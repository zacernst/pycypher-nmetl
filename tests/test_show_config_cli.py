"""Tests for the ``nmetl show-config`` CLI command."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner
from pycypher.cli.main import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestShowConfigTable:
    """Table output format."""

    def test_table_output_shows_all_settings(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["show-config"])
        assert result.exit_code == 0
        assert "Effective PyCypher Configuration" in result.output
        assert "Query timeout" in result.output
        assert "Max cross-join rows" in result.output
        assert "Max unbounded path hops" in result.output
        assert "AST cache max entries" in result.output

    def test_env_override_shown(self, runner: CliRunner) -> None:
        result = runner.invoke(
            cli,
            ["show-config"],
            env={"PYCYPHER_QUERY_TIMEOUT_S": "42"},
        )
        assert result.exit_code == 0
        # The (env) marker should appear for overridden values
        assert "(env)" in result.output


class TestShowConfigJson:
    """JSON output format."""

    def test_json_output_is_valid(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["show-config", "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "PYCYPHER_QUERY_TIMEOUT_S" in data
        assert "PYCYPHER_MAX_CROSS_JOIN_ROWS" in data

    def test_json_source_field(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["show-config", "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        # All should be "default" when no env vars set
        for _key, entry in data.items():
            assert "source" in entry
            assert "value" in entry
            assert "default" in entry
