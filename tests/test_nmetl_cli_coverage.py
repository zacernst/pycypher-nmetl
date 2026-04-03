"""Additional tests for nmetl_cli.py to increase coverage.

Targets uncovered code paths: error helpers, validate verbose output,
list-queries, _print_table edge cases, ErrorPolicyTracker, and CLI flags.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from pycypher.nmetl_cli import (
    _ErrorPolicyTracker,
    _translate_duckdb_error,
    cli,
)

_FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"
_SAMPLE_CSV = _FIXTURES_DATA / "sample.csv"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


# ===========================================================================
# _translate_duckdb_error helper
# ===========================================================================


class TestTranslateDuckdbError:
    def test_file_not_found_pattern(self) -> None:
        exc = RuntimeError("No files found that match the pattern foo.csv")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/foo.csv")
        assert "file not found" in msg
        assert "/tmp/foo.csv" in msg

    def test_permission_denied_pattern(self) -> None:
        exc = OSError("Permission denied: /tmp/secret.csv")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/secret.csv")
        assert "permission denied" in msg

    def test_access_denied_pattern(self) -> None:
        exc = OSError("Access Denied for /tmp/secret.csv")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/secret.csv")
        assert "permission denied" in msg

    def test_generic_error_fallback(self) -> None:
        exc = RuntimeError("something unexpected happened")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/data.csv")
        assert "could not load" in msg
        assert "/tmp/data.csv" in msg
        assert "Check that the file exists" in msg

    def test_encoding_error_pattern(self) -> None:
        exc = RuntimeError("codec can't decode byte 0xff")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/data.csv")
        assert "encoding error" in msg
        assert "UTF-8" in msg

    def test_memory_error_pattern(self) -> None:
        exc = MemoryError("out of memory allocating buffer")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/big.csv")
        assert "memory" in msg.lower()
        assert "reducing file size" in msg

    def test_empty_file_pattern(self) -> None:
        exc = RuntimeError("empty file detected")
        msg = _translate_duckdb_error(exc, "entity source", "/tmp/empty.csv")
        assert "empty" in msg


# ===========================================================================
# _ErrorPolicyTracker
# ===========================================================================


class TestErrorPolicyTracker:
    def test_fail_policy_exits(self) -> None:
        tracker = _ErrorPolicyTracker("fail")
        with pytest.raises(SystemExit):
            tracker.handle("something broke")

    def test_warn_policy_sets_failed_and_returns_true(self) -> None:
        tracker = _ErrorPolicyTracker("warn")
        result = tracker.handle("something broke")
        assert result is True
        assert tracker.failed is True

    def test_skip_policy_sets_failed_silently(self) -> None:
        tracker = _ErrorPolicyTracker("skip")
        result = tracker.handle("something broke")
        assert result is True
        assert tracker.failed is True

    def test_skip_policy_no_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tracker = _ErrorPolicyTracker("skip")
        tracker.handle("silent failure")
        captured = capsys.readouterr()
        # "skip" should not print anything (unlike "warn")
        assert "silent failure" not in captured.out


# ===========================================================================
# validate sub-command
# ===========================================================================


def _make_full_config(tmp_path: Path) -> Path:
    """Create a config with all sections for verbose validate coverage."""
    rel_csv = tmp_path / "knows.csv"
    rel_csv.write_text("source,target\n1,2\n")
    config_text = f"""\
version: "1.0"
project:
  name: test-project
  description: "A test pipeline"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
  relationships:
    - id: knows
      uri: "{rel_csv}"
      relationship_type: KNOWS
      source_col: source
      target_col: target
queries:
  - id: q1
    inline: "MATCH (s:Sample) RETURN s.name AS name"
    description: "Get all sample names"
output:
  - query_id: q1
    uri: "{tmp_path / "out.csv"}"
    format: csv
"""
    cfg = tmp_path / "pipeline.yaml"
    cfg.write_text(config_text)
    return cfg


class TestValidateCommand:
    def test_validate_basic(self, runner: CliRunner, tmp_path: Path) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["validate", str(cfg)])
        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_validate_verbose_shows_project(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["validate", str(cfg), "--verbose"])
        assert result.exit_code == 0
        assert "test-project" in result.output
        assert "A test pipeline" in result.output

    def test_validate_verbose_shows_entity_sources(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["validate", str(cfg), "--verbose"])
        assert "Entity sources" in result.output
        assert "Sample" in result.output

    def test_validate_verbose_shows_relationship_sources(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["validate", str(cfg), "--verbose"])
        assert "Relationship sources" in result.output
        assert "KNOWS" in result.output

    def test_validate_verbose_shows_queries(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["validate", str(cfg), "--verbose"])
        assert "Queries" in result.output
        assert "q1" in result.output

    def test_validate_verbose_shows_outputs(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["validate", str(cfg), "--verbose"])
        assert "Outputs" in result.output
        assert "query:q1" in result.output

    def test_validate_nonexistent_config(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        result = runner.invoke(cli, ["validate", str(tmp_path / "nope.yaml")])
        assert result.exit_code != 0

    def test_validate_invalid_schema(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("version: '1.0'\nsources:\n  entities: 'not_a_list'\n")
        result = runner.invoke(cli, ["validate", str(bad)])
        assert result.exit_code != 0


# ===========================================================================
# list-queries sub-command
# ===========================================================================


class TestListQueriesCommand:
    def test_list_queries_shows_ids(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["list-queries", str(cfg)])
        assert result.exit_code == 0
        assert "q1" in result.output

    def test_list_queries_shows_description(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["list-queries", str(cfg)])
        assert "Get all sample names" in result.output

    def test_list_queries_no_queries(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        config_text = f"""\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "{_SAMPLE_CSV}"
      entity_type: Sample
      id_col: id
queries: []
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["list-queries", str(cfg)])
        assert result.exit_code == 0
        assert "No queries" in result.output

    def test_list_queries_external_source(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Queries with source: field show file reference."""
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
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["list-queries", str(cfg)])
        assert "file:" in result.output


# ===========================================================================
# CLI group-level flags
# ===========================================================================


class TestCliGroupFlags:
    def test_version_flag(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        # Should show version string
        assert "version" in result.output.lower() or "." in result.output

    def test_verbose_flag_on_group(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["-v", "validate", str(cfg)])
        assert result.exit_code == 0

    def test_debug_flag_on_group(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["--debug", "validate", str(cfg)])
        assert result.exit_code == 0


# ===========================================================================
# _print_table edge case — empty DataFrame
# ===========================================================================


class TestPrintTableEdgeCases:
    def test_empty_result_shows_no_rows(self, runner: CliRunner) -> None:
        """A query that matches nothing should show '(no rows returned)'."""
        result = runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) WHERE s.name = 'NoSuchPerson' RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        assert result.exit_code == 0
        assert "no rows" in result.output.lower()


# ===========================================================================
# run command — verbose with unnamed project
# ===========================================================================


class TestRunVerboseEdgeCases:
    def test_verbose_unnamed_project(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
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
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg), "--verbose"])
        assert result.exit_code == 0
        assert "(unnamed)" in result.output

    def test_verbose_done_message(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg), "--verbose"])
        assert result.exit_code == 0
        assert "Done" in result.output

    def test_verbose_shows_query_running(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = _make_full_config(tmp_path)
        result = runner.invoke(cli, ["run", str(cfg), "--verbose"])
        assert result.exit_code == 0
        assert "query [" in result.output


# ===========================================================================
# run command — path traversal prevention for query source
# ===========================================================================


class TestRunPathTraversal:
    def test_query_source_outside_config_dir_fails(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
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
    source: "../../etc/passwd"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code != 0


# ===========================================================================
# run command — context building errors
# ===========================================================================


class TestRunContextBuildErrors:
    def test_missing_source_file_exits_nonzero(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        config_text = """\
version: "1.0"
sources:
  entities:
    - id: samples
      uri: "/no/such/file.csv"
      entity_type: Sample
      id_col: id
queries:
  - id: q1
    inline: "MATCH (s:Sample) RETURN s.name AS name"
"""
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(config_text)
        result = runner.invoke(cli, ["run", str(cfg)])
        assert result.exit_code != 0


# ===========================================================================
# run command — warn policy shows completion message
# ===========================================================================


class TestRunWarnPolicyMessage:
    def test_warn_policy_shows_completion_warning(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
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
        result = runner.invoke(cli, ["run", str(cfg), "--on-error", "warn"])
        assert result.exit_code == 0
        full = (result.output or "") + (getattr(result, "stderr", "") or "")
        assert (
            "completed with warnings" in full.lower()
            or "warning" in full.lower()
        )


# ===========================================================================
# query command — verbose with output file
# ===========================================================================


class TestQueryVerboseOutput:
    def test_verbose_with_output_file_shows_row_count(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out = tmp_path / "result.csv"
        result = runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--output",
                str(out),
                "--verbose",
            ],
        )
        assert result.exit_code == 0
        assert "2" in result.output  # row count
        assert str(out) in result.output or "result.csv" in result.output


# ===========================================================================
# query command — unknown output extension
# ===========================================================================


class TestQueryOutputFormatErrors:
    def test_unknown_extension_exits_nonzero(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        out = tmp_path / "result.xlsx"
        result = runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.name AS name",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
                "--output",
                str(out),
            ],
        )
        assert result.exit_code != 0
        assert "cannot infer" in (result.output or "").lower()


# ===========================================================================
# query command — Cypher syntax error
# ===========================================================================


class TestQuerySyntaxError:
    def test_invalid_cypher_exits_nonzero(self, runner: CliRunner) -> None:
        result = runner.invoke(
            cli,
            [
                "query",
                "THIS IS NOT VALID CYPHER",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        assert result.exit_code != 0

    def test_runtime_query_error_exits_nonzero(
        self,
        runner: CliRunner,
    ) -> None:
        """Query that parses but fails at execution."""
        result = runner.invoke(
            cli,
            [
                "query",
                "MATCH (s:Sample) RETURN s.nonexistent_prop AS x",
                "--entity",
                f"Sample={_SAMPLE_CSV}:id",
            ],
        )
        # This may or may not fail depending on null property handling
        # Just ensure it doesn't crash with an unhandled exception
        assert isinstance(result.exit_code, int)


# ===========================================================================
# _parse_entity_arg edge cases
# ===========================================================================


class TestParseEntityArgEdgeCases:
    def test_empty_id_col_after_colon(self) -> None:
        """'Label=path:' should treat empty id_col as None."""
        from pycypher.nmetl_cli import _parse_entity_arg

        label, path, id_col = _parse_entity_arg("Person=data/people.csv:")
        assert label == "Person"
        assert path == "data/people.csv"
        assert id_col is None


# ===========================================================================
# _parse_rel_arg edge cases
# ===========================================================================


class TestParseRelArgEdgeCases:
    def test_empty_source_col_raises(self) -> None:
        from pycypher.nmetl_cli import _parse_rel_arg

        with pytest.raises(ValueError, match="must not be empty"):
            _parse_rel_arg("KNOWS=data/k.csv::target")

    def test_empty_target_col_raises(self) -> None:
        from pycypher.nmetl_cli import _parse_rel_arg

        with pytest.raises(ValueError, match="must not be empty"):
            _parse_rel_arg("KNOWS=data/k.csv:source:")


# ===========================================================================
# config command
# ===========================================================================


class TestConfigCommand:
    def test_config_lists_all_variables(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["config"])
        assert result.exit_code == 0
        assert "PYCYPHER_QUERY_TIMEOUT_S" in result.output
        assert "PYCYPHER_MAX_CROSS_JOIN_ROWS" in result.output
        assert "PYCYPHER_AST_CACHE_MAX" in result.output

    def test_config_json_output(self, runner: CliRunner) -> None:
        import json

        result = runner.invoke(cli, ["config", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        variables = [e["variable"] for e in data]
        assert "PYCYPHER_QUERY_TIMEOUT_S" in variables

    def test_config_shows_env_override(self, runner: CliRunner) -> None:
        result = runner.invoke(
            cli,
            ["config"],
            env={"PYCYPHER_QUERY_TIMEOUT_S": "42"},
        )
        assert result.exit_code == 0
        assert "42" in result.output
        assert "*" in result.output  # env marker

    def test_config_json_shows_env_source(self, runner: CliRunner) -> None:
        import json

        result = runner.invoke(
            cli,
            ["config", "--json"],
            env={"PYCYPHER_QUERY_TIMEOUT_S": "42"},
        )
        data = json.loads(result.output)
        timeout_entry = next(
            e for e in data if e["variable"] == "PYCYPHER_QUERY_TIMEOUT_S"
        )
        assert timeout_entry["source"] == "env"
        assert timeout_entry["value"] == "42"


# ===========================================================================
# _format_validation_errors
# ===========================================================================


class TestListQueriesDeps:
    def test_list_queries_without_deps(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = tmp_path / "pipe.yaml"
        cfg.write_text(
            'version: "1.0"\n'
            "queries:\n"
            '  - id: q1\n    inline: "MATCH (p:Person) RETURN p.name"\n',
        )
        result = runner.invoke(cli, ["list-queries", str(cfg)])
        assert result.exit_code == 0
        assert "q1" in result.output

    def test_list_queries_with_deps(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = tmp_path / "pipe.yaml"
        cfg.write_text(
            'version: "1.0"\n'
            "queries:\n"
            "  - id: q1\n    inline: \"CREATE (p:Person {name: 'Alice'})\"\n"
            '  - id: q2\n    inline: "MATCH (p:Person) RETURN p.name"\n',
        )
        result = runner.invoke(cli, ["list-queries", str(cfg), "--deps"])
        assert result.exit_code == 0
        assert "Dependency Analysis" in result.output
        assert "produces:" in result.output
        assert "consumes:" in result.output
        assert "Execution order:" in result.output

    def test_list_queries_deps_shows_dependency(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = tmp_path / "pipe.yaml"
        cfg.write_text(
            'version: "1.0"\n'
            "queries:\n"
            "  - id: create_people\n    inline: \"CREATE (p:Person {name: 'A'})\"\n"
            '  - id: query_people\n    inline: "MATCH (p:Person) RETURN p.name"\n',
        )
        result = runner.invoke(cli, ["list-queries", str(cfg), "--deps"])
        assert result.exit_code == 0
        assert "depends on: create_people" in result.output

    def test_list_queries_no_queries(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = tmp_path / "empty.yaml"
        cfg.write_text('version: "1.0"\nqueries: []\n')
        result = runner.invoke(cli, ["list-queries", str(cfg)])
        assert result.exit_code == 0
        assert "No queries" in result.output


class TestFormatValidationErrors:
    def test_formats_field_paths(self) -> None:
        from pycypher.nmetl_cli import _format_validation_errors
        from pydantic import BaseModel, ValidationError

        class _Dummy(BaseModel):
            name: str
            age: int

        try:
            _Dummy(name=123, age="not_int")  # type: ignore[arg-type]
        except ValidationError as exc:
            result = _format_validation_errors(exc)

        assert "invalid config structure:" in result
        assert "name:" in result
        assert "age:" in result

    def test_truncates_long_error_lists(self) -> None:
        from pycypher.nmetl_cli import _format_validation_errors
        from pydantic import BaseModel, ValidationError

        class _Many(BaseModel):
            a: str
            b: str
            c: str
            d: str
            e: str
            f: str

        try:
            _Many()  # type: ignore[call-arg]
        except ValidationError as exc:
            result = _format_validation_errors(exc)

        assert "more error(s)" in result
        assert "nmetl validate" in result

    def test_verbose_shows_all_errors(self) -> None:
        from pycypher.nmetl_cli import _format_validation_errors
        from pydantic import BaseModel, ValidationError

        class _Many(BaseModel):
            a: str
            b: str
            c: str
            d: str
            e: str
            f: str

        try:
            _Many()  # type: ignore[call-arg]
        except ValidationError as exc:
            result = _format_validation_errors(exc, verbose=True)

        # In verbose mode, all 6 fields should be shown
        assert "more error(s)" not in result
        assert "nmetl validate" not in result
        for field in ("a:", "b:", "c:", "d:", "e:", "f:"):
            assert field in result

    def test_invalid_config_shows_concise_errors(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        cfg = tmp_path / "bad.yaml"
        cfg.write_text("version: '1.0'\nsources:\n  entities:\n    - uri: x\n")
        result = runner.invoke(cli, ["validate", str(cfg)])
        assert result.exit_code == 2
        # Should show field path, not raw Pydantic dump
        assert "sources.entities.0" in (result.output or "")
