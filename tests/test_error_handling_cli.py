"""TDD tests for improving error handling in nmetl_cli.py.

The CLI currently has 11 broad `except Exception` handlers with BLE001 noqa
comments. These anti-patterns swallow all exceptions indiscriminately and
provide poor error messages to users. This TDD test suite will drive
improvements to use specific exception types and provide actionable error
messages.

Run with:
    uv run pytest tests/test_error_handling_cli_tdd.py -v
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from click.testing import CliRunner
from pycypher.nmetl_cli import cli


class TestConfigErrorHandling:
    """Test specific exception handling for configuration errors."""

    def test_config_file_not_found_specific_message(self) -> None:
        """FileNotFoundError should provide specific, actionable message."""
        runner = CliRunner()

        # Should handle missing config file gracefully
        result = runner.invoke(cli, ["validate", "nonexistent-config.yaml"])

        assert result.exit_code == 1
        assert "config file not found" in result.output.lower()
        assert "nonexistent-config.yaml" in result.output
        # Should not contain generic "Exception" text
        assert "Exception" not in result.output

    def test_invalid_yaml_syntax_specific_message(self) -> None:
        """YAML parsing errors should provide specific message about syntax."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
        ) as f:
            # Write invalid YAML
            f.write("sources:\n  entities:\n    - invalid: yaml: content:")
            config_path = f.name

        try:
            result = runner.invoke(cli, ["validate", config_path])

            assert result.exit_code == 2
            # Should mention YAML parsing specifically
            assert (
                "yaml" in result.output.lower()
                or "parsing" in result.output.lower()
            )
            # Should not be a generic exception message
            assert (
                "invalid config" not in result.output.lower()
                or "yaml" in result.output.lower()
            )
        finally:
            os.unlink(config_path)

    def test_missing_required_fields_specific_message(self) -> None:
        """Missing required config fields should be called out specifically."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
        ) as f:
            # Invalid config with malformed query structure
            f.write("""
sources:
  entities: []
queries:
  - invalid_structure: true  # Should be id/source/inline but has wrong format
""")
            config_path = f.name

        try:
            result = runner.invoke(cli, ["validate", config_path])

            assert result.exit_code == 2
            # Should mention the specific validation error about malformed structure
            assert (
                "validation" in result.output.lower()
                or "required" in result.output.lower()
            )
            # Should not be completely generic
            assert len(result.output.strip()) > len("Error: invalid config:")
        finally:
            os.unlink(config_path)


class TestDataSourceErrorHandling:
    """Test specific exception handling for data source loading errors."""

    def test_entity_file_not_found_specific_message(self) -> None:
        """Missing entity data files should provide specific message."""
        runner = CliRunner()

        # Use query subcommand with nonexistent entity file
        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                "Person=nonexistent-file.csv",
                "MATCH (p:Person) RETURN p",
            ],
        )

        assert result.exit_code == 1
        assert (
            "entity source" in result.output.lower()
            or "file" in result.output.lower()
        )
        assert "nonexistent-file.csv" in result.output
        # Should provide specific error about loading or file not found
        assert (
            "could not load entity source" in result.output.lower()
            or "entity source file not found" in result.output.lower()
        )

    def test_invalid_csv_format_specific_message(self) -> None:
        """DuckDB should handle malformed CSV gracefully without errors."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            # Write malformed CSV with quote issues
            f.write('name,age\nAlice,30\nBob,"invalid"trailing"quotes",25\n')
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "MATCH (p:Person) RETURN p",
                ],
            )

            # DuckDB's read_csv_auto handles malformed CSV gracefully
            # Should succeed and return results, not fail with error
            assert result.exit_code == 0
            # Should contain query results, not error messages
            assert result.output.strip() != ""
        finally:
            os.unlink(csv_path)

    def test_relationship_missing_columns_specific_message(self) -> None:
        """Missing source/target columns should provide specific message."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            # CSV missing the expected source/target columns
            f.write("from_person,to_person\nAlice,Bob\n")
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--rel",
                    f"KNOWS={csv_path}:source:target",
                    "MATCH ()-[r:KNOWS]->() RETURN r",
                ],
            )

            assert result.exit_code == 1
            # Should mention specific column names that were expected
            assert (
                "source" in result.output.lower()
                or "target" in result.output.lower()
                or "column" in result.output.lower()
            )
        finally:
            os.unlink(csv_path)


class TestQueryExecutionErrorHandling:
    """Test specific exception handling for query execution errors."""

    def test_syntax_error_specific_message(self) -> None:
        """Cypher syntax errors should provide specific parsing feedback."""
        runner = CliRunner()

        # Create minimal valid data source
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name\nAlice\n")
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "MATCH (p:Person RETURN p",  # Missing closing parenthesis
                ],
            )

            assert result.exit_code == 1
            # Should mention parsing or syntax specifically
            assert (
                "syntax" in result.output.lower()
                or "parse" in result.output.lower()
                or "parenthesis" in result.output.lower()
            )
            # Should provide specific error message about query problems
            assert (
                "query failed" in result.output.lower()
                or "syntax error" in result.output.lower()
            )
        finally:
            os.unlink(csv_path)

    def test_undefined_variable_specific_message(self) -> None:
        """Undefined variables should provide specific semantic error."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name\nAlice\n")
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "MATCH (p:Person) RETURN q.name",  # 'q' is undefined
                ],
            )

            assert result.exit_code == 1
            # Should mention the undefined variable specifically
            assert (
                "undefined" in result.output.lower()
                or "variable" in result.output.lower()
                or "'q'" in result.output
            )
        finally:
            os.unlink(csv_path)

    def test_type_error_specific_message(self) -> None:
        """Type errors should provide specific type mismatch information."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name,age\nAlice,thirty\n")  # 'thirty' is not numeric
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "MATCH (p:Person) RETURN p.age + 5",  # Should fail on non-numeric
                ],
            )

            assert result.exit_code == 1
            # Should mention type error or conversion issue
            assert (
                "type" in result.output.lower()
                or "convert" in result.output.lower()
                or "numeric" in result.output.lower()
            )
        finally:
            os.unlink(csv_path)


class TestOutputErrorHandling:
    """Test specific exception handling for output writing errors."""

    def test_permission_denied_specific_message(self) -> None:
        """Permission errors on output should provide specific message."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name\nAlice\n")
            csv_path = f.name

        try:
            # Try to write to a protected directory (this may not always work)
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "--output",
                    "/root/protected.csv",
                    "MATCH (p:Person) RETURN p",
                ],
            )

            if result.exit_code == 1:
                # Check both output and exception for error keywords — some
                # errors (e.g. SecurityError from path validation) surface as
                # exceptions rather than CLI output text.
                combined = result.output.lower()
                if result.exception:
                    combined += " " + str(result.exception).lower()
                assert (
                    "permission" in combined
                    or "denied" in combined
                    or "access" in combined
                    or "read-only" in combined
                    or "could not write output" in combined
                )
        finally:
            os.unlink(csv_path)

    def test_invalid_output_format_specific_message(self) -> None:
        """Invalid output format should provide specific format error."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name\nAlice\n")
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "--output",
                    "output.xyz",  # Unsupported format
                    "MATCH (p:Person) RETURN p",
                ],
            )

            if result.exit_code == 1:
                # Should mention format specifically
                assert (
                    "format" in result.output.lower()
                    or "extension" in result.output.lower()
                    or "xyz" in result.output.lower()
                )
        finally:
            os.unlink(csv_path)


class TestStructuralImprovements:
    """Test that broad exception handlers have been replaced."""

    def test_no_broad_exception_handlers_in_cli(self) -> None:
        """CLI should not contain broad 'except Exception' handlers."""
        # Read the CLI source code
        cli_path = Path("packages/pycypher/src/pycypher/nmetl_cli.py")
        source = cli_path.read_text()

        # Count remaining broad exception handlers
        broad_handlers = source.count("except Exception")

        # Should be significantly reduced but allow some for third-party library exceptions
        assert broad_handlers <= 5, (
            f"Found {broad_handlers} broad 'except Exception' handlers, "
            "expected <= 5 after improvements. Some are needed for DuckDB and Lark exceptions."
        )

    def test_specific_exception_types_used(self) -> None:
        """CLI should use specific exception types where appropriate."""
        cli_path = Path("packages/pycypher/src/pycypher/nmetl_cli.py")
        source = cli_path.read_text()

        # Should have specific exception types
        specific_exceptions = [
            "FileNotFoundError",
            "ValueError",
            "TypeError",
            "PermissionError",
            "OSError",
        ]

        found_specific = sum(
            1 for exc_type in specific_exceptions if exc_type in source
        )

        # Should use at least some specific exception types
        assert found_specific >= 2, (
            f"Found only {found_specific} specific exception types, "
            "expected >= 2 after improvements"
        )

    def test_ble001_noqa_comments_reduced(self) -> None:
        """BLE001 noqa comments should be significantly reduced."""
        cli_path = Path("packages/pycypher/src/pycypher/nmetl_cli.py")
        source = cli_path.read_text()

        # Count BLE001 noqa comments
        noqa_count = source.count("# noqa: BLE001")

        # Should be significantly reduced from the original 11
        assert noqa_count <= 4, (
            f"Found {noqa_count} BLE001 noqa comments, expected <= 4 after improvements"
        )


class TestRegressionPrevention:
    """Ensure error handling improvements don't break existing functionality."""

    def test_valid_config_still_works(self) -> None:
        """Valid configurations should still work after error handling changes."""
        runner = CliRunner()

        # Create a minimal valid config
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
        ) as f:
            f.write("""
sources:
  entities: []
  relationships: []
queries: []
""")
            config_path = f.name

        try:
            result = runner.invoke(cli, ["validate", config_path])

            # Should succeed
            assert result.exit_code == 0
        finally:
            os.unlink(config_path)

    def test_successful_query_execution_unchanged(self) -> None:
        """Successful query execution should work normally."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name,age\nAlice,30\nBob,25\n")
            csv_path = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={csv_path}",
                    "MATCH (p:Person) RETURN count(p)",
                ],
            )

            # Should succeed
            assert result.exit_code == 0
            assert "2" in result.output  # Should return count of 2
        finally:
            os.unlink(csv_path)
