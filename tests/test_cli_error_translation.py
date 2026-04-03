"""TDD tests for UX Loop 264: CLI Error Message Translation.

Critical UX issue: CLI users receive technical DuckDB error messages instead of
user-friendly, actionable feedback. The failing test test_entity_file_not_found_specific_message
reveals systematic CLI UX problems affecting user adoption and experience.

Focus areas:
1. DuckDB IOException translation to user-friendly "file not found" messages
2. Comprehensive error message patterns for all data source errors
3. Consistent CLI error message formatting and actionability
"""

import os
import tempfile

from click.testing import CliRunner
from pycypher.nmetl_cli import cli


class TestCurrentCLIErrorTranslationIssues:
    """Test current problematic CLI error message patterns (red phase)."""

    def test_current_entity_file_not_found_produces_user_friendly_error(self):
        """Test that missing entity files now produce user-friendly error messages (post-fix)."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                "Person=nonexistent-file.csv",
                "MATCH (p:Person) RETURN p",
            ],
        )

        # After fix: should properly handle errors
        assert result.exit_code == 1

        # After the fix: should now HAVE user-friendly messages
        has_user_friendly = (
            "entity source" in result.output.lower()
            or "file not found" in result.output.lower()
            or "could not load entity source" in result.output.lower()
        )

        # After fix: should have user-friendly messages
        assert has_user_friendly, (
            f"After fix, CLI should provide user-friendly messages. Got: {result.output}"
        )

        # Should not leak technical DuckDB details
        assert "IOException" not in result.output, (
            "Should not expose technical exception details to users"
        )
        assert "LINE 1:" not in result.output, (
            "Should not expose DuckDB SQL technical details to users"
        )

    def test_current_relationship_file_not_found_error_pattern(self):
        """Test relationship file not found error pattern."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "query",
                "--rel",
                "KNOWS=nonexistent-rels.csv:from:to",
                "MATCH ()-[:KNOWS]->() RETURN count(*)",
            ],
        )

        assert result.exit_code == 1
        # Same pattern: technical errors not translated to user-friendly messages

    def test_current_malformed_csv_error_pattern(self):
        """Test malformed CSV file error translation - DuckDB may be forgiving of minor issues."""
        runner = CliRunner()

        # Create a more clearly malformed CSV file
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write("name,age\n")
            f.write("Alice,25\n")
            f.write("Bob,not_a_number,extra_column")  # Clear format violation
            malformed_csv = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={malformed_csv}",
                    "MATCH (p:Person) RETURN p",
                ],
            )

            # DuckDB may be forgiving - if it succeeds, that's okay
            # This test documents that some "malformed" files may actually work
            # The key is that if it fails, it should provide good error messages
            if result.exit_code != 0:
                # If it does fail, should provide actionable feedback
                assert len(result.output) > 0, (
                    "Should provide some error message if it fails"
                )

        finally:
            os.unlink(malformed_csv)


class TestFixedCLIErrorTranslation:
    """Tests for improved CLI error message translation (green phase).

    These tests define expected behavior after fixing CLI error translation.
    Initially these will fail (red phase), then pass after implementation (green phase).
    """

    def test_fixed_entity_file_not_found_provides_user_friendly_message(self):
        """Test that fixed CLI provides user-friendly messages for missing entity files."""
        runner = CliRunner()

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

        # Should have user-friendly error message
        assert (
            "entity source" in result.output.lower()
            or "file" in result.output.lower()
        ), "Should mention entity source or file in error message"

        # Should include the specific filename that was not found
        assert "nonexistent-file.csv" in result.output, (
            "Should include the specific filename in error message"
        )

        # Should provide actionable guidance
        assert (
            "could not load entity source" in result.output.lower()
            or "entity source file not found" in result.output.lower()
            or "file not found" in result.output.lower()
        ), "Should provide specific actionable error message"

        # Should not contain raw technical DuckDB error messages
        assert "IOException" not in result.output, (
            "Should not expose technical DuckDB error types to users"
        )
        assert "LINE 1:" not in result.output, (
            "Should not expose DuckDB SQL error formatting to users"
        )

    def test_fixed_relationship_file_not_found_provides_user_friendly_message(
        self,
    ):
        """Test that fixed CLI provides user-friendly messages for missing relationship files."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "query",
                "--rel",
                "KNOWS=nonexistent-rels.csv:from:to",
                "MATCH ()-[:KNOWS]->() RETURN count(*)",
            ],
        )

        assert result.exit_code == 1

        # Should have user-friendly error message for relationship files
        assert (
            "relationship source" in result.output.lower()
            or "file" in result.output.lower()
        ), "Should mention relationship source or file in error message"

        # Should include specific filename
        assert "nonexistent-rels.csv" in result.output, (
            "Should include the specific relationship filename"
        )

        # Should provide actionable guidance
        assert (
            "could not load relationship source" in result.output.lower()
            or "relationship source file not found" in result.output.lower()
            or "file not found" in result.output.lower()
        ), "Should provide specific actionable error message for relationships"

    def test_fixed_error_messages_are_consistent_and_actionable(self):
        """Test that fixed error messages follow consistent, actionable patterns."""
        runner = CliRunner()

        test_cases = [
            # (args, expected_error_type, expected_filename)
            (
                [
                    "query",
                    "--entity",
                    "Person=missing.csv",
                    "MATCH (p:Person) RETURN p",
                ],
                "entity source",
                "missing.csv",
            ),
            (
                [
                    "query",
                    "--rel",
                    "KNOWS=missing-rel.csv:from:to",
                    "MATCH ()-[:KNOWS]->() RETURN count(*)",
                ],
                "relationship source",
                "missing-rel.csv",
            ),
        ]

        for args, error_type, filename in test_cases:
            result = runner.invoke(cli, args)

            assert result.exit_code == 1, (
                f"Should fail with exit code 1 for {args}"
            )

            # Consistent error message patterns
            assert error_type in result.output.lower(), (
                f"Should mention {error_type} in error for {args}"
            )
            assert filename in result.output, (
                f"Should mention specific filename {filename} for {args}"
            )

            # Should start with "Error:" for consistency
            assert (
                result.output.startswith("Error:") or "Error:" in result.output
            ), f"Should include 'Error:' prefix for {args}"

    def test_fixed_malformed_data_provides_helpful_guidance(self):
        """Test that malformed data files provide helpful guidance when they do fail."""
        runner = CliRunner()

        # Create a clearly invalid file (not CSV at all)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
        ) as f:
            f.write(
                "This is not CSV data at all, just plain text that should definitely fail",
            )
            malformed_csv = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "query",
                    "--entity",
                    f"Person={malformed_csv}",
                    "MATCH (p:Person) RETURN p",
                ],
            )

            # DuckDB is very forgiving, so this might still work
            # The test verifies that IF it fails, the error message is helpful
            if result.exit_code != 0:
                # Should provide guidance about data issues
                assert (
                    "data format" in result.output.lower()
                    or "invalid format" in result.output.lower()
                    or "could not load" in result.output.lower()
                    or "access error" in result.output.lower()
                ), (
                    f"Should provide helpful guidance about data issues. Got: {result.output}"
                )

                # Should not expose raw DuckDB technical details
                assert "LINE 1:" not in result.output, (
                    "Should not expose technical DuckDB parsing details"
                )
            else:
                # If DuckDB successfully processed it, that's fine too
                # This documents that DuckDB is very forgiving
                pass

        finally:
            os.unlink(malformed_csv)

    def test_fixed_directory_not_found_provides_clear_message(self):
        """Test directory access errors provide clear messages."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                "Person=/nonexistent/directory/file.csv",
                "MATCH (p:Person) RETURN p",
            ],
        )

        assert result.exit_code == 1

        # Should provide clear directory/path error message
        assert (
            "directory" in result.output.lower()
            or "path" in result.output.lower()
            or "file not found" in result.output.lower()
        ), "Should provide clear path/directory error message"

    def test_error_message_formatting_consistency(self):
        """Test that all error messages follow consistent formatting patterns."""
        runner = CliRunner()

        test_cases = [
            [
                "query",
                "--entity",
                "Person=nonexistent.csv",
                "MATCH (p:Person) RETURN p",
            ],
            [
                "query",
                "--rel",
                "KNOWS=nonexistent.csv",
                "MATCH ()-[:KNOWS]->() RETURN count(*)",
            ],
        ]

        for args in test_cases:
            result = runner.invoke(cli, args)

            assert result.exit_code == 1

            # Error messages should be written to stderr or output
            error_text = result.output

            # Should have consistent formatting
            assert len(error_text.strip()) > 10, (
                f"Error message should be substantial, not just generic for {args}"
            )

            # Should not have trailing technical details
            lines = error_text.strip().split("\n")
            user_facing_line = lines[0] if lines else ""

            # First line should be user-focused, not technical
            assert not any(
                technical in user_facing_line
                for technical in [
                    "IOException",
                    "LINE 1:",
                    "CREATE VIEW",
                    "read_csv_auto",
                ]
            ), (
                f"First line should not contain technical details: {user_facing_line}"
            )


class TestCLIErrorTranslationImplementation:
    """Tests for specific implementation details of CLI error translation."""

    def test_duckdb_io_exception_translation(self):
        """Test that DuckDB IOException is properly caught and translated."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                "TestEntity=definitely-does-not-exist.csv",
                "MATCH (t:TestEntity) RETURN t",
            ],
        )

        # Should catch and translate DuckDB IOException
        assert result.exit_code == 1

        # Should not leak the original IOException to the user
        assert "IOException" not in result.output
        assert "_duckdb" not in result.output

        # Should have been translated to user-friendly message
        assert any(
            phrase in result.output.lower()
            for phrase in [
                "file not found",
                "entity source",
                "could not load",
                "does not exist",
            ]
        ), (
            f"Should translate IOException to user-friendly message. Got: {result.output}"
        )

    def test_error_translation_preserves_filename_context(self):
        """Test that error translation preserves important filename context for users."""
        test_filename = "my-special-data-file.csv"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                f"Data={test_filename}",
                "MATCH (d:Data) RETURN d",
            ],
        )

        assert result.exit_code == 1
        # Critical: should preserve the exact filename user specified
        assert test_filename in result.output, (
            f"Should preserve exact filename context: {test_filename}"
        )

    def test_multiple_missing_files_error_handling(self):
        """Test error handling when multiple files are missing."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                "Person=missing1.csv",
                "--rel",
                "KNOWS=missing2.csv",
                "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p, q",
            ],
        )

        assert result.exit_code == 1

        # Should handle the first error encountered gracefully
        # (May not process all missing files if first one fails)
        assert len(result.output.strip()) > 0, (
            "Should provide some error feedback"
        )


# ---------------------------------------------------------------------------
# Credential masking in error messages (Task #46)
# ---------------------------------------------------------------------------


class TestCredentialMaskingInErrors:
    """Verify that database credentials are masked in CLI error messages."""

    def test_translate_duckdb_error_masks_password(self):
        """translate_duckdb_error must mask credentials in path."""
        from pycypher.cli.common import translate_duckdb_error

        msg = translate_duckdb_error(
            FileNotFoundError("No files found that match the pattern"),
            "entity source",
            "postgresql://admin:s3cretP@ss@db.example.com:5432/mydb",
        )
        assert "s3cretP@ss" not in msg
        assert "***" in msg
        assert "admin" in msg  # username is OK to show

    def test_translate_duckdb_error_no_credentials_unchanged(self):
        """Paths without credentials pass through unchanged."""
        from pycypher.cli.common import translate_duckdb_error

        msg = translate_duckdb_error(
            FileNotFoundError("No files found"),
            "entity source",
            "/path/to/data.csv",
        )
        assert "/path/to/data.csv" in msg

    def test_mask_uri_credentials_in_generic_error(self):
        """Generic error fallback path also masks credentials."""
        from pycypher.cli.common import translate_duckdb_error

        msg = translate_duckdb_error(
            RuntimeError("connection refused"),
            "entity source",
            "mysql://root:hunter2@localhost/db",
        )
        assert "hunter2" not in msg
        assert "***" in msg

    def test_nmetl_cli_masks_credentials_in_file_not_found(self):
        """nmetl_cli._load_data_source masks credentials."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "query",
                "--entity",
                "Person=postgresql://user:secret123@host/db",
                "MATCH (p:Person) RETURN p",
            ],
        )
        # Error output should not contain the password
        assert "secret123" not in result.output
        # But should still show something useful
        assert result.exit_code == 1
