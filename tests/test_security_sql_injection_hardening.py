"""TDD tests for SQL injection security vulnerabilities.

These tests demonstrate SQL injection vulnerabilities in the data source
handling code before they are fixed. Tests should FAIL in the red phase,
showing that malicious SQL can be injected, then PASS in the green phase
after proper input sanitization is implemented.

Run with:
    uv run pytest tests/test_security_sql_injection_tdd.py -v
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from pycypher.ingestion.data_sources import SqlDataSource
from pycypher.ingestion.duckdb_reader import DuckDBReader

pytestmark = pytest.mark.slow


class TestSQLInjectionVulnerabilities:
    """Test that SQL injection attacks are prevented in data source operations."""

    def test_csv_path_injection_prevented(self) -> None:
        """Malicious SQL in CSV file path should be prevented, not executed."""
        # Create a legitimate CSV file
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            df.to_csv(tmp_path, index=False)

            try:
                # Malicious path with SQL injection attempt
                # This tries to inject SQL that would create a malicious table
                malicious_path = (
                    f"{tmp_path}'; CREATE TABLE hacked (data TEXT); --"
                )

                # This should NOT execute the injected SQL
                # If vulnerable, it would create the "hacked" table
                with pytest.raises((ValueError, Exception)) as exc_info:
                    DuckDBReader.from_csv(malicious_path)

                # Should raise a security-related error, not execute the malicious SQL
                assert (
                    "injection" in str(exc_info.value).lower()
                    or "invalid" in str(exc_info.value).lower()
                )

            finally:
                tmp_path.unlink()

    def test_query_injection_prevented(self) -> None:
        """Malicious SQL in query parameter should be prevented."""
        # Create a legitimate CSV file
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            df.to_csv(tmp_path, index=False)

            try:
                # Malicious query with SQL injection
                malicious_query = (
                    "SELECT * FROM source; DROP TABLE IF EXISTS users; --"
                )

                # This should NOT execute the DROP TABLE command
                with pytest.raises((ValueError, Exception)) as exc_info:
                    DuckDBReader.from_csv(str(tmp_path), query=malicious_query)

                # Should raise a security error, not execute malicious SQL
                assert (
                    "injection" in str(exc_info.value).lower()
                    or "invalid" in str(exc_info.value).lower()
                )

            finally:
                tmp_path.unlink()

    def test_parquet_path_injection_prevented(self) -> None:
        """Malicious SQL in parquet file path should be prevented."""
        # Create a legitimate parquet file
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})

        with tempfile.NamedTemporaryFile(
            suffix=".parquet",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            df.to_parquet(tmp_path)

            try:
                # Malicious path with SQL injection
                malicious_path = (
                    f"{tmp_path}'; INSERT INTO source VALUES ('hacked'); --"
                )

                with pytest.raises((ValueError, Exception)) as exc_info:
                    DuckDBReader.from_parquet(malicious_path)

                assert (
                    "injection" in str(exc_info.value).lower()
                    or "invalid" in str(exc_info.value).lower()
                )

            finally:
                tmp_path.unlink()

    def test_json_path_injection_prevented(self) -> None:
        """Malicious SQL in JSON file path should be prevented."""
        # Create a legitimate JSON file
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            df.to_json(tmp_path, orient="records")

            try:
                # Malicious path with SQL injection
                malicious_path = (
                    f"{tmp_path}'; UPDATE source SET name = 'hacked'; --"
                )

                with pytest.raises((ValueError, Exception)) as exc_info:
                    DuckDBReader.from_json(malicious_path)

                assert (
                    "injection" in str(exc_info.value).lower()
                    or "invalid" in str(exc_info.value).lower()
                )

            finally:
                tmp_path.unlink()

    def test_sql_source_query_injection_prevented(self) -> None:
        """Direct SQL queries should be validated to prevent injection."""
        # Mock a SQL connection string (would normally connect to real DB)
        mock_connection = "sqlite:///:memory:"

        # Malicious query that tries to execute multiple statements
        malicious_query = (
            "SELECT 1; DROP TABLE users; INSERT INTO logs VALUES ('hacked');"
        )

        with pytest.raises((ValueError, Exception)) as exc_info:
            DuckDBReader.from_sql(mock_connection, malicious_query)

        # Should prevent multi-statement execution or other injection vectors
        assert (
            "injection" in str(exc_info.value).lower()
            or "invalid" in str(exc_info.value).lower()
        )


class TestDataSourceSecurityIntegration:
    """Test security in the higher-level DataSource classes."""

    def test_file_data_source_path_validation(self) -> None:
        """FileDataSource should validate file paths for security."""
        from pycypher.ingestion.data_sources import CsvFormat, FileDataSource
        from pycypher.ingestion.security import SecurityError

        # Malicious URI with path traversal attempt
        malicious_uri = "../../../etc/passwd"

        with pytest.raises((SecurityError, ValueError)) as exc_info:
            source = FileDataSource(malicious_uri, CsvFormat())
            source.read()

        # Should prevent path traversal and SQL injection
        error_msg = str(exc_info.value).lower()
        assert (
            "security" in error_msg
            or "traversal" in error_msg
            or "path" in error_msg
        )

    def test_sql_data_source_query_validation(self) -> None:
        """SqlDataSource should validate SQL queries."""
        # Mock connection string
        connection = "sqlite:///:memory:"

        # Malicious query with injection attempt
        malicious_query = (
            "SELECT * FROM users WHERE id = 1; DROP TABLE sessions; --"
        )

        with pytest.raises((ValueError, Exception)) as exc_info:
            source = SqlDataSource(connection, malicious_query)
            source.read()

        error_msg = str(exc_info.value).lower()
        assert "injection" in error_msg or "invalid" in error_msg


class TestInputValidationHelpers:
    """Test helper functions for input validation and sanitization."""

    def test_path_sanitization_helper_exists(self) -> None:
        """Helper function for sanitizing file paths should exist and work."""
        # Security module successfully implemented
        from pycypher.ingestion.security import (
            SecurityError,
            sanitize_file_path,
        )

        # Valid paths should pass through; sanitize_file_path returns the
        # resolved (absolute, symlink-free) path, not the original input.
        valid_path = "/home/user/data.csv"
        result = sanitize_file_path(valid_path)
        from pathlib import Path

        assert result == str(Path(valid_path).resolve())

        # Dangerous paths should raise SecurityError
        with pytest.raises(SecurityError):
            sanitize_file_path("../../etc/passwd")

    def test_sql_query_validation_helper_exists(self) -> None:
        """Helper function for validating SQL queries should exist and work."""
        # Security module successfully implemented
        from pycypher.ingestion.security import (
            SecurityError,
            validate_sql_query,
        )

        # Valid queries should pass validation
        validate_sql_query("SELECT * FROM table")
        validate_sql_query("SELECT name, age FROM users WHERE age > 21")

        # Dangerous queries should raise SecurityError
        with pytest.raises(SecurityError):
            validate_sql_query("SELECT *; DROP TABLE users;")
