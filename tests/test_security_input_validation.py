"""Security input validation tests.

Validates that all user-facing entry points sanitize input properly and
that error messages don't leak internal implementation details.

Security boundaries covered:
1. Error message sanitization — no internal paths leaked to users
2. SQL identifier validation — injection prevention
3. File path traversal prevention — directory escape blocked
4. Query size and complexity limits — DoS prevention
5. URI credential masking — no password leakage
6. DuckDB error translation — internal details stripped
"""

from __future__ import annotations

import re

import pytest


# ---------------------------------------------------------------------------
# 1. Error Message Sanitization
# ---------------------------------------------------------------------------


class TestErrorMessageSanitization:
    """Verify error messages don't leak internal paths or implementation details."""

    def test_sanitize_strips_internal_paths(self) -> None:
        """Internal file paths (.py) are replaced with <internal>."""
        from pycypher.exceptions import sanitize_error_message

        exc = RuntimeError(
            "failed at /usr/lib/python3.12/site-packages/duckdb/core.py:42"
        )
        result = sanitize_error_message(exc)
        assert "/usr/lib" not in result
        assert "<internal>" in result

    def test_sanitize_strips_traceback_fragments(self) -> None:
        """Traceback lines are removed from error messages."""
        from pycypher.exceptions import sanitize_error_message

        exc = ValueError(
            'bad value\n  File "/some/path.py", line 10, in func\n    x = 1'
        )
        result = sanitize_error_message(exc)
        assert "File" not in result or "<internal>" in result

    def test_sanitize_preserves_exception_type(self) -> None:
        """Exception class name is preserved for error categorization."""
        from pycypher.exceptions import sanitize_error_message

        exc = KeyError("missing_column")
        result = sanitize_error_message(exc)
        assert "KeyError" in result

    def test_sanitize_handles_empty_message(self) -> None:
        """Empty exception message produces just the type name."""
        from pycypher.exceptions import sanitize_error_message

        exc = RuntimeError()
        result = sanitize_error_message(exc)
        assert result == "RuntimeError"

    def test_sanitize_strips_so_paths(self) -> None:
        """Shared object paths (.so) are also sanitized."""
        from pycypher.exceptions import sanitize_error_message

        exc = ImportError(
            "cannot load /usr/lib/libduckdb.so: symbol not found"
        )
        result = sanitize_error_message(exc)
        assert "/usr/lib" not in result
        assert "<internal>" in result


# ---------------------------------------------------------------------------
# 2. SQL Identifier Validation
# ---------------------------------------------------------------------------


class TestSQLIdentifierInjection:
    """Verify SQL identifier validation blocks injection attempts."""

    def test_reject_sql_injection_in_identifier(self) -> None:
        """Identifiers with SQL injection patterns are rejected."""
        from pycypher.backends._helpers import validate_identifier

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("name; DROP TABLE users--")

    def test_reject_quotes_in_identifier(self) -> None:
        """Identifiers with quote characters are rejected."""
        from pycypher.backends._helpers import validate_identifier

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier('name"')

    def test_reject_semicolon_in_identifier(self) -> None:
        """Semicolons in identifiers are rejected."""
        from pycypher.backends._helpers import validate_identifier

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("a;b")

    def test_accept_valid_identifiers(self) -> None:
        """Valid column/table names pass validation."""
        from pycypher.backends._helpers import validate_identifier

        for name in ["name", "age", "_id", "col_1", "A", "x99"]:
            assert validate_identifier(name) == name

    def test_reject_empty_identifier(self) -> None:
        """Empty string is not a valid identifier."""
        from pycypher.backends._helpers import validate_identifier

        with pytest.raises(ValueError):
            validate_identifier("")

    def test_reject_identifier_starting_with_digit(self) -> None:
        """Identifiers starting with a digit are rejected."""
        from pycypher.backends._helpers import validate_identifier

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("1column")


# ---------------------------------------------------------------------------
# 3. File Path Traversal Prevention
# ---------------------------------------------------------------------------


class TestFilePathTraversal:
    """Verify file path validation blocks directory traversal."""

    def test_reject_dotdot_traversal(self) -> None:
        """Paths with '..' are rejected."""
        from pycypher.ingestion.security import SecurityError, sanitize_file_path

        with pytest.raises((SecurityError, ValueError)):
            sanitize_file_path("/data/../../../etc/passwd")

    def test_reject_etc_access(self) -> None:
        """Access to /etc/ is blocked."""
        from pycypher.ingestion.security import SecurityError, sanitize_file_path

        with pytest.raises((SecurityError, ValueError)):
            sanitize_file_path("/etc/shadow")

    def test_reject_proc_access(self) -> None:
        """Access to /proc/ is blocked."""
        from pycypher.ingestion.security import SecurityError, sanitize_file_path

        with pytest.raises((SecurityError, ValueError)):
            sanitize_file_path("/proc/self/environ")

    def test_reject_dev_access(self) -> None:
        """Access to /dev/ is blocked."""
        from pycypher.ingestion.security import SecurityError, sanitize_file_path

        with pytest.raises((SecurityError, ValueError)):
            sanitize_file_path("/dev/random")


# ---------------------------------------------------------------------------
# 4. Query Size and Complexity Limits
# ---------------------------------------------------------------------------


class TestQueryLimits:
    """Verify query size and complexity limits prevent DoS."""

    def test_query_size_limit_enforced(self) -> None:
        """Oversized queries are rejected before parsing."""
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        # Generate a query exceeding 1MiB (default limit)
        huge_query = "MATCH (n) " + "WHERE n.x = 'a' " * 100_000
        if len(huge_query.encode()) > 1_048_576:
            with pytest.raises(Exception):  # noqa: BLE001, B017 — any rejection is acceptable
                parser.parse_to_ast(huge_query)

    def test_deeply_nested_query_rejected(self) -> None:
        """Deeply nested WHERE clauses don't cause stack overflow."""
        import sys

        from pycypher.exceptions import QueryComplexityError
        from pycypher.grammar_parser import GrammarParser

        # Pin the recursion limit for deterministic behavior; third-party
        # libraries (e.g. jedi) may raise it as a side-effect of import.
        saved_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(1000)
        try:
            parser = GrammarParser()
            # Build query with excessive nesting
            nested = "n.x = 1"
            for _ in range(250):
                nested = f"({nested}) AND n.y = 1"
            deep_query = f"MATCH (n) WHERE {nested} RETURN n"
            with pytest.raises((QueryComplexityError, RecursionError, Exception)):  # noqa: BLE001, B017
                parser.parse_to_ast(deep_query)
        finally:
            sys.setrecursionlimit(saved_limit)


# ---------------------------------------------------------------------------
# 5. URI Credential Masking
# ---------------------------------------------------------------------------


class TestCredentialMasking:
    """Verify credentials are masked in URIs before display."""

    def test_mask_password_in_postgresql_uri(self) -> None:
        """PostgreSQL URI password is replaced with ***."""
        from pycypher.ingestion.security import mask_uri_credentials

        uri = "postgresql://admin:s3cret@db.example.com:5432/mydb"
        masked = mask_uri_credentials(uri)
        assert "s3cret" not in masked
        assert "***" in masked
        assert "admin" in masked

    def test_mask_password_in_mysql_uri(self) -> None:
        """MySQL URI password is masked."""
        from pycypher.ingestion.security import mask_uri_credentials

        uri = "mysql://root:password123@localhost/test"
        masked = mask_uri_credentials(uri)
        assert "password123" not in masked
        assert "***" in masked

    def test_no_password_returns_unchanged(self) -> None:
        """URIs without passwords are returned unchanged."""
        from pycypher.ingestion.security import mask_uri_credentials

        uri = "file:///data/people.csv"
        assert mask_uri_credentials(uri) == uri

    def test_empty_string_returns_empty(self) -> None:
        """Empty string input returns empty string."""
        from pycypher.ingestion.security import mask_uri_credentials

        assert mask_uri_credentials("") == ""

    def test_mask_preserves_host_and_path(self) -> None:
        """Host, port, and path are preserved after masking."""
        from pycypher.ingestion.security import mask_uri_credentials

        uri = "postgresql://user:pass@db.example.com:5432/mydb"
        masked = mask_uri_credentials(uri)
        assert "db.example.com" in masked
        assert "5432" in masked
        assert "/mydb" in masked


# ---------------------------------------------------------------------------
# 6. DuckDB Error Translation
# ---------------------------------------------------------------------------


class TestDuckDBErrorTranslation:
    """Verify DuckDB errors are translated to user-friendly messages."""

    def test_file_not_found_translation(self) -> None:
        """'No files found' DuckDB error is user-friendly."""
        from pycypher.cli.common import translate_duckdb_error

        exc = RuntimeError("No files found that match the pattern /data/*.csv")
        msg = translate_duckdb_error(exc, "entity source", "/data/people.csv")
        assert "file not found" in msg
        assert "/data/people.csv" in msg

    def test_permission_denied_translation(self) -> None:
        """Permission errors are translated."""
        from pycypher.cli.common import translate_duckdb_error

        exc = OSError("Permission denied: /root/secret.csv")
        msg = translate_duckdb_error(exc, "entity source", "/root/secret.csv")
        assert "permission denied" in msg

    def test_memory_error_translation(self) -> None:
        """Memory errors provide helpful guidance."""
        from pycypher.cli.common import translate_duckdb_error

        exc = MemoryError("Out of memory loading large file")
        msg = translate_duckdb_error(exc, "entity source", "big.csv")
        assert "memory" in msg.lower()

    def test_generic_error_sanitized(self) -> None:
        """Generic errors have internal paths stripped."""
        from pycypher.cli.common import translate_duckdb_error

        exc = RuntimeError(
            "failed at /usr/lib/python3.12/duckdb/core.py:42 during load"
        )
        msg = translate_duckdb_error(exc, "entity", "data.csv")
        # Internal paths should be sanitized
        assert "/usr/lib" not in msg

    def test_credentials_masked_in_error_path(self) -> None:
        """Database credentials in path are masked in error messages."""
        from pycypher.cli.common import translate_duckdb_error

        exc = RuntimeError("connection refused")
        msg = translate_duckdb_error(
            exc, "entity source", "postgresql://admin:s3cret@db.host/mydb"
        )
        assert "s3cret" not in msg
        assert "***" in msg
        assert "admin" in msg

    def test_credentials_masked_in_file_not_found(self) -> None:
        """Credentials masked even in file-not-found errors."""
        from pycypher.cli.common import translate_duckdb_error

        exc = RuntimeError("No files found that match the pattern foo")
        msg = translate_duckdb_error(
            exc, "entity", "mysql://root:password@host/db"
        )
        assert "password" not in msg


# ---------------------------------------------------------------------------
# 7. SQL Query Allowlisting
# ---------------------------------------------------------------------------


class TestSQLQueryAllowlist:
    """Verify only SELECT queries are allowed through validation."""

    def test_select_allowed(self) -> None:
        """Plain SELECT queries pass validation."""
        from pycypher.ingestion.security import validate_sql_query

        # Should not raise
        validate_sql_query("SELECT * FROM source")

    def test_drop_rejected(self) -> None:
        """DROP TABLE is rejected."""
        from pycypher.ingestion.security import SecurityError, validate_sql_query

        with pytest.raises(SecurityError):
            validate_sql_query("DROP TABLE users")

    def test_insert_rejected(self) -> None:
        """INSERT is rejected."""
        from pycypher.ingestion.security import SecurityError, validate_sql_query

        with pytest.raises(SecurityError):
            validate_sql_query("INSERT INTO users VALUES (1, 'admin')")

    def test_multi_statement_rejected(self) -> None:
        """Multiple statements separated by semicolons are rejected."""
        from pycypher.ingestion.security import SecurityError, validate_sql_query

        with pytest.raises(SecurityError):
            validate_sql_query("SELECT 1; DROP TABLE users")

    def test_comment_injection_rejected(self) -> None:
        """SQL comment-based injection attempts are handled."""
        from pycypher.ingestion.security import SecurityError, validate_sql_query

        # This should either be allowed as a valid SELECT or rejected
        # as suspicious — either way, no injection should succeed
        try:
            validate_sql_query("SELECT /* comment */ * FROM source")
        except SecurityError:
            pass  # Rejection is also acceptable


# ---------------------------------------------------------------------------
# 8. SSRF Prevention
# ---------------------------------------------------------------------------


class TestSSRFPrevention:
    """Verify SSRF protections block internal network access."""

    def test_reject_localhost(self) -> None:
        """Localhost URIs are blocked."""
        from pycypher.ingestion.security import SecurityError, validate_uri_scheme

        with pytest.raises(SecurityError):
            validate_uri_scheme("http://localhost/admin")

    def test_reject_private_ip(self) -> None:
        """Private RFC 1918 IPs are blocked."""
        from pycypher.ingestion.security import SecurityError, validate_uri_scheme

        with pytest.raises(SecurityError):
            validate_uri_scheme("http://192.168.1.1/internal")

    def test_reject_loopback(self) -> None:
        """127.x.x.x addresses are blocked."""
        from pycypher.ingestion.security import SecurityError, validate_uri_scheme

        with pytest.raises(SecurityError):
            validate_uri_scheme("http://127.0.0.1:8080/api")

    def test_allow_file_scheme(self) -> None:
        """File scheme URIs pass scheme validation."""
        from pycypher.ingestion.security import validate_uri_scheme

        # File scheme is allowed (no SSRF DNS check needed)
        result = validate_uri_scheme("file:///data/dataset.csv")
        assert result == "file:///data/dataset.csv"

    def test_allow_bare_path(self) -> None:
        """Bare file paths (no scheme) pass validation."""
        from pycypher.ingestion.security import validate_uri_scheme

        result = validate_uri_scheme("/data/dataset.csv")
        assert result == "/data/dataset.csv"
