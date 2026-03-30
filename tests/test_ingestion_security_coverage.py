"""Comprehensive tests for pycypher.ingestion.security module.

Covers sanitize_file_path, _strip_sql_comments, _count_unquoted_semicolons,
_reject_dangerous_table_functions, validate_sql_query, sanitize_sql_identifier,
validate_uri_scheme, _check_ssrf_hostname, escape_sql_string_literal, and
parameterize_duckdb_query — targeting the 48% → 90%+ coverage gap.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pycypher.exceptions import SecurityError
from pycypher.ingestion.security import (
    _count_unquoted_semicolons,
    _reject_dangerous_table_functions,
    _strip_sql_comments,
    escape_sql_string_literal,
    parameterize_duckdb_query,
    sanitize_file_path,
    sanitize_sql_identifier,
    validate_sql_query,
    validate_uri_scheme,
)

# ---------------------------------------------------------------------------
# sanitize_file_path
# ---------------------------------------------------------------------------


class TestSanitizeFilePath:
    """Tests for sanitize_file_path()."""

    def test_normal_path_passes(self) -> None:
        from pathlib import Path

        result = sanitize_file_path("data/input.csv")
        # Returns the resolved (absolute) path
        assert result == str(Path("data/input.csv").resolve())

    def test_empty_path_raises(self) -> None:
        with pytest.raises(SecurityError, match="Empty path"):
            sanitize_file_path("")

    def test_path_traversal_raises(self) -> None:
        with pytest.raises(SecurityError, match="Path traversal"):
            sanitize_file_path("../etc/passwd")

    def test_sensitive_etc_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/etc/passwd")

    def test_sensitive_root_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/root/.ssh/id_rsa")

    def test_sensitive_proc_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/proc/self/environ")

    def test_sensitive_sys_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/sys/class/net")

    def test_sensitive_dev_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/dev/null")

    def test_sensitive_var_run_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/var/run/docker.sock")

    def test_sensitive_boot_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/boot/vmlinuz")

    def test_sensitive_sbin_prefix_raises(self) -> None:
        with pytest.raises(SecurityError, match="sensitive system path"):
            sanitize_file_path("/sbin/init")

    def test_invalid_path_raises(self) -> None:
        """Path that causes OSError during resolve."""
        with patch(
            "pycypher.ingestion.security.Path.resolve", side_effect=OSError("bad"),
        ), pytest.raises(SecurityError, match="Invalid path"):
            sanitize_file_path("some_file.csv")


# ---------------------------------------------------------------------------
# _strip_sql_comments
# ---------------------------------------------------------------------------


class TestStripSqlComments:
    """Tests for _strip_sql_comments()."""

    def test_no_comments(self) -> None:
        assert _strip_sql_comments("SELECT * FROM t") == "SELECT * FROM t"

    def test_single_line_dash_comment(self) -> None:
        result = _strip_sql_comments("SELECT * FROM t -- this is a comment")
        assert result.strip() == "SELECT * FROM t"

    def test_single_line_hash_comment(self) -> None:
        result = _strip_sql_comments("SELECT * FROM t # comment")
        assert result.strip() == "SELECT * FROM t"

    def test_block_comment(self) -> None:
        result = _strip_sql_comments("SELECT /* inline */ * FROM t")
        assert "inline" not in result
        assert "SELECT" in result
        assert "FROM t" in result

    def test_unterminated_block_comment_raises(self) -> None:
        with pytest.raises(SecurityError, match="Unterminated block comment"):
            _strip_sql_comments("SELECT * /* never closed")

    def test_single_quotes_preserve_comment_tokens(self) -> None:
        result = _strip_sql_comments("SELECT '-- not a comment' FROM t")
        assert "-- not a comment" in result

    def test_double_quotes_preserve_comment_tokens(self) -> None:
        result = _strip_sql_comments('SELECT "/* not a comment */" FROM t')
        assert "/* not a comment */" in result

    def test_escaped_single_quotes(self) -> None:
        result = _strip_sql_comments("SELECT 'it''s fine' FROM t")
        assert "''" in result

    def test_escaped_double_quotes(self) -> None:
        result = _strip_sql_comments('SELECT "col""name" FROM t')
        assert '""' in result

    def test_dash_comment_with_newline(self) -> None:
        result = _strip_sql_comments("SELECT 1\n-- comment\nFROM t")
        assert "SELECT 1" in result
        assert "FROM t" in result
        assert "comment" not in result

    def test_hash_comment_at_end(self) -> None:
        result = _strip_sql_comments("SELECT 1 # end comment")
        assert result.strip() == "SELECT 1"

    def test_empty_query(self) -> None:
        assert _strip_sql_comments("") == ""


# ---------------------------------------------------------------------------
# _count_unquoted_semicolons
# ---------------------------------------------------------------------------


class TestCountUnquotedSemicolons:
    """Tests for _count_unquoted_semicolons()."""

    def test_no_semicolons(self) -> None:
        assert _count_unquoted_semicolons("SELECT * FROM t") == 0

    def test_one_trailing_semicolon(self) -> None:
        assert _count_unquoted_semicolons("SELECT * FROM t;") == 1

    def test_two_semicolons(self) -> None:
        assert _count_unquoted_semicolons("SELECT 1; DROP TABLE t;") == 2

    def test_semicolon_in_single_quotes(self) -> None:
        assert _count_unquoted_semicolons("SELECT ';' FROM t") == 0

    def test_semicolon_in_double_quotes(self) -> None:
        assert _count_unquoted_semicolons('SELECT "col;name" FROM t') == 0

    def test_escaped_single_quote_with_semicolon(self) -> None:
        assert _count_unquoted_semicolons("SELECT 'it''s;here' FROM t") == 0

    def test_escaped_double_quote_with_semicolon(self) -> None:
        assert _count_unquoted_semicolons('SELECT "a""b;c" FROM t') == 0


# ---------------------------------------------------------------------------
# _reject_dangerous_table_functions
# ---------------------------------------------------------------------------


class TestRejectDangerousTableFunctions:
    """Tests for _reject_dangerous_table_functions()."""

    @pytest.mark.parametrize(
        "func",
        [
            "read_csv",
            "read_parquet",
            "read_json",
            "glob",
            "parquet_scan",
            "read_blob",
            "read_text",
        ],
    )
    def test_rejects_dangerous_functions(self, func: str) -> None:
        query = f"select * from {func}('/etc/passwd')"
        with pytest.raises(SecurityError, match="Dangerous DuckDB table function"):
            _reject_dangerous_table_functions(query)

    @pytest.mark.parametrize(
        "prefix",
        ["duckdb_", "pg_", "pragma_"],
    )
    def test_rejects_dangerous_prefixes(self, prefix: str) -> None:
        query = f"select {prefix}version()"
        with pytest.raises(SecurityError, match="Dangerous DuckDB function"):
            _reject_dangerous_table_functions(query)

    def test_safe_query_passes(self) -> None:
        _reject_dangerous_table_functions("select * from source where id > 0")

    def test_function_with_whitespace_before_paren(self) -> None:
        with pytest.raises(SecurityError):
            _reject_dangerous_table_functions("select * from read_csv  ('/etc/passwd')")


# ---------------------------------------------------------------------------
# validate_sql_query
# ---------------------------------------------------------------------------


class TestValidateSqlQuery:
    """Tests for validate_sql_query()."""

    def test_valid_select(self) -> None:
        validate_sql_query("SELECT * FROM source")

    def test_valid_select_with_trailing_semicolon(self) -> None:
        validate_sql_query("SELECT * FROM source;")

    def test_valid_with_cte(self) -> None:
        validate_sql_query("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_empty_query_raises(self) -> None:
        with pytest.raises(SecurityError, match="Empty query"):
            validate_sql_query("")

    def test_whitespace_only_raises(self) -> None:
        with pytest.raises(SecurityError, match="Empty query"):
            validate_sql_query("   ")

    def test_comments_only_raises(self) -> None:
        with pytest.raises(SecurityError, match="only comments"):
            validate_sql_query("-- just a comment")

    def test_multiple_statements_raises(self) -> None:
        with pytest.raises(SecurityError, match="Multiple SQL statements"):
            validate_sql_query("SELECT 1; DROP TABLE t;")

    def test_semicolon_in_middle_raises(self) -> None:
        with pytest.raises(SecurityError, match="Semicolon found in the middle"):
            validate_sql_query("SELECT 1; SELECT 2")

    def test_insert_rejected(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT queries"):
            validate_sql_query("INSERT INTO t VALUES (1)")

    def test_drop_rejected(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT queries"):
            validate_sql_query("DROP TABLE t")

    def test_update_rejected(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT queries"):
            validate_sql_query("UPDATE t SET x = 1")

    def test_dangerous_function_in_select(self) -> None:
        with pytest.raises(SecurityError, match="Dangerous"):
            validate_sql_query("SELECT * FROM read_csv('/etc/passwd')")

    def test_parse_error_raises_security_error(self) -> None:
        """If comment stripping raises a non-SecurityError, wrap it."""
        with (
            patch(
                "pycypher.ingestion.security._strip_sql_comments",
                side_effect=ValueError("parse boom"),
            ),
            pytest.raises(SecurityError, match="Failed to parse"),
        ):
            validate_sql_query("SELECT 1")


# ---------------------------------------------------------------------------
# sanitize_sql_identifier
# ---------------------------------------------------------------------------


class TestSanitizeSqlIdentifier:
    """Tests for sanitize_sql_identifier()."""

    def test_valid_identifier(self) -> None:
        assert sanitize_sql_identifier("my_table") == "my_table"

    def test_valid_identifier_with_numbers(self) -> None:
        assert sanitize_sql_identifier("col1") == "col1"

    def test_empty_identifier_raises(self) -> None:
        with pytest.raises(SecurityError, match="Empty identifier"):
            sanitize_sql_identifier("")

    def test_invalid_chars_raises(self) -> None:
        with pytest.raises(SecurityError, match="Invalid SQL identifier"):
            sanitize_sql_identifier("table; DROP")

    def test_starts_with_number_raises(self) -> None:
        with pytest.raises(SecurityError, match="Invalid SQL identifier"):
            sanitize_sql_identifier("1table")

    @pytest.mark.parametrize(
        "word",
        ["select", "from", "drop", "delete", "insert", "update", "create"],
    )
    def test_reserved_words_rejected(self, word: str) -> None:
        with pytest.raises(SecurityError, match="reserved word"):
            sanitize_sql_identifier(word)

    def test_sp_prefix_rejected(self) -> None:
        with pytest.raises(SecurityError, match="reserved word"):
            sanitize_sql_identifier("sp_execute")

    def test_xp_prefix_rejected(self) -> None:
        with pytest.raises(SecurityError, match="reserved word"):
            sanitize_sql_identifier("xp_cmdshell")


# ---------------------------------------------------------------------------
# validate_uri_scheme
# ---------------------------------------------------------------------------


class TestValidateUriScheme:
    """Tests for validate_uri_scheme()."""

    def test_empty_uri_raises(self) -> None:
        with pytest.raises(SecurityError, match="Empty URI"):
            validate_uri_scheme("")

    def test_http_allowed(self) -> None:
        assert (
            validate_uri_scheme("http://example.com/data.csv")
            == "http://example.com/data.csv"
        )

    def test_https_allowed(self) -> None:
        assert (
            validate_uri_scheme("https://example.com/data")
            == "https://example.com/data"
        )

    def test_s3_allowed(self) -> None:
        assert validate_uri_scheme("s3://bucket/key") == "s3://bucket/key"

    def test_file_allowed(self) -> None:
        assert validate_uri_scheme("file:///data/file.csv") == "file:///data/file.csv"

    def test_bare_path_allowed(self) -> None:
        assert validate_uri_scheme("/data/file.csv") == "/data/file.csv"

    def test_ftp_rejected(self) -> None:
        with pytest.raises(SecurityError, match="not allowed"):
            validate_uri_scheme("ftp://example.com/file")

    def test_javascript_rejected(self) -> None:
        with pytest.raises(SecurityError, match="not allowed"):
            validate_uri_scheme("javascript:alert(1)")

    def test_http_localhost_ssrf_blocked(self) -> None:
        with pytest.raises(SecurityError, match="SSRF"):
            validate_uri_scheme("http://localhost/api")

    def test_https_with_external_host_passes(self) -> None:
        # Mock DNS to avoid network calls
        with patch("pycypher.ingestion.security._check_ssrf_hostname"):
            assert (
                validate_uri_scheme("https://api.example.com/data")
                == "https://api.example.com/data"
            )


# ---------------------------------------------------------------------------
# _check_ssrf_hostname (imported indirectly via validate_uri_scheme)
# ---------------------------------------------------------------------------


class TestCheckSsrfHostname:
    """Tests for _check_ssrf_hostname()."""

    def test_localhost_blocked(self) -> None:
        from pycypher.ingestion.security import _check_ssrf_hostname

        with pytest.raises(SecurityError, match="SSRF"):
            _check_ssrf_hostname("localhost")

    def test_ip6_localhost_blocked(self) -> None:
        from pycypher.ingestion.security import _check_ssrf_hostname

        with pytest.raises(SecurityError, match="SSRF"):
            _check_ssrf_hostname("ip6-localhost")

    def test_literal_private_ip_blocked(self) -> None:
        from pycypher.ingestion.security import _check_ssrf_hostname

        with pytest.raises(SecurityError, match="SSRF"):
            _check_ssrf_hostname("192.168.1.1")

    def test_literal_loopback_blocked(self) -> None:
        from pycypher.ingestion.security import _check_ssrf_hostname

        with pytest.raises(SecurityError, match="SSRF"):
            _check_ssrf_hostname("127.0.0.1")

    def test_dns_resolution_failure_blocked(self) -> None:
        """If DNS fails, block the request (unresolvable hosts are suspicious)."""
        import socket

        from pycypher.ingestion.security import _check_ssrf_hostname

        with patch("socket.getaddrinfo", side_effect=socket.gaierror("no such host")):
            with pytest.raises(SecurityError, match="SSRF"):
                _check_ssrf_hostname("nonexistent.invalid.test")

    def test_dns_resolves_to_private_blocked(self) -> None:
        import socket

        from pycypher.ingestion.security import _check_ssrf_hostname

        fake_result = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 0))]
        with patch("socket.getaddrinfo", return_value=fake_result):
            with pytest.raises(SecurityError, match="SSRF"):
                _check_ssrf_hostname("evil.attacker.com")

    def test_dns_empty_result_allowed(self) -> None:
        from pycypher.ingestion.security import _check_ssrf_hostname

        with patch("socket.getaddrinfo", return_value=[]):
            _check_ssrf_hostname("empty-dns.test")  # should not raise

    def test_dns_invalid_address_format_allowed(self) -> None:
        import socket

        from pycypher.ingestion.security import _check_ssrf_hostname

        fake_result = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("not-an-ip", 0))]
        with patch("socket.getaddrinfo", return_value=fake_result):
            _check_ssrf_hostname("weird-dns.test")  # should not raise


# ---------------------------------------------------------------------------
# escape_sql_string_literal
# ---------------------------------------------------------------------------


class TestEscapeSqlStringLiteral:
    """Tests for escape_sql_string_literal()."""

    def test_simple_string(self) -> None:
        assert escape_sql_string_literal("hello") == "'hello'"

    def test_string_with_single_quote(self) -> None:
        assert escape_sql_string_literal("it's") == "'it''s'"

    def test_none_returns_null(self) -> None:
        assert escape_sql_string_literal(None) == "NULL"  # type: ignore[arg-type]

    def test_empty_string(self) -> None:
        assert escape_sql_string_literal("") == "''"


# ---------------------------------------------------------------------------
# parameterize_duckdb_query
# ---------------------------------------------------------------------------


class TestParameterizeDuckdbQuery:
    """Tests for parameterize_duckdb_query()."""

    def test_simple_parameterization(self) -> None:
        result = parameterize_duckdb_query(
            "SELECT * FROM source WHERE name = {name}",
            name="Alice",
        )
        assert result == "SELECT * FROM source WHERE name = 'Alice'"

    def test_empty_template_raises(self) -> None:
        with pytest.raises(SecurityError, match="Empty SQL template"):
            parameterize_duckdb_query("")

    def test_missing_parameter_raises(self) -> None:
        with pytest.raises(SecurityError, match="Missing parameter"):
            parameterize_duckdb_query("SELECT {col} FROM source")

    def test_sql_injection_escaped(self) -> None:
        result = parameterize_duckdb_query(
            "SELECT * FROM source WHERE name = {name}",
            name="Robert'; DROP TABLE source--",
        )
        assert "'Robert''; DROP TABLE source--'" in result

    def test_invalid_param_name_raises(self) -> None:
        """Parameter names must be valid SQL identifiers."""
        with pytest.raises(SecurityError):
            parameterize_duckdb_query(
                "SELECT {0bad} FROM source",
                **{"0bad": "value"},
            )
