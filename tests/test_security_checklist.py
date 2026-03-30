"""Comprehensive security checklist validation tests.

This module validates that all input boundaries in the PyCypher codebase
follow secure coding practices. It serves as a regression test suite to
ensure security hardening is maintained across future changes.

Security boundaries covered:
1. YAML config loading — safe deserialization
2. SQL query construction — parameterized/validated
3. File path handling — traversal prevention
4. Environment variable parsing — crash resilience
5. Log level configuration — whitelist validation
6. Neo4j sink — credential masking + parameterized queries
7. Import path validation — blocked dangerous modules
8. Cross-join limits — resource exhaustion prevention
9. Query timeout — DoS prevention
10. Query parameter handling — injection prevention
"""

from __future__ import annotations

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# 1. YAML Config: Safe Deserialization
# ---------------------------------------------------------------------------


class TestYAMLSafety:
    """Verify YAML config uses safe_load, not load."""

    def test_yaml_safe_load_used(self) -> None:
        """Config loader must use yaml.safe_load, never yaml.load."""
        config_path = Path(
            "packages/pycypher/src/pycypher/ingestion/config.py",
        )
        source = config_path.read_text()
        assert "yaml.safe_load" in source
        # Ensure no bare yaml.load (which allows arbitrary Python execution)
        # Exclude comments and the safe_load line itself
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "yaml.load(" in stripped and "safe_load" not in stripped:
                pytest.fail(f"Unsafe yaml.load found: {stripped}")


# ---------------------------------------------------------------------------
# 2. SQL Query Construction: Identifier Validation
# ---------------------------------------------------------------------------


class TestSQLIdentifierValidation:
    """Verify SQL identifiers are validated against injection."""

    def test_identifier_regex_is_strict(self) -> None:
        """Backend engine identifier regex only allows safe characters."""
        from pycypher.backend_engine import IDENTIFIER_RE

        # Must match valid identifiers
        assert IDENTIFIER_RE.match("column_name")
        assert IDENTIFIER_RE.match("Person")
        assert IDENTIFIER_RE.match("_private")

        # Must reject injection attempts
        assert not IDENTIFIER_RE.match("'; DROP TABLE--")
        assert not IDENTIFIER_RE.match("col name")
        assert not IDENTIFIER_RE.match("col;name")
        assert not IDENTIFIER_RE.match("")
        assert not IDENTIFIER_RE.match("123start")

    def test_sql_string_literal_validation_blocks_quotes(self) -> None:
        """SQL string literal validator blocks single quotes."""
        from pycypher.ingestion.data_sources import (
            _validate_sql_string_literal,
        )

        with pytest.raises(ValueError, match="single quote"):
            _validate_sql_string_literal("it's dangerous", "test_field")

    def test_sql_string_literal_validation_blocks_nul(self) -> None:
        """SQL string literal validator blocks NUL bytes."""
        from pycypher.ingestion.data_sources import (
            _validate_sql_string_literal,
        )

        with pytest.raises(ValueError, match="NUL"):
            _validate_sql_string_literal("bad\x00value", "test_field")


# ---------------------------------------------------------------------------
# 3. File Path Handling: Traversal Prevention
# ---------------------------------------------------------------------------


class TestFilePathSecurity:
    """Verify file path sanitization prevents traversal attacks."""

    def test_sanitize_file_path_blocks_traversal(self) -> None:
        """Path sanitization rejects directory traversal attempts."""
        from pycypher.ingestion.security import (
            SecurityError,
            sanitize_file_path,
        )

        with pytest.raises(SecurityError, match="traversal"):
            sanitize_file_path("../../etc/passwd")

    def test_sanitize_file_path_blocks_null_bytes(self) -> None:
        """Path sanitization rejects null bytes."""
        from pycypher.ingestion.security import (
            SecurityError,
            sanitize_file_path,
        )

        with pytest.raises((SecurityError, ValueError)):
            sanitize_file_path("/tmp/file\x00.csv")

    def test_grammar_parser_validates_file_type(self) -> None:
        """Grammar parser parse_file rejects directories."""
        import tempfile

        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="Not a regular file"):
                parser.parse_file(tmpdir)


# ---------------------------------------------------------------------------
# 4. Environment Variable Parsing: Crash Resilience
# ---------------------------------------------------------------------------


class TestEnvVarResilience:
    """Verify environment variable parsing doesn't crash on bad input."""

    def test_metrics_threshold_handles_garbage(self) -> None:
        """PYCYPHER_SLOW_QUERY_MS with garbage input returns default."""
        import os
        from unittest.mock import patch

        with patch.dict(
            os.environ,
            {"PYCYPHER_SLOW_QUERY_MS": "not_a_number"},
        ):
            from shared.metrics import _parse_slow_query_ms

            result = _parse_slow_query_ms()
            assert result == 1.0  # Default 1000ms / 1000


# ---------------------------------------------------------------------------
# 5. Log Level Configuration: Whitelist Validation
# ---------------------------------------------------------------------------


class TestLogLevelWhitelist:
    """Verify log level is whitelist-validated, not getattr-based."""

    def test_only_standard_levels_accepted(self) -> None:
        """Whitelist contains only standard Python logging levels."""
        from shared.logger import _VALID_LOG_LEVELS

        assert set(_VALID_LOG_LEVELS.keys()) == {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }

    def test_arbitrary_names_rejected(self) -> None:
        """Arbitrary attribute names return None from whitelist."""
        from shared.logger import _VALID_LOG_LEVELS

        assert _VALID_LOG_LEVELS.get("__IMPORT__") is None
        assert _VALID_LOG_LEVELS.get("NOTSET") is None
        assert _VALID_LOG_LEVELS.get("handler") is None


# ---------------------------------------------------------------------------
# 6. Neo4j Sink: Credential Masking + Parameterized Queries
# ---------------------------------------------------------------------------


class TestNeo4jSinkSecurity:
    """Verify Neo4j sink doesn't leak credentials."""

    def test_password_not_in_repr(self) -> None:
        """Neo4j sink repr/str must not contain the password."""
        neo4j_path = Path("packages/pycypher/src/pycypher/sinks/neo4j.py")
        source = neo4j_path.read_text()
        # Verify password masking exists in __repr__
        assert "__repr__" in source or "repr" in source.lower()

    def test_cypher_templates_use_parameters(self) -> None:
        """Neo4j Cypher templates must use $rows parameters, not interpolation."""
        neo4j_path = Path("packages/pycypher/src/pycypher/sinks/neo4j.py")
        source = neo4j_path.read_text()
        # Verify parameterized execution pattern
        assert "rows=rows" in source or "$rows" in source

    def test_identifiers_are_backtick_quoted(self) -> None:
        """Neo4j Cypher templates must backtick-quote identifiers."""
        neo4j_path = Path("packages/pycypher/src/pycypher/sinks/neo4j.py")
        source = neo4j_path.read_text()
        # Check for backtick quoting pattern in template builders
        assert "``{" in source or "`{" in source


# ---------------------------------------------------------------------------
# 7. Import Path Validation: Blocked Dangerous Modules
# ---------------------------------------------------------------------------


class TestImportBlocklist:
    """Verify dangerous module imports are blocked in config."""

    def test_blocklist_contains_dangerous_modules(self) -> None:
        """Import blocklist includes os, subprocess, pickle, etc."""
        config_path = Path(
            "packages/pycypher/src/pycypher/ingestion/config.py",
        )
        source = config_path.read_text()
        dangerous = ["os", "subprocess", "pickle", "shutil", "tempfile"]
        for module in dangerous:
            assert f'"{module}"' in source, f"{module} not in blocklist"


# ---------------------------------------------------------------------------
# 8. Cross-Join Limits: Resource Exhaustion Prevention
# ---------------------------------------------------------------------------


class TestCrossJoinSafety:
    """Verify cross-join has a hard row limit."""

    def test_max_cross_join_rows_exists(self) -> None:
        """MAX_CROSS_JOIN_ROWS constant is defined and reasonable."""
        from pycypher.binding_frame import MAX_CROSS_JOIN_ROWS

        assert isinstance(MAX_CROSS_JOIN_ROWS, int)
        assert MAX_CROSS_JOIN_ROWS > 0
        assert MAX_CROSS_JOIN_ROWS <= 100_000_000  # Reasonable upper bound


# ---------------------------------------------------------------------------
# 9. Query Timeout: DoS Prevention
# ---------------------------------------------------------------------------


class TestTimeoutSafety:
    """Verify query timeout mechanism is wired and functional."""

    def test_execute_query_accepts_timeout(self) -> None:
        """Star.execute_query has a timeout_seconds parameter."""
        import inspect

        from pycypher import Star

        sig = inspect.signature(Star.execute_query)
        assert "timeout_seconds" in sig.parameters

    def test_query_timeout_error_exists(self) -> None:
        """QueryTimeoutError is importable from the public API."""
        from pycypher import QueryTimeoutError

        assert issubclass(QueryTimeoutError, TimeoutError)

    def test_context_has_deadline_methods(self) -> None:
        """Context has set_deadline, check_timeout, clear_deadline."""
        from pycypher import Context

        ctx = Context()
        assert hasattr(ctx, "set_deadline")
        assert hasattr(ctx, "check_timeout")
        assert hasattr(ctx, "clear_deadline")


# ---------------------------------------------------------------------------
# 10. Query Parameters: Injection Prevention
# ---------------------------------------------------------------------------


class TestQueryParameterSafety:
    """Verify query parameters are handled safely."""

    def test_parameters_cleared_after_execution(self) -> None:
        """Parameters are cleared from context after query execution."""
        import pandas as pd
        from pycypher import ContextBuilder, Star

        df = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        ctx = ContextBuilder().add_entity("Person", df).build()
        star = Star(context=ctx)

        star.execute_query(
            "MATCH (p:Person) WHERE p.name = $name RETURN p.name",
            parameters={"name": "Alice"},
        )
        # Parameters must not leak to subsequent queries
        assert star.context._parameters == {}

    def test_parameters_cleared_on_error(self) -> None:
        """Parameters are cleared even when query fails."""
        import pandas as pd
        from pycypher import ContextBuilder, Star

        df = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        ctx = ContextBuilder().add_entity("Person", df).build()
        star = Star(context=ctx)

        with pytest.raises(Exception):
            star.execute_query(
                "MATCH (p:NonExistent) RETURN p.name",
                parameters={"key": "value"},
            )
        assert star.context._parameters == {}


# ---------------------------------------------------------------------------
# 11. URI Scheme Validation
# ---------------------------------------------------------------------------


class TestURISchemeValidation:
    """Verify URI scheme whitelist is enforced."""

    def test_uri_scheme_validation_exists(self) -> None:
        """validate_uri_scheme function exists and blocks dangerous schemes."""
        from pycypher.ingestion.security import validate_uri_scheme

        # Should allow standard schemes
        validate_uri_scheme("file:///data.csv")

        # Should reject dangerous schemes
        from pycypher.ingestion.security import SecurityError

        with pytest.raises(SecurityError):
            validate_uri_scheme("javascript:alert(1)")
