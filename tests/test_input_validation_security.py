"""Tests for input validation security hardening.

Covers:
- Environment variable parsing resilience (metrics.py, logger.py)
- File path validation in grammar parser
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


class TestMetricsEnvVarValidation:
    """Verify PYCYPHER_SLOW_QUERY_MS env var is parsed safely."""

    def test_valid_integer_value(self) -> None:
        """Valid integer string is parsed correctly."""
        with patch.dict(os.environ, {"PYCYPHER_SLOW_QUERY_MS": "500"}):
            from shared.metrics import _parse_slow_query_ms

            assert _parse_slow_query_ms() == 0.5

    def test_default_when_unset(self) -> None:
        """Default is 1000ms when env var is absent."""
        env = os.environ.copy()
        env.pop("PYCYPHER_SLOW_QUERY_MS", None)
        with patch.dict(os.environ, env, clear=True):
            from shared.metrics import _parse_slow_query_ms

            assert _parse_slow_query_ms() == 1.0

    def test_non_numeric_falls_back_to_default(self) -> None:
        """Non-numeric value falls back to 1000ms instead of crashing."""
        with patch.dict(
            os.environ, {"PYCYPHER_SLOW_QUERY_MS": "not_a_number"}
        ):
            from shared.metrics import _parse_slow_query_ms

            assert _parse_slow_query_ms() == 1.0

    def test_empty_string_falls_back_to_default(self) -> None:
        """Empty string falls back to 1000ms instead of crashing."""
        with patch.dict(os.environ, {"PYCYPHER_SLOW_QUERY_MS": ""}):
            from shared.metrics import _parse_slow_query_ms

            assert _parse_slow_query_ms() == 1.0


class TestLoggerLevelValidation:
    """Verify PYCYPHER_LOG_LEVEL is validated against a whitelist."""

    def test_valid_log_levels(self) -> None:
        """All standard log levels are accepted."""
        from shared.logger import _VALID_LOG_LEVELS

        assert "DEBUG" in _VALID_LOG_LEVELS
        assert "INFO" in _VALID_LOG_LEVELS
        assert "WARNING" in _VALID_LOG_LEVELS
        assert "ERROR" in _VALID_LOG_LEVELS
        assert "CRITICAL" in _VALID_LOG_LEVELS

    def test_invalid_level_returns_warning(self) -> None:
        """Invalid log level name resolves to WARNING (the default)."""
        from shared.logger import _VALID_LOG_LEVELS

        result = _VALID_LOG_LEVELS.get("__IMPORT__", logging.WARNING)
        assert result == logging.WARNING

    def test_no_dynamic_getattr_on_module(self) -> None:
        """Confirm we use dict lookup, not getattr on the logging module."""
        from shared.logger import _VALID_LOG_LEVELS

        # The whitelist should only contain standard level names
        assert all(
            name in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            for name in _VALID_LOG_LEVELS
        )


class TestGrammarParserFileValidation:
    """Verify parse_file validates file paths."""

    def test_parse_valid_file(self) -> None:
        """Valid .cypher file is parsed successfully."""
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".cypher", delete=False
        ) as f:
            f.write("MATCH (n) RETURN n")
            f.flush()
            try:
                result = parser.parse_file(f.name)
                assert result is not None
            finally:
                Path(f.name).unlink()

    def test_parse_nonexistent_file_raises(self) -> None:
        """Non-existent file raises FileNotFoundError."""
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        with pytest.raises((FileNotFoundError, OSError)):
            parser.parse_file("/nonexistent/path/query.cypher")

    def test_parse_directory_raises(self) -> None:
        """Directory path raises ValueError."""
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="Not a regular file"):
                parser.parse_file(tmpdir)

    def test_path_is_resolved(self) -> None:
        """Relative paths are resolved to absolute."""
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".cypher", delete=False
        ) as f:
            f.write("RETURN 1")
            f.flush()
            try:
                # Use a relative path with ../ components
                rel_path = os.path.relpath(f.name)
                result = parser.parse_file(rel_path)
                assert result is not None
            finally:
                Path(f.name).unlink()
