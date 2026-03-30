"""TDD tests for structured parse failure logging in grammar_parser.py.

Verifies that parse failures emit structured log events with query context,
error position, and expected tokens for operator-side observability.
"""

from __future__ import annotations

import logging

import pytest
from pycypher.exceptions import CypherSyntaxError
from pycypher.grammar_parser import GrammarParser


@pytest.fixture
def parser() -> GrammarParser:
    """Return a fresh GrammarParser instance."""
    return GrammarParser()


class TestParseFailureLogging:
    """Parse failures must emit structured WARNING-level log events."""

    def test_parse_failure_emits_warning_log(
        self,
        parser: GrammarParser,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="shared.logger"):
            with pytest.raises(CypherSyntaxError):
                parser.parse("MATCH (n) RETRN n")  # typo in RETURN
        # Must have at least one WARNING record about parse failure
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
        msg = warning_records[0].message.lower()
        assert "parse" in msg or "syntax" in msg

    def test_parse_failure_log_includes_query_snippet(
        self,
        parser: GrammarParser,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        query = "MATCH (n:Person) RETRN n.name"
        with caplog.at_level(logging.WARNING, logger="shared.logger"):
            with pytest.raises(CypherSyntaxError):
                parser.parse(query)
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
        # Query text (or truncated version) should appear in log
        msg = warning_records[0].message
        assert "MATCH" in msg

    def test_parse_failure_log_includes_position(
        self,
        parser: GrammarParser,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="shared.logger"):
            with pytest.raises(CypherSyntaxError):
                parser.parse("MATCH (n) RETRN n")
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
        msg = warning_records[0].message
        # Should include line/column info
        assert "line" in msg.lower() or "col" in msg.lower()

    def test_long_query_truncated_in_log(
        self,
        parser: GrammarParser,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Build a query longer than 200 chars with a syntax error
        long_query = (
            "MATCH (n:Person) WHERE "
            + " AND ".join(f"n.prop{i} = {i}" for i in range(50))
            + " RETRN n"
        )
        with caplog.at_level(logging.WARNING, logger="shared.logger"):
            with pytest.raises(CypherSyntaxError):
                parser.parse(long_query)
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
        msg = warning_records[0].message
        # Truncated queries should have ellipsis or be capped
        assert len(msg) < len(long_query) + 200  # reasonable cap

    def test_successful_parse_no_warning(
        self,
        parser: GrammarParser,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Successful parses must NOT emit warning logs."""
        with caplog.at_level(logging.WARNING, logger="shared.logger"):
            parser.parse("MATCH (n) RETURN n")
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) == 0

    def test_validate_failure_emits_warning(
        self,
        parser: GrammarParser,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """validate() returning False should also log the parse failure."""
        with caplog.at_level(logging.WARNING, logger="shared.logger"):
            result = parser.validate("INVALID CYPHER !!!")
        assert result is False
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
