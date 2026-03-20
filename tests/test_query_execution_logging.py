"""TDD tests for query-execution observability logging.

These tests assert on log output emitted by Star.execute_query() and
Star._execute_query_binding_frame_inner().  All tests are written before the
implementation (TDD red phase).

Run with:
    uv run pytest tests/test_query_execution_logging.py -v
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_star() -> Star:
    """Three-person context: Alice (30), Bob (25), Carol (35)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ---------------------------------------------------------------------------
# Logging output helpers
# ---------------------------------------------------------------------------

_LOGGER_NAME = "shared.logger"  # the shared logger used throughout pycypher


# ---------------------------------------------------------------------------
# execute_query start log
# ---------------------------------------------------------------------------


class TestExecuteQueryStartLog:
    def test_debug_log_emitted_at_start(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """execute_query emits a DEBUG record containing part of the query string."""
        query = "MATCH (p:Person) RETURN p.name AS name"
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query(query)
        messages = [
            r.message for r in caplog.records if r.levelno == logging.DEBUG
        ]
        # At least one debug message should mention "MATCH" or the query prefix
        assert any("MATCH" in m or "execute_query" in m for m in messages), (
            f"Expected a DEBUG record mentioning the query at start. Got: {messages}"
        )

    def test_start_log_includes_query_prefix(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The start log record includes at least the first token of the query."""
        query = "MATCH (p:Person) WHERE p.age > 25 RETURN p.name"
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query(query)
        all_text = " ".join(r.message for r in caplog.records)
        assert "MATCH" in all_text, (
            "Expected query text to appear somewhere in DEBUG output"
        )

    def test_start_log_with_parameters_mentions_param_keys(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When parameters are passed, the start log mentions them."""
        query = "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name"
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query(query, parameters={"min_age": 28})
        all_text = " ".join(r.message for r in caplog.records)
        assert "min_age" in all_text, (
            "Expected parameter key 'min_age' to appear in DEBUG output"
        )


# ---------------------------------------------------------------------------
# execute_query completion log (INFO level)
# ---------------------------------------------------------------------------


class TestExecuteQueryCompletionLog:
    def test_info_log_emitted_on_completion(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """execute_query emits an INFO record after successful completion."""
        with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        info_messages = [
            r.message for r in caplog.records if r.levelno == logging.INFO
        ]
        assert info_messages, f"Expected at least one INFO record. Got none."

    def test_completion_log_includes_row_count(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The completion log includes the number of output rows."""
        with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
            result = simple_star.execute_query(
                "MATCH (p:Person) RETURN p.name AS name"
            )
        expected_rows = len(result)
        all_text = " ".join(
            r.message for r in caplog.records if r.levelno == logging.INFO
        )
        assert str(expected_rows) in all_text, (
            f"Expected row count {expected_rows} in INFO log. Got: {all_text!r}"
        )

    def test_completion_log_includes_elapsed_time(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The completion log includes an elapsed-time figure."""
        with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        all_text = " ".join(
            r.message for r in caplog.records if r.levelno == logging.INFO
        )
        # Elapsed time should appear as a number followed by 's' or 'ms'
        import re

        assert re.search(r"\d+\.\d+\s*s", all_text) or re.search(
            r"\d+ms", all_text
        ), f"Expected an elapsed-time figure in INFO log. Got: {all_text!r}"

    def test_completion_log_not_emitted_on_exception(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """No completion INFO log is emitted if the query raises."""
        with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
            try:
                simple_star.execute_query(
                    "MATCH (p:NonExistentType) RETURN p.name"
                )
            except Exception:
                pass
        # No INFO completion record should be present (query failed)
        completion_msgs = [
            r.message
            for r in caplog.records
            if r.levelno == logging.INFO
            and ("rows=" in r.message or "done" in r.message)
        ]
        assert not completion_msgs, (
            f"INFO completion log should not be emitted on failure. Got: {completion_msgs}"
        )


# ---------------------------------------------------------------------------
# Per-clause DEBUG logs in _execute_query_binding_frame_inner
# ---------------------------------------------------------------------------


class TestPerClauseLogging:
    def test_match_clause_logged(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A MATCH clause produces a DEBUG log entry naming the clause type."""
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        debug_messages = [
            r.message for r in caplog.records if r.levelno == logging.DEBUG
        ]
        assert any(
            "Match" in m or "MATCH" in m or "match" in m
            for m in debug_messages
        ), f"Expected a DEBUG record for MATCH clause. Got: {debug_messages}"

    def test_with_clause_logged(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A WITH clause produces a DEBUG log entry."""
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query(
                "MATCH (p:Person) WITH p.name AS n RETURN n"
            )
        debug_messages = [
            r.message for r in caplog.records if r.levelno == logging.DEBUG
        ]
        assert any(
            "With" in m or "WITH" in m or "with" in m for m in debug_messages
        ), f"Expected a DEBUG record for WITH clause. Got: {debug_messages}"

    def test_clause_log_includes_frame_size(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Per-clause DEBUG logs include frame row counts."""
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        all_debug = " ".join(
            r.message for r in caplog.records if r.levelno == logging.DEBUG
        )
        # Frame size should appear — 3 rows in the fixture
        assert "3" in all_debug, (
            f"Expected frame size (3) in clause DEBUG logs. Got: {all_debug!r}"
        )


# ---------------------------------------------------------------------------
# Empty-result debug log
# ---------------------------------------------------------------------------


class TestEmptyResultLogging:
    def test_empty_result_triggers_debug_log(
        self, simple_star: Star, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A query that returns 0 rows emits a DEBUG log mentioning the empty result."""
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            result = simple_star.execute_query(
                "MATCH (p:Person) WHERE p.age > 999 RETURN p.name"
            )
        assert len(result) == 0, "Fixture sanity: expected empty result"
        debug_messages = [
            r.message for r in caplog.records if r.levelno == logging.DEBUG
        ]
        assert any(
            "empty" in m.lower() or "0 row" in m or "rows=0" in m
            for m in debug_messages
        ), f"Expected DEBUG log mentioning empty result. Got: {debug_messages}"
