"""Tests for query-ID correlation logging and error-path logging.

Verifies that:
1. Every log record emitted during execute_query carries a ``query_id`` extra.
2. All log records for a single query share the same query_id.
3. Different queries get different query_ids.
4. Failed queries emit an ERROR log with query_id and exc_info.
5. The JSON formatter includes query_id in its output.
"""

from __future__ import annotations

import json
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

_LOGGER_NAME = "shared.logger"


@pytest.fixture
def simple_star() -> Star:
    """Three-person context: Alice (30), Bob (25), Carol (35)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
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
        ),
    )


class TestQueryIdPresence:
    """Every log record from execute_query must carry a query_id extra."""

    def test_debug_records_have_query_id(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        # Filter to records from execute_query / clause execution (not module-load
        # registration logs or grammar transformer delegation logs which are
        # emitted outside the query_id context).
        query_records = [
            r
            for r in caplog.records
            if ("execute_query" in r.message or "clause " in r.message)
            and "CompositeTransformer" not in r.message
        ]
        assert query_records, "Expected query-related log records"
        for record in query_records:
            assert hasattr(record, "query_id"), (
                f"Log record missing query_id: {record.message!r}"
            )
            assert record.query_id is not None  # type: ignore[attr-defined]

    def test_info_records_have_query_id(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        assert info_records, "Expected at least one INFO record"
        for record in info_records:
            assert hasattr(record, "query_id")


class TestQueryIdConsistency:
    """All records from one query share the same query_id."""

    def test_all_records_share_same_query_id(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        query_ids = {
            getattr(r, "query_id", None)
            for r in caplog.records
            if "execute_query" in r.message or "clause" in r.message
        }
        query_ids.discard(None)
        assert len(query_ids) == 1, (
            f"Expected exactly one query_id across all records, got {query_ids}"
        )

    def test_different_queries_get_different_ids(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        qid_1 = caplog.records[0].query_id  # type: ignore[attr-defined]
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            simple_star.execute_query("MATCH (p:Person) RETURN p.age AS age")
        qid_2 = caplog.records[0].query_id  # type: ignore[attr-defined]
        assert qid_1 != qid_2, (
            "Two separate queries must have distinct query_ids"
        )


class TestErrorPathLogging:
    """Failed queries must emit an ERROR log with query_id."""

    def test_error_log_emitted_on_failure(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            with pytest.raises(Exception):
                simple_star.execute_query(
                    "MATCH (x:NonExistentLabel) RETURN x.foo",
                )
        error_records = [
            r for r in caplog.records if r.levelno == logging.ERROR
        ]
        assert error_records, "Expected an ERROR record on query failure"

    def test_error_log_has_query_id(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            with pytest.raises(Exception):
                simple_star.execute_query(
                    "MATCH (x:NonExistentLabel) RETURN x.foo",
                )
        error_records = [
            r for r in caplog.records if r.levelno == logging.ERROR
        ]
        assert error_records
        for record in error_records:
            assert hasattr(record, "query_id")
            assert record.query_id is not None  # type: ignore[attr-defined]

    def test_error_log_includes_exc_info(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            with pytest.raises(Exception):
                simple_star.execute_query(
                    "MATCH (x:NonExistentLabel) RETURN x.foo",
                )
        error_records = [
            r for r in caplog.records if r.levelno == logging.ERROR
        ]
        assert error_records
        assert error_records[0].exc_info is not None, (
            "ERROR record should include exc_info for traceback"
        )

    def test_error_log_mentions_query_text(
        self,
        simple_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            with pytest.raises(Exception):
                simple_star.execute_query(
                    "MATCH (x:NonExistentLabel) RETURN x.foo",
                )
        error_records = [
            r for r in caplog.records if r.levelno == logging.ERROR
        ]
        assert error_records
        assert "NonExistentLabel" in error_records[0].message


class TestJsonFormatterQueryId:
    """The JSON log formatter must include query_id when present."""

    def test_json_formatter_includes_query_id(self) -> None:
        from shared.logger import _JSONFormatter

        formatter = _JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        record.query_id = "abc123"  # type: ignore[attr-defined]
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["query_id"] == "abc123"

    def test_json_formatter_omits_query_id_when_absent(self) -> None:
        from shared.logger import _JSONFormatter, reset_query_id, set_query_id

        # Ensure the contextvar is clean (a prior test may have leaked it).
        token = set_query_id(None)  # type: ignore[arg-type]
        try:
            formatter = _JSONFormatter()
            record = logging.LogRecord(
                name="test",
                level=logging.DEBUG,
                pathname="test.py",
                lineno=1,
                msg="test message",
                args=(),
                exc_info=None,
            )
            output = formatter.format(record)
            parsed = json.loads(output)
            assert "query_id" not in parsed
        finally:
            reset_query_id(token)
