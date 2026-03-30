"""Tests for the opt-in query audit logging system."""

from __future__ import annotations

import json
import logging

import pytest

# ---------------------------------------------------------------------------
# Module import & basic API
# ---------------------------------------------------------------------------


def test_audit_logger_exists():
    """The dedicated audit logger is available."""
    from pycypher.audit import AUDIT_LOGGER

    assert AUDIT_LOGGER.name == "pycypher.audit"


def test_audit_disabled_by_default():
    """Audit logging is off when PYCYPHER_AUDIT_LOG is unset."""
    from pycypher.audit import AUDIT_LOGGER

    # Remove any handlers that might have been added by other tests.
    original_handlers = AUDIT_LOGGER.handlers[:]
    AUDIT_LOGGER.handlers.clear()
    try:
        from pycypher.audit import is_audit_enabled

        assert not is_audit_enabled()
    finally:
        AUDIT_LOGGER.handlers = original_handlers


def test_enable_audit_log_adds_handler():
    """enable_audit_log() adds a StreamHandler and sets level."""
    from pycypher.audit import AUDIT_LOGGER, enable_audit_log

    original_handlers = AUDIT_LOGGER.handlers[:]
    original_level = AUDIT_LOGGER.level
    AUDIT_LOGGER.handlers.clear()
    try:
        enable_audit_log()
        assert len(AUDIT_LOGGER.handlers) == 1
        assert isinstance(AUDIT_LOGGER.handlers[0], logging.StreamHandler)
        assert AUDIT_LOGGER.level == logging.INFO
    finally:
        AUDIT_LOGGER.handlers = original_handlers
        AUDIT_LOGGER.level = original_level


def test_enable_audit_log_idempotent():
    """Calling enable_audit_log() twice doesn't add duplicate handlers."""
    from pycypher.audit import AUDIT_LOGGER, enable_audit_log

    original_handlers = AUDIT_LOGGER.handlers[:]
    original_level = AUDIT_LOGGER.level
    AUDIT_LOGGER.handlers.clear()
    try:
        enable_audit_log()
        enable_audit_log()
        assert len(AUDIT_LOGGER.handlers) == 1
    finally:
        AUDIT_LOGGER.handlers = original_handlers
        AUDIT_LOGGER.level = original_level


# ---------------------------------------------------------------------------
# Record content
# ---------------------------------------------------------------------------


class _CaptureHandler(logging.Handler):
    """Captures log records for assertion."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(self.format(record))


@pytest.fixture
def audit_capture():
    """Enable audit logging with a capture handler, yield records list."""
    from pycypher.audit import AUDIT_LOGGER

    original_handlers = AUDIT_LOGGER.handlers[:]
    original_level = AUDIT_LOGGER.level
    original_propagate = AUDIT_LOGGER.propagate

    handler = _CaptureHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    AUDIT_LOGGER.handlers = [handler]
    AUDIT_LOGGER.setLevel(logging.INFO)
    AUDIT_LOGGER.propagate = False

    yield handler.records

    AUDIT_LOGGER.handlers = original_handlers
    AUDIT_LOGGER.level = original_level
    AUDIT_LOGGER.propagate = original_propagate


def test_audit_query_success_record(audit_capture):
    """Successful query emits a well-formed JSON record."""
    from pycypher.audit import audit_query_success

    audit_query_success(
        query_id="abc123",
        query="MATCH (n) RETURN n",
        elapsed_s=0.042,
        rows=10,
        parameter_keys=["name"],
    )
    assert len(audit_capture) == 1
    record = json.loads(audit_capture[0])
    assert record["event"] == "query"
    assert record["query_id"] == "abc123"
    assert record["status"] == "ok"
    assert record["query"] == "MATCH (n) RETURN n"
    assert record["elapsed_ms"] == 42.0
    assert record["rows"] == 10
    assert record["cached"] is False
    assert record["parameter_keys"] == ["name"]
    assert "timestamp" in record


def test_audit_query_success_cached(audit_capture):
    """Cached query result is flagged in the audit record."""
    from pycypher.audit import audit_query_success

    audit_query_success(
        query_id="def456",
        query="MATCH (n) RETURN n",
        elapsed_s=0.001,
        rows=5,
        cached=True,
    )
    record = json.loads(audit_capture[0])
    assert record["cached"] is True


def test_audit_query_error_record(audit_capture):
    """Failed query emits a well-formed JSON error record."""
    from pycypher.audit import audit_query_error

    audit_query_error(
        query_id="err789",
        query="MATCH (n) RETRUN n",
        elapsed_s=0.005,
        error_type="CypherSyntaxError",
        parameter_keys=[],
    )
    assert len(audit_capture) == 1
    record = json.loads(audit_capture[0])
    assert record["event"] == "query"
    assert record["query_id"] == "err789"
    assert record["status"] == "error"
    assert record["error_type"] == "CypherSyntaxError"
    assert "timestamp" in record


def test_audit_query_truncates_long_queries(audit_capture):
    """Query text longer than the max is truncated."""
    from pycypher.audit import _MAX_QUERY_LENGTH, audit_query_success

    long_query = "MATCH (n) RETURN n " * 500
    assert len(long_query) > _MAX_QUERY_LENGTH

    audit_query_success(
        query_id="long1",
        query=long_query,
        elapsed_s=0.1,
        rows=0,
    )
    record = json.loads(audit_capture[0])
    assert len(record["query"]) == _MAX_QUERY_LENGTH + 3  # +3 for "..."
    assert record["query"].endswith("...")


def test_audit_no_parameter_values_logged(audit_capture):
    """Parameter values are never present in audit records."""
    from pycypher.audit import audit_query_success

    audit_query_success(
        query_id="sec1",
        query="MATCH (n) WHERE n.secret = $password RETURN n",
        elapsed_s=0.01,
        rows=1,
        parameter_keys=["password"],
    )
    raw = audit_capture[0]
    # The word "password" appears as a key name, but no value should be present.
    record = json.loads(raw)
    assert record["parameter_keys"] == ["password"]
    # Ensure no actual values leak — the record should only have known fields.
    allowed_keys = {
        "event",
        "query_id",
        "timestamp",
        "status",
        "query",
        "elapsed_ms",
        "rows",
        "cached",
        "parameter_keys",
    }
    assert set(record.keys()) == allowed_keys


# ---------------------------------------------------------------------------
# Integration: audit records emitted during Star.execute_query()
# ---------------------------------------------------------------------------


def test_audit_integration_with_star(audit_capture):
    """Star.execute_query() emits audit records when audit is enabled."""
    import pandas as pd
    from pycypher.constants import ID_COLUMN
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
    )
    from pycypher.star import Star

    df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["a", "b"]})
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    star = Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )

    result = star.execute_query("MATCH (n:Person) RETURN n.name")
    assert len(result) == 2
    assert len(audit_capture) == 1

    record = json.loads(audit_capture[0])
    assert record["status"] == "ok"
    assert record["rows"] == 2
    assert "MATCH" in record["query"]


def test_audit_integration_error(audit_capture):
    """Star.execute_query() emits error audit records on failure."""
    from pycypher.relational_models import Context, EntityMapping
    from pycypher.star import Star

    star = Star(
        context=Context(entity_mapping=EntityMapping(mapping={})),
    )
    with pytest.raises(Exception):
        star.execute_query("MATCH (n:NonExistent) RETURN n")

    assert len(audit_capture) == 1
    record = json.loads(audit_capture[0])
    assert record["status"] == "error"
    assert record["error_type"] != ""
