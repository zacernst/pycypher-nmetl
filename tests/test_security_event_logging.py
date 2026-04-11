"""Tests for the security event logging system."""

from __future__ import annotations

import json
import logging

import pytest
from pycypher.ingestion.security import (
    SECURITY_LOGGER,
    SecurityError,
    enable_security_log,
    is_security_log_enabled,
    sanitize_file_path,
    sanitize_sql_identifier,
    security_event_log,
    validate_sql_query,
    validate_uri_scheme,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def _capture_security_logs(caplog):
    """Enable the security logger and capture its output."""
    original_handlers = SECURITY_LOGGER.handlers[:]
    original_level = SECURITY_LOGGER.level
    original_propagate = SECURITY_LOGGER.propagate
    SECURITY_LOGGER.handlers.clear()
    # Use caplog propagation to capture records.
    SECURITY_LOGGER.addHandler(logging.StreamHandler())
    SECURITY_LOGGER.setLevel(logging.WARNING)
    SECURITY_LOGGER.propagate = False
    yield
    SECURITY_LOGGER.handlers = original_handlers
    SECURITY_LOGGER.level = original_level
    SECURITY_LOGGER.propagate = original_propagate


@pytest.fixture()
def security_records():
    """Capture structured JSON records from the security logger."""
    records: list[dict] = []
    original_handlers = SECURITY_LOGGER.handlers[:]
    original_level = SECURITY_LOGGER.level
    original_propagate = SECURITY_LOGGER.propagate

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                records.append(json.loads(record.getMessage()))
            except json.JSONDecodeError:
                pass

    handler = _Capture()
    SECURITY_LOGGER.handlers = [handler]
    SECURITY_LOGGER.setLevel(logging.WARNING)
    SECURITY_LOGGER.propagate = False
    yield records
    SECURITY_LOGGER.handlers = original_handlers
    SECURITY_LOGGER.level = original_level
    SECURITY_LOGGER.propagate = original_propagate


# ---------------------------------------------------------------------------
# Logger basics
# ---------------------------------------------------------------------------


def test_security_logger_exists():
    """The dedicated security logger is available."""
    assert SECURITY_LOGGER.name == "pycypher.security"


def test_security_log_disabled_by_default():
    """Security event logging is off when no handlers are configured.

    Isolates the logger by removing handlers, disabling propagation,
    and setting the level above WARNING so ``isEnabledFor`` returns False.
    """
    original_handlers = SECURITY_LOGGER.handlers[:]
    original_propagate = SECURITY_LOGGER.propagate
    original_level = SECURITY_LOGGER.level
    SECURITY_LOGGER.handlers.clear()
    SECURITY_LOGGER.propagate = False
    SECURITY_LOGGER.setLevel(logging.CRITICAL + 1)
    try:
        assert not is_security_log_enabled()
    finally:
        SECURITY_LOGGER.handlers = original_handlers
        SECURITY_LOGGER.propagate = original_propagate
        SECURITY_LOGGER.level = original_level


def test_enable_security_log_adds_handler():
    """enable_security_log() adds a StreamHandler."""
    original_handlers = SECURITY_LOGGER.handlers[:]
    original_level = SECURITY_LOGGER.level
    SECURITY_LOGGER.handlers.clear()
    try:
        enable_security_log()
        assert len(SECURITY_LOGGER.handlers) == 1
        assert isinstance(SECURITY_LOGGER.handlers[0], logging.StreamHandler)
        assert SECURITY_LOGGER.level == logging.WARNING
    finally:
        SECURITY_LOGGER.handlers = original_handlers
        SECURITY_LOGGER.level = original_level


def test_enable_security_log_idempotent():
    """Calling enable_security_log() twice does not duplicate handlers."""
    original_handlers = SECURITY_LOGGER.handlers[:]
    original_level = SECURITY_LOGGER.level
    SECURITY_LOGGER.handlers.clear()
    try:
        enable_security_log()
        enable_security_log()
        assert len(SECURITY_LOGGER.handlers) == 1
    finally:
        SECURITY_LOGGER.handlers = original_handlers
        SECURITY_LOGGER.level = original_level


# ---------------------------------------------------------------------------
# security_event_log() direct calls
# ---------------------------------------------------------------------------


def test_security_event_log_emits_json(security_records):
    """security_event_log() emits a structured JSON record."""
    security_event_log(
        event_type="test_event",
        input_sample="bad input",
        source_function="test_func",
        detail="testing",
    )
    assert len(security_records) == 1
    rec = security_records[0]
    assert rec["event"] == "security"
    assert rec["event_type"] == "test_event"
    assert rec["input_sample"] == "bad input"
    assert rec["source_function"] == "test_func"
    assert rec["detail"] == "testing"
    assert "timestamp" in rec


def test_security_event_log_truncates_long_input(security_records):
    """Long input samples are truncated."""
    long_input = "x" * 500
    security_event_log(event_type="test", input_sample=long_input)
    assert len(security_records) == 1
    sample = security_records[0]["input_sample"]
    assert len(sample) <= 256 + 3  # max + "..."
    assert sample.endswith("...")


def test_security_event_log_noop_when_disabled():
    """security_event_log() is a no-op when the logger has no handlers."""
    original_handlers = SECURITY_LOGGER.handlers[:]
    SECURITY_LOGGER.handlers.clear()
    try:
        # Should not raise or produce output.
        security_event_log(event_type="should_not_appear")
    finally:
        SECURITY_LOGGER.handlers = original_handlers


# ---------------------------------------------------------------------------
# NUL byte validation in sanitize_sql_identifier
# ---------------------------------------------------------------------------


def test_sanitize_sql_identifier_rejects_nul_byte():
    """NUL bytes in SQL identifiers are rejected."""
    with pytest.raises(SecurityError, match="NUL byte"):
        sanitize_sql_identifier("valid\x00evil")


def test_sanitize_sql_identifier_nul_byte_logs_event(security_records):
    """NUL byte rejection emits a security event."""
    with pytest.raises(SecurityError):
        sanitize_sql_identifier("col\x00drop")
    assert len(security_records) == 1
    assert security_records[0]["event_type"] == "sql_injection"
    assert security_records[0]["source_function"] == "sanitize_sql_identifier"


# ---------------------------------------------------------------------------
# Integration: security events from validation functions
# ---------------------------------------------------------------------------


def test_path_traversal_logs_security_event(security_records):
    """Path traversal rejection emits a security event."""
    with pytest.raises(SecurityError):
        sanitize_file_path("../../etc/passwd")
    assert len(security_records) >= 1
    assert any(r["event_type"] == "path_traversal" for r in security_records)


def test_sql_injection_stacking_logs_event(security_records):
    """Multiple-statement SQL injection logs a security event."""
    with pytest.raises(SecurityError):
        validate_sql_query("SELECT 1; DROP TABLE users;")
    assert len(security_records) >= 1
    assert any(r["event_type"] == "sql_injection" for r in security_records)


def test_sql_injection_disallowed_prefix_logs_event(security_records):
    """Disallowed SQL statement prefix logs a security event."""
    with pytest.raises(SecurityError):
        validate_sql_query("DROP TABLE users")
    assert len(security_records) >= 1
    assert any(r["event_type"] == "sql_injection" for r in security_records)


def test_dangerous_table_function_logs_event(security_records):
    """Dangerous DuckDB table function detected logs a security event."""
    with pytest.raises(SecurityError):
        validate_sql_query("SELECT * FROM read_csv('/etc/passwd')")
    assert len(security_records) >= 1
    assert any(r["event_type"] == "sql_injection" for r in security_records)


def test_ssrf_localhost_logs_event(security_records):
    """SSRF protection for localhost logs a security event."""
    with pytest.raises(SecurityError):
        validate_uri_scheme("http://localhost/secret")
    assert len(security_records) >= 1
    assert any(r["event_type"] == "ssrf" for r in security_records)


def test_nul_byte_in_string_literal_logs_event(security_records):
    """NUL byte in SQL string literal emits a security event."""
    from pycypher.ingestion.security import escape_sql_string_literal

    with pytest.raises(SecurityError):
        escape_sql_string_literal("value\x00evil")
    assert len(security_records) >= 1
    assert any(r["event_type"] == "sql_injection" for r in security_records)
