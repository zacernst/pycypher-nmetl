"""Tests for OpenTelemetry tracing integration.

Verifies that the OTel adapter works correctly in both enabled (mocked)
and disabled (no-op) modes without requiring opentelemetry-api installed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from shared.otel import (
    _extract_operation,
    _NullSpan,
    _NullTracer,
    get_tracer,
    record_metrics_to_span,
    trace_phase,
    trace_query,
)


class TestNullObjects:
    """Verify no-op tracer and span silently discard all calls."""

    def test_null_span_set_attribute(self) -> None:
        span = _NullSpan()
        span.set_attribute("key", "value")

    def test_null_span_set_status(self) -> None:
        span = _NullSpan()
        span.set_status("ERROR", "something broke")

    def test_null_span_add_event(self) -> None:
        span = _NullSpan()
        span.add_event("test-event", {"key": "val"})

    def test_null_span_record_exception(self) -> None:
        span = _NullSpan()
        span.record_exception(ValueError("test"))

    def test_null_span_context_manager(self) -> None:
        with _NullSpan() as span:
            assert isinstance(span, _NullSpan)

    def test_null_tracer_returns_null_span(self) -> None:
        tracer = _NullTracer()
        span = tracer.start_as_current_span("test")
        assert isinstance(span, _NullSpan)

    def test_null_tracer_span_is_context_manager(self) -> None:
        tracer = _NullTracer()
        with tracer.start_as_current_span("test") as span:
            assert isinstance(span, _NullSpan)


class TestGetTracer:
    """Verify get_tracer returns appropriate tracer based on state."""

    def test_returns_null_tracer_when_disabled(self) -> None:
        tracer = get_tracer()
        assert isinstance(tracer, _NullTracer)


class TestTraceQuery:
    """Verify trace_query context manager in disabled mode."""

    def test_yields_null_span_when_disabled(self) -> None:
        with trace_query("MATCH (n) RETURN n") as span:
            assert isinstance(span, _NullSpan)

    def test_with_query_id(self) -> None:
        with trace_query(
            "MATCH (n) RETURN n",
            query_id="test-123",
        ) as span:
            assert isinstance(span, _NullSpan)

    def test_with_parameters(self) -> None:
        with trace_query(
            "MATCH (n) WHERE n.age > $min RETURN n",
            parameters={"min": 25},
        ) as span:
            assert isinstance(span, _NullSpan)

    def test_exception_propagates(self) -> None:
        """Exceptions inside trace_query must propagate normally."""
        import pytest

        with pytest.raises(ValueError, match="test error"):
            with trace_query("MATCH (n) RETURN n"):
                raise ValueError("test error")


class TestTracePhase:
    """Verify trace_phase context manager in disabled mode."""

    def test_yields_null_span_when_disabled(self) -> None:
        with trace_phase("parse") as span:
            assert isinstance(span, _NullSpan)

    def test_exception_propagates(self) -> None:
        import pytest

        with pytest.raises(RuntimeError, match="phase error"):
            with trace_phase("execute"):
                raise RuntimeError("phase error")


class TestRecordMetricsToSpan:
    """Verify record_metrics_to_span attaches data to span."""

    def test_sets_attributes_on_span(self) -> None:
        span = MagicMock()
        snapshot = MagicMock()
        snapshot.total_queries = 100
        snapshot.total_errors = 5
        snapshot.error_rate = 0.05
        snapshot.timing_p50_ms = 12.3
        snapshot.timing_p99_ms = 45.6
        snapshot.health_status.return_value = "healthy"

        record_metrics_to_span(span, snapshot)

        span.set_attribute.assert_any_call(
            "pycypher.total_queries",
            100,
        )
        span.set_attribute.assert_any_call(
            "pycypher.total_errors",
            5,
        )
        span.set_attribute.assert_any_call(
            "pycypher.error_rate",
            0.05,
        )
        span.set_attribute.assert_any_call(
            "pycypher.timing_p50_ms",
            12.3,
        )
        span.set_attribute.assert_any_call(
            "pycypher.health_status",
            "healthy",
        )

    def test_works_with_null_span(self) -> None:
        """NullSpan should silently accept all attribute calls."""
        span = _NullSpan()
        snapshot = MagicMock()
        snapshot.total_queries = 0
        snapshot.total_errors = 0
        snapshot.error_rate = 0.0
        snapshot.timing_p50_ms = 0.0
        snapshot.timing_p99_ms = 0.0
        snapshot.health_status.return_value = "healthy"
        record_metrics_to_span(span, snapshot)


class TestExtractOperation:
    """Verify Cypher operation extraction from query strings."""

    def test_match_query(self) -> None:
        assert _extract_operation("MATCH (n) RETURN n") == "MATCH"

    def test_create_query(self) -> None:
        assert _extract_operation("CREATE (n:Person {name: 'Alice'})") == "CREATE"

    def test_merge_query(self) -> None:
        assert _extract_operation("MERGE (n:Person {id: 1})") == "MERGE"

    def test_with_query(self) -> None:
        assert _extract_operation("WITH 1 AS x RETURN x") == "WITH"

    def test_unwind_query(self) -> None:
        assert _extract_operation("UNWIND [1,2,3] AS x RETURN x") == "UNWIND"

    def test_return_only(self) -> None:
        assert _extract_operation("RETURN 42") == "RETURN"

    def test_empty_query(self) -> None:
        assert _extract_operation("") is None

    def test_unknown_keyword(self) -> None:
        assert _extract_operation("FOOBAR baz") is None

    def test_case_insensitive(self) -> None:
        assert _extract_operation("match (n) return n") == "MATCH"


class TestOtelEnabled:
    """Test behavior when OTEL_ENABLED would be True (mocked)."""

    def test_trace_query_with_mocked_tracer(self) -> None:
        """Simulate OTel being enabled by patching module globals."""
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value = mock_span

        mock_status_code = MagicMock()
        mock_status_code.ERROR = "ERROR"

        with (
            patch("shared.otel.OTEL_ENABLED", True),
            patch("shared.otel._tracer", mock_tracer),
            patch("shared.otel._StatusCode", mock_status_code),
        ):
            with trace_query(
                "MATCH (p:Person) RETURN p.name",
                query_id="q-42",
            ) as span:
                span.set_attribute("result.rows", 100)

            mock_tracer.start_as_current_span.assert_called_once_with(
                "pycypher.MATCH",
            )
            mock_span.set_attribute.assert_any_call(
                "db.system",
                "pycypher",
            )
            mock_span.set_attribute.assert_any_call(
                "pycypher.query_id",
                "q-42",
            )

    def test_trace_query_records_exception_when_enabled(self) -> None:
        """When OTel is enabled, exceptions should be recorded on the span."""
        import pytest

        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value = mock_span

        mock_status_code = MagicMock()
        mock_status_code.ERROR = "ERROR"

        with (
            patch("shared.otel.OTEL_ENABLED", True),
            patch("shared.otel._tracer", mock_tracer),
            patch("shared.otel._StatusCode", mock_status_code),
        ):
            with pytest.raises(ValueError, match="test"):
                with trace_query("MATCH (n) RETURN n"):
                    raise ValueError("test")

            mock_span.set_status.assert_called_once()
            mock_span.record_exception.assert_called_once()

    def test_trace_phase_with_mocked_tracer(self) -> None:
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value = mock_span

        mock_status_code = MagicMock()

        with (
            patch("shared.otel.OTEL_ENABLED", True),
            patch("shared.otel._tracer", mock_tracer),
            patch("shared.otel._StatusCode", mock_status_code),
        ):
            with trace_phase("parse", query_id="q-99") as span:
                span.set_attribute("tokens", 42)

            mock_tracer.start_as_current_span.assert_called_once_with(
                "pycypher.parse",
            )
            mock_span.set_attribute.assert_any_call(
                "pycypher.phase",
                "parse",
            )
