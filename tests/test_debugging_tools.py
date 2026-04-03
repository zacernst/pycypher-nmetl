"""Tests for advanced debugging and profiling tools (Task #25).

Covers:
  - CLI --explain flag: typed AST display without query execution
  - CLI --profile flag: profiled execution with timing breakdown
  - REPL EXPLAIN command: improved typed AST output
  - QueryProfiler integration via REPL PROFILE command
"""

from __future__ import annotations

import io
from unittest.mock import patch

import pytest


def _capture_echo() -> tuple[io.StringIO, object]:
    """Create a click.echo capture context."""
    buf = io.StringIO()

    def _echo(msg="", **kwargs):
        buf.write(str(msg) + "\n")

    return buf, _echo


# ===========================================================================
# CLI --explain flag
# ===========================================================================


class TestCliExplainFlag:
    """Tests for the --explain flag on the CLI query command."""

    def test_explain_shows_typed_ast(self) -> None:
        """--explain should show the typed AST tree with pretty() output."""
        from pycypher.cli.query import _explain_query

        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            _explain_query("MATCH (n:Person) RETURN n.name")

        output = buf.getvalue()
        # Should contain AST node types
        assert "Query" in output
        assert "Match" in output
        assert "Return" in output
        # Should contain timing info
        assert "Parse:" in output
        assert "Convert:" in output
        # Should contain validation result
        assert "Validation:" in output

    def test_explain_shows_labels(self) -> None:
        """--explain should display label information from patterns."""
        from pycypher.cli.query import _explain_query

        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            _explain_query("MATCH (p:Person)-[:KNOWS]->(f:Friend) RETURN p.name")

        output = buf.getvalue()
        assert "Person" in output

    def test_explain_syntax_error_handled(self) -> None:
        """--explain on invalid syntax should show an error, not crash."""
        from pycypher.cli.query import _explain_query

        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            with pytest.raises(Exception):
                _explain_query("MATCH (n:Person RETURN broken")

    def test_explain_validation_ok(self) -> None:
        """A valid query should show 'Validation: OK'."""
        from pycypher.cli.query import _explain_query

        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            _explain_query("MATCH (n:Person) RETURN n.name AS name")

        output = buf.getvalue()
        assert "Validation: OK" in output

    def test_explain_root_type(self) -> None:
        """Root node type should be displayed."""
        from pycypher.cli.query import _explain_query

        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            _explain_query("MATCH (n) RETURN n")

        output = buf.getvalue()
        assert "Root: Query" in output


# ===========================================================================
# REPL EXPLAIN command (improved)
# ===========================================================================


class TestReplExplain:
    """Tests for the improved REPL EXPLAIN command using typed AST."""

    def test_repl_explain_uses_pretty_output(self) -> None:
        """REPL EXPLAIN should use typed AST pretty() instead of raw repr."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl(entity_specs=[], rel_specs=[])
        # Bypass context requirement for EXPLAIN (parse-only)
        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            repl._explain_query("MATCH (n:Person) RETURN n.name")

        output = buf.getvalue()
        # Should contain structured AST output, not raw repr
        assert "Query" in output
        assert "Match" in output
        assert "Parse:" in output
        assert "Convert:" in output

    def test_repl_explain_shows_validation(self) -> None:
        """REPL EXPLAIN should show validation results."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl(entity_specs=[], rel_specs=[])
        buf, echo = _capture_echo()
        with patch("click.echo", side_effect=echo):
            repl._explain_query("MATCH (n) RETURN n")

        output = buf.getvalue()
        assert "Validation:" in output


# ===========================================================================
# QueryProfiler unit tests
# ===========================================================================


class TestQueryProfilerIntegration:
    """Tests for QueryProfiler report format and content."""

    def test_profile_report_str_format(self) -> None:
        """ProfileReport.__str__ should produce readable output."""
        from pycypher.query_profiler import ProfileReport

        report = ProfileReport(
            query="MATCH (n:Person) RETURN n.name",
            total_time_ms=42.5,
            parse_time_ms=5.2,
            plan_time_ms=3.1,
            clause_timings={"Match": 25.0, "Return": 8.0},
            row_count=100,
            hotspot="Match",
            recommendations=["Consider adding WHERE predicates."],
            memory_delta_mb=1.2,
        )

        text = str(report)
        assert "42.5ms" in text
        assert "parse=5.2ms" in text
        assert "HOTSPOT" in text
        assert "Match" in text
        assert "Recommendations:" in text
        assert "WHERE predicates" in text

    def test_profile_report_with_backend_timings(self) -> None:
        """ProfileReport should display backend timings when present."""
        from pycypher.query_profiler import ProfileReport

        report = ProfileReport(
            query="MATCH (n) RETURN n",
            total_time_ms=50.0,
            parse_time_ms=2.0,
            plan_time_ms=1.0,
            clause_timings={"Match": 30.0},
            row_count=10,
            hotspot="Match",
            recommendations=[],
            backend_timings={"scan": {"total_ms": 25.0, "count": 3}},
        )

        text = str(report)
        assert "Backend operations:" in text
        assert "scan:" in text
        assert "25.0ms" in text
        assert "3 calls" in text


# ===========================================================================
# Workload analysis
# ===========================================================================


class TestWorkloadAnalysis:
    """Tests for the analyze_workload function."""

    def test_empty_history(self) -> None:
        from pycypher.query_profiler import analyze_workload

        assert analyze_workload([]) == []

    def test_slow_parse_recommendation(self) -> None:
        from pycypher.query_profiler import ProfileReport, analyze_workload

        # Create 10 reports with slow parse times
        history = [
            ProfileReport(
                query=f"MATCH (n) RETURN n  -- {i}",
                total_time_ms=100.0,
                parse_time_ms=60.0,  # above _SLOW_PARSE_MS threshold
                plan_time_ms=5.0,
                clause_timings={"Match": 30.0},
                row_count=50,
                hotspot="Match",
                recommendations=[],
            )
            for i in range(10)
        ]

        recs = analyze_workload(history)
        assert any("parse" in r.lower() for r in recs)

    def test_large_result_recommendation(self) -> None:
        from pycypher.query_profiler import ProfileReport, analyze_workload

        history = [
            ProfileReport(
                query=f"MATCH (n) RETURN n  -- {i}",
                total_time_ms=500.0,
                parse_time_ms=5.0,
                plan_time_ms=5.0,
                clause_timings={"Match": 400.0},
                row_count=20_000,  # above _LARGE_RESULT_ROWS
                hotspot="Match",
                recommendations=[],
            )
            for i in range(10)
        ]

        recs = analyze_workload(history)
        assert any("10K rows" in r or "pagination" in r.lower() for r in recs)
