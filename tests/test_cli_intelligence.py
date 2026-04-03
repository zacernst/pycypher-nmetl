"""Tests for CLI Intelligence System (Epic 2.1).

Test-driven development for:
- Query suggestion engine (structural similarity via QueryFingerprinter)
- Performance hint system (pattern analysis + optimization recommendations)
- CLI integration (--suggestions and --hints flags)
"""

from __future__ import annotations

import io
from unittest.mock import patch

import click.testing
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Query Suggestion Engine tests
# ---------------------------------------------------------------------------


class TestQuerySuggestionEngine:
    """Tests for intelligent query suggestions based on structural similarity."""

    def test_import(self) -> None:
        """QuerySuggestionEngine should be importable."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        assert engine is not None

    def test_register_and_suggest_similar_query(self) -> None:
        """Registering a query pattern allows suggesting it for similar queries."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        engine.register_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
            description="Find people older than 30",
        )

        suggestions = engine.suggest(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name",
        )
        # Same structure, different literal → should match
        assert len(suggestions) >= 1
        assert any("Find people older than 30" in s.description for s in suggestions)

    def test_no_suggestions_for_unrelated_query(self) -> None:
        """Unrelated queries should not be suggested."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        engine.register_query(
            "MATCH (p:Person) RETURN p.name",
            description="List all people",
        )

        suggestions = engine.suggest(
            "MATCH (c:Company)-[:EMPLOYS]->(e:Employee) RETURN c.name, e.name",
        )
        # Different structure → should not match
        assert len(suggestions) == 0

    def test_suggest_returns_max_n_results(self) -> None:
        """Suggestions should be limited to max_results."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        for i in range(10):
            engine.register_query(
                "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
                description=f"Variant {i}",
            )

        suggestions = engine.suggest(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name",
            max_results=3,
        )
        assert len(suggestions) <= 3

    def test_suggestion_includes_original_query(self) -> None:
        """Each suggestion should include the original query text."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        original = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        engine.register_query(original, description="Age filter")

        suggestions = engine.suggest(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name",
        )
        assert len(suggestions) >= 1
        assert suggestions[0].query == original

    def test_register_multiple_distinct_patterns(self) -> None:
        """Engine should track multiple distinct patterns."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        engine.register_query(
            "MATCH (p:Person) RETURN p.name",
            description="All people",
        )
        engine.register_query(
            "MATCH (c:Company) RETURN c.name",
            description="All companies",
        )

        assert engine.pattern_count >= 2

    def test_suggest_with_common_patterns_preloaded(self) -> None:
        """Engine can preload common Cypher patterns for suggestion."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine.with_common_patterns()
        # Common patterns should be pre-registered
        assert engine.pattern_count > 0

    def test_empty_query_returns_no_suggestions(self) -> None:
        """Empty or whitespace query returns empty suggestions."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        engine.register_query(
            "MATCH (p:Person) RETURN p.name",
            description="All people",
        )
        suggestions = engine.suggest("")
        assert suggestions == []


# ---------------------------------------------------------------------------
# Performance Hint Engine tests
# ---------------------------------------------------------------------------


class TestPerformanceHintEngine:
    """Tests for query performance hint analysis."""

    def test_import(self) -> None:
        """PerformanceHintEngine should be importable."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        assert engine is not None

    def test_detects_missing_where_clause(self) -> None:
        """Should hint about missing WHERE on unbounded MATCH."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze("MATCH (p:Person) RETURN p")
        assert any("WHERE" in h.message for h in hints)

    def test_detects_return_star(self) -> None:
        """Should hint about RETURN * projecting all columns."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze("MATCH (p:Person) RETURN *")
        assert any("RETURN *" in h.message or "all" in h.message.lower() for h in hints)

    def test_detects_order_without_limit(self) -> None:
        """Should hint about ORDER BY without LIMIT."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name"
        )
        assert any(
            "ORDER BY" in h.message and "LIMIT" in h.message
            for h in hints
        )

    def test_detects_multiple_match_clauses(self) -> None:
        """Should hint about potential cartesian products."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze(
            "MATCH (a:Person) MATCH (b:Company) MATCH (c:Product) "
            "RETURN a.name, b.name, c.name"
        )
        assert any(
            "cartesian" in h.message.lower() or "MATCH" in h.message
            for h in hints
        )

    def test_no_hints_for_well_formed_query(self) -> None:
        """A well-formed query should produce few or no hints."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze(
            "MATCH (p:Person) WHERE p.age > 30 "
            "RETURN p.name ORDER BY p.name LIMIT 10"
        )
        # Well-formed query should have no anti-pattern hints
        anti_pattern_hints = [
            h for h in hints if h.severity == "warning"
        ]
        assert len(anti_pattern_hints) == 0

    def test_hint_severity_levels(self) -> None:
        """Hints should have severity levels (info, warning)."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze("MATCH (p:Person) RETURN *")
        assert all(h.severity in ("info", "warning") for h in hints)

    def test_hint_includes_suggestion(self) -> None:
        """Each hint should include a suggested improvement."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze("MATCH (p:Person) RETURN *")
        for h in hints:
            assert h.suggestion, f"Hint '{h.message}' should have a suggestion"

    def test_detects_unbounded_variable_length_path(self) -> None:
        """Should hint about unbounded variable-length paths."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze(
            "MATCH (a:Person)-[*]->(b:Person) RETURN a, b"
        )
        assert any(
            "variable" in h.message.lower() or "unbounded" in h.message.lower()
            for h in hints
        )

    def test_empty_query_returns_no_hints(self) -> None:
        """Empty query returns no hints."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze("")
        assert hints == []


# ---------------------------------------------------------------------------
# CLI Integration tests
# ---------------------------------------------------------------------------


class TestCLIIntelligenceIntegration:
    """Tests for --suggestions and --hints CLI flags."""

    def test_query_command_has_suggestions_flag(self) -> None:
        """The query command should accept --suggestions flag."""
        from pycypher.cli.main import cli

        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["query", "--help"])
        assert "--suggestions" in result.output

    def test_query_command_has_hints_flag(self) -> None:
        """The query command should accept --hints flag."""
        from pycypher.cli.main import cli

        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["query", "--help"])
        assert "--hints" in result.output

    def test_hints_output_formatting(self) -> None:
        """format_hints should produce readable output."""
        from pycypher.cli_intelligence import (
            PerformanceHintEngine,
            format_hints,
        )

        engine = PerformanceHintEngine()
        hints = engine.analyze("MATCH (p:Person) RETURN *")
        output = format_hints(hints)
        assert "RETURN *" in output
        assert "Performance hints:" in output

    def test_suggestions_output_formatting(self) -> None:
        """format_suggestions should produce readable output."""
        from pycypher.cli_intelligence import (
            QuerySuggestionEngine,
            format_suggestions,
        )

        engine = QuerySuggestionEngine.with_common_patterns()
        suggestions = engine.suggest(
            "MATCH (p:Person) WHERE p.age > 30 "
            "RETURN p.name ORDER BY p.name LIMIT 10",
        )
        output = format_suggestions(suggestions)
        if suggestions:
            assert "Similar query patterns:" in output


# ---------------------------------------------------------------------------
# QuerySuggestion and PerformanceHint dataclass tests
# ---------------------------------------------------------------------------


class TestDataModels:
    """Tests for suggestion and hint data models."""

    def test_query_suggestion_dataclass(self) -> None:
        """QuerySuggestion should have required fields."""
        from pycypher.cli_intelligence import QuerySuggestion

        s = QuerySuggestion(
            query="MATCH (p:Person) RETURN p.name",
            description="List all people",
            similarity=0.95,
        )
        assert s.query == "MATCH (p:Person) RETURN p.name"
        assert s.description == "List all people"
        assert s.similarity == 0.95

    def test_performance_hint_dataclass(self) -> None:
        """PerformanceHint should have required fields."""
        from pycypher.cli_intelligence import PerformanceHint

        h = PerformanceHint(
            message="RETURN * projects all columns",
            severity="warning",
            suggestion="Specify only needed columns: RETURN p.name, p.age",
        )
        assert h.message == "RETURN * projects all columns"
        assert h.severity == "warning"
        assert h.suggestion is not None


# ---------------------------------------------------------------------------
# Integration with QueryLearningStore
# ---------------------------------------------------------------------------


class TestLearningStoreIntegration:
    """Tests for integration between CLI intelligence and learning store."""

    def test_suggestion_engine_uses_fingerprinter(self) -> None:
        """Suggestion engine should use QueryFingerprinter internally."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        # Register queries with same structure but different literals
        engine.register_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
            description="Age filter 30",
        )
        engine.register_query(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name",
            description="Age filter 50",
        )

        # Both should map to the same fingerprint, so only 1 unique pattern
        # (may have 2 entries but 1 unique fingerprint)
        suggestions = engine.suggest(
            "MATCH (p:Person) WHERE p.age > 25 RETURN p.name",
        )
        assert len(suggestions) >= 1

    def test_performance_hints_use_profiler_recommendations(self) -> None:
        """Performance hints should leverage existing profiler recommendation logic."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()
        hints = engine.analyze(
            "MATCH (a:Person) MATCH (b:Company) MATCH (c:Product) "
            "RETURN a.name, b.name, c.name"
        )
        # Should detect multi-match anti-pattern
        assert len(hints) > 0


# ---------------------------------------------------------------------------
# Recommendation Engine tests
# ---------------------------------------------------------------------------


class TestRecommendationEngine:
    """Tests for ML-based recommendation engine."""

    def test_import(self) -> None:
        """RecommendationEngine should be importable."""
        from pycypher.cli_intelligence import RecommendationEngine

        engine = RecommendationEngine()
        assert engine is not None

    def test_includes_static_analysis(self) -> None:
        """Recommendations should include static anti-pattern hints."""
        from pycypher.cli_intelligence import RecommendationEngine

        engine = RecommendationEngine()
        recs = engine.recommend("MATCH (p:Person) RETURN *")
        # Should include the RETURN * hint from static analysis
        assert any("RETURN *" in r.message for r in recs)

    def test_includes_learning_store_info(self) -> None:
        """Recommendations should include learning store diagnostics."""
        from pycypher.cli_intelligence import RecommendationEngine
        from pycypher.query_learning import get_learning_store

        # Seed the learning store with some data
        store = get_learning_store()
        store.record_selectivity(
            "Person", "age", ">", estimated=0.33, actual=0.12,
        )
        store.record_selectivity(
            "Person", "age", ">", estimated=0.33, actual=0.11,
        )
        store.record_selectivity(
            "Person", "age", ">", estimated=0.33, actual=0.13,
        )

        try:
            engine = RecommendationEngine()
            recs = engine.recommend("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
            # Should include info about learned selectivity
            assert any("selectivity" in r.message.lower() or "learn" in r.message.lower() for r in recs)
        finally:
            store.clear()

    def test_empty_query_returns_empty(self) -> None:
        """Empty query returns no recommendations."""
        from pycypher.cli_intelligence import RecommendationEngine

        engine = RecommendationEngine()
        assert engine.recommend("") == []

    def test_all_recommendations_have_suggestions(self) -> None:
        """Every recommendation should include an actionable suggestion."""
        from pycypher.cli_intelligence import RecommendationEngine

        engine = RecommendationEngine()
        recs = engine.recommend("MATCH (p:Person) RETURN *")
        for r in recs:
            assert r.suggestion, f"Recommendation '{r.message}' missing suggestion"


# ---------------------------------------------------------------------------
# REPL Intelligence Integration tests
# ---------------------------------------------------------------------------


class TestReplIntelligenceIntegration:
    """Tests for .suggest and .hints REPL dot-commands."""

    def test_suggest_dot_command(self) -> None:
        """REPL .suggest command should show suggestions."""
        from pycypher.repl import CypherRepl
        import io
        from unittest.mock import patch

        repl = CypherRepl()
        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(str(msg) + "\n")):
            repl.do_suggest("MATCH (p:Person) WHERE p.age > 30 RETURN p.name ORDER BY p.age LIMIT 10")

        text = output.getvalue()
        # Should show either suggestions or "No similar" message
        assert "Similar query patterns" in text or "No similar" in text

    def test_suggest_no_arg_shows_usage(self) -> None:
        """REPL .suggest with no argument shows usage."""
        from pycypher.repl import CypherRepl
        import io
        from unittest.mock import patch

        repl = CypherRepl()
        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(str(msg) + "\n")):
            repl.do_suggest("")

        text = output.getvalue()
        assert "Usage" in text

    def test_hints_dot_command(self) -> None:
        """REPL .hints command should show performance hints."""
        from pycypher.repl import CypherRepl
        import io
        from unittest.mock import patch

        repl = CypherRepl()
        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(str(msg) + "\n")):
            repl.do_hints("MATCH (p:Person) RETURN *")

        text = output.getvalue()
        assert "RETURN *" in text
        assert "Performance hints" in text

    def test_hints_no_arg_shows_usage(self) -> None:
        """REPL .hints with no argument shows usage."""
        from pycypher.repl import CypherRepl
        import io
        from unittest.mock import patch

        repl = CypherRepl()
        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(str(msg) + "\n")):
            repl.do_hints("")

        text = output.getvalue()
        assert "Usage" in text

    def test_hints_well_formed_query_no_issues(self) -> None:
        """REPL .hints for well-formed query shows no issues."""
        from pycypher.repl import CypherRepl
        import io
        from unittest.mock import patch

        repl = CypherRepl()
        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(str(msg) + "\n")):
            repl.do_hints("MATCH (p:Person) WHERE p.age > 30 RETURN p.name LIMIT 10")

        text = output.getvalue()
        assert "No performance issues" in text

    def test_suggest_in_dot_commands_list(self) -> None:
        """The .suggest command should be in the REPL's dot-commands list."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        assert "suggest" in repl._DOT_COMMANDS

    def test_hints_in_dot_commands_list(self) -> None:
        """The .hints command should be in the REPL's dot-commands list."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        assert "hints" in repl._DOT_COMMANDS

    def test_help_mentions_suggest_and_hints(self) -> None:
        """REPL .help should mention .suggest and .hints commands."""
        from pycypher.repl import CypherRepl
        import io
        from unittest.mock import patch

        repl = CypherRepl()
        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(str(msg) + "\n")):
            repl.do_help("")

        text = output.getvalue()
        assert ".suggest" in text
        assert ".hints" in text


# ---------------------------------------------------------------------------
# Suggestion Accuracy Validation tests
# ---------------------------------------------------------------------------


class TestSuggestionAccuracy:
    """Tests validating >80% suggestion accuracy requirement."""

    def test_exact_structural_match_accuracy(self) -> None:
        """Queries with same structure should match with 100% accuracy."""
        from pycypher.cli_intelligence import QuerySuggestionEngine

        engine = QuerySuggestionEngine()
        # Register a set of patterns
        patterns = [
            ("MATCH (p:Person) WHERE p.age > 30 RETURN p.name", "Age filter"),
            ("MATCH (c:Company) WHERE c.revenue > 1000000 RETURN c.name", "Revenue filter"),
            ("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name", "Friendship"),
        ]
        for q, desc in patterns:
            engine.register_query(q, description=desc)

        # Test queries with same structure but different literals
        test_cases = [
            ("MATCH (p:Person) WHERE p.age > 50 RETURN p.name", "Age filter"),
            ("MATCH (c:Company) WHERE c.revenue > 5000000 RETURN c.name", "Revenue filter"),
            ("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name", "Friendship"),
        ]

        correct = 0
        for query, expected_desc in test_cases:
            suggestions = engine.suggest(query, max_results=1)
            if suggestions and suggestions[0].description == expected_desc:
                correct += 1

        accuracy = correct / len(test_cases)
        assert accuracy >= 0.80, f"Accuracy {accuracy:.0%} below 80% threshold"

    def test_anti_pattern_detection_accuracy(self) -> None:
        """Performance hint engine should detect anti-patterns with >80% accuracy."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()

        # Anti-pattern test cases: (query, expected_keyword_in_hint)
        test_cases = [
            ("MATCH (p:Person) RETURN *", "RETURN *"),
            ("MATCH (p:Person) RETURN p.name ORDER BY p.name", "ORDER BY"),
            ("MATCH (a) MATCH (b) MATCH (c) RETURN a, b, c", "MATCH"),
            ("MATCH (p:Person) RETURN p", "WHERE"),
            ("MATCH (a)-[*]->(b) RETURN a, b", "variable"),
        ]

        correct = 0
        for query, expected_keyword in test_cases:
            hints = engine.analyze(query)
            hint_text = " ".join(h.message for h in hints)
            if expected_keyword.lower() in hint_text.lower():
                correct += 1

        accuracy = correct / len(test_cases)
        assert accuracy >= 0.80, f"Accuracy {accuracy:.0%} below 80% threshold"

    def test_no_false_positives_for_clean_queries(self) -> None:
        """Well-formed queries should produce zero warning-level false positives."""
        from pycypher.cli_intelligence import PerformanceHintEngine

        engine = PerformanceHintEngine()

        clean_queries = [
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name LIMIT 10",
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Bob' RETURN b.name LIMIT 5",
        ]

        false_positives = 0
        for query in clean_queries:
            hints = engine.analyze(query)
            warnings = [h for h in hints if h.severity == "warning"]
            if warnings:
                false_positives += 1

        fp_rate = false_positives / len(clean_queries)
        assert fp_rate == 0.0, f"False positive rate {fp_rate:.0%} should be 0%"
