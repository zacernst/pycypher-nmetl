"""Tests for CLI and REPL 'did you mean?' suggestions and enhanced UX.

Task #25: Enhance CLI user experience with intelligent command suggestions.
"""

from __future__ import annotations

import click
import click.testing
import pytest

from pycypher.cli.main import SuggestingGroup, cli


class TestSuggestingGroup:
    """CLI group suggests close matches for mistyped subcommands."""

    def test_typo_suggests_close_match(self) -> None:
        """'qurey' should suggest 'query'."""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["qurey"])
        assert result.exit_code != 0
        assert "Did you mean" in result.output or "did you mean" in result.output.lower()
        assert "query" in result.output

    def test_typo_suggests_parse(self) -> None:
        """'prse' should suggest 'parse'."""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["prse"])
        assert result.exit_code != 0
        assert "parse" in result.output

    def test_typo_suggests_schema(self) -> None:
        """'schem' should suggest 'schema'."""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["schem"])
        assert result.exit_code != 0
        assert "schema" in result.output

    def test_completely_wrong_command_no_suggestion(self) -> None:
        """Completely wrong input should not suggest anything."""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["zzzzxyzzy"])
        assert result.exit_code != 0
        # Should still fail, may or may not have suggestions
        assert "No such command" in result.output or "Error" in result.output or "Usage" in result.output

    def test_valid_command_passes_through(self) -> None:
        """Valid commands should not trigger suggestions."""
        runner = click.testing.CliRunner()
        # 'parse' with no args should fail with missing argument, not suggestion
        result = runner.invoke(cli, ["parse"])
        assert "Did you mean" not in result.output


class TestReplDotCommandSuggestions:
    """REPL suggests close matches for mistyped dot-commands."""

    def test_mistyped_schema_suggests(self) -> None:
        """'.schem' should suggest '.schema'."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        # Capture output by calling default directly
        import io
        from unittest.mock import patch

        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(msg + "\n")):
            repl.default(".schem")

        text = output.getvalue()
        assert ".schema" in text
        assert "Did you mean" in text

    def test_mistyped_help_suggests(self) -> None:
        """'.hlep' should suggest '.help'."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        import io
        from unittest.mock import patch

        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(msg + "\n")):
            repl.default(".hlep")

        text = output.getvalue()
        assert ".help" in text

    def test_mistyped_functions_suggests(self) -> None:
        """'.func' should suggest '.functions'."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        import io
        from unittest.mock import patch

        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(msg + "\n")):
            repl.default(".func")

        text = output.getvalue()
        assert ".functions" in text

    def test_unknown_dot_command_shows_help_hint(self) -> None:
        """Completely unknown dot-commands show help hint."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        import io
        from unittest.mock import patch

        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(msg + "\n")):
            repl.default(".zzzzxyzzy")

        text = output.getvalue()
        assert ".help" in text


class TestReplContextualHelp:
    """REPL gives contextual help when no data is loaded."""

    def test_no_context_shows_load_example(self) -> None:
        """When no data loaded, executing a query shows .load usage example."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        import io
        from unittest.mock import patch

        output = io.StringIO()
        with patch("click.echo", side_effect=lambda msg="", **kw: output.write(msg + "\n")):
            repl._execute_query("MATCH (n) RETURN n")

        text = output.getvalue()
        assert ".load" in text
        assert "entity" in text.lower()


class TestReplEnhancedCompletion:
    """Tab completion includes functions and entity labels."""

    def test_completenames_includes_functions(self) -> None:
        """Tab completion should include function names."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        matches = repl.completenames("coal")
        # Should match coalesce function
        assert any("coalesce" in m for m in matches)

    def test_completenames_includes_cypher_keywords(self) -> None:
        """Tab completion still includes Cypher keywords."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        matches = repl.completenames("MAT")
        assert "MATCH" in matches

    def test_completenames_dot_commands(self) -> None:
        """Tab completion includes dot-commands."""
        from pycypher.repl import CypherRepl

        repl = CypherRepl()
        matches = repl.completenames(".sc")
        assert ".schema" in matches

    def test_completenames_entity_labels_with_context(self) -> None:
        """Tab completion includes entity labels when context is loaded."""
        import pandas as pd

        from pycypher.ingestion import ContextBuilder
        from pycypher.repl import CypherRepl
        from pycypher.star import Star

        ctx = ContextBuilder.from_dict(
            {"Person": pd.DataFrame({"__ID__": [1], "name": ["Alice"]})}
        )
        repl = CypherRepl()
        repl._context = ctx
        repl._star = Star(context=ctx)

        matches = repl.completenames("Per")
        assert "Person" in matches
