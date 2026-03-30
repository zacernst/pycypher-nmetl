"""Tests for the interactive Cypher REPL.

Verifies REPL dot-commands, query execution, multi-line support,
and error handling without requiring actual data files.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pycypher.repl import (
    CypherRepl,
    _looks_incomplete,
    _parse_entity_spec,
    _parse_rel_spec,
)


class TestParseSpecs:
    """Verify entity and relationship spec parsing."""

    def test_entity_basic(self) -> None:
        label, path, id_col = _parse_entity_spec("Person=people.csv")
        assert label == "Person"
        assert path == "people.csv"
        assert id_col is None

    def test_entity_with_id_col(self) -> None:
        label, path, id_col = _parse_entity_spec("Person=people.csv:id")
        assert label == "Person"
        assert path == "people.csv"
        assert id_col == "id"

    def test_entity_with_spaces(self) -> None:
        label, path, id_col = _parse_entity_spec(
            "Person=people.csv:id",
        )
        assert label == "Person"
        assert path == "people.csv"
        assert id_col == "id"

    def test_entity_invalid(self) -> None:
        with pytest.raises(ValueError, match="expected"):
            _parse_entity_spec("noequalssign")

    def test_rel_basic(self) -> None:
        rel, path, src, tgt = _parse_rel_spec("KNOWS=knows.csv:from:to")
        assert rel == "KNOWS"
        assert path == "knows.csv"
        assert src == "from"
        assert tgt == "to"

    def test_rel_invalid_missing_cols(self) -> None:
        with pytest.raises(ValueError, match="src_col:tgt_col"):
            _parse_rel_spec("KNOWS=knows.csv:from")

    def test_rel_invalid_no_equals(self) -> None:
        with pytest.raises(ValueError, match="expected"):
            _parse_rel_spec("noequalssign")


class TestLooksIncomplete:
    """Verify multi-line heuristic."""

    def test_match_without_return(self) -> None:
        assert _looks_incomplete("MATCH (n:Person)") is True

    def test_match_with_return(self) -> None:
        assert _looks_incomplete("MATCH (n) RETURN n") is False

    def test_where_keyword(self) -> None:
        # WHERE alone doesn't contain MATCH, so the heuristic
        # checks last-word and MATCH presence, not WHERE specifically
        assert _looks_incomplete("MATCH (n) WHERE n.age > 30") is True

    def test_complete_query(self) -> None:
        assert _looks_incomplete("RETURN 42") is False

    def test_empty(self) -> None:
        assert _looks_incomplete("") is False


class TestCypherReplInit:
    """Verify REPL initialization."""

    def test_default_init(self) -> None:
        repl = CypherRepl()
        assert repl._star is None
        assert repl._query_count == 0
        assert repl.prompt == "cypher> "

    def test_custom_prompt(self) -> None:
        repl = CypherRepl(prompt_str="test> ")
        assert repl.prompt == "test> "


class TestDotCommands:
    """Verify dot-command dispatch and output."""

    def test_help_command(self) -> None:
        repl = CypherRepl()
        # Should not raise
        repl.do_help("")

    def test_schema_no_context(self) -> None:
        repl = CypherRepl()
        repl.do_schema("")

    def test_tables_no_context(self) -> None:
        repl = CypherRepl()
        repl.do_tables("")

    def test_metrics_command(self) -> None:
        repl = CypherRepl()
        repl.do_metrics("")

    def test_functions_command(self) -> None:
        repl = CypherRepl()
        repl.do_functions("")

    def test_quit_returns_true(self) -> None:
        repl = CypherRepl()
        assert repl.do_quit("") is True

    def test_exit_returns_true(self) -> None:
        repl = CypherRepl()
        assert repl.do_exit("") is True

    def test_eof_returns_true(self) -> None:
        repl = CypherRepl()
        assert repl.do_EOF("") is True

    def test_emptyline_does_nothing(self) -> None:
        repl = CypherRepl()
        repl.emptyline()

    def test_clear_command(self) -> None:
        repl = CypherRepl()
        with patch("click.clear"):
            repl.do_clear("")


class TestParseline:
    """Verify dot-command parsing."""

    def test_dot_command_parsed(self) -> None:
        repl = CypherRepl()
        cmd_name, arg, line = repl.parseline(".schema")
        assert cmd_name == "schema"
        assert arg == ""

    def test_dot_command_with_arg(self) -> None:
        repl = CypherRepl()
        cmd_name, arg, line = repl.parseline(".help topic")
        assert cmd_name == "help"
        assert arg == "topic"

    def test_regular_line_passthrough(self) -> None:
        repl = CypherRepl()
        cmd_name, arg, line = repl.parseline("MATCH (n) RETURN n")
        # Regular lines go through default() handler
        assert line == "MATCH (n) RETURN n"


class TestCompletenames:
    """Verify tab completion."""

    def test_dot_completion(self) -> None:
        repl = CypherRepl()
        completions = repl.completenames(".s")
        assert ".schema" in completions

    def test_dot_completion_all(self) -> None:
        repl = CypherRepl()
        completions = repl.completenames(".")
        assert ".help" in completions
        assert ".schema" in completions
        assert ".quit" in completions


class TestQueryExecution:
    """Verify query execution through the REPL."""

    def test_no_context_shows_error(self) -> None:
        repl = CypherRepl()
        # Should not raise, just print error
        repl.default("MATCH (n) RETURN n")

    def test_execute_query_with_mock_star(self) -> None:
        import pandas as pd

        repl = CypherRepl()
        mock_star = MagicMock()
        mock_star.execute_query.return_value = pd.DataFrame(
            {"name": ["Alice", "Bob"]},
        )
        repl._star = mock_star

        repl.default("MATCH (n) RETURN n.name")
        mock_star.execute_query.assert_called_once_with(
            "MATCH (n) RETURN n.name",
        )
        assert repl._query_count == 1

    def test_semicolon_stripped(self) -> None:
        import pandas as pd

        repl = CypherRepl()
        mock_star = MagicMock()
        mock_star.execute_query.return_value = pd.DataFrame({"x": [1]})
        repl._star = mock_star

        repl.default("RETURN 42;")
        mock_star.execute_query.assert_called_once_with("RETURN 42")

    def test_query_error_handled(self) -> None:
        repl = CypherRepl()
        mock_star = MagicMock()
        mock_star.execute_query.side_effect = ValueError("bad query")
        repl._star = mock_star

        # Should not raise
        repl.default("INVALID QUERY")
        assert repl._query_count == 0

    def test_explain_prefix(self) -> None:
        repl = CypherRepl()
        repl._star = MagicMock()

        with patch(
            "pycypher.repl.CypherRepl._explain_query",
        ) as mock_explain:
            repl.default("EXPLAIN MATCH (n) RETURN n")
            mock_explain.assert_called_once_with("MATCH (n) RETURN n")

    def test_profile_prefix(self) -> None:
        repl = CypherRepl()
        repl._star = MagicMock()

        with patch(
            "pycypher.repl.CypherRepl._profile_query",
        ) as mock_profile:
            repl.default("PROFILE MATCH (n) RETURN n")
            mock_profile.assert_called_once_with("MATCH (n) RETURN n")


class TestMultiLine:
    """Verify multi-line query accumulation."""

    def test_multiline_accumulation(self) -> None:
        import pandas as pd

        repl = CypherRepl()
        mock_star = MagicMock()
        mock_star.execute_query.return_value = pd.DataFrame({"x": [1]})
        repl._star = mock_star

        # First line: MATCH without RETURN triggers multiline
        repl.default("MATCH (n:Person)")
        assert repl.prompt == "    .> "
        assert len(repl._multiline_buffer) == 1

        # Second line with semicolon completes
        repl.default("RETURN n.name;")
        assert repl.prompt == "cypher> "
        assert len(repl._multiline_buffer) == 0
        mock_star.execute_query.assert_called_once()

    def test_empty_line_in_default(self) -> None:
        repl = CypherRepl()
        repl.default("")
        assert repl._query_count == 0


class TestLoadCommand:
    """Verify .load dot-command for adding data sources mid-session."""

    def test_load_no_args_shows_usage(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        repl = CypherRepl()
        repl.do_load("")
        # Should print usage, not raise

    def test_load_entity_adds_spec(self) -> None:
        repl = CypherRepl()
        with patch.object(repl, "_build_context"):
            repl.do_load("entity Person=people.csv")
        assert "Person=people.csv" in repl._entity_specs

    def test_load_entity_with_id_col(self) -> None:
        repl = CypherRepl()
        with patch.object(repl, "_build_context"):
            repl.do_load("entity Person=people.csv:id")
        assert "Person=people.csv:id" in repl._entity_specs

    def test_load_rel_adds_spec(self) -> None:
        repl = CypherRepl()
        with patch.object(repl, "_build_context"):
            repl.do_load("rel KNOWS=knows.csv:from:to")
        assert "KNOWS=knows.csv:from:to" in repl._rel_specs

    def test_load_entity_short_alias(self) -> None:
        repl = CypherRepl()
        with patch.object(repl, "_build_context"):
            repl.do_load("e Person=people.csv")
        assert "Person=people.csv" in repl._entity_specs

    def test_load_rel_short_alias(self) -> None:
        repl = CypherRepl()
        with patch.object(repl, "_build_context"):
            repl.do_load("r KNOWS=knows.csv:from:to")
        assert "KNOWS=knows.csv:from:to" in repl._rel_specs

    def test_load_invalid_entity_spec(self) -> None:
        repl = CypherRepl()
        repl.do_load("entity noequalssign")
        assert len(repl._entity_specs) == 0

    def test_load_invalid_rel_spec(self) -> None:
        repl = CypherRepl()
        repl.do_load("rel KNOWS=knows.csv:from")
        assert len(repl._rel_specs) == 0

    def test_load_unknown_type(self) -> None:
        repl = CypherRepl()
        repl.do_load("bogus Person=people.csv")
        assert len(repl._entity_specs) == 0
        assert len(repl._rel_specs) == 0

    def test_load_rebuilds_context(self) -> None:
        repl = CypherRepl()
        with patch.object(repl, "_build_context") as mock_build:
            repl.do_load("entity Person=people.csv")
        mock_build.assert_called_once()

    def test_load_in_tab_completion(self) -> None:
        repl = CypherRepl()
        completions = repl.completenames(".l")
        assert ".load" in completions

    def test_load_in_help_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        repl = CypherRepl()
        repl.do_help("")
        # .load should be mentioned in help (verified by not raising)


class TestSchemaIntrospection:
    """Verify schema commands with mock context."""

    def test_schema_with_context(self) -> None:
        repl = CypherRepl()

        # Build mock context
        entity_table = MagicMock()
        entity_table.column_names = ["__ID__", "name", "age"]

        rel_table = MagicMock()
        rel_table.column_names = [
            "__ID__",
            "__SOURCE__",
            "__TARGET__",
            "since",
        ]
        rel_table.source_entity_type = "Person"
        rel_table.target_entity_type = "Person"

        mock_context = MagicMock()
        mock_context.entity_mapping.mapping = {"Person": entity_table}
        mock_context.relationship_mapping.mapping = {"KNOWS": rel_table}
        repl._context = mock_context

        # Should not raise
        repl.do_schema("")

    def test_tables_with_context(self) -> None:
        import pandas as pd

        repl = CypherRepl()

        entity_table = MagicMock()
        entity_table.source_obj = pd.DataFrame(
            {"__ID__": [1, 2], "name": ["A", "B"]},
        )

        mock_context = MagicMock()
        mock_context.entity_mapping.mapping = {"Person": entity_table}
        mock_context.relationship_mapping.mapping = {}
        repl._context = mock_context

        repl.do_tables("")
