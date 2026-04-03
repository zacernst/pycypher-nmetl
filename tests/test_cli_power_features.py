"""Tests for advanced CLI power user features (Task #41).

Covers: history search, output format switching, query templates,
batch execution, contextual examples, and updated help.
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path
from unittest.mock import patch

import click
import pandas as pd
import pytest

from pycypher.repl import CypherRepl, _display_result


def _capture(fn, *args, **kwargs) -> str:
    """Call fn while capturing click.echo output."""
    buf = io.StringIO()
    with patch("click.echo", side_effect=lambda msg="", **kw: buf.write(str(msg) + "\n")):
        fn(*args, **kwargs)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# .search
# ---------------------------------------------------------------------------


class TestHistorySearch:
    """Tests for .search dot-command."""

    def test_search_no_keyword_shows_usage(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_search, "")
        assert "Usage" in text

    def test_search_no_match(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_search, "xyzzy_nonexistent_9999")
        assert "No history entries" in text

    def test_search_finds_match(self) -> None:
        import readline

        repl = CypherRepl()
        # Add an item to history
        readline.add_history("MATCH (n:Person) RETURN n.name")
        text = _capture(repl.do_search, "Person")
        assert "Person" in text
        assert "match" in text.lower() or "MATCH" in text


# ---------------------------------------------------------------------------
# .format
# ---------------------------------------------------------------------------


class TestOutputFormat:
    """Tests for .format dot-command and output rendering."""

    def test_format_default_is_table(self) -> None:
        repl = CypherRepl()
        assert repl._output_format == "table"

    def test_format_set_csv(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_format, "csv")
        assert repl._output_format == "csv"
        assert "csv" in text

    def test_format_set_json(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_format, "json")
        assert repl._output_format == "json"
        assert "json" in text

    def test_format_set_table(self) -> None:
        repl = CypherRepl()
        repl._output_format = "csv"
        text = _capture(repl.do_format, "table")
        assert repl._output_format == "table"

    def test_format_invalid(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_format, "xml")
        assert "Unknown format" in text
        assert repl._output_format == "table"

    def test_format_no_arg_shows_current(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_format, "")
        assert "table" in text

    def test_display_result_csv(self) -> None:
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        text = _capture(_display_result, df, fmt="csv")
        assert "Alice" in text
        assert "," in text  # CSV uses commas

    def test_display_result_json(self) -> None:
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})
        text = _capture(_display_result, df, fmt="json")
        assert '"name"' in text
        assert '"Alice"' in text

    def test_display_result_table_default(self) -> None:
        df = pd.DataFrame({"x": [1, 2]})
        text = _capture(_display_result, df, fmt="table")
        assert "1" in text
        assert "2" in text


# ---------------------------------------------------------------------------
# .template
# ---------------------------------------------------------------------------


class TestQueryTemplates:
    """Tests for .template dot-command."""

    def test_template_no_args_shows_usage(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_template, "")
        assert "Usage" in text

    def test_template_save(self) -> None:
        repl = CypherRepl()
        text = _capture(
            repl.do_template,
            "save find_person MATCH (p:Person {name: $name}) RETURN p",
        )
        assert "saved" in text
        assert "find_person" in repl._templates

    def test_template_list_empty(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_template, "list")
        assert "No templates" in text

    def test_template_list_shows_saved(self) -> None:
        repl = CypherRepl()
        repl._templates["my_query"] = "MATCH (n) RETURN n"
        text = _capture(repl.do_template, "list")
        assert "my_query" in text
        assert "1 template" in text

    def test_template_run_substitutes_params(self) -> None:
        repl = CypherRepl()
        repl._templates["t1"] = "MATCH (p:Person {name: $name}) RETURN p"
        # Will fail to execute (no star), but we can check the query is printed
        text = _capture(repl.do_template, "run t1 name=Alice")
        assert "Running:" in text
        assert "Alice" in text
        assert "$name" not in text

    def test_template_run_warns_unsubstituted(self) -> None:
        repl = CypherRepl()
        repl._templates["t2"] = "MATCH (p {name: $name, age: $age}) RETURN p"
        text = _capture(repl.do_template, "run t2 name=Bob")
        assert "unsubstituted" in text.lower()
        assert "$age" in text

    def test_template_run_not_found(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_template, "run nonexistent")
        assert "No template" in text

    def test_template_delete(self) -> None:
        repl = CypherRepl()
        repl._templates["deleteme"] = "RETURN 1"
        text = _capture(repl.do_template, "delete deleteme")
        assert "deleted" in text
        assert "deleteme" not in repl._templates

    def test_template_unknown_action(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_template, "foobar")
        assert "Unknown template action" in text


# ---------------------------------------------------------------------------
# .batch
# ---------------------------------------------------------------------------


class TestBatchExecution:
    """Tests for .batch dot-command."""

    def test_batch_no_arg_shows_usage(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_batch, "")
        assert "Usage" in text

    def test_batch_file_not_found(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_batch, "/nonexistent/path.cypher")
        assert "not found" in text.lower()

    def test_batch_runs_queries_from_file(self) -> None:
        from pycypher.ingestion import ContextBuilder
        from pycypher.star import Star

        ctx = ContextBuilder.from_dict(
            {"Widget": pd.DataFrame({"__ID__": [1, 2], "label": ["a", "b"]})}
        )
        repl = CypherRepl()
        repl._context = ctx
        repl._star = Star(context=ctx)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".cypher", delete=False
        ) as f:
            f.write("-- comment line\n")
            f.write("MATCH (w:Widget) RETURN w.label;\n")
            f.write("MATCH (w:Widget) RETURN count(w) AS cnt;\n")
            path = f.name

        try:
            text = _capture(repl.do_batch, path)
            assert "2 queries" in text
            assert "Batch complete" in text
        finally:
            Path(path).unlink()

    def test_batch_skips_comments(self) -> None:
        repl = CypherRepl()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".cypher", delete=False
        ) as f:
            f.write("-- this is a comment\n")
            f.write("-- another comment\n")
            f.write("\n")
            path = f.name

        try:
            text = _capture(repl.do_batch, path)
            assert "No queries" in text
        finally:
            Path(path).unlink()


# ---------------------------------------------------------------------------
# .examples
# ---------------------------------------------------------------------------


class TestExamples:
    """Tests for .examples dot-command."""

    def test_examples_no_context_shows_generic(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_examples, "")
        assert "MATCH" in text
        assert "RETURN" in text
        assert "Query Examples" in text

    def test_examples_with_context_uses_labels(self) -> None:
        from pycypher.ingestion import ContextBuilder
        from pycypher.star import Star

        ctx = ContextBuilder.from_dict(
            {"Person": pd.DataFrame({"__ID__": [1], "name": ["Alice"], "age": [30]})}
        )
        repl = CypherRepl()
        repl._context = ctx
        repl._star = Star(context=ctx)

        text = _capture(repl.do_examples, "")
        assert "Person" in text
        assert "name" in text or "age" in text

    def test_examples_shows_explain_tip(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_examples, "")
        assert "EXPLAIN" in text


# ---------------------------------------------------------------------------
# Updated .help
# ---------------------------------------------------------------------------


class TestUpdatedHelp:
    """Tests that .help includes new commands."""

    def test_help_shows_search(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_help, "")
        assert ".search" in text

    def test_help_shows_format(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_help, "")
        assert ".format" in text

    def test_help_shows_template(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_help, "")
        assert ".template" in text

    def test_help_shows_batch(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_help, "")
        assert ".batch" in text

    def test_help_shows_examples(self) -> None:
        repl = CypherRepl()
        text = _capture(repl.do_help, "")
        assert ".examples" in text
