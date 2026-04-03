"""Tests for the Cypher Language Server Protocol implementation.

Verifies LSP message handling, diagnostics, completion, and formatting
without requiring a real stdin/stdout transport.
"""

from __future__ import annotations

from unittest.mock import patch

from pycypher.cypher_lsp import (
    _documents,
    _format_document,
    _get_completions,
    _get_function_context,
    _get_word_at_position,
    _handle_hover,
    _handle_message,
    _handle_signature_help,
    _publish_diagnostics,
)


class TestInitialize:
    """Verify LSP initialize handshake."""

    def test_initialize_responds_with_capabilities(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {},
                },
            )

            mock_respond.assert_called_once()
            call_args = mock_respond.call_args
            result = call_args[0][1]
            assert "capabilities" in result
            caps = result["capabilities"]
            assert caps["textDocumentSync"]["openClose"] is True
            assert "completionProvider" in caps
            assert caps["documentFormattingProvider"] is True

    def test_initialize_includes_server_info(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {},
                },
            )

            result = mock_respond.call_args[0][1]
            assert result["serverInfo"]["name"] == "pycypher-lsp"


class TestDocumentSync:
    """Verify document open/change tracking."""

    def test_did_open_stores_document(self) -> None:
        _documents.clear()
        with patch("pycypher.cypher_lsp._notify"):
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.cypher",
                            "text": "MATCH (n) RETURN n",
                        },
                    },
                },
            )

        assert _documents.get("file:///test.cypher") == "MATCH (n) RETURN n"

    def test_did_change_updates_document(self) -> None:
        _documents.clear()
        _documents["file:///test.cypher"] = "old text"

        with patch("pycypher.cypher_lsp._notify"):
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didChange",
                    "params": {
                        "textDocument": {"uri": "file:///test.cypher"},
                        "contentChanges": [{"text": "RETURN 42"}],
                    },
                },
            )

        assert _documents["file:///test.cypher"] == "RETURN 42"


class TestDiagnostics:
    """Verify diagnostic publishing."""

    def test_valid_query_no_parse_errors(self) -> None:
        diagnostics_sent: list[dict] = []

        def capture_notify(method: str, params: dict) -> None:
            if method == "textDocument/publishDiagnostics":
                diagnostics_sent.append(params)

        with patch(
            "pycypher.cypher_lsp._notify",
            side_effect=capture_notify,
        ):
            _publish_diagnostics(
                "file:///test.cypher",
                "MATCH (n:Person) RETURN n.name",
            )

        assert len(diagnostics_sent) == 1
        diags = diagnostics_sent[0]["diagnostics"]
        # Should have no parse errors (severity 1)
        parse_errors = [d for d in diags if d.get("severity") == 1]
        assert len(parse_errors) == 0

    def test_invalid_query_produces_error(self) -> None:
        diagnostics_sent: list[dict] = []

        def capture_notify(method: str, params: dict) -> None:
            if method == "textDocument/publishDiagnostics":
                diagnostics_sent.append(params)

        with patch(
            "pycypher.cypher_lsp._notify",
            side_effect=capture_notify,
        ):
            _publish_diagnostics(
                "file:///test.cypher",
                "INVALID GARBAGE QUERY @@#$",
            )

        assert len(diagnostics_sent) == 1
        diags = diagnostics_sent[0]["diagnostics"]
        # Should have at least one error
        errors = [d for d in diags if d.get("severity") == 1]
        assert len(errors) > 0


class TestCompletion:
    """Verify completion items."""

    def test_includes_keywords(self) -> None:
        items = _get_completions()
        labels = {i["label"] for i in items}
        assert "MATCH" in labels
        assert "RETURN" in labels
        assert "WHERE" in labels
        assert "WITH" in labels
        assert "CREATE" in labels

    def test_includes_functions(self) -> None:
        items = _get_completions()
        labels = {i["label"] for i in items}
        # Should have aggregate functions
        assert "count()" in labels
        assert "sum()" in labels
        assert "avg()" in labels

    def test_includes_scalar_functions(self) -> None:
        items = _get_completions()
        labels = {i["label"] for i in items}
        # Check for some known scalar functions
        func_labels = {l for l in labels if l.endswith("()")}
        assert len(func_labels) > 10

    def test_keyword_kind(self) -> None:
        items = _get_completions()
        keyword_items = [i for i in items if i["kind"] == 14]
        assert len(keyword_items) > 0

    def test_function_kind(self) -> None:
        items = _get_completions()
        func_items = [i for i in items if i["kind"] == 3]
        assert len(func_items) > 0

    def test_completion_handler(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "textDocument/completion",
                    "params": {
                        "textDocument": {"uri": "file:///test.cypher"},
                        "position": {"line": 0, "character": 5},
                    },
                },
            )

            mock_respond.assert_called_once()
            items = mock_respond.call_args[0][1]
            assert isinstance(items, list)
            assert len(items) > 0


class TestFormatting:
    """Verify document formatting."""

    def test_format_simple_query(self) -> None:
        edits = _format_document(
            "match (n:Person) where n.age > 30 return n.name",
        )
        assert len(edits) == 1
        assert "MATCH" in edits[0]["newText"]
        assert "RETURN" in edits[0]["newText"]

    def test_format_empty_document(self) -> None:
        edits = _format_document("")
        # Empty doc should produce no edits or identity edit
        assert isinstance(edits, list)

    def test_format_handler(self) -> None:
        _documents["file:///test.cypher"] = "match (n) return n"
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "textDocument/formatting",
                    "params": {
                        "textDocument": {"uri": "file:///test.cypher"},
                        "options": {},
                    },
                },
            )

            mock_respond.assert_called_once()
            edits = mock_respond.call_args[0][1]
            assert isinstance(edits, list)


class TestWordAtPosition:
    """Verify word extraction utility."""

    def test_simple_word(self) -> None:
        assert _get_word_at_position("MATCH (n) RETURN n", 0, 2) == "MATCH"

    def test_word_at_end(self) -> None:
        assert _get_word_at_position("RETURN n", 0, 7) == "n"

    def test_empty_position(self) -> None:
        assert _get_word_at_position("MATCH (n)", 0, 6) == ""

    def test_multiline(self) -> None:
        text = "MATCH (n)\nRETURN n"
        assert _get_word_at_position(text, 1, 0) == "RETURN"

    def test_out_of_bounds(self) -> None:
        assert _get_word_at_position("hello", 5, 0) == ""


class TestFunctionContext:
    """Verify function context detection."""

    def test_inside_function_call(self) -> None:
        assert _get_function_context("toUpper(n.name)", 0, 10) == "toUpper"

    def test_outside_function_call(self) -> None:
        assert _get_function_context("MATCH (n) RETURN n", 0, 10) is None

    def test_nested_parens(self) -> None:
        assert _get_function_context("toUpper(trim(x))", 0, 14) == "trim"

    def test_after_comma(self) -> None:
        assert (
            _get_function_context("substring(s, 0, 5)", 0, 15) == "substring"
        )


class TestHover:
    """Verify hover documentation."""

    def test_hover_scalar_function(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN toUpper(n.name)"
        result = _handle_hover("file:///t.cypher", 0, 9)
        assert result is not None
        assert "markdown" in result["contents"]["kind"]
        assert "toUpper" in result["contents"]["value"]

    def test_hover_aggregate_function(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN count(n)"
        result = _handle_hover("file:///t.cypher", 0, 9)
        assert result is not None
        assert "count" in result["contents"]["value"].lower()
        assert "Aggregate" in result["contents"]["value"]

    def test_hover_keyword(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "MATCH (n) RETURN n"
        result = _handle_hover("file:///t.cypher", 0, 2)
        assert result is not None
        assert "MATCH" in result["contents"]["value"]

    def test_hover_unknown_word(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN xyzzy"
        result = _handle_hover("file:///t.cypher", 0, 9)
        assert result is None

    def test_hover_empty_position(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "MATCH (n)"
        result = _handle_hover("file:///t.cypher", 0, 6)
        assert result is None

    def test_hover_via_handler(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN toUpper(n.name)"
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "textDocument/hover",
                    "params": {
                        "textDocument": {"uri": "file:///t.cypher"},
                        "position": {"line": 0, "character": 9},
                    },
                },
            )
            mock_respond.assert_called_once()
            result = mock_respond.call_args[0][1]
            assert result is not None
            assert "toUpper" in result["contents"]["value"]

    def test_hover_function_description(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN trim(s)"
        result = _handle_hover("file:///t.cypher", 0, 8)
        assert result is not None
        content = result["contents"]["value"]
        assert "trim" in content.lower()

    def test_hover_where_keyword(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "MATCH (n) WHERE n.age > 5 RETURN n"
        result = _handle_hover("file:///t.cypher", 0, 12)
        assert result is not None
        assert "WHERE" in result["contents"]["value"]


class TestSignatureHelp:
    """Verify signature help for function calls."""

    def test_scalar_function_signature(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN toUpper(n.name)"
        result = _handle_signature_help("file:///t.cypher", 0, 15)
        assert result is not None
        assert len(result["signatures"]) == 1
        sig = result["signatures"][0]
        assert "toUpper" in sig["label"]
        assert "parameters" in sig

    def test_aggregate_function_signature(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN count(n)"
        result = _handle_signature_help("file:///t.cypher", 0, 13)
        assert result is not None
        assert "count" in result["signatures"][0]["label"]

    def test_no_signature_outside_call(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "MATCH (n) RETURN n"
        result = _handle_signature_help("file:///t.cypher", 0, 10)
        assert result is None

    def test_active_parameter_tracking(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN substring(s, 0, 5)"
        # After second comma, active param should be 2
        result = _handle_signature_help("file:///t.cypher", 0, 23)
        assert result is not None
        assert result["activeParameter"] == 2

    def test_signature_via_handler(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN toUpper(x)"
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "textDocument/signatureHelp",
                    "params": {
                        "textDocument": {"uri": "file:///t.cypher"},
                        "position": {"line": 0, "character": 15},
                    },
                },
            )
            mock_respond.assert_called_once()
            result = mock_respond.call_args[0][1]
            assert result is not None

    def test_signature_multiarg_function(self) -> None:
        _documents.clear()
        _documents["file:///t.cypher"] = "RETURN replace(s, a, b)"
        result = _handle_signature_help("file:///t.cypher", 0, 16)
        assert result is not None
        sig = result["signatures"][0]
        assert len(sig["parameters"]) >= 1


class TestInitializeCapabilities:
    """Verify new capabilities are advertised."""

    def test_hover_provider_advertised(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {},
                },
            )
            caps = mock_respond.call_args[0][1]["capabilities"]
            assert caps["hoverProvider"] is True

    def test_signature_help_provider_advertised(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {},
                },
            )
            caps = mock_respond.call_args[0][1]["capabilities"]
            assert "signatureHelpProvider" in caps
            assert "(" in caps["signatureHelpProvider"]["triggerCharacters"]


class TestShutdown:
    """Verify shutdown handling."""

    def test_shutdown_responds_null(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 99,
                    "method": "shutdown",
                    "params": {},
                },
            )

            mock_respond.assert_called_once_with(99, None)

    def test_exit_calls_sys_exit(self) -> None:
        import pytest

        with pytest.raises(SystemExit):
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "method": "exit",
                    "params": {},
                },
            )

    def test_unknown_request_returns_null(self) -> None:
        with patch("pycypher.cypher_lsp._respond") as mock_respond:
            _handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 100,
                    "method": "textDocument/unknownMethod",
                    "params": {},
                },
            )

            mock_respond.assert_called_once_with(100, None)
