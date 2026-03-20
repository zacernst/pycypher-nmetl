"""Minimal Language Server Protocol (LSP) server for Cypher.

Provides real-time diagnostics (parse errors, semantic validation) and
intelligent completion (keywords, functions, labels) for any LSP-capable
editor — VS Code, Neovim, Emacs, Sublime, JetBrains, etc.

Uses only stdlib (no ``pygls`` dependency).  Communicates via JSON-RPC
over stdin/stdout per the LSP specification.

Start the server::

    python -m pycypher.cypher_lsp

VS Code configuration (in settings.json)::

    {
        "pycypher.lspServer.path": "python -m pycypher.cypher_lsp"
    }

Neovim configuration (via nvim-lspconfig)::

    require('lspconfig.configs').pycypher = {
        default_config = {
            cmd = { 'python', '-m', 'pycypher.cypher_lsp' },
            filetypes = { 'cypher' },
            root_dir = function() return vim.fn.getcwd() end,
        },
    }

Supported LSP methods:

- ``initialize`` / ``initialized``
- ``textDocument/didOpen`` / ``textDocument/didChange`` — triggers diagnostics
- ``textDocument/completion`` — keyword, function, and label completion
- ``textDocument/formatting`` — query formatting
- ``shutdown`` / ``exit``
"""

from __future__ import annotations

import json
import sys
from typing import Any

from shared.logger import LOGGER

# ---------------------------------------------------------------------------
# JSON-RPC transport over stdin/stdout
# ---------------------------------------------------------------------------


_MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MiB — reject oversized payloads


def _read_message() -> dict[str, Any] | None:
    """Read a single LSP JSON-RPC message from stdin."""
    # Read headers
    content_length = 0
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        line_str = line.decode("utf-8").strip()
        if not line_str:
            break
        if line_str.startswith("Content-Length:"):
            try:
                content_length = int(line_str.split(":")[1].strip())
            except (ValueError, IndexError):
                LOGGER.warning("Malformed Content-Length header: %r", line_str)
                return None
            if content_length < 0 or content_length > _MAX_CONTENT_LENGTH:
                LOGGER.warning(
                    "Content-Length out of range: %d (max %d)",
                    content_length,
                    _MAX_CONTENT_LENGTH,
                )
                return None

    if content_length == 0:
        return None

    body = sys.stdin.buffer.read(content_length)
    return json.loads(body.decode("utf-8"))


def _send_message(msg: dict[str, Any]) -> None:
    """Send a JSON-RPC message to stdout."""
    body = json.dumps(msg)
    header = f"Content-Length: {len(body)}\r\n\r\n"
    sys.stdout.buffer.write(header.encode("utf-8"))
    sys.stdout.buffer.write(body.encode("utf-8"))
    sys.stdout.buffer.flush()


def _respond(request_id: int | str | None, result: Any) -> None:
    """Send a JSON-RPC response."""
    _send_message(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result,
        }
    )


def _notify(method: str, params: dict[str, Any]) -> None:
    """Send a JSON-RPC notification (no id)."""
    _send_message(
        {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
    )


# ---------------------------------------------------------------------------
# Document store (in-memory)
# ---------------------------------------------------------------------------

_MAX_DOCUMENTS = 128  # Evict oldest documents beyond this limit

_documents: dict[str, str] = {}  # insertion-ordered (Python 3.7+)


def _store_document(uri: str, text: str) -> None:
    """Store a document, evicting the oldest entry if at capacity."""
    # Remove first so re-insertion moves it to the end (most-recent)
    _documents.pop(uri, None)
    _documents[uri] = text
    while len(_documents) > _MAX_DOCUMENTS:
        oldest = next(iter(_documents))
        _documents.pop(oldest)


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def _publish_diagnostics(uri: str, text: str) -> None:
    """Parse query text and publish diagnostics."""
    diagnostics: list[dict[str, Any]] = []

    # Try parsing
    try:
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        ast = parser.parse(text)

        # Run semantic validation
        try:
            from pycypher.semantic_validator import SemanticValidator

            validator = SemanticValidator()
            errors = validator.validate(ast)
            for err in errors:
                # Use precise line/column when available (0-indexed for LSP)
                if err.line is not None:
                    start_line = max(0, err.line - 1)
                    start_char = max(0, (err.column or 1) - 1)
                    err_range = {
                        "start": {
                            "line": start_line,
                            "character": start_char,
                        },
                        "end": {
                            "line": start_line,
                            "character": start_char + 20,
                        },
                    }
                else:
                    err_range = {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 0, "character": len(text)},
                    }
                diagnostics.append(
                    {
                        "range": err_range,
                        "severity": 2,  # Warning
                        "source": "pycypher",
                        "message": str(err),
                    }
                )
        except Exception:
            LOGGER.debug(
                "Semantic validation failed for %s", uri, exc_info=True
            )

    except Exception as exc:
        # Parse error — report as error diagnostic
        msg = str(exc)
        # Use line/column from CypherSyntaxError when available
        err_line = getattr(exc, "line", None)
        err_col = getattr(exc, "column", None)
        if err_line is not None and err_line > 0:
            start_line = err_line - 1  # LSP uses 0-based lines
            start_char = max(0, (err_col or 1) - 1)
            diag_range = {
                "start": {"line": start_line, "character": start_char},
                "end": {"line": start_line, "character": start_char + 1},
            }
        else:
            diag_range = {
                "start": {"line": 0, "character": 0},
                "end": {
                    "line": 0,
                    "character": len(text.split("\n")[0]),
                },
            }
        diagnostics.append(
            {
                "range": diag_range,
                "severity": 1,  # Error
                "source": "pycypher",
                "message": f"Parse error: {msg}",
            }
        )

    # Lint warnings
    try:
        from pycypher.query_formatter import lint_query

        for issue in lint_query(text):
            if issue.severity == "warning":
                diagnostics.append(
                    {
                        "range": {
                            "start": {
                                "line": issue.line - 1,
                                "character": max(0, issue.column - 1),
                            },
                            "end": {
                                "line": issue.line - 1,
                                "character": issue.column + 10,
                            },
                        },
                        "severity": 2,  # Warning
                        "source": "pycypher-lint",
                        "message": issue.message,
                    }
                )
    except Exception:
        LOGGER.debug("Lint query failed for %s", uri, exc_info=True)

    _notify(
        "textDocument/publishDiagnostics",
        {"uri": uri, "diagnostics": diagnostics},
    )


# ---------------------------------------------------------------------------
# Completion
# ---------------------------------------------------------------------------


def _get_completions() -> list[dict[str, Any]]:
    """Generate completion items for Cypher keywords and functions."""
    items: list[dict[str, Any]] = []

    # Clause keywords
    _KEYWORDS = [
        "MATCH",
        "OPTIONAL MATCH",
        "WHERE",
        "RETURN",
        "WITH",
        "ORDER BY",
        "SKIP",
        "LIMIT",
        "CREATE",
        "MERGE",
        "DELETE",
        "DETACH DELETE",
        "SET",
        "REMOVE",
        "UNWIND",
        "FOREACH",
        "CALL",
        "YIELD",
        "UNION",
        "UNION ALL",
        "AND",
        "OR",
        "NOT",
        "IN",
        "AS",
        "DISTINCT",
        "IS NULL",
        "IS NOT NULL",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "EXISTS",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "TRUE",
        "FALSE",
        "NULL",
        "ASC",
        "DESC",
    ]
    for kw in _KEYWORDS:
        items.append(
            {
                "label": kw,
                "kind": 14,  # Keyword
                "detail": "Cypher keyword",
                "insertText": kw,
            }
        )

    # Scalar functions
    try:
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        for name in sorted(registry._functions.keys()):
            items.append(
                {
                    "label": f"{name}()",
                    "kind": 3,  # Function
                    "detail": "Scalar function",
                    "insertText": f"{name}($0)",
                    "insertTextFormat": 2,  # Snippet
                }
            )
    except Exception:
        LOGGER.debug(
            "Scalar function registry introspection failed", exc_info=True
        )

    # Aggregate functions
    _AGGREGATES = [
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "collect",
        "stDev",
        "stDevP",
        "percentileDisc",
        "percentileCont",
    ]
    for func in _AGGREGATES:
        items.append(
            {
                "label": f"{func}()",
                "kind": 3,  # Function
                "detail": "Aggregate function",
                "insertText": f"{func}($0)",
                "insertTextFormat": 2,  # Snippet
            }
        )

    return items


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _format_document(text: str) -> list[dict[str, Any]]:
    """Format the full document and return TextEdit array."""
    try:
        from pycypher.query_formatter import format_query

        formatted = format_query(text)
        lines = text.split("\n")
        last_line = len(lines) - 1
        last_char = len(lines[-1]) if lines else 0

        return [
            {
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": last_line, "character": last_char},
                },
                "newText": formatted,
            }
        ]
    except Exception:
        LOGGER.debug("Query formatting failed", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Main dispatch loop
# ---------------------------------------------------------------------------


def _handle_message(msg: dict[str, Any]) -> None:
    """Dispatch a single JSON-RPC message."""
    method = msg.get("method", "")
    params = msg.get("params", {})
    request_id = msg.get("id")

    if method == "initialize":
        _respond(
            request_id,
            {
                "capabilities": {
                    "textDocumentSync": {
                        "openClose": True,
                        "change": 1,  # Full document sync
                    },
                    "completionProvider": {
                        "triggerCharacters": [".", ":", "("],
                    },
                    "documentFormattingProvider": True,
                },
                "serverInfo": {
                    "name": "pycypher-lsp",
                    "version": "0.1.0",
                },
            },
        )

    elif method == "initialized":
        pass  # Client acknowledges init

    elif method == "textDocument/didOpen":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        text = td.get("text", "")
        _store_document(uri, text)
        _publish_diagnostics(uri, text)

    elif method == "textDocument/didChange":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        changes = params.get("contentChanges", [])
        if changes:
            text = changes[-1].get("text", "")
            _store_document(uri, text)
            _publish_diagnostics(uri, text)

    elif method == "textDocument/didClose":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        _documents.pop(uri, None)

    elif method == "textDocument/completion":
        _respond(request_id, _get_completions())

    elif method == "textDocument/formatting":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        text = _documents.get(uri, "")
        _respond(request_id, _format_document(text))

    elif method == "shutdown":
        _respond(request_id, None)

    elif method == "exit":
        sys.exit(0)

    elif request_id is not None:
        # Unknown request — return empty result
        _respond(request_id, None)


def main() -> None:
    """Run the Cypher LSP server (stdin/stdout)."""
    LOGGER.info("PyCypher LSP server starting...")
    while True:
        msg = _read_message()
        if msg is None:
            break
        try:
            _handle_message(msg)
        except Exception:
            LOGGER.exception("Error handling LSP message")


if __name__ == "__main__":
    main()
