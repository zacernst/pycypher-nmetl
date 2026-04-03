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
- ``textDocument/hover`` — function documentation on hover
- ``textDocument/signatureHelp`` — parameter hints for function calls
- ``textDocument/formatting`` — query formatting
- ``shutdown`` / ``exit``
"""

from __future__ import annotations

import json
import re
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
        },
    )


def _notify(method: str, params: dict[str, Any]) -> None:
    """Send a JSON-RPC notification (no id)."""
    _send_message(
        {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        },
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
# Go-to-definition: variable binding extraction
# ---------------------------------------------------------------------------


def _extract_variable_bindings(text: str) -> dict[str, tuple[int, int]]:
    """Parse text and extract variable binding positions.

    Returns a mapping from variable name to (line, character) of its
    first binding site in the source text.  Binding sites are:
    - Node patterns: ``(n:Label)`` — the variable ``n``
    - Relationship patterns: ``-[r:TYPE]->`` — the variable ``r``
    - UNWIND ... AS alias
    - WITH expr AS alias
    - RETURN expr AS alias
    - FOREACH (x IN ...)
    """
    binding_names: list[str] = []

    try:
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        ast = parser.parse(text)
        if ast is None:
            return {}
        _collect_binding_names(ast, binding_names)
    except (SyntaxError, ValueError, KeyError, AttributeError, ImportError):
        LOGGER.debug("Go-to-definition: parse failed", exc_info=True)
        return {}

    # Find the first occurrence of each binding name in the source text.
    # We look specifically for pattern-context occurrences to avoid matching
    # variable references in WHERE/RETURN clauses.
    result: dict[str, tuple[int, int]] = {}
    for var_name in binding_names:
        if var_name in result:
            continue
        pos = _find_binding_position(text, var_name)
        if pos is not None:
            result[var_name] = pos

    return result


def _collect_binding_names(node: Any, names: list[str]) -> None:
    """Recursively walk AST and collect variable names from binding sites."""
    from pycypher.ast_models import (
        Foreach,
        NodePattern,
        RelationshipPattern,
        ReturnItem,
        Unwind,
        With,
    )

    if isinstance(node, NodePattern) or isinstance(node, RelationshipPattern):
        if node.variable is not None:
            names.append(node.variable.name)
    elif isinstance(node, Unwind):
        if node.alias:
            names.append(node.alias)
    elif isinstance(node, Foreach):
        if node.variable:
            names.append(node.variable)

    # WITH/RETURN aliases
    if isinstance(node, (With,)):
        for item in getattr(node, "items", []):
            if isinstance(item, ReturnItem) and item.alias:
                names.append(item.alias)

    # Recurse into child fields
    if hasattr(node, "__dataclass_fields__"):
        for field_name in node.__dataclass_fields__:
            child = getattr(node, field_name, None)
            if child is None:
                continue
            if isinstance(child, list):
                for item in child:
                    if hasattr(item, "__dataclass_fields__"):
                        _collect_binding_names(item, names)
            elif hasattr(child, "__dataclass_fields__"):
                _collect_binding_names(child, names)
    # Pydantic model support
    elif hasattr(node, "model_fields"):
        for field_name in node.model_fields:
            child = getattr(node, field_name, None)
            if child is None:
                continue
            if isinstance(child, list):
                for item in child:
                    if hasattr(item, "model_fields") or hasattr(
                        item,
                        "__dataclass_fields__",
                    ):
                        _collect_binding_names(item, names)
            elif hasattr(child, "model_fields") or hasattr(
                child,
                "__dataclass_fields__",
            ):
                _collect_binding_names(child, names)


def _find_binding_position(
    text: str,
    var_name: str,
) -> tuple[int, int] | None:
    """Find the source position of a variable binding.

    Searches for the variable name in binding contexts:
    - After ``(`` in node patterns
    - After ``[`` in relationship patterns
    - After ``AS`` keyword
    - After ``FOREACH (``
    """
    lines = text.split("\n")

    # Pattern contexts where a variable is bound (not just referenced):
    # (varName   — node pattern
    # [varName   — relationship pattern
    # AS varName — alias
    # FOREACH (varName — foreach binding
    binding_patterns = [
        # Node pattern: ( followed by optional whitespace then var name
        re.compile(
            r"\(\s*" + re.escape(var_name) + r"(?=[\s:)\]]|\b)", re.IGNORECASE
        ),
        # Relationship pattern: [ followed by optional whitespace then var name
        re.compile(
            r"\[\s*" + re.escape(var_name) + r"(?=[\s:|\])]|\b)", re.IGNORECASE
        ),
        # AS alias
        re.compile(r"\bAS\s+" + re.escape(var_name) + r"\b", re.IGNORECASE),
        # FOREACH (var
        re.compile(
            r"\bFOREACH\s*\(\s*" + re.escape(var_name) + r"\b",
            re.IGNORECASE,
        ),
    ]

    for line_idx, line_text in enumerate(lines):
        for pat in binding_patterns:
            m = pat.search(line_text)
            if m:
                # Find the exact position of the variable name within the match
                var_start = m.group().lower().rfind(var_name.lower())
                if var_start >= 0:
                    char_pos = m.start() + var_start
                else:
                    char_pos = m.start()
                return (line_idx, char_pos)

    # Fallback: first occurrence of the variable name as a whole word
    word_pat = re.compile(r"\b" + re.escape(var_name) + r"\b")
    for line_idx, line_text in enumerate(lines):
        m = word_pat.search(line_text)
        if m:
            return (line_idx, m.start())

    return None


def _handle_definition(
    uri: str,
    line: int,
    character: int,
) -> dict[str, Any] | None:
    """Return go-to-definition location for the variable at cursor."""
    text = _documents.get(uri, "")
    word = _get_word_at_position(text, line, character)
    if not word:
        return None

    bindings = _extract_variable_bindings(text)
    if word not in bindings:
        return None

    bind_line, bind_char = bindings[word]
    return {
        "uri": uri,
        "range": {
            "start": {"line": bind_line, "character": bind_char},
            "end": {"line": bind_line, "character": bind_char + len(word)},
        },
    }


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
                    },
                )
        except (ValueError, TypeError, KeyError, AttributeError, ImportError):
            LOGGER.debug(
                "Semantic validation failed for %s",
                uri,
                exc_info=True,
            )

    except Exception as exc:  # noqa: BLE001 — LSP: report any error as diagnostic to editor
        # Parse error — report as error diagnostic
        from pycypher.exceptions import sanitize_error_message

        msg = sanitize_error_message(exc)
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
            },
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
                    },
                )
    except (
        SyntaxError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        ImportError,
    ):
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
            },
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
                },
            )
    except (ImportError, AttributeError, KeyError):
        LOGGER.debug(
            "Scalar function registry introspection failed",
            exc_info=True,
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
            },
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
            },
        ]
    except (
        SyntaxError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        ImportError,
    ):
        LOGGER.debug("Query formatting failed", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

_WORD_RE = re.compile(r"[a-zA-Z_]\w*")


def _get_word_at_position(text: str, line: int, character: int) -> str:
    """Extract the word under the cursor position."""
    lines = text.split("\n")
    if line < 0 or line >= len(lines):
        return ""
    row = lines[line]
    for m in _WORD_RE.finditer(row):
        if m.start() <= character <= m.end():
            return m.group()
    return ""


def _get_function_context(
    text: str,
    line: int,
    character: int,
) -> str | None:
    """Return function name if cursor is inside a function call's parens."""
    lines = text.split("\n")
    if line < 0 or line >= len(lines):
        return None
    row = lines[line]
    prefix = row[:character]
    # Walk backwards through parens to find the enclosing function name
    depth = 0
    for i in range(len(prefix) - 1, -1, -1):
        ch = prefix[i]
        if ch == ")":
            depth += 1
        elif ch == "(":
            if depth > 0:
                depth -= 1
            else:
                # Found the opening paren — extract preceding word
                m = re.search(r"([a-zA-Z_]\w*)\s*$", prefix[:i])
                if m:
                    return m.group(1)
                return None
    return None


# ---------------------------------------------------------------------------
# Hover
# ---------------------------------------------------------------------------

# Cypher keyword documentation for hover
_KEYWORD_DOCS: dict[str, str] = {
    "match": "**MATCH** — Find graph patterns.\n\nBinds variables to nodes and relationships matching a given pattern.",
    "where": "**WHERE** — Filter results.\n\nApplies a boolean predicate to filter rows from MATCH or WITH.",
    "return": "**RETURN** — Project results.\n\nDefines which expressions to include in the query output.",
    "with": "**WITH** — Intermediate projection.\n\nPipes results between query parts, enabling chained transformations.",
    "create": "**CREATE** — Create graph elements.\n\nInserts new nodes and relationships into the graph.",
    "merge": "**MERGE** — Match or create.\n\nEnsures a pattern exists; creates it if missing.",
    "delete": "**DELETE** — Remove graph elements.\n\nRemoves nodes or relationships (use DETACH DELETE for nodes with relationships).",
    "set": "**SET** — Update properties.\n\nSets or updates property values on nodes and relationships.",
    "unwind": "**UNWIND** — Expand a list.\n\nTransforms a list into individual rows for processing.",
    "foreach": "**FOREACH** — Iterate and mutate.\n\nApplies mutation operations for each element in a list.",
    "order by": "**ORDER BY** — Sort results.\n\nSorts output rows by one or more expressions (ASC or DESC).",
    "skip": "**SKIP** — Skip rows.\n\nSkips the first N rows of the result set.",
    "limit": "**LIMIT** — Limit rows.\n\nRestricts the result set to at most N rows.",
    "union": "**UNION** — Combine results.\n\nCombines results from multiple queries (deduplicates by default; use UNION ALL to keep duplicates).",
    "exists": "**EXISTS** — Subquery existence check.\n\nReturns true if the subquery pattern has at least one match.",
    "case": "**CASE** — Conditional expression.\n\nReturns different values based on conditions (WHEN/THEN/ELSE/END).",
    "distinct": "**DISTINCT** — Deduplicate results.\n\nRemoves duplicate rows from the output.",
    "optional match": "**OPTIONAL MATCH** — Left outer join pattern.\n\nLike MATCH, but returns NULL for variables that have no match.",
    "call": "**CALL** — Invoke a procedure.\n\nExecutes a stored procedure and optionally YIELDs its output columns.",
}

# Aggregate function documentation
_AGGREGATE_DOCS: dict[str, tuple[str, str, int, int | None]] = {
    # name -> (description, example, min_args, max_args)
    "count": (
        "Count the number of values or rows",
        "count(n) or count(*)",
        0,
        1,
    ),
    "sum": ("Sum numeric values", "sum(n.price)", 1, 1),
    "avg": ("Calculate arithmetic mean", "avg(n.score)", 1, 1),
    "min": ("Return the minimum value", "min(n.age)", 1, 1),
    "max": ("Return the maximum value", "max(n.age)", 1, 1),
    "collect": ("Collect values into a list", "collect(n.name)", 1, 1),
    "stdev": ("Standard deviation (sample)", "stDev(n.value)", 1, 1),
    "stdevp": ("Standard deviation (population)", "stDevP(n.value)", 1, 1),
    "percentiledisc": (
        "Discrete percentile",
        "percentileDisc(n.score, 0.5)",
        2,
        2,
    ),
    "percentilecont": (
        "Continuous percentile",
        "percentileCont(n.score, 0.5)",
        2,
        2,
    ),
}


def _handle_hover(
    uri: str, line: int, character: int
) -> dict[str, Any] | None:
    """Return hover documentation for the word at the given position."""
    text = _documents.get(uri, "")
    word = _get_word_at_position(text, line, character)
    if not word:
        return None

    word_lower = word.lower()

    # Check scalar functions first
    try:
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        if word_lower in registry._functions:
            meta = registry._functions[word_lower]
            args_str = f"{meta.min_args}"
            if meta.max_args is None:
                args_str += "+"
            elif meta.max_args != meta.min_args:
                args_str = f"{meta.min_args}-{meta.max_args}"

            md = f"**{meta.name}**({args_str} args) — Scalar function\n\n"
            if meta.description:
                md += f"{meta.description}\n\n"
            if meta.example:
                md += f"```\n{meta.example}\n```"

            return {"contents": {"kind": "markdown", "value": md}}
    except (ImportError, AttributeError, KeyError):
        LOGGER.debug("Hover: scalar function lookup failed", exc_info=True)

    # Check aggregate functions
    if word_lower in _AGGREGATE_DOCS:
        desc, example, min_a, max_a = _AGGREGATE_DOCS[word_lower]
        md = f"**{word}**  — Aggregate function\n\n{desc}\n\n```\n{example}\n```"
        return {"contents": {"kind": "markdown", "value": md}}

    # Check keywords (match two-word keywords by checking preceding word)
    if word_lower in _KEYWORD_DOCS:
        return {
            "contents": {
                "kind": "markdown",
                "value": _KEYWORD_DOCS[word_lower],
            },
        }

    return None


# ---------------------------------------------------------------------------
# Signature help
# ---------------------------------------------------------------------------


def _handle_signature_help(
    uri: str,
    line: int,
    character: int,
) -> dict[str, Any] | None:
    """Return signature help when inside a function call."""
    text = _documents.get(uri, "")
    func_name = _get_function_context(text, line, character)
    if not func_name:
        return None

    func_lower = func_name.lower()
    signatures: list[dict[str, Any]] = []

    # Check scalar functions
    try:
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        if func_lower in registry._functions:
            meta = registry._functions[func_lower]
            params = []
            for i in range(meta.min_args):
                params.append({"label": f"arg{i + 1}"})
            if meta.max_args is not None:
                for i in range(meta.min_args, meta.max_args):
                    params.append({"label": f"[arg{i + 1}]"})

            param_labels = ", ".join(p["label"] for p in params)
            label = f"{meta.name}({param_labels})"
            sig: dict[str, Any] = {"label": label, "parameters": params}
            if meta.description:
                sig["documentation"] = {
                    "kind": "markdown",
                    "value": meta.description,
                }
            signatures.append(sig)
    except (ImportError, AttributeError, KeyError):
        LOGGER.debug("SignatureHelp: scalar lookup failed", exc_info=True)

    # Check aggregate functions
    if not signatures and func_lower in _AGGREGATE_DOCS:
        desc, example, min_a, max_a = _AGGREGATE_DOCS[func_lower]
        params = [{"label": f"arg{i + 1}"} for i in range(min_a or 1)]
        param_labels = ", ".join(p["label"] for p in params)
        label = f"{func_name}({param_labels})"
        signatures.append(
            {
                "label": label,
                "documentation": {"kind": "markdown", "value": desc},
                "parameters": params,
            },
        )

    if not signatures:
        return None

    # Count commas before cursor to determine active parameter
    lines = text.split("\n")
    row = lines[line] if line < len(lines) else ""
    prefix = row[:character]
    # Count commas at the current paren depth
    active_param = 0
    depth = 0
    for ch in prefix:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth > 0:
            active_param += 1

    return {
        "signatures": signatures,
        "activeSignature": 0,
        "activeParameter": active_param,
    }


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
                    "hoverProvider": True,
                    "definitionProvider": True,
                    "signatureHelpProvider": {
                        "triggerCharacters": ["(", ","],
                    },
                    "documentFormattingProvider": True,
                },
                "serverInfo": {
                    "name": "pycypher-lsp",
                    "version": "0.2.0",
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

    elif method == "textDocument/hover":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        pos = params.get("position", {})
        result = _handle_hover(
            uri, pos.get("line", 0), pos.get("character", 0)
        )
        _respond(request_id, result)

    elif method == "textDocument/definition":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        pos = params.get("position", {})
        result = _handle_definition(
            uri,
            pos.get("line", 0),
            pos.get("character", 0),
        )
        _respond(request_id, result)

    elif method == "textDocument/signatureHelp":
        td = params.get("textDocument", {})
        uri = td.get("uri", "")
        pos = params.get("position", {})
        result = _handle_signature_help(
            uri,
            pos.get("line", 0),
            pos.get("character", 0),
        )
        _respond(request_id, result)

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
        except Exception:  # noqa: BLE001 — LSP server must not crash on malformed messages
            LOGGER.exception("Error handling LSP message")


if __name__ == "__main__":
    main()
