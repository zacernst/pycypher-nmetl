"""Parse-time and AST conversion exceptions.

This module defines exceptions raised during query parsing and AST conversion:
``CypherSyntaxError``, ``ASTConversionError``, and ``GrammarTransformerSyncError``.
"""

from __future__ import annotations

from pycypher.exceptions.base import _docs_hint


class ASTConversionError(ValueError):
    """Exception raised when AST conversion from grammar parse tree fails.

    This exception is thrown when the grammar parser successfully parses a query
    but the conversion to typed AST models fails, typically due to grammar/AST
    model synchronization issues or unsupported syntax constructs.

    Attributes:
        query_fragment: The portion of the query that failed conversion.
        node_type: The AST node type that could not be converted.

    """

    def __init__(
        self,
        message: str,
        query_fragment: str = "",
        node_type: str = "",
    ) -> None:
        """Initialize with error message and optional context.

        Args:
            message: Human-readable description of the conversion failure.
            query_fragment: The query text that triggered the error.
            node_type: The AST node type that failed conversion.

        """
        self.query_fragment = query_fragment
        self.node_type = node_type

        # Build concise, actionable error message
        if query_fragment:
            short_fragment = (
                query_fragment[:50] + "..."
                if len(query_fragment) > 50
                else query_fragment
            )
            full_message = f"{message} Query: {short_fragment!r}"
        else:
            full_message = message

        full_message += _docs_hint("ASTConversionError")
        super().__init__(full_message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        parts = (
            [f"query_fragment={self.query_fragment!r}"]
            if self.query_fragment
            else []
        )
        if self.node_type:
            parts.append(f"node_type={self.node_type!r}")
        detail = ", ".join(parts)
        return (
            f"ASTConversionError({detail})"
            if detail
            else f"ASTConversionError({str(self)!r})"
        )


class GrammarTransformerSyncError(ASTConversionError):
    """Exception raised when grammar transformer and AST models are out of sync.

    This is a specialized ASTConversionError for cases where the grammar parser
    produces a node type that has no corresponding AST model class.

    Attributes:
        missing_node_type: The node type that lacks a corresponding AST class.

    """

    def __init__(
        self,
        message: str,
        missing_node_type: str = "",
        query_fragment: str = "",
    ) -> None:
        """Initialize with sync error details.

        Args:
            message: Description of the synchronization issue.
            missing_node_type: The node type missing from AST models.
            query_fragment: The query text that triggered the error.

        """
        self.missing_node_type = missing_node_type

        sync_message = (
            "Grammar and AST models are out of sync. "
            f"{message} "
            f"Missing node type: {missing_node_type!r}"
            if missing_node_type
            else message
        )
        sync_message += _docs_hint("GrammarTransformerSyncError")

        super().__init__(
            sync_message,
            query_fragment=query_fragment,
            node_type=missing_node_type,
        )

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        parts = (
            [f"missing_node_type={self.missing_node_type!r}"]
            if self.missing_node_type
            else []
        )
        if self.query_fragment:
            parts.append(f"query_fragment={self.query_fragment!r}")
        detail = ", ".join(parts)
        return (
            f"GrammarTransformerSyncError({detail})"
            if detail
            else f"GrammarTransformerSyncError({str(self)!r})"
        )


class CypherSyntaxError(SyntaxError):
    """User-friendly wrapper around Lark parse errors.

    Wraps low-level Lark ``UnexpectedInput`` exceptions into a message that
    shows the offending line, column position, and what the parser expected,
    without leaking internal terminal names like ``__ANON_0``.

    Includes keyword typo detection: when the token at the error position is
    close to a known Cypher keyword, a "Did you mean ...?" suggestion is
    appended automatically.

    Attributes:
        query: The original query string.
        line: 1-based line number of the error.
        column: 1-based column number of the error.
        keyword_suggestion: Suggested keyword if a typo was detected, or ``""``.

    """

    # Comprehensive set of Cypher keywords for typo detection.
    _CYPHER_KEYWORDS: tuple[str, ...] = (
        "MATCH",
        "OPTIONAL",
        "WHERE",
        "WITH",
        "RETURN",
        "ORDER",
        "BY",
        "SKIP",
        "LIMIT",
        "CREATE",
        "MERGE",
        "DELETE",
        "DETACH",
        "SET",
        "REMOVE",
        "UNWIND",
        "FOREACH",
        "CALL",
        "YIELD",
        "UNION",
        "ON",
        "AND",
        "OR",
        "XOR",
        "NOT",
        "IN",
        "IS",
        "NULL",
        "STARTS",
        "ENDS",
        "CONTAINS",
        "AS",
        "DISTINCT",
        "ALL",
        "ANY",
        "NONE",
        "SINGLE",
        "EXISTS",
        "ASC",
        "DESC",
        "ASCENDING",
        "DESCENDING",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "TRUE",
        "FALSE",
    )

    def __init__(
        self,
        query: str,
        original: Exception,
    ) -> None:
        """Initialize from an original Lark exception.

        Args:
            query: The Cypher query that failed to parse.
            original: The Lark exception that triggered this error.

        """
        self.query = query
        self.original = original
        self.keyword_suggestion: str = ""

        # Extract line/column from Lark exceptions when available
        raw_line = getattr(original, "line", None)
        raw_col = getattr(original, "column", None)
        # Lark uses -1 for "unknown" position; treat as None
        self.line: int | None = (
            raw_line if raw_line is not None and raw_line > 0 else None
        )
        self.column: int | None = (
            raw_col if raw_col is not None and raw_col > 0 else None
        )

        # Build user-friendly message
        parts: list[str] = ["Cypher syntax error"]
        if self.line is not None:
            parts.append(f" at line {self.line}")
            if self.column is not None:
                parts.append(f", column {self.column}")

        # Show the offending line with a pointer
        if self.line is not None:
            lines = query.splitlines()
            if 0 < self.line <= len(lines):
                parts.append(f"\n  {lines[self.line - 1]}")
                if self.column is not None and self.column > 0:
                    parts.append(f"\n  {' ' * (self.column - 1)}^")

        # Extract expected tokens, filtering out internal names.
        # Lark uses "expected" for UnexpectedToken and "allowed" for
        # UnexpectedCharacters -- check both.
        _FRIENDLY_NAMES: dict[str, str] = {
            "LPAR": "(",
            "RPAR": ")",
            "LBRACE": "{",
            "RBRACE": "}",
            "LSQB": "[",
            "RSQB": "]",
            "COMMA": ",",
            "COLON": ":",
            "SEMICOLON": ";",
            "DOT": ".",
            "VBAR": "|",
            "PLUS": "+",
            "MINUS": "-",
            "STAR": "*",
            "SLASH": "/",
            "PERCENT": "%",
            "EQ": "=",
            "NEQ": "<>",
            "LT": "<",
            "GT": ">",
            "LTE": "<=",
            "GTE": ">=",
            "ARROW_LEFT": "<-",
            "ARROW_RIGHT": "->",
            "DASH": "-",
        }
        expected = getattr(original, "expected", None) or getattr(
            original,
            "allowed",
            None,
        )
        if expected:
            clean = sorted(
                set(
                    _FRIENDLY_NAMES.get(t.strip("\"'"), t.strip("\"'"))
                    for t in expected
                    if not t.startswith("__") and not t.startswith("_")
                ),
            )
            if clean:
                parts.append(f"\nExpected: {', '.join(clean)}")

        # --- Keyword typo detection ---
        # Extract the token at the error position and check if it's close
        # to a known Cypher keyword.
        self.keyword_suggestion = self._detect_keyword_typo(query)
        if self.keyword_suggestion:
            parts.append(f"\n{self.keyword_suggestion}")

        # --- Contextual guidance ---
        guidance = self._contextual_guidance(query)
        if guidance:
            parts.append(f"\n{guidance}")

        # --- Documentation link ---
        doc_hint = _docs_hint("CypherSyntaxError")
        if doc_hint:
            parts.append(doc_hint)

        super().__init__("".join(parts))

    def _detect_keyword_typo(self, query: str) -> str:
        """Check if the token at the error position is a misspelling of a keyword.

        Returns a suggestion string like ``"Did you mean 'MATCH'?"`` or ``""``.
        """
        import difflib

        token = self._extract_error_token(query)
        if not token or len(token) < 2:
            return ""

        token_upper = token.upper()
        # Don't suggest if it's already a valid keyword
        if token_upper in self._CYPHER_KEYWORDS:
            return ""

        matches = difflib.get_close_matches(
            token_upper,
            self._CYPHER_KEYWORDS,
            n=1,
            cutoff=0.6,
        )
        if matches:
            return f"Did you mean '{matches[0]}'?"
        return ""

    def _extract_error_token(self, query: str) -> str:
        """Extract the identifier/word token at the error position."""
        if self.line is None or self.column is None:
            return ""
        lines = query.splitlines()
        if not (0 < self.line <= len(lines)):
            return ""
        line_text = lines[self.line - 1]
        col_idx = self.column - 1  # 0-based
        if col_idx >= len(line_text):
            return ""

        # Walk backwards to find start of word
        start = col_idx
        while start > 0 and (
            line_text[start - 1].isalnum() or line_text[start - 1] == "_"
        ):
            start -= 1
        # Walk forwards to find end of word
        end = col_idx
        while end < len(line_text) and (
            line_text[end].isalnum() or line_text[end] == "_"
        ):
            end += 1

        return line_text[start:end]

    def _contextual_guidance(self, query: str) -> str:
        """Return contextual guidance based on the error pattern."""
        query_stripped = query.strip()
        query_upper = query_stripped.upper()

        # Missing RETURN clause
        if "RETURN" not in query_upper and query_upper.startswith("MATCH"):
            return "Hint: MATCH queries require a RETURN clause."

        # WHERE without preceding MATCH
        if query_upper.startswith("WHERE"):
            return "Hint: WHERE must follow a MATCH, WITH, or OPTIONAL MATCH clause."

        # Unclosed string literal
        single_quotes = query.count("'")
        double_quotes = query.count('"')
        if single_quotes % 2 != 0:
            return "Hint: Unclosed string literal — check for a missing closing quote (')."
        if double_quotes % 2 != 0:
            return 'Hint: Unclosed string literal — check for a missing closing quote (").'

        # Unclosed parentheses/brackets/braces
        if query.count("(") > query.count(")"):
            return "Hint: Unclosed parenthesis — check for a missing ')'."
        if query.count("[") > query.count("]"):
            return "Hint: Unclosed bracket — check for a missing ']'."
        if query.count("{") > query.count("}"):
            return "Hint: Unclosed brace — check for a missing '}'."

        return ""

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"CypherSyntaxError(line={self.line}, column={self.column})"
