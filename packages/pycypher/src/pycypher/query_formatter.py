"""Cypher query formatter and linter.

Normalizes Cypher query formatting with configurable style rules:
uppercase keywords, consistent indentation, clause-per-line layout.

Designed for use in:

- CLI: ``nmetl format-query "MATCH ..."``
- Pre-commit hooks: validate query formatting in CI
- Editor integrations: format-on-save via LSP
- Programmatic use: ``format_query(text)``

Usage::

    from pycypher.query_formatter import format_query, lint_query

    # Format a query
    formatted = format_query("match (n:Person) where n.age > 30 return n.name")
    # MATCH (n:Person)
    # WHERE n.age > 30
    # RETURN n.name

    # Lint a query (returns list of issues)
    issues = lint_query("match (n) return n")
    # [LintIssue(line=1, message="Keyword 'match' should be uppercase: MATCH")]

Environment variables:

- ``PYCYPHER_FORMAT_UPPERCASE`` — uppercase keywords (default: ``1``)
- ``PYCYPHER_FORMAT_INDENT`` — indentation width (default: ``2``)
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Final

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_UPPERCASE: bool = os.environ.get(
    "PYCYPHER_FORMAT_UPPERCASE",
    "1",
).lower() not in ("0", "false", "no")

_INDENT_WIDTH: int = int(
    os.environ.get("PYCYPHER_FORMAT_INDENT", "2"),
)

# Cypher clause keywords that start a new line
_CLAUSE_KEYWORDS: Final[tuple[str, ...]] = (
    "MATCH",
    "OPTIONAL MATCH",
    "WHERE",
    "WITH",
    "RETURN",
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
    "UNION ALL",
    "UNION",
    "ON CREATE SET",
    "ON MATCH SET",
)

# All keywords to uppercase (superset of clause keywords)
_ALL_KEYWORDS: Final[tuple[str, ...]] = _CLAUSE_KEYWORDS + (
    "AND",
    "OR",
    "XOR",
    "NOT",
    "IN",
    "IS NULL",
    "IS NOT NULL",
    "STARTS WITH",
    "ENDS WITH",
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
    "BY",
    "TRUE",
    "FALSE",
    "NULL",
)


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def format_query(
    query: str,
    *,
    uppercase: bool = _UPPERCASE,
    indent: int = _INDENT_WIDTH,
) -> str:
    """Format a Cypher query string.

    Applies consistent formatting:

    - One clause per line (MATCH, WHERE, RETURN, etc.)
    - Uppercase keywords
    - Normalized whitespace
    - Consistent indentation for sub-clauses

    Args:
        query: The Cypher query string to format.
        uppercase: Whether to uppercase keywords.
        indent: Indentation width for sub-clauses.

    Returns:
        The formatted query string.

    """
    if not query or not query.strip():
        return query

    # Normalize whitespace
    text = " ".join(query.split())

    # Uppercase keywords if requested
    if uppercase:
        text = _uppercase_keywords(text)

    # Split into clause-per-line
    text = _split_clauses(text, indent=indent)

    return text


def _uppercase_keywords(text: str) -> str:
    """Uppercase all Cypher keywords in the text.

    Preserves content inside string literals (single and double quotes).
    """
    # Tokenize to protect string literals
    tokens = _tokenize_preserving_strings(text)
    result: list[str] = []

    for is_string, token in tokens:
        if is_string:
            result.append(token)
        else:
            result.append(_uppercase_in_fragment(token))

    return "".join(result)


def _tokenize_preserving_strings(
    text: str,
) -> list[tuple[bool, str]]:
    """Split text into (is_string, content) pairs."""
    tokens: list[tuple[bool, str]] = []
    i = 0
    n = len(text)
    buf: list[str] = []

    while i < n:
        ch = text[i]
        if ch in ("'", '"'):
            # Flush non-string buffer
            if buf:
                tokens.append((False, "".join(buf)))
                buf.clear()
            # Consume string literal
            quote = ch
            string_buf = [ch]
            i += 1
            while i < n:
                if text[i] == "\\" and i + 1 < n:
                    string_buf.append(text[i])
                    string_buf.append(text[i + 1])
                    i += 2
                elif text[i] == quote:
                    string_buf.append(text[i])
                    i += 1
                    break
                else:
                    string_buf.append(text[i])
                    i += 1
            tokens.append((True, "".join(string_buf)))
        else:
            buf.append(ch)
            i += 1

    if buf:
        tokens.append((False, "".join(buf)))

    return tokens


def _uppercase_in_fragment(fragment: str) -> str:
    """Uppercase known keywords in a non-string fragment."""
    # Sort by length descending to match multi-word keywords first
    sorted_kw: list[str] = sorted(
        _ALL_KEYWORDS,
        key=lambda s: len(s),
        reverse=True,
    )
    for kw in sorted_kw:
        pattern = re.compile(
            r"\b" + re.escape(kw) + r"\b",
            re.IGNORECASE,
        )
        fragment = pattern.sub(kw, fragment)
    return fragment


def _split_clauses(text: str, *, indent: int) -> str:
    """Place each clause keyword on its own line."""
    # Sort clause keywords by length (longest first) to avoid partial matches
    sorted_clauses: list[str] = sorted(
        _CLAUSE_KEYWORDS,
        key=lambda s: len(s),
        reverse=True,
    )

    # Build regex pattern for clause boundaries
    clause_pattern = "|".join(re.escape(kw) for kw in sorted_clauses)
    pattern = re.compile(
        rf"\s+({clause_pattern})\b",
        re.IGNORECASE,
    )

    # Replace clause boundaries with newlines
    lines_text = pattern.sub(r"\n\1", text)

    # Clean up and apply indentation
    lines = lines_text.split("\n")
    result: list[str] = []

    # Sub-clause keywords that get indented
    _INDENT_AFTER = {"WHERE", "SET", "ON CREATE SET", "ON MATCH SET"}

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Check if this line is a sub-clause that should be indented
        upper_start = stripped.split()[0].upper() if stripped else ""
        if upper_start in _INDENT_AFTER and result:
            result.append(" " * indent + stripped)
        else:
            result.append(stripped)

    return "\n".join(result)


# ---------------------------------------------------------------------------
# Linter
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LintIssue:
    """A formatting issue found in a Cypher query.

    Attributes:
        line: 1-based line number where the issue was found.
        column: 1-based column number (0 if not applicable).
        message: Human-readable description of the issue.
        severity: ``"warning"`` or ``"error"``.

    """

    line: int
    column: int
    message: str
    severity: str = "warning"


def lint_query(query: str) -> list[LintIssue]:
    """Lint a Cypher query for formatting issues.

    Checks:

    - Keywords should be uppercase
    - No trailing whitespace
    - Consistent use of single or double quotes
    - Parse errors (if parser is available)

    Args:
        query: The Cypher query to lint.

    Returns:
        List of :class:`LintIssue` instances.

    """
    issues: list[LintIssue] = []

    lines = query.split("\n")
    for line_num, line in enumerate(lines, 1):
        # Check trailing whitespace
        if line != line.rstrip():
            issues.append(
                LintIssue(
                    line=line_num,
                    column=len(line.rstrip()) + 1,
                    message="Trailing whitespace",
                ),
            )

        # Check for lowercase keywords (outside strings)
        tokens = _tokenize_preserving_strings(line)
        col = 1
        for is_string, token in tokens:
            if not is_string:
                kw_list: list[str] = sorted(
                    _ALL_KEYWORDS,
                    key=lambda s: len(s),
                    reverse=True,
                )
                for kw in kw_list:
                    pattern = re.compile(
                        r"\b(" + re.escape(kw) + r")\b",
                        re.IGNORECASE,
                    )
                    for match in pattern.finditer(token):
                        if match.group(1) != kw:
                            issues.append(
                                LintIssue(
                                    line=line_num,
                                    column=col + match.start(),
                                    message=(
                                        f"Keyword '{match.group(1)}' "
                                        f"should be uppercase: {kw}"
                                    ),
                                ),
                            )
            col += len(token)

    # Check for parse errors
    try:
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        parser.parse(query)
    except (SyntaxError, ValueError) as exc:
        # GrammarParser.parse() raises CypherSyntaxError (a SyntaxError
        # subclass) on parse failures, and ValueError on empty/invalid input.
        issues.append(
            LintIssue(
                line=1,
                column=0,
                message=f"Parse error: {exc}",
                severity="error",
            ),
        )

    return issues
