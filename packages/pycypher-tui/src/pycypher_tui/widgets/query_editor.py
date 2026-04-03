"""Cypher query editor widget with VIM keybindings and syntax awareness.

Provides a text editing widget specifically designed for Cypher queries,
with syntax highlighting tokens, line numbers, bracket matching, and
autocomplete support.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget


# Cypher keywords for syntax awareness and autocomplete
CYPHER_KEYWORDS: tuple[str, ...] = (
    "MATCH", "OPTIONAL", "WHERE", "WITH", "RETURN", "ORDER", "BY",
    "SKIP", "LIMIT", "CREATE", "MERGE", "DELETE", "DETACH", "SET",
    "REMOVE", "UNWIND", "FOREACH", "CALL", "YIELD", "UNION",
    "ON", "AND", "OR", "XOR", "NOT", "IN", "IS", "NULL", "TRUE",
    "FALSE", "AS", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END",
    "EXISTS", "COUNT", "COLLECT", "SUM", "AVG", "MIN", "MAX",
)

CYPHER_FUNCTIONS: tuple[str, ...] = (
    "count", "collect", "sum", "avg", "min", "max",
    "size", "length", "type", "id", "labels", "keys",
    "head", "last", "tail", "range", "reverse",
    "toInteger", "toFloat", "toString", "toBoolean",
    "trim", "ltrim", "rtrim", "replace", "substring",
    "toLower", "toUpper", "split", "left", "right",
    "abs", "ceil", "floor", "round", "sign", "rand",
    "coalesce", "timestamp", "date", "datetime",
    "startNode", "endNode", "properties", "nodes", "relationships",
)

# Regex patterns for syntax token identification
_KEYWORD_PATTERN = re.compile(
    r"\b(" + "|".join(CYPHER_KEYWORDS) + r")\b", re.IGNORECASE
)
_FUNCTION_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(f) for f in CYPHER_FUNCTIONS) + r")\s*\(", re.IGNORECASE
)
_STRING_PATTERN = re.compile(r"'[^']*'|\"[^\"]*\"")
_NUMBER_PATTERN = re.compile(r"\b\d+(\.\d+)?\b")
_COMMENT_PATTERN = re.compile(r"//.*$", re.MULTILINE)
_NODE_PATTERN = re.compile(r"\((\w+)?(?::(\w+))?\)")
_REL_PATTERN = re.compile(r"\[(\w+)?(?::(\w+))?\]")


@dataclass
class SyntaxToken:
    """A token in a Cypher query with its position and type."""

    start: int
    end: int
    token_type: str  # "keyword", "function", "string", "number", "comment", "node", "rel"
    text: str


@dataclass
class CursorPosition:
    """Position of the cursor in the editor."""

    line: int = 0
    col: int = 0

    def copy(self) -> CursorPosition:
        return CursorPosition(self.line, self.col)


@dataclass
class EditorBuffer:
    """Text buffer for the query editor with line-based operations."""

    lines: list[str] = field(default_factory=lambda: [""])

    @property
    def text(self) -> str:
        return "\n".join(self.lines)

    @text.setter
    def text(self, value: str) -> None:
        self.lines = value.split("\n") if value else [""]

    @property
    def line_count(self) -> int:
        return len(self.lines)

    def get_line(self, index: int) -> str:
        if 0 <= index < len(self.lines):
            return self.lines[index]
        return ""

    def set_line(self, index: int, text: str) -> None:
        if 0 <= index < len(self.lines):
            self.lines[index] = text

    def insert_char(self, pos: CursorPosition, char: str) -> CursorPosition:
        """Insert a character at the given position, return new cursor position."""
        line = self.get_line(pos.line)
        col = min(pos.col, len(line))
        new_line = line[:col] + char + line[col:]
        self.set_line(pos.line, new_line)
        return CursorPosition(pos.line, col + 1)

    def insert_newline(self, pos: CursorPosition) -> CursorPosition:
        """Insert a newline at cursor, splitting the current line."""
        line = self.get_line(pos.line)
        col = min(pos.col, len(line))
        before = line[:col]
        after = line[col:]
        self.set_line(pos.line, before)
        self.lines.insert(pos.line + 1, after)
        return CursorPosition(pos.line + 1, 0)

    def delete_char(self, pos: CursorPosition) -> CursorPosition:
        """Delete character before cursor (backspace). Return new cursor position."""
        if pos.col > 0:
            line = self.get_line(pos.line)
            new_line = line[: pos.col - 1] + line[pos.col:]
            self.set_line(pos.line, new_line)
            return CursorPosition(pos.line, pos.col - 1)
        elif pos.line > 0:
            # Join with previous line
            prev_line = self.get_line(pos.line - 1)
            curr_line = self.get_line(pos.line)
            new_col = len(prev_line)
            self.set_line(pos.line - 1, prev_line + curr_line)
            self.lines.pop(pos.line)
            return CursorPosition(pos.line - 1, new_col)
        return pos.copy()

    def delete_char_forward(self, pos: CursorPosition) -> None:
        """Delete character at cursor (delete key)."""
        line = self.get_line(pos.line)
        if pos.col < len(line):
            new_line = line[: pos.col] + line[pos.col + 1:]
            self.set_line(pos.line, new_line)
        elif pos.line < self.line_count - 1:
            # Join with next line
            next_line = self.get_line(pos.line + 1)
            self.set_line(pos.line, line + next_line)
            self.lines.pop(pos.line + 1)

    def delete_line(self, line_index: int) -> None:
        """Delete an entire line."""
        if self.line_count > 1 and 0 <= line_index < self.line_count:
            self.lines.pop(line_index)
        elif self.line_count == 1:
            self.lines[0] = ""


def tokenize_line(line: str) -> list[SyntaxToken]:
    """Extract syntax tokens from a single line of Cypher.

    Results are cached via :func:`_tokenize_line_cached` so repeated
    renders of unchanged lines (the common case during scrolling and
    cursor movement) pay only a dict-lookup cost.
    """
    return list(_tokenize_line_cached(line))


@lru_cache(maxsize=256)
def _tokenize_line_cached(line: str) -> tuple[SyntaxToken, ...]:
    """Cached inner tokenizer — returns a tuple for hashability."""
    tokens: list[SyntaxToken] = []

    # Comments take precedence
    for m in _COMMENT_PATTERN.finditer(line):
        tokens.append(SyntaxToken(m.start(), m.end(), "comment", m.group()))

    # Strings
    for m in _STRING_PATTERN.finditer(line):
        tokens.append(SyntaxToken(m.start(), m.end(), "string", m.group()))

    # Functions (check before keywords so count( is "function" not "keyword")
    for m in _FUNCTION_PATTERN.finditer(line):
        fn_end = m.start() + len(m.group().rstrip("("))
        if not _overlaps_any(m.start(), fn_end, tokens):
            tokens.append(SyntaxToken(m.start(), fn_end, "function", m.group().rstrip("(")))

    # Keywords (only if not inside string/comment/function)
    for m in _KEYWORD_PATTERN.finditer(line):
        if not _overlaps_any(m.start(), m.end(), tokens):
            tokens.append(SyntaxToken(m.start(), m.end(), "keyword", m.group()))

    # Numbers
    for m in _NUMBER_PATTERN.finditer(line):
        if not _overlaps_any(m.start(), m.end(), tokens):
            tokens.append(SyntaxToken(m.start(), m.end(), "number", m.group()))

    tokens.sort(key=lambda t: t.start)
    return tuple(tokens)


def _overlaps_any(start: int, end: int, tokens: list[SyntaxToken]) -> bool:
    """Check if a span overlaps with any existing token."""
    for t in tokens:
        if start < t.end and end > t.start:
            return True
    return False


def find_matching_bracket(text: str, pos: int) -> int | None:
    """Find matching bracket/paren/brace for the character at pos.

    Returns the index of the matching bracket, or None.
    """
    if pos < 0 or pos >= len(text):
        return None

    char = text[pos]
    brackets = {"(": ")", ")": "(", "[": "]", "]": "[", "{": "}", "}": "{"}
    if char not in brackets:
        return None

    match_char = brackets[char]
    forward = char in "([{"
    depth = 0

    if forward:
        for i in range(pos, len(text)):
            if text[i] == char:
                depth += 1
            elif text[i] == match_char:
                depth -= 1
                if depth == 0:
                    return i
    else:
        for i in range(pos, -1, -1):
            if text[i] == char:
                depth += 1
            elif text[i] == match_char:
                depth -= 1
                if depth == 0:
                    return i

    return None


def get_completions(prefix: str, entity_types: list[str] | None = None) -> list[str]:
    """Get autocomplete suggestions for a partial Cypher token.

    Args:
        prefix: The partial text to complete.
        entity_types: Optional list of entity types from the pipeline config.

    Returns:
        List of completion suggestions sorted alphabetically.
    """
    if not prefix:
        return []

    upper = prefix.upper()
    results: list[str] = []

    # Keywords
    results.extend(kw for kw in CYPHER_KEYWORDS if kw.startswith(upper))

    # Functions (case-sensitive)
    results.extend(fn for fn in CYPHER_FUNCTIONS if fn.lower().startswith(prefix.lower()))

    # Entity types from config
    if entity_types:
        results.extend(et for et in entity_types if et.lower().startswith(prefix.lower()))

    return sorted(set(results))


@dataclass
class QueryHistory:
    """History of executed queries with navigation."""

    entries: list[str] = field(default_factory=list)
    _position: int = -1
    max_size: int = 100

    def add(self, query: str) -> None:
        """Add a query to history."""
        query = query.strip()
        if not query:
            return
        # Remove duplicate if already most recent
        if self.entries and self.entries[-1] == query:
            return
        self.entries.append(query)
        if len(self.entries) > self.max_size:
            self.entries.pop(0)
        self._position = -1

    def previous(self) -> str | None:
        """Navigate to previous entry."""
        if not self.entries:
            return None
        if self._position == -1:
            self._position = len(self.entries) - 1
        elif self._position > 0:
            self._position -= 1
        return self.entries[self._position]

    def next(self) -> str | None:
        """Navigate to next entry."""
        if not self.entries or self._position == -1:
            return None
        if self._position < len(self.entries) - 1:
            self._position += 1
            return self.entries[self._position]
        self._position = -1
        return ""

    def reset_position(self) -> None:
        self._position = -1

    @property
    def count(self) -> int:
        return len(self.entries)


class CypherEditor(Widget):
    """A Cypher query editor widget with VIM keybindings.

    Supports:
        Normal mode:
            h/j/k/l     - Cursor movement
            w/b/e       - Word movement
            0/$         - Line start/end
            gg/G        - First/last line
            i/a/o/O     - Enter insert mode
            dd          - Delete line
            yy          - Yank line
            p           - Paste yanked line
            u           - Undo
            /pattern    - Search
            n/N         - Next/prev search match

        Insert mode:
            Text input, Enter, Backspace, Delete, Tab
            Escape      - Return to normal mode

    Messages:
        QuerySubmitted  - When user executes query (Ctrl+Enter or :run)
        ContentChanged  - When buffer content changes
    """

    cursor: reactive[CursorPosition] = reactive(CursorPosition)
    mode: reactive[str] = reactive("normal")

    class QuerySubmitted(Message):
        """Posted when the user executes the query."""

        def __init__(self, query: str) -> None:
            super().__init__()
            self.query = query

    class ContentChanged(Message):
        """Posted when the buffer content changes."""

        def __init__(self, content: str) -> None:
            super().__init__()
            self.content = content

    def __init__(
        self,
        initial_text: str = "",
        entity_types: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.buffer = EditorBuffer()
        if initial_text:
            self.buffer.text = initial_text
        self.cursor = CursorPosition(0, 0)
        self._entity_types = entity_types or []
        self._pending_keys: list[str] = []
        self._yanked_line: str | None = None
        self._undo_stack: list[str] = []
        self._redo_stack: list[str] = []
        self.history = QueryHistory()
        self._search_pattern: str = ""
        self._search_matches: list[tuple[int, int]] = []  # (line, col)
        self._search_index: int = -1

    @property
    def text(self) -> str:
        return self.buffer.text

    @text.setter
    def text(self, value: str) -> None:
        self._save_undo()
        self.buffer.text = value
        self.cursor = CursorPosition(0, 0)

    @property
    def line_count(self) -> int:
        return self.buffer.line_count

    @property
    def current_line(self) -> str:
        return self.buffer.get_line(self.cursor.line)

    def _save_undo(self) -> None:
        """Save current state for undo."""
        self._undo_stack.append(self.buffer.text)
        self._redo_stack.clear()
        if len(self._undo_stack) > 100:
            self._undo_stack.pop(0)

    def undo(self) -> None:
        """Undo last change."""
        if self._undo_stack:
            self._redo_stack.append(self.buffer.text)
            self.buffer.text = self._undo_stack.pop()
            self._clamp_cursor()

    def redo(self) -> None:
        """Redo last undone change."""
        if self._redo_stack:
            self._undo_stack.append(self.buffer.text)
            self.buffer.text = self._redo_stack.pop()
            self._clamp_cursor()

    def _clamp_cursor(self) -> None:
        """Ensure cursor is within buffer bounds."""
        self.cursor.line = max(0, min(self.cursor.line, self.buffer.line_count - 1))
        line_len = len(self.buffer.get_line(self.cursor.line))
        self.cursor.col = max(0, min(self.cursor.col, max(0, line_len - 1) if self.mode == "normal" else line_len))

    def handle_key_normal(self, key: str) -> bool:
        """Handle a key press in normal mode. Returns True if handled."""
        if self._pending_keys:
            return self._handle_pending_normal(key)

        match key:
            # Cursor movement
            case "h" | "left":
                self.cursor.col = max(0, self.cursor.col - 1)
            case "j" | "down":
                self.cursor.line = min(self.cursor.line + 1, self.buffer.line_count - 1)
                self._clamp_cursor()
            case "k" | "up":
                self.cursor.line = max(0, self.cursor.line - 1)
                self._clamp_cursor()
            case "l" | "right":
                line_len = len(self.current_line)
                self.cursor.col = min(self.cursor.col + 1, max(0, line_len - 1))
            # Word movement
            case "w":
                self._move_word_forward()
            case "b":
                self._move_word_backward()
            case "e":
                self._move_word_end()
            # Line movement
            case "0" | "home":
                self.cursor.col = 0
            case "dollar" | "end":
                self.cursor.col = max(0, len(self.current_line) - 1)
            # Document movement
            case "G":
                self.cursor.line = self.buffer.line_count - 1
                self._clamp_cursor()
            case "g":
                self._pending_keys.append("g")
            # Enter insert mode
            case "i":
                self.mode = "insert"
            case "a":
                self.cursor.col = min(self.cursor.col + 1, len(self.current_line))
                self.mode = "insert"
            case "o":
                self._save_undo()
                self.buffer.lines.insert(self.cursor.line + 1, "")
                self.cursor.line += 1
                self.cursor.col = 0
                self.mode = "insert"
            case "O":
                self._save_undo()
                self.buffer.lines.insert(self.cursor.line, "")
                self.cursor.col = 0
                self.mode = "insert"
            case "A":
                self.cursor.col = len(self.current_line)
                self.mode = "insert"
            case "I":
                # Move to first non-whitespace character
                line = self.current_line
                self.cursor.col = len(line) - len(line.lstrip())
                self.mode = "insert"
            # Delete/yank
            case "d":
                self._pending_keys.append("d")
            case "y":
                self._pending_keys.append("y")
            case "p":
                if self._yanked_line is not None:
                    self._save_undo()
                    self.buffer.lines.insert(self.cursor.line + 1, self._yanked_line)
                    self.cursor.line += 1
                    self.cursor.col = 0
            case "x":
                if self.current_line:
                    self._save_undo()
                    self.buffer.delete_char_forward(self.cursor)
                    self._clamp_cursor()
            # Undo/redo
            case "u":
                self.undo()
            case "ctrl+r":
                self.redo()
            # Search
            case "n":
                self._search_next()
            case "N":
                self._search_prev()
            case _:
                return False
        return True

    def _handle_pending_normal(self, key: str) -> bool:
        """Handle multi-key sequences in normal mode."""
        if key == "escape":
            self._pending_keys.clear()
            return True

        sequence = "".join(self._pending_keys) + key
        self._pending_keys.clear()

        match sequence:
            case "gg":
                self.cursor.line = 0
                self._clamp_cursor()
            case "dd":
                self._save_undo()
                self._yanked_line = self.buffer.get_line(self.cursor.line)
                self.buffer.delete_line(self.cursor.line)
                self._clamp_cursor()
            case "yy":
                self._yanked_line = self.buffer.get_line(self.cursor.line)
            case _:
                return False
        return True

    def handle_key_insert(self, key: str) -> bool:
        """Handle a key press in insert mode. Returns True if handled."""
        match key:
            case "escape":
                self.mode = "normal"
                self.cursor.col = max(0, self.cursor.col - 1)
            case "enter":
                self._save_undo()
                self.cursor = self.buffer.insert_newline(self.cursor)
            case "backspace":
                self._save_undo()
                self.cursor = self.buffer.delete_char(self.cursor)
            case "delete":
                self._save_undo()
                self.buffer.delete_char_forward(self.cursor)
            case "tab":
                self._save_undo()
                # Insert 2 spaces for indentation
                for _ in range(2):
                    self.cursor = self.buffer.insert_char(self.cursor, " ")
            case "left":
                self.cursor.col = max(0, self.cursor.col - 1)
            case "right":
                self.cursor.col = min(self.cursor.col + 1, len(self.current_line))
            case "up":
                self.cursor.line = max(0, self.cursor.line - 1)
                self._clamp_cursor()
            case "down":
                self.cursor.line = min(self.cursor.line + 1, self.buffer.line_count - 1)
                self._clamp_cursor()
            case "home":
                self.cursor.col = 0
            case "end":
                self.cursor.col = len(self.current_line)
            case _:
                # Single printable character
                if len(key) == 1 and key.isprintable():
                    self._save_undo()
                    self.cursor = self.buffer.insert_char(self.cursor, key)
                else:
                    return False
        return True

    def handle_key(self, key: str) -> bool:
        """Route key to appropriate mode handler."""
        if self.mode == "normal":
            return self.handle_key_normal(key)
        elif self.mode == "insert":
            return self.handle_key_insert(key)
        return False

    def execute_query(self) -> None:
        """Submit the current buffer content as a query."""
        query = self.buffer.text.strip()
        if query:
            self.history.add(query)
            self.post_message(self.QuerySubmitted(query))

    def get_completions(self) -> list[str]:
        """Get autocomplete suggestions for text at cursor."""
        line = self.current_line
        col = self.cursor.col

        # Extract the word being typed
        start = col
        while start > 0 and (line[start - 1].isalnum() or line[start - 1] == "_"):
            start -= 1

        prefix = line[start:col]
        return get_completions(prefix, self._entity_types)

    def get_tokens(self, line_index: int) -> list[SyntaxToken]:
        """Get syntax tokens for a specific line."""
        line = self.buffer.get_line(line_index)
        return tokenize_line(line)

    def search(self, pattern: str) -> None:
        """Search for pattern in the buffer."""
        self._search_pattern = pattern
        self._search_matches.clear()
        self._search_index = -1

        if not pattern:
            return

        try:
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error:
            return

        for line_idx, line in enumerate(self.buffer.lines):
            for m in regex.finditer(line):
                self._search_matches.append((line_idx, m.start()))

        if self._search_matches:
            # Jump to first match at or after cursor
            for i, (line, col) in enumerate(self._search_matches):
                if line > self.cursor.line or (line == self.cursor.line and col >= self.cursor.col):
                    self._search_index = i
                    self.cursor.line = line
                    self.cursor.col = col
                    return
            # Wrap to first match
            self._search_index = 0
            self.cursor.line = self._search_matches[0][0]
            self.cursor.col = self._search_matches[0][1]

    def _search_next(self) -> None:
        if not self._search_matches:
            return
        self._search_index = (self._search_index + 1) % len(self._search_matches)
        line, col = self._search_matches[self._search_index]
        self.cursor.line = line
        self.cursor.col = col

    def _search_prev(self) -> None:
        if not self._search_matches:
            return
        self._search_index = (self._search_index - 1) % len(self._search_matches)
        line, col = self._search_matches[self._search_index]
        self.cursor.line = line
        self.cursor.col = col

    def _move_word_forward(self) -> None:
        """Move cursor to start of next word."""
        line = self.current_line
        col = self.cursor.col

        # Skip current word
        while col < len(line) and (line[col].isalnum() or line[col] == "_"):
            col += 1
        # Skip whitespace
        while col < len(line) and not (line[col].isalnum() or line[col] == "_"):
            col += 1

        if col >= len(line) and self.cursor.line < self.buffer.line_count - 1:
            self.cursor.line += 1
            self.cursor.col = 0
        else:
            self.cursor.col = min(col, max(0, len(line) - 1))

    def _move_word_backward(self) -> None:
        """Move cursor to start of previous word."""
        line = self.current_line
        col = self.cursor.col

        # Skip whitespace backward
        while col > 0 and not (line[col - 1].isalnum() or line[col - 1] == "_"):
            col -= 1
        # Skip word backward
        while col > 0 and (line[col - 1].isalnum() or line[col - 1] == "_"):
            col -= 1

        if col == 0 and self.cursor.col == 0 and self.cursor.line > 0:
            self.cursor.line -= 1
            self.cursor.col = max(0, len(self.buffer.get_line(self.cursor.line)) - 1)
        else:
            self.cursor.col = col

    def _move_word_end(self) -> None:
        """Move cursor to end of current/next word."""
        line = self.current_line
        col = self.cursor.col + 1

        # Skip whitespace
        while col < len(line) and not (line[col].isalnum() or line[col] == "_"):
            col += 1
        # Move to end of word
        while col < len(line) and (line[col].isalnum() or line[col] == "_"):
            col += 1

        self.cursor.col = max(0, min(col - 1, len(line) - 1))

    def render(self) -> str:
        """Render the editor content with line numbers."""
        lines = []
        width = len(str(self.buffer.line_count))
        for i, line in enumerate(self.buffer.lines):
            num = str(i + 1).rjust(width)
            cursor_marker = ">" if i == self.cursor.line else " "
            lines.append(f"{cursor_marker}{num} | {line}")
        return "\n".join(lines)
