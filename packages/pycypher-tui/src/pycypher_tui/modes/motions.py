"""VIM-style motion commands for text navigation.

Provides word motions, character find/till, and text object ranges
for VIM-compatible text editing operations.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MotionResult:
    """Result of a motion command execution."""

    new_position: int = 0
    handled: bool = False


@dataclass
class TextObjectRange:
    """Range selected by a text object command (e.g., iw, aw, i(, a()."""

    start: int = 0
    end: int = 0


def word_forward(text: str, pos: int) -> int:
    """Move forward to the start of the next word."""
    length = len(text)
    if pos >= length:
        return pos
    # Skip current word characters
    while pos < length and not text[pos].isspace():
        pos += 1
    # Skip whitespace
    while pos < length and text[pos].isspace():
        pos += 1
    return pos


def word_backward(text: str, pos: int) -> int:
    """Move backward to the start of the previous word."""
    if pos <= 0:
        return 0
    pos -= 1
    # Skip whitespace
    while pos > 0 and text[pos].isspace():
        pos -= 1
    # Skip word characters backward
    while pos > 0 and not text[pos - 1].isspace():
        pos -= 1
    return pos


def word_end(text: str, pos: int) -> int:
    """Move forward to the end of the current/next word."""
    length = len(text)
    if pos >= length - 1:
        return max(0, length - 1)
    pos += 1
    # Skip whitespace
    while pos < length and text[pos].isspace():
        pos += 1
    # Move to end of word
    while pos < length - 1 and not text[pos + 1].isspace():
        pos += 1
    return pos


def find_char_forward(text: str, pos: int, char: str) -> int:
    """Find next occurrence of char after pos (f motion)."""
    idx = text.find(char, pos + 1)
    return idx if idx >= 0 else pos


def find_char_backward(text: str, pos: int, char: str) -> int:
    """Find previous occurrence of char before pos (F motion)."""
    idx = text.rfind(char, 0, pos)
    return idx if idx >= 0 else pos


def till_char_forward(text: str, pos: int, char: str) -> int:
    """Find position before next occurrence of char (t motion)."""
    idx = text.find(char, pos + 1)
    return idx - 1 if idx > 0 else pos


def till_char_backward(text: str, pos: int, char: str) -> int:
    """Find position after previous occurrence of char (T motion)."""
    idx = text.rfind(char, 0, pos)
    return idx + 1 if idx >= 0 else pos


def find_inner_word(text: str, pos: int) -> TextObjectRange:
    """Select the inner word at pos (iw text object)."""
    if not text or pos >= len(text):
        return TextObjectRange(pos, pos)
    start = pos
    end = pos
    while start > 0 and not text[start - 1].isspace():
        start -= 1
    while end < len(text) - 1 and not text[end + 1].isspace():
        end += 1
    return TextObjectRange(start, end + 1)


def find_around_word(text: str, pos: int) -> TextObjectRange:
    """Select the word and surrounding space at pos (aw text object)."""
    inner = find_inner_word(text, pos)
    start = inner.start
    end = inner.end
    # Include trailing whitespace
    while end < len(text) and text[end].isspace():
        end += 1
    if end == inner.end:
        # No trailing whitespace — include leading whitespace
        while start > 0 and text[start - 1].isspace():
            start -= 1
    return TextObjectRange(start, end)


_PAIRS = {"(": ")", "[": "]", "{": "}", "<": ">", '"': '"', "'": "'"}


def find_inner_pair(text: str, pos: int, open_char: str) -> TextObjectRange:
    """Select content inside a matched pair (e.g., i( text object)."""
    close_char = _PAIRS.get(open_char, open_char)
    start = text.rfind(open_char, 0, pos + 1)
    if start < 0:
        return TextObjectRange(pos, pos)
    end = text.find(close_char, pos if open_char != close_char else start + 1)
    if end < 0:
        return TextObjectRange(pos, pos)
    return TextObjectRange(start + 1, end)


def find_around_pair(text: str, pos: int, open_char: str) -> TextObjectRange:
    """Select content including the matched pair delimiters (e.g., a( text object)."""
    close_char = _PAIRS.get(open_char, open_char)
    start = text.rfind(open_char, 0, pos + 1)
    if start < 0:
        return TextObjectRange(pos, pos)
    end = text.find(close_char, pos if open_char != close_char else start + 1)
    if end < 0:
        return TextObjectRange(pos, pos)
    return TextObjectRange(start, end + 1)
