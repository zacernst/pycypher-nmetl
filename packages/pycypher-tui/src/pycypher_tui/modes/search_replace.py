"""Search and replace engine for VIM-style :s commands.

Supports:
    :s/pattern/replacement/       - Replace first on current line
    :s/pattern/replacement/g      - Replace all on current line
    :%s/pattern/replacement/g     - Replace all in buffer
    :%s/pattern/replacement/gc    - Replace all with confirmation
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SearchReplaceCommand:
    """Parsed :s command."""

    pattern: str
    replacement: str
    global_flag: bool = False       # g - replace all occurrences
    confirm_flag: bool = False      # c - confirm each replacement
    whole_buffer: bool = False      # % prefix - apply to all lines
    case_insensitive: bool = False  # i - case insensitive


@dataclass(frozen=True)
class ReplacementResult:
    """Result of a search/replace operation."""

    new_text: str
    count: int
    success: bool = True
    error: str | None = None


def parse_substitute_command(command: str) -> SearchReplaceCommand | None:
    """Parse a VIM :s or :%s command string.

    Accepts commands in the forms:
        s/pattern/replacement/flags
        %s/pattern/replacement/flags

    The separator can be any non-alphanumeric character (/, #, |, etc).

    Returns None if the command doesn't match the expected format.
    """
    text = command.strip()

    whole_buffer = False
    if text.startswith("%"):
        whole_buffer = True
        text = text[1:]

    if not text.startswith("s") or len(text) < 2:
        return None

    sep = text[1]
    if sep.isalnum():
        return None

    # Split by separator, handling escaped separators
    parts = _split_respecting_escapes(text[2:], sep)

    if len(parts) < 2:
        return None

    pattern = parts[0]
    replacement = parts[1]
    flags_str = parts[2] if len(parts) > 2 else ""

    if not pattern:
        return None

    return SearchReplaceCommand(
        pattern=pattern,
        replacement=replacement,
        global_flag="g" in flags_str,
        confirm_flag="c" in flags_str,
        whole_buffer=whole_buffer,
        case_insensitive="i" in flags_str,
    )


def _split_respecting_escapes(text: str, sep: str) -> list[str]:
    """Split text by separator, respecting backslash-escaped separators.

    Only treats backslash as escape when it precedes the separator character.
    Other backslash sequences (like \\d for regex) are preserved literally.
    """
    parts: list[str] = []
    current: list[str] = []
    i = 0
    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text) and text[i + 1] == sep:
            current.append(sep)
            i += 2
        elif text[i] == sep:
            parts.append("".join(current))
            current = []
            i += 1
        else:
            current.append(text[i])
            i += 1
    parts.append("".join(current))
    return parts


def execute_substitute(
    lines: list[str],
    command: SearchReplaceCommand,
    current_line: int = 0,
) -> ReplacementResult:
    """Execute a substitute command on text lines.

    Args:
        lines: The text lines to operate on.
        command: The parsed substitute command.
        current_line: Index of the current cursor line (for non-% commands).

    Returns:
        ReplacementResult with the modified text and replacement count.
    """
    try:
        flags = re.IGNORECASE if command.case_insensitive else 0
        regex = re.compile(command.pattern, flags)
    except re.error as e:
        return ReplacementResult(
            new_text="\n".join(lines),
            count=0,
            success=False,
            error=f"Invalid regex: {e}",
        )

    total_count = 0
    result_lines = list(lines)

    if command.whole_buffer:
        line_range = range(len(result_lines))
    else:
        if 0 <= current_line < len(result_lines):
            line_range = range(current_line, current_line + 1)
        else:
            return ReplacementResult(
                new_text="\n".join(lines),
                count=0,
                success=False,
                error="Line out of range",
            )

    for i in line_range:
        line = result_lines[i]
        if command.global_flag:
            new_line, count = regex.subn(command.replacement, line)
        else:
            new_line, count = regex.subn(command.replacement, line, count=1)
        result_lines[i] = new_line
        total_count += count

    return ReplacementResult(
        new_text="\n".join(result_lines),
        count=total_count,
    )
