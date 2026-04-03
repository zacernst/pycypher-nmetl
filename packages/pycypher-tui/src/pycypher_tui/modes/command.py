"""Command mode - ex-command line interface."""

from __future__ import annotations

from dataclasses import dataclass, field

from pycypher_tui.modes.base import BaseMode, KeyResult, ModeType


@dataclass
class CommandHistory:
    """Tracks command history for recall with up/down arrows."""

    entries: list[str] = field(default_factory=list)
    _position: int = -1
    max_entries: int = 100

    def add(self, command: str) -> None:
        if command and (
            not self.entries or self.entries[-1] != command
        ):
            self.entries.append(command)
            if len(self.entries) > self.max_entries:
                self.entries.pop(0)
        self._position = -1

    def previous(self) -> str | None:
        if not self.entries:
            return None
        if self._position == -1:
            self._position = len(self.entries) - 1
        elif self._position > 0:
            self._position -= 1
        return self.entries[self._position]

    def next(self) -> str | None:
        if not self.entries or self._position == -1:
            return None
        if self._position < len(self.entries) - 1:
            self._position += 1
            return self.entries[self._position]
        self._position = -1
        return ""

    def reset(self) -> None:
        self._position = -1


class CommandMode(BaseMode):
    """VIM Command mode (ex-command line).

    Enter with ':' from normal mode. Supports commands like:
        :w          - Save configuration
        :q          - Quit
        :wq         - Save and quit
        :e <file>   - Open file
        :set <opt>  - Set option
        /pattern    - Search forward
    """

    def __init__(self, manager):
        super().__init__(manager)
        self.buffer: str = ""
        self.prefix: str = ":"
        self.history = CommandHistory()

    @property
    def mode_type(self) -> ModeType:
        return ModeType.COMMAND

    @property
    def display_name(self) -> str:
        return "COMMAND"

    @property
    def style_color(self) -> str:
        return "#e0af68"  # Yellow/amber

    @property
    def display_text(self) -> str:
        """Full command line text including prefix."""
        return f"{self.prefix}{self.buffer}"

    def on_enter(self) -> None:
        super().on_enter()
        self.buffer = ""
        self.prefix = ":"

    def handle_key(self, key: str) -> KeyResult:
        match key:
            case "escape":
                self.buffer = ""
                self.history.reset()
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                )
            case "enter":
                command = self.buffer.strip()
                self.history.add(command)
                self.buffer = ""
                if command:
                    return KeyResult(
                        handled=True,
                        transition_to=ModeType.NORMAL,
                        command=f"ex:{self.prefix}{command}",
                    )
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                )
            case "backspace":
                if self.buffer:
                    self.buffer = self.buffer[:-1]
                    return KeyResult(handled=True)
                # Empty buffer + backspace = exit command mode
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                )
            case "up":
                prev = self.history.previous()
                if prev is not None:
                    self.buffer = prev
                return KeyResult(handled=True)
            case "down":
                nxt = self.history.next()
                if nxt is not None:
                    self.buffer = nxt
                return KeyResult(handled=True)
            case _:
                # Map Textual key names to printable characters
                char = _KEY_TO_CHAR.get(key, key)
                if len(char) == 1 and char.isprintable():
                    self.buffer += char
                    return KeyResult(handled=True)
                return KeyResult(handled=False)


# Mapping from Textual key names to their printable characters.
# Most single-character keys have key==char, but some use names.
_KEY_TO_CHAR: dict[str, str] = {
    "space": " ",
    "slash": "/",
    "backslash": "\\",
    "full_stop": ".",
    "comma": ",",
    "semicolon": ";",
    "minus": "-",
    "plus": "+",
    "underscore": "_",
    "tilde": "~",
    "at": "@",
    "exclamation_mark": "!",
    "question_mark": "?",
    "number_sign": "#",
    "dollar_sign": "$",
    "percent_sign": "%",
    "ampersand": "&",
    "asterisk": "*",
    "circumflex_accent": "^",
    "left_parenthesis": "(",
    "right_parenthesis": ")",
    "left_square_bracket": "[",
    "right_square_bracket": "]",
    "left_curly_bracket": "{",
    "right_curly_bracket": "}",
    "less_than_sign": "<",
    "greater_than_sign": ">",
    "vertical_line": "|",
    "apostrophe": "'",
    "quotation_mark": '"',
    "grave_accent": "`",
    "equals_sign": "=",
    "colon": ":",
    "tab": "\t",
}
