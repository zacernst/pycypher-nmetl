"""Insert mode - text editing."""

from __future__ import annotations

from pycypher_tui.modes.base import BaseMode, KeyResult, ModeType


class InsertMode(BaseMode):
    """VIM Insert mode for text editing.

    Keybindings:
        Escape      - Return to normal mode
        All other   - Pass through as text input
    """

    @property
    def mode_type(self) -> ModeType:
        return ModeType.INSERT

    @property
    def display_name(self) -> str:
        return "INSERT"

    @property
    def style_color(self) -> str:
        return "#9ece6a"  # Green

    def handle_key(self, key: str) -> KeyResult:
        match key:
            case "escape":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                )
            case _:
                # Single printable characters are text input
                if len(key) == 1 and key.isprintable():
                    return KeyResult(
                        handled=True, text_input=key
                    )
                # Named keys that produce text
                match key:
                    case "enter":
                        return KeyResult(
                            handled=True, text_input="\n"
                        )
                    case "tab":
                        return KeyResult(
                            handled=True, text_input="\t"
                        )
                    case "backspace":
                        return KeyResult(
                            handled=True,
                            command="edit:backspace",
                        )
                    case "delete":
                        return KeyResult(
                            handled=True,
                            command="edit:delete",
                        )
                    case _:
                        return KeyResult(handled=False)
