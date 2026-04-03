"""Visual mode - selection."""

from __future__ import annotations

from pycypher_tui.modes.base import BaseMode, KeyResult, ModeType


class VisualMode(BaseMode):
    """VIM Visual mode for selection operations.

    Keybindings:
        h/j/k/l     - Extend selection directionally
        Escape      - Cancel selection, return to normal
        y           - Yank selection, return to normal
        d           - Delete selection, return to normal
        v           - Toggle back to normal mode
    """

    @property
    def mode_type(self) -> ModeType:
        return ModeType.VISUAL

    @property
    def display_name(self) -> str:
        return "VISUAL"

    @property
    def style_color(self) -> str:
        return "#bb9af7"  # Purple

    def handle_key(self, key: str) -> KeyResult:
        match key:
            case "escape" | "v":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                    command="selection:clear",
                )
            case "h" | "left":
                return KeyResult(
                    handled=True,
                    command="selection:extend_left",
                )
            case "j" | "down":
                return KeyResult(
                    handled=True,
                    command="selection:extend_down",
                )
            case "k" | "up":
                return KeyResult(
                    handled=True,
                    command="selection:extend_up",
                )
            case "l" | "right":
                return KeyResult(
                    handled=True,
                    command="selection:extend_right",
                )
            case "y":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                    command="selection:yank",
                )
            case "d":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.NORMAL,
                    command="selection:delete",
                )
            case _:
                return KeyResult(handled=False)
