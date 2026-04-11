"""Mode manager - coordinates mode transitions and state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from pycypher_tui.modes.base import BaseMode, KeyResult, ModeType


@dataclass
class ModeTransition:
    """Record of a mode transition for debugging/history."""

    from_mode: ModeType
    to_mode: ModeType
    trigger_key: str


class ModeManager:
    """Manages VIM mode state and transitions.

    Central coordinator for the modal system. Holds instances
    of all modes and handles transitions between them.
    """

    def __init__(self) -> None:
        self._modes: dict[ModeType, BaseMode] = {}
        self._current_type: ModeType = ModeType.NORMAL
        self._transition_history: list[ModeTransition] = []
        self._listeners: list[
            Callable[[ModeType, ModeType], None]
        ] = []

        # Initialize modes - import here to avoid circular imports
        from pycypher_tui.modes.command import CommandMode
        from pycypher_tui.modes.insert import InsertMode
        from pycypher_tui.modes.normal import NormalMode
        from pycypher_tui.modes.visual import VisualMode

        self._modes[ModeType.NORMAL] = NormalMode(self)
        self._modes[ModeType.INSERT] = InsertMode(self)
        self._modes[ModeType.VISUAL] = VisualMode(self)
        self._modes[ModeType.COMMAND] = CommandMode(self)

    @property
    def current_mode(self) -> BaseMode:
        """The currently active mode instance."""
        return self._modes[self._current_type]

    @property
    def current_type(self) -> ModeType:
        """The current mode type."""
        return self._current_type

    @property
    def display_name(self) -> str:
        """Display name of current mode for status bar."""
        return self.current_mode.display_name

    @property
    def style_color(self) -> str:
        """Color of current mode for status bar."""
        return self.current_mode.style_color

    def add_listener(
        self, callback: Callable[[ModeType, ModeType], None]
    ) -> None:
        """Register a callback for mode transitions.

        Args:
            callback: Called with (old_mode, new_mode) on transition.
        """
        self._listeners.append(callback)

    def transition_to(
        self, mode_type: ModeType, trigger_key: str = ""
    ) -> None:
        """Transition to a new mode.

        Args:
            mode_type: The target mode.
            trigger_key: The key that caused this transition.
        """
        if mode_type == self._current_type:
            return

        old_type = self._current_type
        self._modes[old_type].on_exit()
        self._current_type = mode_type
        self._modes[mode_type].on_enter()

        self._transition_history.append(
            ModeTransition(old_type, mode_type, trigger_key)
        )

        for listener in self._listeners:
            listener(old_type, mode_type)

    def handle_key(self, key: str) -> KeyResult:
        """Route a key press to the current mode.

        If the mode requests a transition, execute it.

        Args:
            key: The key identifier from Textual.

        Returns:
            The KeyResult from the mode's handler.
        """
        result = self.current_mode.handle_key(key)

        if result.transition_to is not None:
            self.transition_to(result.transition_to, key)

        return result

    def get_mode(self, mode_type: ModeType) -> BaseMode:
        """Get a specific mode instance."""
        return self._modes[mode_type]
