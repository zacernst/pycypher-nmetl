"""Base mode abstraction for VIM-style modal system."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pycypher_tui.modes.manager import ModeManager


class ModeType(Enum):
    """Available VIM-style modes."""

    NORMAL = auto()
    INSERT = auto()
    VISUAL = auto()
    COMMAND = auto()


@dataclass
class KeyResult:
    """Result of processing a key press in a mode.

    Attributes:
        handled: Whether the key was consumed by this mode.
        transition_to: If set, request transition to this mode.
        command: If set, an ex-command string to execute.
        text_input: If set, text to insert at cursor.
        pending: If True, more keys needed (e.g., 'g' waiting for second 'g').
    """

    handled: bool = False
    transition_to: ModeType | None = None
    command: str | None = None
    text_input: str | None = None
    pending: bool = False


class BaseMode(ABC):
    """Abstract base for all VIM modes.

    Each mode defines how key presses are interpreted and what
    transitions are valid from this mode.
    """

    def __init__(self, manager: ModeManager) -> None:
        self._manager = manager
        self._pending_keys: list[str] = []
        self._pending_timestamp: float = 0.0

    @property
    @abstractmethod
    def mode_type(self) -> ModeType:
        """The type identifier for this mode."""

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Display name shown in the status bar."""

    @property
    @abstractmethod
    def style_color(self) -> str:
        """CSS color for mode indicator."""

    @abstractmethod
    def handle_key(self, key: str) -> KeyResult:
        """Process a key press and return the result.

        Args:
            key: The key identifier string from Textual.

        Returns:
            KeyResult indicating what happened.
        """

    def on_enter(self) -> None:
        """Called when transitioning into this mode."""
        self._pending_keys.clear()

    def on_exit(self) -> None:
        """Called when transitioning out of this mode."""
        self._pending_keys.clear()
