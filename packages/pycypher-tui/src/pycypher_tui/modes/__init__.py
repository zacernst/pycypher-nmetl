"""VIM-style modal system for the TUI."""

from pycypher_tui.modes.base import BaseMode, ModeType
from pycypher_tui.modes.command import CommandMode
from pycypher_tui.modes.insert import InsertMode
from pycypher_tui.modes.manager import ModeManager
from pycypher_tui.modes.normal import NormalMode
from pycypher_tui.modes.registers import RegisterFile
from pycypher_tui.modes.search_replace import (
    ReplacementResult,
    SearchReplaceCommand,
    execute_substitute,
    parse_substitute_command,
)
from pycypher_tui.modes.visual import VisualMode

__all__ = [
    "BaseMode",
    "CommandMode",
    "InsertMode",
    "ModeManager",
    "ModeType",
    "NormalMode",
    "RegisterFile",
    "ReplacementResult",
    "SearchReplaceCommand",
    "VisualMode",
    "execute_substitute",
    "parse_substitute_command",
]
