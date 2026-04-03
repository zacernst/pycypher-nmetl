"""Named register system for VIM-style yank/paste and macro recording.

Registers a-z store text (yank/paste) or key sequences (macros).
Special registers:
    " - default (unnamed) register
    0 - yank register (last yank)
    + - system clipboard proxy
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Register:
    """A single register holding content."""

    content: str = ""
    is_linewise: bool = False


class RegisterFile:
    """Named register storage (a-z, 0-9, and special registers).

    Provides VIM-style register semantics for both text operations
    and macro recording/playback.
    """

    NAMED = set("abcdefghijklmnopqrstuvwxyz")
    VALID = NAMED | set("0123456789\"+-")

    def __init__(self) -> None:
        self._registers: dict[str, Register] = {}
        self._default_register = "\""

    def get(self, name: str) -> Register:
        """Get register by name. Returns empty register if unset."""
        name = name.lower()
        return self._registers.get(name, Register())

    def set(self, name: str, content: str, linewise: bool = False) -> None:
        """Set register content."""
        name = name.lower()
        if name not in self.VALID:
            return
        self._registers[name] = Register(content=content, is_linewise=linewise)

    def append(self, name: str, content: str) -> None:
        """Append to a named register (uppercase register in VIM)."""
        name = name.lower()
        if name not in self.NAMED:
            return
        existing = self._registers.get(name, Register())
        self._registers[name] = Register(
            content=existing.content + content,
            is_linewise=existing.is_linewise,
        )

    def yank(self, content: str, register: str | None = None, linewise: bool = False) -> None:
        """Yank content into register (default: unnamed and yank registers)."""
        self.set("\"", content, linewise)
        self.set("0", content, linewise)
        if register:
            self.set(register, content, linewise)

    def paste(self, register: str | None = None) -> str:
        """Get content from register for pasting."""
        name = register or "\""
        return self.get(name).content

    def clear(self, name: str) -> None:
        """Clear a specific register."""
        name = name.lower()
        self._registers.pop(name, None)

    def clear_all(self) -> None:
        """Clear all registers."""
        self._registers.clear()

    def list_nonempty(self) -> dict[str, str]:
        """Return dict of register name → content for non-empty registers."""
        return {
            name: reg.content
            for name, reg in sorted(self._registers.items())
            if reg.content
        }

    @staticmethod
    def is_valid_name(name: str) -> bool:
        """Check if a register name is valid."""
        return name.lower() in RegisterFile.VALID


class MacroRecorder:
    """Records and plays back key sequences as VIM macros.

    Usage:
        q{register} - start recording into register
        q           - stop recording
        @{register} - play back macro
        @@          - replay last macro
    """

    def __init__(self, registers: RegisterFile) -> None:
        self._registers = registers
        self._recording: bool = False
        self._record_register: str = ""
        self._key_buffer: list[str] = []
        self._last_played: str = ""

    @property
    def is_recording(self) -> bool:
        return self._recording

    @property
    def record_register(self) -> str:
        """Register currently being recorded to, or '' if not recording."""
        return self._record_register if self._recording else ""

    def start_recording(self, register: str) -> bool:
        """Start recording keys into a register.

        Returns True if recording started, False if invalid register.
        """
        register = register.lower()
        if register not in RegisterFile.NAMED:
            return False
        self._recording = True
        self._record_register = register
        self._key_buffer = []
        return True

    def stop_recording(self) -> str:
        """Stop recording and save the macro.

        Returns the register name that was recorded.
        """
        if not self._recording:
            return ""
        register = self._record_register
        # Store the key sequence as pipe-separated keys
        macro_content = "|".join(self._key_buffer)
        self._registers.set(register, macro_content)
        self._recording = False
        self._record_register = ""
        self._key_buffer = []
        return register

    def record_key(self, key: str) -> None:
        """Record a key press during macro recording.

        Should be called for every key EXCEPT the q that stops recording.
        """
        if self._recording:
            self._key_buffer.append(key)

    def get_macro(self, register: str) -> list[str]:
        """Get the key sequence for a macro register.

        Returns list of keys to replay, or empty list if no macro.
        """
        register = register.lower()
        content = self._registers.get(register).content
        if not content:
            return []
        self._last_played = register
        return content.split("|")

    def get_last_macro(self) -> list[str]:
        """Get the last played macro for @@ replay."""
        if not self._last_played:
            return []
        return self.get_macro(self._last_played)

    @property
    def last_played_register(self) -> str:
        return self._last_played
