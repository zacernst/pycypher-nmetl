"""Normal mode - navigation and command dispatching."""

from __future__ import annotations

import time

from pycypher_tui.modes.base import BaseMode, KeyResult, ModeType

# Timeout in seconds for multi-key sequences (e.g., dd, gg).
# If the second key doesn't arrive within this window, the pending
# sequence is cleared and the new key is treated as a fresh input.
# Set generously to accommodate Textual's test harness where each
# pilot.press() takes ~2s due to full render cycle processing.
_PENDING_KEY_TIMEOUT = 5.0


class NormalMode(BaseMode):
    """VIM Normal mode for navigation and command dispatching.

    Keybindings:
        h/j/k/l     - Directional movement (left/down/up/right)
        w/b/e       - Word forward/backward/end
        f{char}     - Find char forward
        t{char}     - Till char forward
        F{char}     - Find char backward
        T{char}     - Till char backward
        gg          - Jump to first item
        G           - Jump to last item
        i           - Enter insert mode (before cursor)
        a           - Enter insert mode (after cursor)
        o           - Enter insert mode (new line below)
        :           - Enter command mode
        /           - Enter search (command mode with /)
        v           - Enter visual mode
        Escape      - Clear pending keys
        y           - Yank (copy)
        p           - Paste
        d           - Delete
        dd          - Delete line
        u           - Undo
        ctrl+r      - Redo
        Enter       - Activate / confirm selection
        q{reg}      - Start/stop macro recording
        @{reg}      - Play macro
        @@          - Replay last macro
        ci{char}    - Change inside delimiter
        ca{char}    - Change around delimiter
        "{reg}      - Select register for next yank/paste
    """

    @property
    def mode_type(self) -> ModeType:
        return ModeType.NORMAL

    @property
    def display_name(self) -> str:
        return "NORMAL"

    @property
    def style_color(self) -> str:
        return "#7aa2f7"  # Blue

    def _start_pending(self, key: str) -> None:
        """Append a key to the pending sequence and record the timestamp."""
        self._pending_keys.append(key)
        self._pending_timestamp = time.monotonic()

    def handle_key(self, key: str) -> KeyResult:
        # Handle pending multi-key sequences with timeout
        if self._pending_keys:
            elapsed = time.monotonic() - self._pending_timestamp
            if elapsed > _PENDING_KEY_TIMEOUT:
                # Sequence timed out — clear and treat as fresh key
                self._pending_keys.clear()
            else:
                return self._handle_pending(key)

        # Single-key bindings
        match key:
            # Navigation
            case "h" | "left":
                return KeyResult(
                    handled=True, command="navigate:left"
                )
            case "j" | "down":
                return KeyResult(
                    handled=True, command="navigate:down"
                )
            case "k" | "up":
                return KeyResult(
                    handled=True, command="navigate:up"
                )
            case "l" | "right":
                return KeyResult(
                    handled=True, command="navigate:right"
                )

            # Word motions
            case "w":
                return KeyResult(
                    handled=True, command="motion:word_forward"
                )
            case "b":
                return KeyResult(
                    handled=True, command="motion:word_backward"
                )
            case "e":
                return KeyResult(
                    handled=True, command="motion:word_end"
                )

            # Character find motions (pending for char argument)
            case "f":
                self._start_pending("f")
                return KeyResult(handled=True, pending=True)
            case "t":
                self._start_pending("t")
                return KeyResult(handled=True, pending=True)
            case "F":
                self._start_pending("F")
                return KeyResult(handled=True, pending=True)
            case "T":
                self._start_pending("T")
                return KeyResult(handled=True, pending=True)

            # Jump navigation
            case "G":
                return KeyResult(
                    handled=True, command="navigate:last"
                )
            case "g":
                self._start_pending("g")
                return KeyResult(handled=True, pending=True)

            # Mode transitions
            case "i":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.INSERT,
                )
            case "a":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.INSERT,
                    command="cursor:after",
                )
            case "o":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.INSERT,
                    command="line:new_below",
                )
            case "v":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.VISUAL,
                )
            case "colon":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.COMMAND,
                )
            case "slash":
                return KeyResult(
                    handled=True,
                    transition_to=ModeType.COMMAND,
                    command="command:search",
                )

            # Operations
            case "y":
                return KeyResult(
                    handled=True, command="clipboard:yank"
                )
            case "p":
                return KeyResult(
                    handled=True, command="clipboard:paste"
                )
            case "d":
                self._start_pending("d")
                return KeyResult(handled=True, pending=True)
            case "c":
                self._start_pending("c")
                return KeyResult(handled=True, pending=True)
            case "u":
                return KeyResult(
                    handled=True, command="edit:undo"
                )
            case "ctrl+r":
                return KeyResult(
                    handled=True, command="edit:redo"
                )
            case "enter":
                return KeyResult(
                    handled=True, command="action:confirm"
                )
            case "escape":
                self._pending_keys.clear()
                return KeyResult(handled=True)

            # Macro recording/playback
            case "q":
                self._start_pending("q")
                return KeyResult(handled=True, pending=True)
            case "at":
                self._start_pending("@")
                return KeyResult(handled=True, pending=True)

            # Register selection
            case "quotation_mark":
                self._start_pending("\"")
                return KeyResult(handled=True, pending=True)

            case _:
                return KeyResult(handled=False)

    def _handle_pending(self, key: str) -> KeyResult:
        """Handle multi-key sequences like gg, dd, f{char}, q{reg}, @{reg}, ci{char}."""
        if key == "escape":
            self._pending_keys.clear()
            return KeyResult(handled=True)

        sequence = "".join(self._pending_keys) + key
        prefix = self._pending_keys[0] if self._pending_keys else ""

        # Character find motions: f/t/F/T + any char
        if prefix in ("f", "t", "F", "T") and len(self._pending_keys) == 1:
            self._pending_keys.clear()
            if len(key) == 1:
                motion_map = {
                    "f": "motion:find_char:",
                    "t": "motion:till_char:",
                    "F": "motion:find_char_back:",
                    "T": "motion:till_char_back:",
                }
                return KeyResult(
                    handled=True,
                    command=f"{motion_map[prefix]}{key}",
                )
            return KeyResult(handled=False)

        # Macro: q{register} to start/stop recording
        if prefix == "q" and len(self._pending_keys) == 1:
            self._pending_keys.clear()
            if len(key) == 1 and key.isalpha():
                return KeyResult(
                    handled=True,
                    command=f"macro:toggle_record:{key}",
                )
            return KeyResult(handled=False)

        # Macro playback: @{register} or @@
        if prefix == "@" and len(self._pending_keys) == 1:
            self._pending_keys.clear()
            if key == "at" or key == "@":
                return KeyResult(
                    handled=True,
                    command="macro:replay_last",
                )
            if len(key) == 1 and key.isalpha():
                return KeyResult(
                    handled=True,
                    command=f"macro:play:{key}",
                )
            return KeyResult(handled=False)

        # Register selection: "{register}
        if prefix == "\"" and len(self._pending_keys) == 1:
            self._pending_keys.clear()
            if len(key) == 1:
                return KeyResult(
                    handled=True,
                    command=f"register:select:{key}",
                )
            return KeyResult(handled=False)

        # Change inside/around: ci{char} or ca{char}
        if prefix == "c" and len(self._pending_keys) == 1:
            if key in ("i", "a"):
                self._start_pending(key)
                return KeyResult(handled=True, pending=True)
            if key == "c":
                # cc = change entire line
                self._pending_keys.clear()
                return KeyResult(
                    handled=True,
                    command="edit:change_line",
                    transition_to=ModeType.INSERT,
                )
            self._pending_keys.clear()
            return KeyResult(handled=False)

        if len(self._pending_keys) == 2 and self._pending_keys[0] == "c":
            obj_type = self._pending_keys[1]  # "i" or "a"
            self._pending_keys.clear()
            if len(key) == 1:
                return KeyResult(
                    handled=True,
                    command=f"textobj:change_{obj_type}:{key}",
                    transition_to=ModeType.INSERT,
                )
            return KeyResult(handled=False)

        # Standard two-key sequences
        self._pending_keys.clear()

        match sequence:
            case "gg":
                return KeyResult(
                    handled=True, command="navigate:first"
                )
            case "dd":
                return KeyResult(
                    handled=True, command="edit:delete_line"
                )
            case _:
                return KeyResult(handled=False)
