"""Modal dialog system with VIM keybindings."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable

from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.css.query import NoMatches
from textual.message import Message
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static


class DialogResult(Enum):
    """Result of a dialog interaction."""

    CONFIRMED = auto()
    CANCELLED = auto()


@dataclass
class DialogResponse:
    """Response from a dialog."""

    result: DialogResult
    value: Any = None


class VimDialog(ModalScreen[DialogResponse]):
    """Base modal dialog with VIM keybindings.

    Supports:
        Escape / q  - Cancel / close
        Enter / y   - Confirm (in confirmation dialogs)
        n           - Deny (in confirmation dialogs)
    """

    CSS = """
    VimDialog {
        align: center middle;
    }

    #dialog-container {
        width: 60;
        max-height: 20;
        border: thick $accent;
        padding: 1 2;
        background: $surface;
    }

    #dialog-title {
        text-style: bold;
        width: 100%;
        content-align: center middle;
        margin-bottom: 1;
    }

    #dialog-body {
        width: 100%;
        margin-bottom: 1;
    }

    #dialog-buttons {
        width: 100%;
        align: center middle;
        height: auto;
    }

    #dialog-buttons Button {
        margin: 0 1;
    }
    """

    def __init__(
        self,
        title: str = "",
        body: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dialog_title = title
        self.dialog_body = body

    def compose(self) -> ComposeResult:
        with Container(id="dialog-container"):
            yield Label(self.dialog_title, id="dialog-title")
            yield Label(self.dialog_body, id="dialog-body")
            yield from self.compose_buttons()

    def compose_buttons(self) -> ComposeResult:
        """Override to customize dialog buttons."""
        with Horizontal(id="dialog-buttons"):
            yield Button("OK", id="btn-ok", variant="primary")
            yield Button("Cancel", id="btn-cancel")

    def on_key(self, event) -> None:
        match event.key:
            case "escape" | "q":
                self.dismiss(
                    DialogResponse(DialogResult.CANCELLED)
                )
                event.prevent_default()
                event.stop()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-ok":
            self.dismiss(
                DialogResponse(DialogResult.CONFIRMED)
            )
        elif event.button.id == "btn-cancel":
            self.dismiss(
                DialogResponse(DialogResult.CANCELLED)
            )


class ConfirmDialog(VimDialog):
    """Confirmation dialog with y/n quick keys.

    Keybindings:
        y / Enter   - Confirm
        n / Escape  - Cancel
    """

    def compose_buttons(self) -> ComposeResult:
        with Horizontal(id="dialog-buttons"):
            yield Button(
                "[y]es", id="btn-ok", variant="primary"
            )
            yield Button("[n]o", id="btn-cancel")

    def on_key(self, event) -> None:
        match event.key:
            case "y" | "enter":
                self.dismiss(
                    DialogResponse(DialogResult.CONFIRMED)
                )
                event.prevent_default()
                event.stop()
            case "n" | "escape" | "q":
                self.dismiss(
                    DialogResponse(DialogResult.CANCELLED)
                )
                event.prevent_default()
                event.stop()


class InputDialog(VimDialog):
    """Input dialog with text field.

    Keybindings:
        Enter       - Confirm with current input value
        Escape      - Cancel
    """

    def __init__(
        self,
        title: str = "",
        body: str = "",
        placeholder: str = "",
        default_value: str = "",
        **kwargs,
    ) -> None:
        super().__init__(title=title, body=body, **kwargs)
        self.placeholder = placeholder
        self.default_value = default_value

    def compose(self) -> ComposeResult:
        with Container(id="dialog-container"):
            yield Label(self.dialog_title, id="dialog-title")
            yield Label(self.dialog_body, id="dialog-body")
            yield Input(
                value=self.default_value,
                placeholder=self.placeholder,
                id="dialog-input",
            )
            yield from self.compose_buttons()

    def on_key(self, event) -> None:
        match event.key:
            case "escape":
                self.dismiss(
                    DialogResponse(DialogResult.CANCELLED)
                )
                event.prevent_default()
                event.stop()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(
            DialogResponse(
                DialogResult.CONFIRMED, value=event.value
            )
        )
