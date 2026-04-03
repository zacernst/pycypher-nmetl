"""VimEditableScreen — base for editor screens with INSERT mode field editing.

Extends VimNavigableScreen with:
- INSERT mode field-by-field editing with Tab/Shift+Tab navigation
- Dirty-state tracking via ConfigManager
- Undo/redo integration
- Save confirmation on exit when dirty

Editor screens display a form-like layout with editable fields.
Each field is represented by an EditableField dataclass, and the screen
manages focus cycling between fields.
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Input, Label, Static

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import VimNavigableScreen, BaseListItem, BaseDetailPanel
from pycypher_tui.widgets.dialog import ConfirmDialog, DialogResult

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class EditableField:
    """Descriptor for a single editable field in a form."""

    name: str  # field identifier (e.g., "source_id")
    label: str  # display label (e.g., "Source ID")
    value: str = ""  # current value
    placeholder: str = ""  # placeholder text
    required: bool = False
    readonly: bool = False
    validation_error: str | None = None


@dataclass
class FieldValidationResult:
    """Result of validating a single field."""

    valid: bool
    error: str | None = None


class EditableFieldWidget(Static):
    """Widget for a single editable field with label and input."""

    CSS = """
    EditableFieldWidget {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin-bottom: 1;
    }

    EditableFieldWidget .field-label {
        width: 100%;
        height: 1;
        color: #7aa2f7;
    }

    EditableFieldWidget .field-label-required {
        width: 100%;
        height: 1;
        color: #f7768e;
    }

    EditableFieldWidget .field-input {
        width: 100%;
        height: 3;
    }

    EditableFieldWidget .field-error {
        width: 100%;
        height: 1;
        color: #f7768e;
    }

    EditableFieldWidget .field-readonly {
        width: 100%;
        height: 1;
        color: #565f89;
        padding: 0 1;
    }

    EditableFieldWidget.field-focused .field-label {
        text-style: bold;
    }
    """

    focused_field: reactive[bool] = reactive(False)

    def __init__(self, field_def: EditableField, **kwargs) -> None:
        super().__init__(**kwargs)
        self.field_def = field_def
        self._input: Input | None = None

    def compose(self) -> ComposeResult:
        label_class = "field-label-required" if self.field_def.required else "field-label"
        required_marker = " *" if self.field_def.required else ""
        yield Label(
            f"{self.field_def.label}{required_marker}:",
            classes=label_class,
        )

        if self.field_def.readonly:
            yield Label(
                self.field_def.value or "(empty)",
                classes="field-readonly",
            )
        else:
            self._input = Input(
                value=self.field_def.value,
                placeholder=self.field_def.placeholder,
                classes="field-input",
                id=f"input-{self.field_def.name}",
            )
            yield self._input

        if self.field_def.validation_error:
            yield Label(
                f"  {self.field_def.validation_error}",
                classes="field-error",
            )

    def watch_focused_field(self, focused: bool) -> None:
        if focused:
            self.add_class("field-focused")
            if self._input is not None:
                self._input.focus()
        else:
            self.remove_class("field-focused")

    @property
    def current_value(self) -> str:
        """Get the current input value."""
        if self._input is not None:
            return self._input.value
        return self.field_def.value

    def set_error(self, error: str | None) -> None:
        """Update the validation error display."""
        self.field_def.validation_error = error
        # Remove existing error labels
        for child in self.query(".field-error"):
            child.remove()
        if error:
            self.mount(Label(f"  {error}", classes="field-error"))


class ValidationPanel(Static):
    """Panel showing form-level validation results."""

    CSS = """
    ValidationPanel {
        width: 100%;
        height: auto;
        padding: 1 2;
        border-top: solid #283457;
        margin-top: 1;
    }

    ValidationPanel .validation-header {
        color: #e0af68;
        text-style: bold;
        width: 100%;
        margin-bottom: 1;
    }

    ValidationPanel .validation-pass {
        color: #9ece6a;
    }

    ValidationPanel .validation-warn {
        color: #e0af68;
    }

    ValidationPanel .validation-error {
        color: #f7768e;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("Validation", classes="validation-header")

    def update_issues(self, issues: list[tuple[str, str, str]]) -> None:
        """Update validation display.

        Args:
            issues: List of (level, icon, message) tuples.
        """
        self.remove_children()
        self.mount(Label("Validation", classes="validation-header"))

        if not issues:
            self.mount(Label("[ok] All fields valid", classes="validation-pass"))
            return

        for level, icon, message in issues:
            css_class = f"validation-{level}"
            self.mount(Label(f"{icon} {message}", classes=css_class))


class VimEditableScreen(VimNavigableScreen[T], Generic[T]):
    """Base for editor screens with INSERT mode field editing.

    Extends VimNavigableScreen with form field management, dirty-state
    tracking, and save confirmation on exit.

    Subclasses must implement:
    - ``get_fields(item)`` — return EditableField list for an item
    - ``validate_field(name, value)`` — validate a single field value
    - ``apply_changes(item, field_values)`` — persist field changes
    - Plus all VimNavigableScreen abstract methods

    Key bindings in editor context:
    - Tab: next field
    - Shift+Tab: previous field
    - Enter: submit form (if validation passes)
    - Escape: cancel (confirm if dirty)
    - Ctrl+U: clear current field
    """

    class FormSubmitted(Message):
        """Emitted when the form is successfully submitted."""

        def __init__(self, item: object) -> None:
            super().__init__()
            self.item = item

    class FormCancelled(Message):
        """Emitted when the form is cancelled."""

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._fields: list[EditableField] = []
        self._field_widgets: list[EditableFieldWidget] = []
        self._field_cursor: int = 0
        self._editing_item: T | None = None
        self._initial_values: dict[str, str] = {}
        self._validation_panel: ValidationPanel | None = None

    # -- Subclass interface -------------------------------------------------

    @abstractmethod
    def get_fields(self, item: T | None) -> list[EditableField]:
        """Return the list of editable fields for the given item.

        If item is None, return fields for a new item (add mode).
        """

    @abstractmethod
    def validate_field(self, name: str, value: str) -> FieldValidationResult:
        """Validate a single field value.

        Returns a FieldValidationResult indicating validity and any error.
        """

    @abstractmethod
    def apply_changes(self, item: T | None, field_values: dict[str, str]) -> None:
        """Persist the form field values to the config.

        If item is None, create a new item. Otherwise update existing.
        """

    # -- Dirty state --------------------------------------------------------

    @property
    def is_form_dirty(self) -> bool:
        """True if any field value differs from the initial value."""
        for widget in self._field_widgets:
            name = widget.field_def.name
            if widget.current_value != self._initial_values.get(name, ""):
                return True
        return False

    # -- Field management ---------------------------------------------------

    def start_editing(self, item: T | None = None) -> None:
        """Begin editing an item (or create new if None).

        Populates fields and captures initial values for dirty tracking.
        """
        self._editing_item = item
        self._fields = self.get_fields(item)
        self._initial_values = {f.name: f.value for f in self._fields}
        self._field_cursor = 0

    def _get_field_values(self) -> dict[str, str]:
        """Collect current values from all field widgets."""
        return {
            widget.field_def.name: widget.current_value
            for widget in self._field_widgets
        }

    def _focus_field(self, index: int) -> None:
        """Move field focus to the given index."""
        if not self._field_widgets:
            return

        # Skip readonly fields
        attempts = 0
        while attempts < len(self._field_widgets):
            idx = index % len(self._field_widgets)
            if not self._field_widgets[idx].field_def.readonly:
                break
            index += 1
            attempts += 1
        else:
            return  # all fields readonly

        old = self._field_cursor
        self._field_cursor = idx

        if 0 <= old < len(self._field_widgets):
            self._field_widgets[old].focused_field = False
        self._field_widgets[self._field_cursor].focused_field = True

    def next_field(self) -> None:
        """Move to the next editable field."""
        self._focus_field(self._field_cursor + 1)

    def prev_field(self) -> None:
        """Move to the previous editable field."""
        self._focus_field(self._field_cursor - 1)

    def clear_current_field(self) -> None:
        """Clear the current field's value."""
        if 0 <= self._field_cursor < len(self._field_widgets):
            widget = self._field_widgets[self._field_cursor]
            if widget._input is not None and not widget.field_def.readonly:
                widget._input.value = ""

    # -- Validation ---------------------------------------------------------

    def validate_all_fields(self) -> bool:
        """Run validation on all fields. Returns True if all pass."""
        all_valid = True
        issues: list[tuple[str, str, str]] = []

        for widget in self._field_widgets:
            if widget.field_def.readonly:
                continue

            result = self.validate_field(widget.field_def.name, widget.current_value)
            widget.set_error(result.error)

            if result.valid:
                issues.append(("pass", "[ok]", f"{widget.field_def.label} valid"))
            else:
                all_valid = False
                issues.append(
                    ("error", "[XX]", f"{widget.field_def.label}: {result.error}")
                )

        # Check required fields
        for widget in self._field_widgets:
            if widget.field_def.required and not widget.current_value.strip():
                all_valid = False
                issues.append(
                    ("error", "[XX]", f"{widget.field_def.label} is required")
                )
                widget.set_error("This field is required")

        if self._validation_panel is not None:
            self._validation_panel.update_issues(issues)

        return all_valid

    # -- Form actions -------------------------------------------------------

    def submit_form(self) -> None:
        """Validate and submit the form."""
        if not self.validate_all_fields():
            return

        field_values = self._get_field_values()
        self.apply_changes(self._editing_item, field_values)
        self.post_message(self.FormSubmitted(self._editing_item))

    def cancel_form(self) -> None:
        """Cancel editing, with confirmation if dirty."""
        if self.is_form_dirty:
            def _on_confirm(response):
                if response.result == DialogResult.CONFIRMED:
                    self.post_message(self.FormCancelled())

            self.app.push_screen(
                ConfirmDialog(
                    title="Unsaved Changes",
                    body="Discard unsaved changes?",
                ),
                callback=_on_confirm,
            )
        else:
            self.post_message(self.FormCancelled())

    # -- Undo/redo integration ----------------------------------------------

    def undo(self) -> None:
        """Undo the last config change."""
        if self.config_manager.can_undo():
            self.config_manager.undo()
            self.run_worker(self.refresh_from_config(), exclusive=True)

    def redo(self) -> None:
        """Redo the last undone config change."""
        if self.config_manager.can_redo():
            self.config_manager.redo()
            self.run_worker(self.refresh_from_config(), exclusive=True)

    # -- Key handling -------------------------------------------------------

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"tab", "shift+tab", "ctrl+u", "escape"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "tab":
                self.next_field()
                return True
            case "shift+tab":
                self.prev_field()
                return True
            case "ctrl+u":
                self.clear_current_field()
                return True
            case "escape":
                self.cancel_form()
                return True
            case _:
                return False

    def _dispatch_command(self, command: str | None) -> None:
        """Extend command dispatch with editor-specific commands."""
        if command is None:
            return

        match command:
            case "action:confirm":
                self.submit_form()
            case "edit:undo":
                self.undo()
            case "edit:redo":
                self.redo()
            case _:
                super()._dispatch_command(command)
