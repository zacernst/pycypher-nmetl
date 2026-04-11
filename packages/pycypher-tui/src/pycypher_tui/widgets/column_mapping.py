"""Visual column mapping validation component for relationship sources.

Provides an interactive ASCII-based visualization of column mappings between
relationship source and target columns, with real-time validation and editing
capabilities integrated with the existing TUI architecture.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.ingestion.introspector import DataSourceIntrospector
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.events import Key
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Input, Label, LoadingIndicator, Select, Static

logger = logging.getLogger(__name__)


@dataclass
class ColumnMapping:
    """Column mapping between source and target entities."""
    source_col: str
    target_col: str
    source_type: str | None = None
    target_type: str | None = None
    validation_status: str = "unknown"  # "valid", "warning", "error", "unknown"
    validation_message: str = ""


@dataclass
class MappingValidationResult:
    """Result of column mapping validation."""
    mappings: list[ColumnMapping] = field(default_factory=list)
    source_schema: dict[str, str] = field(default_factory=dict)
    target_entities: list[str] = field(default_factory=list)
    overall_status: str = "unknown"
    issues: list[dict[str, str]] = field(default_factory=list)


class ColumnMappingWidget(Static):
    """Interactive visual column mapping display with VIM-style navigation.

    Shows ASCII-based visualization of relationship column mappings with:
    - Source columns on left
    - Target columns on right
    - Connecting lines with validation status colors
    - Interactive editing capabilities
    """

    CSS = """
    ColumnMappingWidget {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .mapping-container {
        width: 100%;
        height: 100%;
    }

    .mapping-header {
        color: #e0af68;
        text-style: bold;
        margin-bottom: 1;
    }

    .column-section {
        width: 1fr;
        padding: 0 2;
    }

    .column-item {
        width: 100%;
        height: 1;
        margin-bottom: 0;
    }

    .column-item-selected {
        background: #283457;
        color: #7aa2f7;
    }

    .source-column {
        color: #9ece6a;
    }

    .target-column {
        color: #bb9af7;
    }

    .mapping-line {
        color: #565f89;
        width: 100%;
        text-align: center;
    }

    .mapping-line-valid {
        color: #9ece6a;
    }

    .mapping-line-warning {
        color: #e0af68;
    }

    .mapping-line-error {
        color: #f7768e;
    }

    .validation-status {
        width: 100%;
        margin-top: 1;
        padding: 1;
        border: solid #565f89;
    }

    .status-valid {
        border: solid #9ece6a;
        color: #9ece6a;
    }

    .status-warning {
        border: solid #e0af68;
        color: #e0af68;
    }

    .status-error {
        border: solid #f7768e;
        color: #f7768e;
    }

    .help-text {
        color: #565f89;
        text-style: italic;
        margin-top: 1;
    }

    .mapping-editor {
        width: 100%;
        padding: 1;
        margin: 1;
        border: solid #7aa2f7;
        background: #1a1b26;
    }

    .info-label {
        color: #7aa2f7;
        text-style: bold;
        width: 20;
    }

    #source-column-select, #target-column-select {
        width: 1fr;
        margin-left: 2;
    }
    """

    selected_index = reactive(0)

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._validation_result: MappingValidationResult | None = None
        self._relationship_sources: list[Any] = []
        self._editing_mode = False

    class MappingChanged(Message):
        """Message sent when column mapping is modified."""

        def __init__(self, source_id: str, new_mapping: dict[str, str]) -> None:
            super().__init__()
            self.source_id = source_id
            self.new_mapping = new_mapping

    def compose(self) -> ComposeResult:
        with Container(classes="mapping-container"):
            yield Label("Column Mapping Validation", classes="mapping-header")
            yield LoadingIndicator(id="mapping-loading")

    def update_relationship_sources(self, relationship_sources: list[Any]) -> None:
        """Update the widget with new relationship sources and validate mappings.

        This method performs blocking I/O (DataSourceIntrospector) and must
        be called from a background thread via ``run_worker(thread=True)``.
        """
        self._relationship_sources = relationship_sources

        if not relationship_sources:
            self.call_after_refresh(self._show_empty_state)
            return

        # Show loading
        self.call_after_refresh(self._show_loading)

        # Validate mappings (blocking I/O — runs in background thread)
        try:
            validation_result = self._validate_column_mappings()
            self._validation_result = validation_result
            self.call_after_refresh(self._update_display)
        except Exception as exc:
            logger.exception("Failed to validate column mappings: %s", exc)
            self.call_after_refresh(lambda: self._show_error(str(exc)))

    def _show_loading(self) -> None:
        """Show loading indicator."""
        try:
            container = self.query_one(".mapping-container")
            container.remove_children()
            container.mount(Label("Column Mapping Validation", classes="mapping-header"))
            container.mount(LoadingIndicator(id="mapping-loading-refresh"))
        except Exception as exc:
            logger.debug("Failed to show loading: %s", exc)

    def _show_empty_state(self) -> None:
        """Show empty state when no relationship sources."""
        try:
            container = self.query_one(".mapping-container")
            container.remove_children()
            container.mount(Label("Column Mapping Validation", classes="mapping-header"))
            container.mount(Label("No relationship sources to validate.", classes="help-text"))
        except Exception as exc:
            logger.debug("Failed to show empty state: %s", exc)

    def _show_error(self, error: str) -> None:
        """Show error message."""
        try:
            container = self.query_one(".mapping-container")
            container.remove_children()
            container.mount(Label("Column Mapping Validation", classes="mapping-header"))
            container.mount(Label(f"Error: {error}", classes="status-error"))
        except Exception as exc:
            logger.debug("Failed to show error: %s", exc)

    def _validate_column_mappings(self) -> MappingValidationResult:
        """Validate column mappings for all relationship sources (blocking I/O)."""
        result = MappingValidationResult()

        for source in self._relationship_sources:
            try:
                # Introspect the source data
                introspector = DataSourceIntrospector(source.uri)
                schema = introspector.get_schema()
                source_columns = {col["name"]: col["type"] for col in schema.columns}

                # Validate source and target column mappings
                mapping = ColumnMapping(
                    source_col=source.source_col,
                    target_col=source.target_col,
                    source_type=source_columns.get(source.source_col),
                    target_type=source_columns.get(source.target_col),
                )

                # Perform validation checks
                mapping.validation_status, mapping.validation_message = self._validate_mapping(
                    mapping, source_columns
                )

                result.mappings.append(mapping)
                result.source_schema.update(source_columns)

                # Add validation issues
                if mapping.validation_status == "error":
                    result.issues.append({
                        "type": "error",
                        "source_id": source.id,
                        "message": mapping.validation_message
                    })
                elif mapping.validation_status == "warning":
                    result.issues.append({
                        "type": "warning",
                        "source_id": source.id,
                        "message": mapping.validation_message
                    })

            except Exception as exc:
                logger.warning("Failed to validate source %s: %s", source.id, exc)
                result.issues.append({
                    "type": "error",
                    "source_id": source.id,
                    "message": f"Failed to load source: {exc}"
                })

        # Determine overall status
        if any(issue["type"] == "error" for issue in result.issues):
            result.overall_status = "error"
        elif any(issue["type"] == "warning" for issue in result.issues):
            result.overall_status = "warning"
        else:
            result.overall_status = "valid"

        return result

    def _validate_mapping(self, mapping: ColumnMapping, source_columns: dict[str, str]) -> tuple[str, str]:
        """Validate a single column mapping."""
        issues = []

        # Check if source column exists
        if mapping.source_col not in source_columns:
            return "error", f"Source column '{mapping.source_col}' not found in data"

        # Check if target column exists
        if mapping.target_col not in source_columns:
            return "error", f"Target column '{mapping.target_col}' not found in data"

        # Check type compatibility
        source_type = source_columns.get(mapping.source_col, "")
        target_type = source_columns.get(mapping.target_col, "")

        if source_type and target_type:
            if not self._types_compatible(source_type, target_type):
                return "warning", f"Type mismatch: {source_type} -> {target_type}"

        # Check for same column mapped to both
        if mapping.source_col == mapping.target_col:
            return "warning", "Source and target columns are the same"

        return "valid", "Mapping is valid"

    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two column types are compatible for mapping."""
        # Normalize types
        type1 = type1.lower()
        type2 = type2.lower()

        # Exact match
        if type1 == type2:
            return True

        # Compatible numeric types
        numeric_types = {"int", "integer", "bigint", "float", "double", "decimal", "number"}
        if type1 in numeric_types and type2 in numeric_types:
            return True

        # Compatible string types
        string_types = {"string", "varchar", "text", "char"}
        if type1 in string_types and type2 in string_types:
            return True

        return False

    def _update_display(self) -> None:
        """Update the visual display with validation results."""
        if not self._validation_result:
            return

        try:
            container = self.query_one(".mapping-container")
            container.remove_children()

            # Header with overall status
            status_class = f"status-{self._validation_result.overall_status}"
            container.mount(Label("Column Mapping Validation", classes="mapping-header"))
            container.mount(
                Label(
                    f"Overall Status: {self._validation_result.overall_status.upper()}",
                    classes=status_class
                )
            )

            # Mapping visualization
            if self._validation_result.mappings:
                container.mount(Label("", classes="help-text"))  # spacer
                container.mount(Label("Column Mappings:", classes="mapping-header"))

                for i, mapping in enumerate(self._validation_result.mappings):
                    self._render_mapping_row(container, mapping, i == self.selected_index)

            # Issues section
            if self._validation_result.issues:
                container.mount(Label("", classes="help-text"))  # spacer
                container.mount(Label("Validation Issues:", classes="mapping-header"))

                for issue in self._validation_result.issues:
                    issue_class = f"status-{issue['type']}"
                    container.mount(
                        Label(
                            f"  {issue['type'].upper()}: {issue['message']}",
                            classes=issue_class
                        )
                    )

            # Help text
            container.mount(Label("", classes="help-text"))  # spacer
            container.mount(
                Label(
                    "j/k: navigate  Enter: edit mapping  Esc: exit edit mode",
                    classes="help-text"
                )
            )

        except Exception as exc:
            logger.exception("Failed to update display: %s", exc)

    def _render_mapping_row(self, container: Container, mapping: ColumnMapping, selected: bool) -> None:
        """Render a single mapping row with ASCII visualization."""
        # Create horizontal layout for mapping visualization
        with Horizontal():
            # Source column (left side)
            source_classes = "source-column"
            if selected:
                source_classes += " column-item-selected"
            container.mount(
                Label(
                    f"{mapping.source_col} ({mapping.source_type or 'unknown'})",
                    classes=source_classes
                )
            )

            # Connection line with status
            line_classes = f"mapping-line-{mapping.validation_status}"
            arrow = "━━━━━>" if mapping.validation_status == "valid" else "━━━━━✗"
            container.mount(Label(arrow, classes=line_classes))

            # Target column (right side)
            target_classes = "target-column"
            if selected:
                target_classes += " column-item-selected"
            container.mount(
                Label(
                    f"{mapping.target_col} ({mapping.target_type or 'unknown'})",
                    classes=target_classes
                )
            )

        # Validation message below if not valid
        if mapping.validation_status != "valid":
            container.mount(
                Label(
                    f"    → {mapping.validation_message}",
                    classes=f"status-{mapping.validation_status}"
                )
            )

    def on_key(self, event: Key) -> None:
        """Handle VIM-style navigation keys."""
        if not self._validation_result or not self._validation_result.mappings:
            return

        if event.key == "j" or event.key == "down":
            # Move down
            self.selected_index = min(
                self.selected_index + 1,
                len(self._validation_result.mappings) - 1
            )
            event.prevent_default()
        elif event.key == "k" or event.key == "up":
            # Move up
            self.selected_index = max(self.selected_index - 1, 0)
            event.prevent_default()
        elif event.key == "enter" and not self._editing_mode:
            # Enter edit mode
            self._enter_edit_mode()
            event.prevent_default()
        elif event.key == "escape" and self._editing_mode:
            # Exit edit mode
            self._exit_edit_mode()
            event.prevent_default()

    def _enter_edit_mode(self) -> None:
        """Enter interactive editing mode for selected mapping."""
        if not self._validation_result or not self._validation_result.mappings:
            return

        self._editing_mode = True
        selected_mapping = self._validation_result.mappings[self.selected_index]

        # Show inline editor for column selection
        self._show_mapping_editor(selected_mapping)

    def _exit_edit_mode(self) -> None:
        """Exit interactive editing mode."""
        self._editing_mode = False
        self._update_display()  # Refresh to remove editor

    def _show_mapping_editor(self, mapping: ColumnMapping) -> None:
        """Show inline editor for modifying column mapping."""
        try:
            container = self.query_one(".mapping-container")

            # Find all available columns from source schema
            available_columns = list(self._validation_result.source_schema.keys())

            # Create editor container
            editor_container = Container(classes="mapping-editor")

            # Header
            editor_container.mount(
                Label("Edit Column Mapping (Tab to switch, Enter to save, Esc to cancel):",
                      classes="mapping-header")
            )

            # Source column selector
            with Horizontal():
                editor_container.mount(Label("Source Column:", classes="info-label"))
                source_select = Select(
                    [(col, col) for col in available_columns],
                    value=mapping.source_col,
                    id="source-column-select"
                )
                editor_container.mount(source_select)

            # Target column selector
            with Horizontal():
                editor_container.mount(Label("Target Column:", classes="info-label"))
                target_select = Select(
                    [(col, col) for col in available_columns],
                    value=mapping.target_col,
                    id="target-column-select"
                )
                editor_container.mount(target_select)

            # Add editor to main container
            container.mount(editor_container)

        except Exception as exc:
            logger.exception("Failed to show mapping editor: %s", exc)

    def _save_mapping_changes(self) -> None:
        """Save changes from the mapping editor."""
        try:
            source_select = self.query_one("#source-column-select", Select)
            target_select = self.query_one("#target-column-select", Select)

            new_source_col = source_select.value
            new_target_col = target_select.value

            # Update the mapping
            if (self._validation_result and
                0 <= self.selected_index < len(self._validation_result.mappings)):

                mapping = self._validation_result.mappings[self.selected_index]
                old_source = mapping.source_col
                old_target = mapping.target_col

                # Update mapping
                mapping.source_col = new_source_col
                mapping.target_col = new_target_col

                # Re-validate the updated mapping
                source_columns = self._validation_result.source_schema
                mapping.validation_status, mapping.validation_message = self._validate_mapping(
                    mapping, source_columns
                )

                # Send change notification
                self.post_message(
                    self.MappingChanged(
                        source_id=f"mapping_{self.selected_index}",
                        new_mapping={
                            "source_col": new_source_col,
                            "target_col": new_target_col
                        }
                    )
                )

                logger.info(
                    "Updated mapping %d: %s->%s to %s->%s",
                    self.selected_index, old_source, old_target, new_source_col, new_target_col
                )

        except Exception as exc:
            logger.exception("Failed to save mapping changes: %s", exc)

    def on_key(self, event: Key) -> None:
        """Handle VIM-style navigation keys."""
        if not self._validation_result or not self._validation_result.mappings:
            return

        if self._editing_mode:
            # Handle editing mode keys
            if event.key == "enter":
                self._save_mapping_changes()
                self._exit_edit_mode()
                event.prevent_default()
            elif event.key == "escape":
                self._exit_edit_mode()
                event.prevent_default()
            elif event.key == "tab":
                # Tab between source/target selectors
                try:
                    focused = self.app.focused
                    if focused and focused.id == "source-column-select":
                        target_select = self.query_one("#target-column-select", Select)
                        target_select.focus()
                    elif focused and focused.id == "target-column-select":
                        source_select = self.query_one("#source-column-select", Select)
                        source_select.focus()
                except Exception:
                    pass
                event.prevent_default()
        else:
            # Handle navigation mode keys
            if event.key == "j" or event.key == "down":
                # Move down
                self.selected_index = min(
                    self.selected_index + 1,
                    len(self._validation_result.mappings) - 1
                )
                event.prevent_default()
            elif event.key == "k" or event.key == "up":
                # Move up
                self.selected_index = max(self.selected_index - 1, 0)
                event.prevent_default()
            elif event.key == "enter":
                # Enter edit mode
                self._enter_edit_mode()
                event.prevent_default()

    def watch_selected_index(self, old_value: int, new_value: int) -> None:
        """React to selection changes."""
        if old_value != new_value and not self._editing_mode:
            self._update_display()