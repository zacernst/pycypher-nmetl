"""Cypher Query Editor Screen - full-featured editor with VIM keybindings.

Provides a screen for editing Cypher queries with syntax awareness,
autocomplete, query execution, and result display.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Label, Static

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.widgets.query_editor import CypherEditor

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a query execution."""

    query: str
    columns: list[str]
    rows: list[dict[str, Any]]
    error: str | None = None
    execution_time_ms: float = 0.0

    @property
    def is_error(self) -> bool:
        return self.error is not None

    @property
    def row_count(self) -> int:
        return len(self.rows)


class QueryEditorScreen(Screen):
    """Screen for editing and executing Cypher queries.

    VIM Navigation:
        Normal mode: Full VIM navigation (h/j/k/l, w/b, gg/G, etc.)
        Insert mode: Text editing with Escape to return
        i/a/o/O     - Enter insert mode
        dd          - Delete line
        yy/p        - Yank/paste line
        u/Ctrl+r    - Undo/redo
        Ctrl+Enter  - Execute query
        /pattern    - Search
        n/N         - Next/prev match
        Escape      - Return to normal mode (from insert)
        q           - Quit editor (normal mode, if not editing)
    """

    CSS = """
    QueryEditorScreen {
        layout: vertical;
    }

    #editor-header {
        width: 100%;
        height: 1;
        background: #24283b;
        color: #7aa2f7;
        text-style: bold;
        padding: 0 2;
    }

    #editor-container {
        width: 100%;
        height: 1fr;
        padding: 0 1;
    }

    #editor-widget {
        width: 100%;
        height: 1fr;
        color: #c0caf5;
        background: #1a1b26;
    }

    #result-panel {
        width: 100%;
        height: auto;
        max-height: 10;
        border-top: solid #414868;
        padding: 0 2;
        color: #a9b1d6;
    }

    .result-error {
        color: #f7768e;
    }

    .result-ok {
        color: #9ece6a;
    }

    #completions-panel {
        width: auto;
        max-width: 40;
        max-height: 8;
        display: none;
        border: solid #414868;
        background: #24283b;
        padding: 0 1;
    }

    #completions-panel.visible {
        display: block;
    }

    #editor-footer {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
    }

    #editor-mode-label {
        width: auto;
        text-style: bold;
        padding: 0 1;
    }

    .mode-normal {
        background: #7aa2f7;
        color: #1a1b26;
    }

    .mode-insert {
        background: #9ece6a;
        color: #1a1b26;
    }

    #editor-position-label {
        width: auto;
        color: #565f89;
        padding: 0 1;
    }

    #editor-hints-label {
        width: 1fr;
        color: #565f89;
    }
    """

    class QueryExecuted(Message):
        """Posted when a query is executed."""

        def __init__(self, result: QueryResult) -> None:
            super().__init__()
            self.result = result

    class EditorClosed(Message):
        """Posted when the editor screen is closed."""

        def __init__(self, query_text: str, query_id: str | None = None) -> None:
            super().__init__()
            self.query_text = query_text
            self.query_id = query_id

    def __init__(
        self,
        query_id: str | None = None,
        initial_text: str = "",
        config_manager: ConfigManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._query_id = query_id
        self._initial_text = initial_text
        self._config_manager = config_manager
        self._editor: CypherEditor | None = None
        self._last_result: QueryResult | None = None
        self._completions_visible = False

    def compose(self) -> ComposeResult:
        title = f"Query: {self._query_id}" if self._query_id else "New Query"
        yield Static(f" {title} ", id="editor-header")

        with Container(id="editor-container"):
            entity_types = self._get_entity_types()
            editor = CypherEditor(
                initial_text=self._initial_text,
                entity_types=entity_types,
                id="editor-widget",
            )
            self._editor = editor
            yield editor

        yield Static("", id="completions-panel")
        yield Static("", id="result-panel")

        with Container(id="editor-footer"):
            with Horizontal():
                yield Label("NORMAL", id="editor-mode-label", classes="mode-normal")
                yield Label("Ln 1, Col 1", id="editor-position-label")
                yield Label(
                    " i:insert  Ctrl+Enter:run  :w:save  q:close",
                    id="editor-hints-label",
                )

    def on_mount(self) -> None:
        self._update_position_display()

    def _get_entity_types(self) -> list[str]:
        """Extract entity types from config for autocomplete."""
        if not self._config_manager:
            return []
        cfg = self._config_manager.get_config()
        if not cfg.sources:
            return []
        return [e.entity_type for e in cfg.sources.entities]

    def on_key(self, event) -> None:
        """Route keys through the editor widget."""
        if not self._editor:
            return

        key = event.key

        # Ctrl+Enter executes query in any mode
        if key == "ctrl+enter":
            self._execute_query()
            event.prevent_default()
            event.stop()
            return

        # Ctrl+S saves query to config in any mode
        if key == "ctrl+s":
            self._save_query()
            event.prevent_default()
            event.stop()
            return

        # In normal mode, 'q' closes editor
        if self._editor.mode == "normal" and key == "q":
            self._close_editor()
            event.prevent_default()
            event.stop()
            return

        handled = self._editor.handle_key(key)
        if handled:
            event.prevent_default()
            event.stop()
            self._update_display()

    def _update_display(self) -> None:
        """Update all display elements after a key press."""
        if not self._editor:
            return

        self._update_mode_display()
        self._update_position_display()
        self._update_editor_content()

    def _update_mode_display(self) -> None:
        """Update mode indicator."""
        if not self._editor:
            return
        try:
            label = self.query_one("#editor-mode-label", Label)
            mode = self._editor.mode.upper()
            label.update(mode)
            label.remove_class("mode-normal", "mode-insert")
            label.add_class(f"mode-{self._editor.mode}")
        except NoMatches:
            logger.debug("_update_mode_display: #editor-mode-label not found")

        # Update hints based on mode
        try:
            hints = self.query_one("#editor-hints-label", Label)
            if self._editor.mode == "normal":
                hints.update(" i:insert  Ctrl+Enter:run  Ctrl+S:save  q:close")
            else:
                hints.update(" Esc:normal  Ctrl+Enter:run  Type to edit")
        except NoMatches:
            logger.debug("_update_mode_display: #editor-hints-label not found")

    def _update_position_display(self) -> None:
        """Update cursor position display."""
        if not self._editor:
            return
        try:
            label = self.query_one("#editor-position-label", Label)
            pos = self._editor.cursor
            label.update(f"Ln {pos.line + 1}, Col {pos.col + 1}")
        except NoMatches:
            logger.debug("_update_position_display: #editor-position-label not found")

    def _update_editor_content(self) -> None:
        """Re-render editor content."""
        if not self._editor:
            return
        try:
            widget = self.query_one("#editor-widget", CypherEditor)
            widget.refresh()
        except NoMatches:
            logger.debug("_update_editor_content: #editor-widget not found")

    def _execute_query(self) -> None:
        """Execute the current query and display results."""
        if not self._editor:
            return

        query = self._editor.text.strip()
        if not query:
            self._show_result_error("No query to execute")
            return

        # Try to parse/validate the query using pycypher
        result = self._run_query(query)
        self._last_result = result
        self._display_result(result)
        self.post_message(self.QueryExecuted(result))

    def _run_query(self, query: str) -> QueryResult:
        """Run a query and return the result."""
        import time

        start = time.monotonic()
        try:
            # Validate the query syntax using pycypher
            from pycypher import validate_query

            errors = validate_query(query)
            elapsed = (time.monotonic() - start) * 1000

            if errors:
                return QueryResult(
                    query=query,
                    columns=[],
                    rows=[],
                    error=errors[0].message,
                    execution_time_ms=elapsed,
                )

            return QueryResult(
                query=query,
                columns=[],
                rows=[],
                execution_time_ms=elapsed,
            )
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            return QueryResult(
                query=query,
                columns=[],
                rows=[],
                error=str(e),
                execution_time_ms=elapsed,
            )

    def _display_result(self, result: QueryResult) -> None:
        """Display query result in the result panel."""
        try:
            panel = self.query_one("#result-panel", Static)
            if result.is_error:
                panel.update(f" Error: {result.error}")
                panel.remove_class("result-ok")
                panel.add_class("result-error")
            else:
                msg = f" Query parsed successfully ({result.execution_time_ms:.1f}ms)"
                panel.update(msg)
                panel.remove_class("result-error")
                panel.add_class("result-ok")
        except NoMatches:
            logger.debug("_execute_query: #result-panel not found")

    def _show_result_error(self, message: str) -> None:
        """Show an error message in the result panel."""
        try:
            panel = self.query_one("#result-panel", Static)
            panel.update(f" {message}")
            panel.remove_class("result-ok")
            panel.add_class("result-error")
        except NoMatches:
            logger.debug("_show_result_error: #result-panel not found")

    def _save_query(self) -> None:
        """Save the current query text to the pipeline configuration."""
        if not self._editor or not self._config_manager:
            self._show_result_error("No config manager available")
            return

        query_text = self._editor.text.strip()
        if not query_text:
            self._show_result_error("No query to save")
            return

        if self._query_id:
            # Update existing query — remove and re-add
            try:
                self._config_manager.remove_query(self._query_id)
            except KeyError:
                logger.debug("_save_query: query %s not found for removal", self._query_id)
            self._config_manager.add_query(
                self._query_id, inline=query_text,
            )
            self._show_result_ok(f"Query '{self._query_id}' saved")
        else:
            # New query — generate an ID and save
            cfg = self._config_manager.get_config()
            existing_ids = {q.id for q in cfg.queries}
            idx = 1
            while f"query_{idx}" in existing_ids:
                idx += 1
            new_id = f"query_{idx}"
            self._config_manager.add_query(new_id, inline=query_text)
            self._query_id = new_id
            self._show_result_ok(f"Query saved as '{new_id}'")
            # Update header
            try:
                header = self.query_one("#editor-header", Static)
                header.update(f" Query: {new_id} ")
            except NoMatches:
                logger.debug("_save_query: #editor-header not found")

    def _show_result_ok(self, message: str) -> None:
        """Show a success message in the result panel."""
        try:
            panel = self.query_one("#result-panel", Static)
            panel.update(f" {message}")
            panel.remove_class("result-error")
            panel.add_class("result-ok")
        except NoMatches:
            logger.debug("_show_result_ok: #result-panel not found")

    def _close_editor(self) -> None:
        """Close the editor and return the query text."""
        if self._editor:
            self.post_message(
                self.EditorClosed(self._editor.text, self._query_id)
            )
        self.app.pop_screen()

    @property
    def query_text(self) -> str:
        """Current query text."""
        return self._editor.text if self._editor else ""

    @property
    def query_id(self) -> str | None:
        return self._query_id
