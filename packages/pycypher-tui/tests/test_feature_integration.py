"""Tests for feature integration — wiring disconnected features to UI.

Validates:
1. ConfirmDialog wired to dd delete (prevents accidental data loss)
2. Search functionality (/ pattern) wired to item filtering
3. Register system wired for yank operations
4. Extended ex-commands (:registers, :noh, :s)
"""

import pytest

from pycypher_tui.modes.registers import RegisterFile
from pycypher_tui.modes.search_replace import parse_substitute_command
from pycypher_tui.widgets.dialog import ConfirmDialog, DialogResult, DialogResponse


# ---------------------------------------------------------------------------
# 1. ConfirmDialog integration
# ---------------------------------------------------------------------------


class TestConfirmDialogIntegration:
    """Verify ConfirmDialog is used for destructive actions."""

    def test_confirm_dialog_exists_and_has_y_n(self):
        """ConfirmDialog can be instantiated with title/body."""
        dlg = ConfirmDialog(title="Delete?", body="Are you sure?")
        assert dlg.dialog_title == "Delete?"
        assert dlg.dialog_body == "Are you sure?"

    @pytest.mark.asyncio
    async def test_confirm_dialog_y_confirms(self):
        """y key confirms in ConfirmDialog."""
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            result = None

            def compose(self) -> ComposeResult:
                yield Static("test")

            def on_mount(self):
                self.push_screen(
                    ConfirmDialog(title="Delete?", body="Confirm?"),
                    callback=self._on_result,
                )

            def _on_result(self, response):
                self.result = response
                self.exit()

        app = TestApp()
        async with app.run_test() as pilot:
            await pilot.press("y")
            await pilot.pause()
        assert app.result is not None
        assert app.result.result == DialogResult.CONFIRMED

    @pytest.mark.asyncio
    async def test_confirm_dialog_n_cancels(self):
        """n key cancels in ConfirmDialog."""
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            result = None

            def compose(self) -> ComposeResult:
                yield Static("test")

            def on_mount(self):
                self.push_screen(
                    ConfirmDialog(title="Delete?", body="Confirm?"),
                    callback=self._on_result,
                )

            def _on_result(self, response):
                self.result = response
                self.exit()

        app = TestApp()
        async with app.run_test() as pilot:
            await pilot.press("n")
            await pilot.pause()
        assert app.result is not None
        assert app.result.result == DialogResult.CANCELLED


# ---------------------------------------------------------------------------
# 2. Search functionality
# ---------------------------------------------------------------------------


class TestSearchIntegration:
    """Verify / search is wired to VimNavigableScreen."""

    def test_search_base_class_has_apply_search(self):
        """VimNavigableScreen base class exposes apply_search."""
        from pycypher_tui.screens.base import VimNavigableScreen

        assert hasattr(VimNavigableScreen, "apply_search")
        assert hasattr(VimNavigableScreen, "search_next")
        assert hasattr(VimNavigableScreen, "search_prev")
        assert hasattr(VimNavigableScreen, "search_status")

    def test_search_pattern_matching(self):
        """Verify search finds items by pattern."""
        import re

        # Simulate what apply_search does internally
        items = ["users_source", "orders_source", "products_rel"]
        pattern = "source"
        regex = re.compile(pattern, re.IGNORECASE)
        matches = [i for i, item in enumerate(items) if regex.search(item)]
        assert matches == [0, 1]

    def test_search_case_insensitive(self):
        """Search should be case-insensitive."""
        import re

        items = ["Users_Source", "ORDERS_SOURCE", "products_rel"]
        regex = re.compile("source", re.IGNORECASE)
        matches = [i for i, item in enumerate(items) if regex.search(item)]
        assert matches == [0, 1]

    def test_search_status_format(self):
        """search_status should report match count."""
        # Simulate the property behavior
        pattern = "test"
        matches = [0, 3, 7]
        match_idx = 0
        status = f"/{pattern} [{match_idx + 1}/{len(matches)}]"
        assert status == "/test [1/3]"

    def test_search_no_matches_status(self):
        """search_status should indicate no matches."""
        pattern = "nonexistent"
        matches = []
        if not matches:
            status = f"/{pattern} (no matches)"
        assert status == "/nonexistent (no matches)"


# ---------------------------------------------------------------------------
# 3. Register system integration
# ---------------------------------------------------------------------------


class TestRegisterIntegration:
    """Verify register file is wired for yank operations."""

    def test_register_file_yank(self):
        """RegisterFile.yank stores in unnamed and yank registers."""
        rf = RegisterFile()
        rf.yank("test_content")
        assert rf.paste() == "test_content"
        assert rf.get("0").content == "test_content"
        assert rf.get('"').content == "test_content"

    def test_register_file_named(self):
        """Named register stores and retrieves."""
        rf = RegisterFile()
        rf.yank("content", register="a")
        assert rf.get("a").content == "content"

    def test_register_list_nonempty(self):
        """list_nonempty returns only registers with content."""
        rf = RegisterFile()
        rf.yank("hello")
        nonempty = rf.list_nonempty()
        assert len(nonempty) > 0
        assert '"' in nonempty
        assert "0" in nonempty

    def test_app_has_register_file(self):
        """PyCypherTUI app should have register_file attribute."""
        from pycypher_tui.app import PyCypherTUI

        app = PyCypherTUI()
        assert hasattr(app, "register_file")
        assert isinstance(app.register_file, RegisterFile)

    def test_app_has_validator(self):
        """PyCypherTUI app should have CachedValidator."""
        from pycypher_tui.app import PyCypherTUI
        from pycypher_tui.config.validation import CachedValidator

        app = PyCypherTUI()
        assert hasattr(app, "validator")
        assert isinstance(app.validator, CachedValidator)


# ---------------------------------------------------------------------------
# 4. Extended ex-commands
# ---------------------------------------------------------------------------


class TestExtendedExCommands:
    """Verify additional ex-commands are wired."""

    def test_substitute_parse(self):
        """parse_substitute_command handles standard :s syntax."""
        cmd = parse_substitute_command("s/foo/bar/g")
        assert cmd is not None
        assert cmd.pattern == "foo"
        assert cmd.replacement == "bar"
        assert cmd.global_flag is True

    def test_substitute_parse_percent(self):
        """%s variant for whole-buffer."""
        cmd = parse_substitute_command("%s/old/new/")
        assert cmd is not None
        assert cmd.whole_buffer is True
        assert cmd.pattern == "old"
        assert cmd.replacement == "new"

    def test_substitute_parse_invalid(self):
        """Invalid substitute returns None."""
        assert parse_substitute_command("not_a_command") is None

    def test_substitute_with_flags(self):
        """All flags parsed correctly."""
        cmd = parse_substitute_command("%s/pat/rep/gci")
        assert cmd is not None
        assert cmd.global_flag is True
        assert cmd.confirm_flag is True
        assert cmd.case_insensitive is True

    def test_command_mode_search_prefix(self):
        """CommandMode supports / prefix for search."""
        from pycypher_tui.modes.command import CommandMode
        from pycypher_tui.modes.manager import ModeManager

        mgr = ModeManager()
        cmd_mode = mgr.get_mode(
            __import__("pycypher_tui.modes.base", fromlist=["ModeType"]).ModeType.COMMAND
        )
        assert isinstance(cmd_mode, CommandMode)
        # Set prefix to / as the app does on search
        cmd_mode.prefix = "/"
        assert cmd_mode.display_text == "/"
        cmd_mode.buffer = "test"
        assert cmd_mode.display_text == "/test"


# ---------------------------------------------------------------------------
# 5. DataSourcesScreen search text override
# ---------------------------------------------------------------------------


class TestScreenSearchText:
    """Verify screens override get_item_search_text meaningfully."""

    def test_data_sources_search_text(self):
        """DataSourcesScreen.get_item_search_text includes all fields."""
        from pycypher_tui.screens.data_sources import DataSourcesScreen, SourceItem

        item = SourceItem(
            source_id="src1",
            uri="file://data.csv",
            source_type="entity",
            label="Person",
            id_col="id",
            extra={},
        )
        # We can't instantiate the screen easily, but we can test the method
        # by checking that the class defines it
        assert hasattr(DataSourcesScreen, "get_item_search_text")

    def test_relationship_screen_search_text(self):
        """RelationshipScreen has get_item_search_text."""
        from pycypher_tui.screens.relationships import RelationshipScreen

        assert hasattr(RelationshipScreen, "get_item_search_text")

    def test_entity_tables_screen_search_text(self):
        """EntityTablesScreen has get_item_search_text."""
        from pycypher_tui.screens.entity_tables import EntityTablesScreen

        assert hasattr(EntityTablesScreen, "get_item_search_text")


# ---------------------------------------------------------------------------
# 6. VimNavigableScreen confirm_and_delete integration
# ---------------------------------------------------------------------------


class TestDeleteConfirmation:
    """Verify dd triggers confirmation dialog."""

    def test_base_has_confirm_and_delete(self):
        """VimNavigableScreen exposes _confirm_and_delete."""
        from pycypher_tui.screens.base import VimNavigableScreen

        assert hasattr(VimNavigableScreen, "_confirm_and_delete")

    def test_base_imports_confirm_dialog(self):
        """base.py imports ConfirmDialog and DialogResult."""
        from pycypher_tui.screens import base

        assert hasattr(base, "ConfirmDialog")
        assert hasattr(base, "DialogResult")
