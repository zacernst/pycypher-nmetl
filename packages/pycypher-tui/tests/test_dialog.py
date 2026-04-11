"""Tests for the modal dialog system."""

import pytest

from pycypher_tui.widgets.dialog import (
    ConfirmDialog,
    DialogResponse,
    DialogResult,
    InputDialog,
    VimDialog,
)


class TestDialogResponse:
    def test_confirmed(self):
        r = DialogResponse(DialogResult.CONFIRMED)
        assert r.result == DialogResult.CONFIRMED
        assert r.value is None

    def test_confirmed_with_value(self):
        r = DialogResponse(DialogResult.CONFIRMED, value="test")
        assert r.value == "test"

    def test_cancelled(self):
        r = DialogResponse(DialogResult.CANCELLED)
        assert r.result == DialogResult.CANCELLED


class TestVimDialogInit:
    def test_title_and_body(self):
        d = VimDialog(title="My Title", body="My Body")
        assert d.dialog_title == "My Title"
        assert d.dialog_body == "My Body"


class TestConfirmDialogInit:
    def test_creates(self):
        d = ConfirmDialog(title="Delete?", body="Are you sure?")
        assert d.dialog_title == "Delete?"


class TestInputDialogInit:
    def test_creates_with_defaults(self):
        d = InputDialog(
            title="Enter name",
            placeholder="Name...",
            default_value="default",
        )
        assert d.dialog_title == "Enter name"
        assert d.placeholder == "Name..."
        assert d.default_value == "default"


class TestDialogAsync:
    """Async tests using Textual pilot."""

    @pytest.mark.asyncio
    async def test_confirm_dialog_y_confirms(self):
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            result = None

            def compose(self) -> ComposeResult:
                yield Static("test")

            def on_mount(self):
                self.push_screen(
                    ConfirmDialog(
                        title="Test", body="Confirm?"
                    ),
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
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            result = None

            def compose(self) -> ComposeResult:
                yield Static("test")

            def on_mount(self):
                self.push_screen(
                    ConfirmDialog(
                        title="Test", body="Confirm?"
                    ),
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

    @pytest.mark.asyncio
    async def test_confirm_dialog_escape_cancels(self):
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            result = None

            def compose(self) -> ComposeResult:
                yield Static("test")

            def on_mount(self):
                self.push_screen(
                    ConfirmDialog(
                        title="Test", body="Confirm?"
                    ),
                    callback=self._on_result,
                )

            def _on_result(self, response):
                self.result = response
                self.exit()

        app = TestApp()
        async with app.run_test() as pilot:
            await pilot.press("escape")
            await pilot.pause()
        assert app.result is not None
        assert app.result.result == DialogResult.CANCELLED

    @pytest.mark.asyncio
    async def test_vim_dialog_q_cancels(self):
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            result = None

            def compose(self) -> ComposeResult:
                yield Static("test")

            def on_mount(self):
                self.push_screen(
                    VimDialog(title="Test", body="Body"),
                    callback=self._on_result,
                )

            def _on_result(self, response):
                self.result = response
                self.exit()

        app = TestApp()
        async with app.run_test() as pilot:
            await pilot.press("q")
            await pilot.pause()
        assert app.result is not None
        assert app.result.result == DialogResult.CANCELLED
