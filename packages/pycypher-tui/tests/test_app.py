"""Tests for the main PyCypher TUI application."""

import pytest

from textual.app import App

from pycypher_tui.app import (
    PyCypherTUI,
    ModeIndicator,
    CommandLine,
    StatusBar,
)
from pycypher_tui.modes.base import ModeType


class TestPyCypherTUIInit:
    def test_app_is_textual_app(self):
        app = PyCypherTUI()
        assert isinstance(app, App)

    def test_default_config_path_none(self):
        app = PyCypherTUI()
        assert app.config_path is None

    def test_config_path_from_string(self):
        app = PyCypherTUI(config_path="/tmp/test.yaml")
        assert str(app.config_path) == "/tmp/test.yaml"

    def test_initial_mode_is_normal(self):
        app = PyCypherTUI()
        assert (
            app.mode_manager.current_type == ModeType.NORMAL
        )

    def test_app_title(self):
        app = PyCypherTUI()
        assert app.TITLE == "PyCypher ETL Configuration"


class TestModeIndicator:
    def test_default_values(self):
        widget = ModeIndicator()
        assert widget.mode_name == "NORMAL"
        assert widget.mode_color == "#7aa2f7"

    def test_render(self):
        widget = ModeIndicator()
        assert "NORMAL" in widget.render()


class TestCommandLine:
    def test_default_empty(self):
        widget = CommandLine()
        assert widget.text == ""

    def test_render_empty(self):
        widget = CommandLine()
        assert widget.render() == ""

    def test_render_with_text(self):
        widget = CommandLine()
        widget.text = ":wq"
        assert widget.render() == ":wq"


class TestPyCypherTUIAsync:
    """Async tests using Textual's pilot testing system."""

    @pytest.fixture
    def app(self):
        return PyCypherTUI()

    @pytest.mark.asyncio
    async def test_app_mounts(self, app):
        async with app.run_test() as pilot:
            # App should be running
            assert app.is_running

    @pytest.mark.asyncio
    async def test_initial_mode_indicator_shows_normal(
        self, app
    ):
        async with app.run_test() as pilot:
            indicator = app.query_one(
                "#mode-indicator", ModeIndicator
            )
            assert indicator.mode_name == "NORMAL"

    @pytest.mark.asyncio
    async def test_press_i_enters_insert_mode(self, app):
        async with app.run_test() as pilot:
            await pilot.press("i")
            assert (
                app.mode_manager.current_type
                == ModeType.INSERT
            )
            indicator = app.query_one(
                "#mode-indicator", ModeIndicator
            )
            assert indicator.mode_name == "INSERT"

    @pytest.mark.asyncio
    async def test_press_escape_returns_to_normal(self, app):
        async with app.run_test() as pilot:
            await pilot.press("i")
            assert (
                app.mode_manager.current_type
                == ModeType.INSERT
            )
            await pilot.press("escape")
            assert (
                app.mode_manager.current_type
                == ModeType.NORMAL
            )

    @pytest.mark.asyncio
    async def test_command_mode_shows_command_line(self, app):
        async with app.run_test() as pilot:
            await pilot.press("colon")
            cmd_line = app.query_one(
                "#command-line", CommandLine
            )
            assert cmd_line.has_class("visible")

    @pytest.mark.asyncio
    async def test_command_line_hidden_in_normal(self, app):
        async with app.run_test() as pilot:
            cmd_line = app.query_one(
                "#command-line", CommandLine
            )
            assert not cmd_line.has_class("visible")

    @pytest.mark.asyncio
    async def test_welcome_message_displayed(self, app):
        async with app.run_test() as pilot:
            welcome = app.query_one("#welcome-message")
            assert welcome is not None

    @pytest.mark.asyncio
    async def test_status_bar_present(self, app):
        async with app.run_test() as pilot:
            status = app.query_one(
                "#status-bar", StatusBar
            )
            assert status is not None

    @pytest.mark.asyncio
    async def test_quit_command(self, app):
        async with app.run_test() as pilot:
            await pilot.press("colon")
            await pilot.press("q")
            await pilot.press("enter")
            # App should have exited (or be exiting)

    @pytest.mark.asyncio
    async def test_mode_cycle_normal_visual_normal(self, app):
        async with app.run_test() as pilot:
            await pilot.press("v")
            assert (
                app.mode_manager.current_type
                == ModeType.VISUAL
            )
            await pilot.press("escape")
            assert (
                app.mode_manager.current_type
                == ModeType.NORMAL
            )

    @pytest.mark.asyncio
    async def test_config_path_shown_in_status(self):
        app = PyCypherTUI(config_path="/tmp/pipeline.yaml")
        async with app.run_test() as pilot:
            # Status bar should show the file path
            status = app.query_one(
                "#status-bar", StatusBar
            )
            assert status is not None
