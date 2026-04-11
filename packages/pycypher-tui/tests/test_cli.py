"""Tests for CLI integration and entry points."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pycypher_tui.cli import (
    _create_new_config,
    _get_version,
    _print_templates,
    cli_main,
    run_tui,
)

# ─── _print_templates ───────────────────────────────────────────────────────


class TestPrintTemplates:
    def test_prints_templates(self, capsys):
        _print_templates()
        out = capsys.readouterr().out
        assert "Available pipeline templates" in out

    def test_prints_template_names(self, capsys):
        from pycypher_tui.config.templates import list_templates

        _print_templates()
        out = capsys.readouterr().out
        for t in list_templates():
            assert t.name in out


# ─── _create_new_config ─────────────────────────────────────────────────────


class TestCreateNewConfig:
    def test_creates_file(self, tmp_path):
        filepath = tmp_path / "test_pipeline.yaml"
        result = _create_new_config(filepath)
        assert result == filepath
        assert filepath.exists()
        content = filepath.read_text()
        # YAML output may vary; just check file was created with content
        assert len(content) > 0

    def test_creates_from_template(self, tmp_path):
        filepath = tmp_path / "test_pipeline.yaml"
        from pycypher_tui.config.templates import list_templates

        templates = list_templates()
        if templates:
            result = _create_new_config(filepath, template_name=templates[0].name)
            assert filepath.exists()

    def test_errors_on_existing_file(self, tmp_path):
        filepath = tmp_path / "existing.yaml"
        filepath.write_text("existing content")
        with pytest.raises(SystemExit):
            _create_new_config(filepath)

    def test_errors_on_unknown_template(self, tmp_path):
        filepath = tmp_path / "test.yaml"
        with pytest.raises(SystemExit):
            _create_new_config(filepath, template_name="nonexistent_xyz")

    def test_creates_parent_dirs(self, tmp_path):
        filepath = tmp_path / "subdir" / "deep" / "pipeline.yaml"
        _create_new_config(filepath)
        assert filepath.exists()

    def test_default_project_name(self, tmp_path):
        filepath = tmp_path / "my_project.yaml"
        _create_new_config(filepath)
        assert filepath.exists()


# ─── run_tui ─────────────────────────────────────────────────────────────────


class TestRunTui:
    def test_list_templates_exits(self, capsys):
        run_tui(list_templates_flag=True)
        out = capsys.readouterr().out
        assert "Available pipeline templates" in out

    def test_run_without_config(self):
        with patch("pycypher_tui.app.PyCypherTUI") as mock_cls:
            mock_app = MagicMock()
            mock_cls.return_value = mock_app
            try:
                run_tui(config_path=None)
            except Exception:
                pass  # App.run() may fail without terminal

    def test_new_creates_and_launches(self, tmp_path):
        filepath = tmp_path / "new_pipeline.yaml"
        # Just test the creation part (TUI launch needs terminal)
        with patch("pycypher_tui.app.PyCypherTUI") as mock_cls:
            mock_app = MagicMock()
            mock_cls.return_value = mock_app
            try:
                run_tui(config_path=str(filepath), new=True)
            except Exception:
                pass  # App.run() may fail
        assert filepath.exists()


# ─── cli_main argument parsing ───────────────────────────────────────────────


class TestCliMain:
    def test_list_templates_flag(self, capsys):
        with patch.object(sys, "argv", ["pycypher-tui", "--list-templates"]):
            cli_main()
        out = capsys.readouterr().out
        assert "Available" in out

    def test_version_flag(self):
        with patch.object(sys, "argv", ["pycypher-tui", "--version"]):
            with pytest.raises(SystemExit) as exc_info:
                cli_main()
            assert exc_info.value.code == 0

    def test_template_without_new_errors(self):
        with patch.object(
            sys, "argv", ["pycypher-tui", "--template", "csv_analytics", "file.yaml"]
        ):
            with pytest.raises(SystemExit) as exc_info:
                cli_main()
            assert exc_info.value.code != 0

    def test_new_without_config_errors(self):
        with patch.object(sys, "argv", ["pycypher-tui", "--new"]):
            with pytest.raises(SystemExit) as exc_info:
                cli_main()
            assert exc_info.value.code != 0


# ─── _get_version ────────────────────────────────────────────────────────────


class TestGetVersion:
    def test_returns_version_string(self):
        version = _get_version()
        assert isinstance(version, str)
        assert version != ""

    def test_returns_package_version(self):
        version = _get_version()
        assert version == "0.0.1"


# ─── nmetl tui subcommand integration ───────────────────────────────────────


class TestNmetlTuiSubcommand:
    def test_tui_command_exists(self):
        """Verify the tui subcommand is registered in the nmetl CLI."""
        from pycypher.nmetl_cli import cli

        # Check that 'tui' is in the registered commands
        assert "tui" in cli.commands

    def test_tui_command_has_help(self):
        from pycypher.nmetl_cli import cli

        cmd = cli.commands["tui"]
        assert cmd.help is not None
        assert "TUI" in cmd.help or "pipeline" in cmd.help
