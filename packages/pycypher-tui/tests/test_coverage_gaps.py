"""Tests filling coverage gaps in TUI foundation components.

Covers edge cases in:
- ConfigManager: error paths, from_config, remove operations
- CachedValidator: edge cases
- Templates: all templates, error paths
- App: ex-command execution, StatusBar methods
- ModeManager: multiple listeners, style_color
- Integration: mode transitions with app state
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    ProjectConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)

from pycypher_tui.app import (
    CommandLine,
    ModeIndicator,
    PyCypherTUI,
    StatusBar,
)
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import (
    PipelineTemplate,
    get_template,
    list_templates,
)
from pycypher_tui.config.validation import CachedValidator
from pycypher_tui.modes.base import ModeType
from pycypher_tui.modes.manager import ModeManager
from pycypher_tui.modes.normal import NormalMode

# ===========================================================================
# ConfigManager edge cases
# ===========================================================================


class TestConfigManagerEdgeCases:
    """Edge cases not covered in existing test_config.py."""

    def test_save_without_path_raises(self):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        with pytest.raises(ValueError, match="No file path"):
            mgr.save()

    def test_from_config(self):
        config = PipelineConfig(
            version="1.0",
            project=ProjectConfig(name="test"),
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="p", uri="file:///p.csv", entity_type="Person"
                    )
                ]
            ),
        )
        mgr = ConfigManager.from_config(config)
        assert not mgr.is_empty()
        assert mgr.get_config().project.name == "test"

    def test_is_empty_with_only_relationships(self):
        mgr = ConfigManager()
        mgr.add_relationship_source(
            "rel", "file:///r.csv", "KNOWS", "src", "tgt"
        )
        assert not mgr.is_empty()

    def test_is_empty_with_only_queries(self):
        mgr = ConfigManager()
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        assert not mgr.is_empty()

    def test_is_empty_with_only_outputs(self):
        mgr = ConfigManager()
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.add_output("q1", "file:///out.csv")
        assert not mgr.is_empty()

    def test_remove_relationship_source(self):
        mgr = ConfigManager()
        mgr.add_relationship_source(
            "rel", "file:///r.csv", "KNOWS", "src", "tgt"
        )
        assert len(mgr.get_config().sources.relationships) == 1
        mgr.remove_relationship_source("rel")
        assert len(mgr.get_config().sources.relationships) == 0

    def test_remove_output(self):
        mgr = ConfigManager()
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.add_output("q1", "file:///out.csv")
        assert len(mgr.get_config().output) == 1
        mgr.remove_output("q1", "file:///out.csv")
        assert len(mgr.get_config().output) == 0

    def test_can_undo_on_empty(self):
        mgr = ConfigManager()
        assert not mgr.can_undo()

    def test_can_redo_on_empty(self):
        mgr = ConfigManager()
        assert not mgr.can_redo()

    def test_save_then_not_dirty(self, tmp_path: Path):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        assert mgr.is_dirty()
        mgr.save(tmp_path / "out.yaml")
        assert not mgr.is_dirty()

    def test_save_overwrites_existing(self, tmp_path: Path):
        path = tmp_path / "pipeline.yaml"
        mgr = ConfigManager()
        mgr.add_entity_source("p1", "file:///p1.csv", "Person")
        mgr.save(path)

        mgr2 = ConfigManager.from_file(path)
        mgr2.add_entity_source("p2", "file:///p2.csv", "Product")
        mgr2.save(path)

        loaded = yaml.safe_load(path.read_text())
        entities = loaded["sources"]["entities"]
        assert len(entities) == 2

    def test_save_uses_stored_path_from_from_file(self, tmp_path: Path):
        path = tmp_path / "pipeline.yaml"
        path.write_text(
            yaml.dump({"version": "1.0"}),
            encoding="utf-8",
        )
        mgr = ConfigManager.from_file(path)
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        # Should use the path from from_file()
        mgr.save()
        loaded = yaml.safe_load(path.read_text())
        assert loaded["sources"]["entities"][0]["id"] == "p"

    def test_add_query_with_source_file(self):
        mgr = ConfigManager()
        mgr.add_query("q1", source="queries/q1.cypher", description="Test query")
        cfg = mgr.get_config()
        assert len(cfg.queries) == 1
        assert cfg.queries[0].source == "queries/q1.cypher"

    def test_add_entity_source_with_schema_hints(self):
        mgr = ConfigManager()
        mgr.add_entity_source(
            "events",
            "file:///events.csv",
            "Event",
            id_col="event_id",
            schema_hints={"timestamp": "TIMESTAMP"},
        )
        cfg = mgr.get_config()
        entity = cfg.sources.entities[0]
        assert entity.id_col == "event_id"
        assert entity.schema_hints == {"timestamp": "TIMESTAMP"}

    def test_multiple_undo_redo_cycle(self):
        mgr = ConfigManager()
        mgr.add_entity_source("a", "file:///a.csv", "A")
        mgr.add_entity_source("b", "file:///b.csv", "B")
        mgr.add_entity_source("c", "file:///c.csv", "C")

        assert len(mgr.get_config().sources.entities) == 3
        mgr.undo()
        assert len(mgr.get_config().sources.entities) == 2
        mgr.undo()
        assert len(mgr.get_config().sources.entities) == 1
        mgr.redo()
        assert len(mgr.get_config().sources.entities) == 2
        mgr.redo()
        assert len(mgr.get_config().sources.entities) == 3


# ===========================================================================
# Template edge cases
# ===========================================================================


class TestTemplateEdgeCases:
    """Edge cases for pipeline templates."""

    def test_template_without_builder_raises(self):
        t = PipelineTemplate(
            name="broken",
            description="No builder",
            category="test",
        )
        with pytest.raises(RuntimeError, match="has no builder"):
            t.instantiate(project_name="x")

    def test_time_series_template(self):
        t = get_template("time_series")
        assert t is not None
        config = t.instantiate(
            project_name="ts_test",
            data_dir="/tmp/data",
        )
        assert isinstance(config, PipelineConfig)
        assert config.project.name == "ts_test"
        assert len(config.sources.entities) >= 1
        # Should have schema hints for timestamp
        event_source = next(
            s for s in config.sources.entities if s.id == "events"
        )
        assert event_source.schema_hints is not None
        assert "timestamp" in event_source.schema_hints

    def test_all_templates_instantiate_successfully(self):
        for template in list_templates():
            config = template.instantiate(
                project_name="test",
                data_dir="/data",
            )
            assert isinstance(config, PipelineConfig)
            assert config.version == "1.0"

    def test_all_templates_have_categories(self):
        categories = {t.category for t in list_templates()}
        assert len(categories) >= 1
        assert all(isinstance(c, str) for c in categories)

    def test_template_parameters_list(self):
        for template in list_templates():
            assert "project_name" in template.parameters
            assert "data_dir" in template.parameters

    def test_ecommerce_has_relationships(self):
        t = get_template("ecommerce_pipeline")
        config = t.instantiate()
        assert len(config.sources.relationships) >= 1

    def test_social_network_has_multiple_relationships(self):
        t = get_template("social_network")
        config = t.instantiate()
        assert len(config.sources.relationships) >= 2

    def test_csv_analytics_has_output(self):
        t = get_template("csv_analytics")
        config = t.instantiate()
        assert len(config.output) >= 1

    def test_ecommerce_has_multiple_queries(self):
        t = get_template("ecommerce_pipeline")
        config = t.instantiate()
        assert len(config.queries) >= 2

    def test_template_default_data_dir(self):
        t = get_template("csv_analytics")
        config = t.instantiate(project_name="test")
        # Default data_dir should be "data"
        assert any("data/" in s.uri for s in config.sources.entities)

    def test_template_frozen_dataclass(self):
        t = get_template("csv_analytics")
        with pytest.raises(AttributeError):
            t.name = "changed"


# ===========================================================================
# ModeManager edge cases
# ===========================================================================


class TestModeManagerEdgeCases:
    """Edge cases for mode manager not in existing tests."""

    def test_multiple_listeners(self):
        mgr = ModeManager()
        calls1 = []
        calls2 = []
        mgr.add_listener(lambda o, n: calls1.append((o, n)))
        mgr.add_listener(lambda o, n: calls2.append((o, n)))
        mgr.transition_to(ModeType.INSERT)
        assert len(calls1) == 1
        assert len(calls2) == 1

    def test_style_color_per_mode(self):
        mgr = ModeManager()
        assert mgr.style_color == "#7aa2f7"  # NORMAL
        mgr.transition_to(ModeType.INSERT)
        assert mgr.style_color == "#9ece6a"
        mgr.transition_to(ModeType.NORMAL)
        mgr.transition_to(ModeType.VISUAL)
        assert mgr.style_color == "#bb9af7"
        mgr.transition_to(ModeType.NORMAL)
        mgr.transition_to(ModeType.COMMAND)
        assert mgr.style_color == "#e0af68"

    def test_ctrl_f_not_handled_by_normal_mode(self):
        """Ctrl+F/B are handled at widget level, not mode level."""
        mgr = ModeManager()
        result = mgr.handle_key("ctrl+f")
        assert result.handled is False

    def test_ctrl_b_not_handled_by_normal_mode(self):
        """Ctrl+F/B are handled at widget level, not mode level."""
        mgr = ModeManager()
        result = mgr.handle_key("ctrl+b")
        assert result.handled is False

    def test_rapid_mode_transitions(self):
        """Test that rapid mode transitions don't corrupt state."""
        mgr = ModeManager()
        for _ in range(100):
            mgr.handle_key("i")
            assert mgr.current_type == ModeType.INSERT
            mgr.handle_key("escape")
            assert mgr.current_type == ModeType.NORMAL

    def test_command_mode_full_command_flow(self):
        """Test entering command, typing, and executing."""
        mgr = ModeManager()
        mgr.handle_key("colon")
        assert mgr.current_type == ModeType.COMMAND
        mgr.handle_key("w")
        mgr.handle_key("q")
        result = mgr.handle_key("enter")
        assert result.command == "ex::wq"
        assert mgr.current_type == ModeType.NORMAL


# ===========================================================================
# NormalMode additional coverage
# ===========================================================================


class TestNormalModeAdditional:
    """Additional normal mode tests."""

    @pytest.fixture
    def normal(self):
        mgr = ModeManager()
        return mgr.get_mode(ModeType.NORMAL)

    def test_g_then_non_g_clears_pending(self, normal):
        """g followed by a non-g key should not handle."""
        r1 = normal.handle_key("g")
        assert r1.pending is True
        r2 = normal.handle_key("j")
        assert r2.handled is False

    def test_d_then_non_d_clears_pending(self, normal):
        """d followed by a non-d key should not handle."""
        r1 = normal.handle_key("d")
        assert r1.pending is True
        r2 = normal.handle_key("j")
        assert r2.handled is False

    def test_ctrl_f_not_handled_at_mode_level(self, normal):
        """Ctrl+F page navigation is handled at widget level."""
        result = normal.handle_key("ctrl+f")
        assert result.handled is False

    def test_ctrl_b_not_handled_at_mode_level(self, normal):
        """Ctrl+B page navigation is handled at widget level."""
        result = normal.handle_key("ctrl+b")
        assert result.handled is False


# ===========================================================================
# StatusBar widget tests
# ===========================================================================


class TestStatusBarWidget:
    """Tests for StatusBar reactive properties."""

    def test_default_file_path_empty(self):
        sb = StatusBar()
        assert sb.file_path == ""

    def test_default_validation_empty(self):
        sb = StatusBar()
        assert sb.validation_status == ""


# ===========================================================================
# ModeIndicator widget tests
# ===========================================================================


class TestModeIndicatorWidget:
    """Additional ModeIndicator tests."""

    def test_render_with_custom_mode(self):
        widget = ModeIndicator()
        widget.mode_name = "INSERT"
        assert "INSERT" in widget.render()

    def test_render_with_visual_mode(self):
        widget = ModeIndicator()
        widget.mode_name = "VISUAL"
        assert "VISUAL" in widget.render()


# ===========================================================================
# App async edge cases
# ===========================================================================


class TestAppAsyncEdgeCases:
    """Async tests for app functionality gaps."""

    @pytest.fixture
    def app(self):
        return PyCypherTUI()

    @pytest.mark.asyncio
    async def test_open_file_command_unit(self):
        """Test :e command via direct _execute_command call."""
        app = PyCypherTUI()
        async with app.run_test():
            await app._execute_command("ex::e test.yaml")
            assert str(app.config_path) == "test.yaml"

    @pytest.mark.asyncio
    async def test_execute_ex_quit(self):
        """Test :q command parses correctly."""
        app = PyCypherTUI()
        async with app.run_test():
            # Just verify it doesn't raise
            await app._execute_command("ex::w")
            await app._execute_command("ex::wq")

    @pytest.mark.asyncio
    async def test_visual_mode_indicator_updates(self, app):
        async with app.run_test() as pilot:
            await pilot.press("v")
            indicator = app.query_one(
                "#mode-indicator", ModeIndicator
            )
            assert indicator.mode_name == "VISUAL"

    @pytest.mark.asyncio
    async def test_command_mode_indicator_updates(self, app):
        async with app.run_test() as pilot:
            await pilot.press("colon")
            indicator = app.query_one(
                "#mode-indicator", ModeIndicator
            )
            assert indicator.mode_name == "COMMAND"

    @pytest.mark.asyncio
    async def test_escape_from_command_hides_command_line(self, app):
        async with app.run_test() as pilot:
            await pilot.press("colon")
            cmd_line = app.query_one("#command-line", CommandLine)
            assert cmd_line.has_class("visible")
            await pilot.press("escape")
            assert not cmd_line.has_class("visible")

    @pytest.mark.asyncio
    async def test_mode_cycle_all_modes(self, app):
        """Cycle through all four modes and back."""
        async with app.run_test() as pilot:
            # Normal -> Insert
            await pilot.press("i")
            assert app.mode_manager.current_type == ModeType.INSERT
            # Insert -> Normal
            await pilot.press("escape")
            assert app.mode_manager.current_type == ModeType.NORMAL
            # Normal -> Visual
            await pilot.press("v")
            assert app.mode_manager.current_type == ModeType.VISUAL
            # Visual -> Normal
            await pilot.press("escape")
            assert app.mode_manager.current_type == ModeType.NORMAL
            # Normal -> Command
            await pilot.press("colon")
            assert app.mode_manager.current_type == ModeType.COMMAND
            # Command -> Normal
            await pilot.press("escape")
            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_command_line_shows_typed_text(self, app):
        async with app.run_test() as pilot:
            await pilot.press("colon")
            await pilot.press("w")
            cmd_line = app.query_one("#command-line", CommandLine)
            assert "w" in cmd_line.text


# ===========================================================================
# CachedValidator edge cases
# ===========================================================================


class TestCachedValidatorEdgeCases:
    """Edge cases for the cached validator."""

    def test_validate_empty_config(self):
        cfg = PipelineConfig(version="1.0")
        validator = CachedValidator()
        result = validator.validate(cfg)
        assert result.is_valid

    def test_cache_key_changes_with_project(self):
        cfg1 = PipelineConfig(version="1.0")
        cfg2 = PipelineConfig(
            version="1.0",
            project=ProjectConfig(name="test"),
        )
        validator = CachedValidator()
        r1 = validator.validate(cfg1)
        r2 = validator.validate(cfg2)
        assert r1 is not r2

    def test_clear_cache_then_revalidate(self):
        cfg = PipelineConfig(version="1.0")
        validator = CachedValidator()
        r1 = validator.validate(cfg)
        validator.clear_cache()
        r2 = validator.validate(cfg)
        # Same content, different objects
        assert r1 is not r2
        assert r1.is_valid == r2.is_valid

    def test_multiple_clears(self):
        validator = CachedValidator()
        validator.clear_cache()
        validator.clear_cache()
        # Should not raise
        assert validator._last_key is None
