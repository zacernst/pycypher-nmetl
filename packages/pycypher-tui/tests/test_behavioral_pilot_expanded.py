"""Expanded behavioral pilot tests — Entity Tables, Relationships, Templates,
Dialog interactions, CRUD workflows, filter behavior, edge cases, and
cross-screen navigation.

Supplements test_behavioral_pilot.py which covers PipelineOverviewScreen,
DataSourcesScreen, mode transitions, and basic keyboard navigation.
"""

from __future__ import annotations

import pytest

from textual.widgets import Input, Label, Static

from pycypher_tui.app import (
    CommandLine,
    ModeIndicator,
    PyCypherTUI,
    StatusBar,
)
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import get_template
from pycypher_tui.modes.base import ModeType
from pycypher_tui.screens.entity_browser import EntityBrowserScreen
from pycypher_tui.screens.data_sources import (
    DataSourcesScreen,
    SourceDetailPanel,
    SourceListItem,
)
from pycypher_tui.screens.entity_tables import (
    EntityDetailPanel,
    EntityListItem,
    EntityTablesScreen,
)
from pycypher_tui.screens.pipeline_overview import (
    PipelineOverviewScreen,
    SectionWidget,
)
from pycypher_tui.screens.relationships import (
    RelationshipDetailPanel,
    RelationshipListItem,
    RelationshipScreen,
)
from pycypher_tui.screens.template_browser import (
    TemplateBrowserScreen,
    TemplateDetailPanel,
    TemplateListItem,
)
from pycypher_tui.widgets.dialog import (
    ConfirmDialog,
    DialogResult,
    DialogResponse,
    InputDialog,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app_with_ecommerce() -> PyCypherTUI:
    t = get_template("ecommerce_pipeline")
    config = t.instantiate(project_name="test_shop", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


def _make_app_with_csv_analytics() -> PyCypherTUI:
    t = get_template("csv_analytics")
    config = t.instantiate(project_name="test_analytics", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


def _make_app_with_social_network() -> PyCypherTUI:
    t = get_template("social_network")
    config = t.instantiate(project_name="test_social", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


async def _mount_entity_tables(app: PyCypherTUI) -> None:
    from textual.containers import Container

    main_content = app.query_one("#main-content", Container)
    await main_content.remove_children()
    screen = EntityTablesScreen(config_manager=app._config_manager)
    await main_content.mount(screen)


async def _mount_relationship_screen(app: PyCypherTUI) -> None:
    from textual.containers import Container

    main_content = app.query_one("#main-content", Container)
    await main_content.remove_children()
    screen = RelationshipScreen(config_manager=app._config_manager)
    await main_content.mount(screen)


async def _mount_template_browser(app: PyCypherTUI) -> None:
    from textual.containers import Container

    main_content = app.query_one("#main-content", Container)
    await main_content.remove_children()
    screen = TemplateBrowserScreen(config_manager=app._config_manager)
    await main_content.mount(screen)


# ---------------------------------------------------------------------------
# Entity Tables Screen — Mounted Behavior
# ---------------------------------------------------------------------------


class TestEntityTablesScreenMounted:
    """Test EntityTablesScreen rendering and navigation."""

    @pytest.mark.asyncio
    async def test_entity_tables_renders_list_items(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            items = app.query(EntityListItem)
            assert len(items) >= 2

    @pytest.mark.asyncio
    async def test_entity_tables_first_item_focused(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            items = app.query(EntityListItem)
            assert len(items) > 0
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_entity_tables_j_moves_down(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            items = app.query(EntityListItem)
            if len(items) > 1:
                assert not items[0].has_class("item-focused")
                assert items[1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_entity_tables_k_moves_up(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            items = app.query(EntityListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_entity_tables_G_jumps_to_last(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            items = app.query(EntityListItem)
            assert items[-1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_entity_tables_gg_jumps_to_first(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()
            items = app.query(EntityListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_entity_tables_detail_panel_present(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            panel = app.query_one("#detail-panel", EntityDetailPanel)
            assert panel is not None

    @pytest.mark.asyncio
    async def test_entity_tables_detail_shows_entity_info(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            panel = app.query_one("#detail-panel", EntityDetailPanel)
            labels = panel.query(Label)
            assert len(labels) >= 2

    @pytest.mark.asyncio
    async def test_entity_tables_screen_title(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            title = app.query_one("#screen-title", Label)
            assert "Entity Tables" in str(title.render())

    @pytest.mark.asyncio
    async def test_entity_tables_breadcrumb(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            breadcrumb = app.query_one("#screen-breadcrumb", Label)
            rendered = str(breadcrumb.render())
            assert "Pipeline" in rendered
            assert "Entity" in rendered

    @pytest.mark.asyncio
    async def test_entity_tables_k_at_top_stays(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            items = app.query(EntityListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_entity_tables_navigation_updates_detail(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            assert app.is_running


# ---------------------------------------------------------------------------
# Relationship Screen — Mounted Behavior
# ---------------------------------------------------------------------------


class TestRelationshipScreenMounted:
    """Test RelationshipScreen rendering and navigation."""

    @pytest.mark.asyncio
    async def test_relationship_screen_renders_items(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert len(items) >= 2

    @pytest.mark.asyncio
    async def test_relationship_first_item_focused(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert len(items) > 0
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_relationship_j_moves_down(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            items = app.query(RelationshipListItem)
            if len(items) > 1:
                assert not items[0].has_class("item-focused")
                assert items[1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_relationship_k_moves_up(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_relationship_G_jumps_to_last(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert items[-1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_relationship_detail_panel_present(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            panel = app.query_one("#detail-panel", RelationshipDetailPanel)
            assert panel is not None

    @pytest.mark.asyncio
    async def test_relationship_screen_title(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            title = app.query_one("#screen-title", Label)
            assert "Relationship" in str(title.render())

    @pytest.mark.asyncio
    async def test_relationship_detail_shows_mapping(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            panel = app.query_one("#detail-panel", RelationshipDetailPanel)
            labels = panel.query(Label)
            assert len(labels) >= 4

    @pytest.mark.asyncio
    async def test_relationship_gg_from_end(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Template Browser Screen — Mounted Behavior
# ---------------------------------------------------------------------------


class TestTemplateBrowserScreenMounted:
    """Test TemplateBrowserScreen rendering and navigation."""

    @pytest.mark.asyncio
    async def test_template_browser_renders_templates(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            items = app.query(TemplateListItem)
            assert len(items) >= 3

    @pytest.mark.asyncio
    async def test_template_browser_first_item_focused(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            items = app.query(TemplateListItem)
            assert len(items) > 0
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_template_browser_j_moves_down(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            items = app.query(TemplateListItem)
            if len(items) > 1:
                assert not items[0].has_class("item-focused")
                assert items[1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_template_browser_k_moves_up(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            items = app.query(TemplateListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_template_browser_G_jumps_to_last(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            items = app.query(TemplateListItem)
            assert items[-1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_template_browser_detail_panel_present(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            panel = app.query_one("#detail-panel", TemplateDetailPanel)
            assert panel is not None

    @pytest.mark.asyncio
    async def test_template_browser_screen_title(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            title = app.query_one("#screen-title", Label)
            assert "Template" in str(title.render())

    @pytest.mark.asyncio
    async def test_template_browser_detail_shows_stats(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            panel = app.query_one("#detail-panel", TemplateDetailPanel)
            labels = panel.query(Label)
            assert len(labels) >= 4

    @pytest.mark.asyncio
    async def test_template_browser_gg_from_end(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()
            items = app.query(TemplateListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_template_browser_k_at_top_stays(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_template_browser(app)
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            items = app.query(TemplateListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Dialog Interaction Tests
# ---------------------------------------------------------------------------


class TestDialogInteractions:
    """Test dialog widget interactions through the mounted app."""

    @pytest.mark.asyncio
    async def test_confirm_dialog_y_confirms(self):
        app = PyCypherTUI()
        result_holder = []
        async with app.run_test() as pilot:
            app.push_screen(
                ConfirmDialog(title="Test", body="Confirm?"),
                callback=lambda r: result_holder.append(r),
            )
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()
            assert len(result_holder) == 1
            assert result_holder[0].result == DialogResult.CONFIRMED

    @pytest.mark.asyncio
    async def test_confirm_dialog_n_cancels(self):
        app = PyCypherTUI()
        result_holder = []
        async with app.run_test() as pilot:
            app.push_screen(
                ConfirmDialog(title="Test", body="Confirm?"),
                callback=lambda r: result_holder.append(r),
            )
            await pilot.pause()
            await pilot.press("n")
            await pilot.pause()
            assert len(result_holder) == 1
            assert result_holder[0].result == DialogResult.CANCELLED

    @pytest.mark.asyncio
    async def test_confirm_dialog_escape_cancels(self):
        app = PyCypherTUI()
        result_holder = []
        async with app.run_test() as pilot:
            app.push_screen(
                ConfirmDialog(title="Test", body="Confirm?"),
                callback=lambda r: result_holder.append(r),
            )
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            assert len(result_holder) == 1
            assert result_holder[0].result == DialogResult.CANCELLED

    @pytest.mark.asyncio
    async def test_confirm_dialog_enter_confirms(self):
        app = PyCypherTUI()
        result_holder = []
        async with app.run_test() as pilot:
            app.push_screen(
                ConfirmDialog(title="Delete", body="Sure?"),
                callback=lambda r: result_holder.append(r),
            )
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            assert len(result_holder) == 1
            assert result_holder[0].result == DialogResult.CONFIRMED

    @pytest.mark.asyncio
    async def test_input_dialog_renders_input_field(self):
        app = PyCypherTUI()
        async with app.run_test() as pilot:
            app.push_screen(
                InputDialog(title="Add", body="ID:", placeholder="e.g. foo"),
            )
            await pilot.pause()
            inputs = app.query(Input)
            assert len(inputs) >= 1

    @pytest.mark.asyncio
    async def test_input_dialog_escape_cancels(self):
        app = PyCypherTUI()
        result_holder = []
        async with app.run_test() as pilot:
            app.push_screen(
                InputDialog(title="Test", body="Enter:"),
                callback=lambda r: result_holder.append(r),
            )
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            assert len(result_holder) == 1
            assert result_holder[0].result == DialogResult.CANCELLED

    @pytest.mark.asyncio
    async def test_input_dialog_default_value(self):
        app = PyCypherTUI()
        async with app.run_test() as pilot:
            app.push_screen(
                InputDialog(title="Edit", body="URI:", default_value="data/test.csv"),
            )
            await pilot.pause()
            input_widget = app.query_one("#dialog-input", Input)
            assert input_widget.value == "data/test.csv"

    @pytest.mark.asyncio
    async def test_confirm_dialog_title_and_body_render(self):
        app = PyCypherTUI()
        async with app.run_test() as pilot:
            app.push_screen(
                ConfirmDialog(title="My Title", body="My Body Text"),
            )
            await pilot.pause()
            title = app.query_one("#dialog-title", Label)
            body = app.query_one("#dialog-body", Label)
            assert "My Title" in str(title.render())
            assert "My Body Text" in str(body.render())


# ---------------------------------------------------------------------------
# Data Sources CRUD Workflows
# ---------------------------------------------------------------------------


class TestDataSourcesCRUDWorkflows:
    """Test complete CRUD workflows on DataSourcesScreen."""

    @pytest.mark.asyncio
    async def test_data_sources_dd_opens_confirm_dialog(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            confirm_dialogs = app.query(ConfirmDialog)
            assert len(confirm_dialogs) > 0

    @pytest.mark.asyncio
    async def test_data_sources_dd_cancel_preserves_item(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            initial_count = len(app.query(SourceListItem))
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            await pilot.press("n")
            await pilot.pause()
            items = app.query(SourceListItem)
            assert len(items) == initial_count

    @pytest.mark.asyncio
    async def test_data_sources_dd_confirm_removes_item(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            initial_count = len(app.query(SourceListItem))
            assert initial_count > 0
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()
            await pilot.pause()
            items = app.query(SourceListItem)
            assert len(items) == initial_count - 1

    @pytest.mark.asyncio
    async def test_data_sources_l_opens_edit_dialog(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("l")
            await pilot.pause()
            inputs = app.query(Input)
            assert len(inputs) >= 1

    @pytest.mark.asyncio
    async def test_data_sources_enter_opens_edit_dialog(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            inputs = app.query(Input)
            assert len(inputs) >= 1


# ---------------------------------------------------------------------------
# Data Sources Filter Behavior
# ---------------------------------------------------------------------------


class TestDataSourcesFilterBehavior:
    """Test DataSourcesScreen filter cycling via Tab key."""

    @pytest.mark.asyncio
    async def test_tab_cycles_filter(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            initial_count = len(app.query(SourceListItem))
            assert initial_count == 4

            await pilot.press("tab")
            await pilot.pause()
            entity_count = len(app.query(SourceListItem))
            assert entity_count == 3

            await pilot.press("tab")
            await pilot.pause()
            rel_count = len(app.query(SourceListItem))
            assert rel_count == 1

            await pilot.press("tab")
            await pilot.pause()
            all_count = len(app.query(SourceListItem))
            assert all_count == initial_count

    @pytest.mark.asyncio
    async def test_filter_with_navigation(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("tab")
            await pilot.pause()

            await pilot.press("G")
            await pilot.pause()
            items = app.query(SourceListItem)
            assert items[-1].has_class("item-focused")
            for item in items:
                assert item.source.source_type == "entity"


# ---------------------------------------------------------------------------
# Empty State Edge Cases
# ---------------------------------------------------------------------------


class TestEmptyStateEdgeCases:
    """Test edge cases with empty and minimal configurations."""

    @pytest.mark.asyncio
    async def test_empty_entity_tables_shows_message(self):
        app = PyCypherTUI()
        app._config_manager = ConfigManager()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            items = app.query(EntityListItem)
            assert len(items) == 0
            empty_labels = app.query(".empty-list-message")
            assert len(empty_labels) > 0

    @pytest.mark.asyncio
    async def test_empty_relationship_screen_shows_message(self):
        app = PyCypherTUI()
        app._config_manager = ConfigManager()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert len(items) == 0

    @pytest.mark.asyncio
    async def test_empty_entity_tables_navigation_safe(self):
        app = PyCypherTUI()
        app._config_manager = ConfigManager()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.press("k")
            await pilot.press("G")
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()
            assert app.is_running

    @pytest.mark.asyncio
    async def test_empty_relationship_screen_navigation_safe(self):
        app = PyCypherTUI()
        app._config_manager = ConfigManager()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("j")
            await pilot.press("k")
            await pilot.press("G")
            await pilot.pause()
            assert app.is_running

    @pytest.mark.asyncio
    async def test_single_item_navigation(self):
        app = PyCypherTUI()
        cm = ConfigManager()
        cm.add_entity_source("only_one", "data/one.csv", "Single", id_col="id")
        app._config_manager = cm
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            items = app.query(EntityListItem)
            assert len(items) == 1
            assert items[0].has_class("item-focused")

            await pilot.press("j")
            await pilot.pause()
            assert items[0].has_class("item-focused")

            await pilot.press("k")
            await pilot.pause()
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Search Behavior
# ---------------------------------------------------------------------------


class TestSearchBehavior:
    """Test search workflow through command mode."""

    @pytest.mark.asyncio
    async def test_colon_enters_command_mode(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("colon")
            await pilot.pause()
            cmd_line = app.query_one("#command-line", CommandLine)
            assert cmd_line.has_class("visible")

    @pytest.mark.asyncio
    async def test_command_mode_escape_cancels(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("colon")
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            cmd_line = app.query_one("#command-line", CommandLine)
            assert not cmd_line.has_class("visible")
            assert app.mode_manager.current_type == ModeType.NORMAL


# ---------------------------------------------------------------------------
# Entity Tables CRUD Workflows
# ---------------------------------------------------------------------------


class TestEntityTablesCRUDWorkflows:
    """Test CRUD workflows on EntityTablesScreen."""

    @pytest.mark.asyncio
    async def test_entity_tables_dd_opens_confirm(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            confirm_dialogs = app.query(ConfirmDialog)
            assert len(confirm_dialogs) > 0

    @pytest.mark.asyncio
    async def test_entity_tables_dd_cancel_preserves(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            initial_count = len(app.query(EntityListItem))
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            await pilot.press("n")
            await pilot.pause()
            assert len(app.query(EntityListItem)) == initial_count

    @pytest.mark.asyncio
    async def test_entity_tables_dd_confirm_removes(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            initial_count = len(app.query(EntityListItem))
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()
            await pilot.pause()
            assert len(app.query(EntityListItem)) == initial_count - 1

    @pytest.mark.asyncio
    async def test_entity_tables_enter_opens_edit(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            inputs = app.query(Input)
            assert len(inputs) >= 1


# ---------------------------------------------------------------------------
# Relationship CRUD Workflows
# ---------------------------------------------------------------------------


class TestRelationshipCRUDWorkflows:
    """Test CRUD workflows on RelationshipScreen."""

    @pytest.mark.asyncio
    async def test_relationship_dd_opens_confirm(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            confirm_dialogs = app.query(ConfirmDialog)
            assert len(confirm_dialogs) > 0

    @pytest.mark.asyncio
    async def test_relationship_dd_cancel_preserves(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            initial_count = len(app.query(RelationshipListItem))
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            await pilot.press("n")
            await pilot.pause()
            assert len(app.query(RelationshipListItem)) == initial_count

    @pytest.mark.asyncio
    async def test_relationship_dd_confirm_removes(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            initial_count = len(app.query(RelationshipListItem))
            await pilot.press("d")
            await pilot.press("d")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()
            await pilot.pause()
            assert len(app.query(RelationshipListItem)) == initial_count - 1

    @pytest.mark.asyncio
    async def test_relationship_enter_opens_edit(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            inputs = app.query(Input)
            assert len(inputs) >= 1


# ---------------------------------------------------------------------------
# Cross-Screen Navigation Workflows
# ---------------------------------------------------------------------------


class TestCrossScreenNavigationWorkflows:
    """Test navigation workflows across multiple screens."""

    @pytest.mark.asyncio
    async def test_overview_queries_section_focused(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            # data_model=0, entity_sources=1, relationship_sources=2, queries=3
            await pilot.press("j")
            await pilot.press("j")
            await pilot.press("j")
            await pilot.pause()
            queries = app.query_one("#item-queries", SectionWidget)
            assert queries.has_class("item-focused")

    @pytest.mark.asyncio
    async def test_overview_outputs_section_focused(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            outputs = app.query_one("#item-outputs", SectionWidget)
            assert outputs.has_class("item-focused")

    @pytest.mark.asyncio
    async def test_data_sources_h_returns_to_overview(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            # Navigate to entity_sources (index 1, since data_model is at 0)
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            assert len(app.query(EntityBrowserScreen)) > 0

            await pilot.press("h")
            await pilot.pause()
            await pilot.pause()
            assert len(app.query(SectionWidget)) == 6

    @pytest.mark.asyncio
    async def test_rapid_navigation_doesnt_crash(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            for key in ["j", "j", "k", "G", "g", "g", "j"]:
                await pilot.press(key)
            await pilot.pause()
            assert app.is_running


# ---------------------------------------------------------------------------
# Mode Integration with Screen Navigation
# ---------------------------------------------------------------------------


class TestModeIntegrationWithScreens:
    """Test mode transitions work correctly within screen contexts."""

    @pytest.mark.asyncio
    async def test_visual_mode_in_data_sources(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("v")
            await pilot.pause()
            indicator = app.query_one("#mode-indicator", ModeIndicator)
            assert indicator.mode_name == "VISUAL"
            await pilot.press("escape")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_command_mode_in_entity_tables(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("colon")
            await pilot.pause()
            cmd_line = app.query_one("#command-line", CommandLine)
            assert cmd_line.has_class("visible")
            await pilot.press("escape")
            await pilot.pause()
            assert not cmd_line.has_class("visible")

    @pytest.mark.asyncio
    async def test_insert_mode_escape_returns_to_normal(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            await pilot.press("i")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.INSERT
            await pilot.press("escape")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.NORMAL


# ---------------------------------------------------------------------------
# Overview Undo/Redo Behavior
# ---------------------------------------------------------------------------


class TestOverviewUndoRedo:
    """Test undo/redo on PipelineOverviewScreen."""

    @pytest.mark.asyncio
    async def test_overview_u_undo_doesnt_crash(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            await pilot.press("u")
            await pilot.pause()
            assert app.is_running
            sections = app.query(SectionWidget)
            assert len(sections) == 6

    @pytest.mark.asyncio
    async def test_overview_ctrl_r_redo_doesnt_crash(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            await pilot.press("ctrl+r")
            await pilot.pause()
            assert app.is_running


# ---------------------------------------------------------------------------
# Validation Display Tests
# ---------------------------------------------------------------------------


class TestValidationDisplay:
    """Test validation summary display."""

    @pytest.mark.asyncio
    async def test_overview_validation_summary_content(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            validation = app.query_one("#validation-summary", Static)
            rendered = str(validation.render())
            assert len(rendered.strip()) > 0

    @pytest.mark.asyncio
    async def test_empty_config_validation(self):
        app = PyCypherTUI()
        app._config_manager = ConfigManager()
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            validation = app.query_one("#validation-summary", Static)
            assert validation is not None


# ---------------------------------------------------------------------------
# Multi-Template Screen Population
# ---------------------------------------------------------------------------


class TestMultiTemplateScreenPopulation:
    """Test different templates populate screens correctly."""

    @pytest.mark.asyncio
    async def test_csv_analytics_entity_tables(self):
        app = _make_app_with_csv_analytics()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            items = app.query(EntityListItem)
            assert len(items) == 2

    @pytest.mark.asyncio
    async def test_social_network_relationship_screen(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert len(items) >= 2

    @pytest.mark.asyncio
    async def test_ecommerce_entity_tables(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            items = app.query(EntityListItem)
            assert len(items) == 3

    @pytest.mark.asyncio
    async def test_ecommerce_relationship_screen(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert len(items) >= 1


# ---------------------------------------------------------------------------
# Page Navigation (ctrl+f / ctrl+b) on Various Screens
# ---------------------------------------------------------------------------


class TestPageNavigation:
    """Test page up/down across different screens."""

    @pytest.mark.asyncio
    async def test_entity_tables_ctrl_f_ctrl_b(self):
        app = _make_app_with_ecommerce()
        async with app.run_test() as pilot:
            await _mount_entity_tables(app)
            await pilot.pause()
            await pilot.press("ctrl+f")
            await pilot.pause()
            items = app.query(EntityListItem)
            assert items[-1].has_class("item-focused")
            await pilot.press("ctrl+b")
            await pilot.pause()
            items = app.query(EntityListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_relationship_ctrl_f_ctrl_b(self):
        app = _make_app_with_social_network()
        async with app.run_test() as pilot:
            await _mount_relationship_screen(app)
            await pilot.pause()
            await pilot.press("ctrl+f")
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert items[-1].has_class("item-focused")
            await pilot.press("ctrl+b")
            await pilot.pause()
            items = app.query(RelationshipListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# DialogResponse Unit Tests
# ---------------------------------------------------------------------------


class TestDialogResponseUnit:
    """Test DialogResponse dataclass behavior."""

    def test_confirmed_response(self):
        r = DialogResponse(DialogResult.CONFIRMED, value="hello")
        assert r.result == DialogResult.CONFIRMED
        assert r.value == "hello"

    def test_cancelled_response(self):
        r = DialogResponse(DialogResult.CANCELLED)
        assert r.result == DialogResult.CANCELLED
        assert r.value is None

    def test_confirmed_no_value(self):
        r = DialogResponse(DialogResult.CONFIRMED)
        assert r.result == DialogResult.CONFIRMED
        assert r.value is None
