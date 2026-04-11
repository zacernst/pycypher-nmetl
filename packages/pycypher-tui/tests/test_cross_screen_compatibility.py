"""Cross-screen compatibility validation tests.

Validates that VIM key sequences, mode transitions, and user workflows
behave identically across all screens — both VimNavigableScreen subclasses
(DataSourcesScreen, EntityTablesScreen, RelationshipScreen, TemplateBrowserScreen)
and the direct Screen implementation (PipelineOverviewScreen).

KNOWN INTENTIONAL DIVERGENCES:
- PipelineOverviewScreen intercepts 'i' as "edit" action (not INSERT mode)
- PipelineOverviewScreen intercepts 'a' as "add" action
- PipelineOverviewScreen handles 'h'/'left' as back navigation (VimNavigableScreen
  delegates to ModeManager which produces navigate:left -> NavigateBack)

These divergences are acceptable because PipelineOverviewScreen is a dashboard
with different UX semantics than list-detail screens.
"""

from __future__ import annotations

import pytest

from pycypher_tui.app import ModeIndicator, PyCypherTUI
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import get_template
from pycypher_tui.modes.base import ModeType
from pycypher_tui.screens.base import BaseListItem, VimNavigableScreen
from pycypher_tui.screens.data_sources import DataSourcesScreen, SourceListItem
from pycypher_tui.screens.entity_browser import EntityBrowserScreen
from pycypher_tui.screens.entity_tables import (
    EntityListItem,
    EntityTablesScreen,
)
from pycypher_tui.screens.pipeline_overview import (
    PipelineOverviewScreen,
    SectionWidget,
)
from pycypher_tui.screens.relationships import (
    RelationshipListItem,
    RelationshipScreen,
)
from pycypher_tui.screens.template_browser import (
    TemplateBrowserScreen,
    TemplateListItem,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app() -> PyCypherTUI:
    """Create a PyCypherTUI app with social_network template (has both entities and relationships)."""
    t = get_template("social_network")
    config = t.instantiate(project_name="test_social", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


async def _show_overview_and_wait(app, pilot):
    """Show overview and wait for async mount to complete."""
    await app._show_overview()
    await pilot.pause()
    await pilot.pause()  # Extra pause for async widget mount


# After Task #11 refactoring, PipelineOverviewScreen uses VimNavigableScreen.
# Section IDs follow BaseListItem convention: "item-{key}" not "section-{key}".
# Focus class is "item-focused" (from BaseListItem), not "section-focused".
_SECTION_ID_PREFIX = "item-"
_FOCUS_CLASS = "item-focused"


# ---------------------------------------------------------------------------
# VimNavigableScreen subclass interface compatibility
# ---------------------------------------------------------------------------


class TestVimNavigableScreenInterfaceCompat:
    """All VimNavigableScreen subclasses implement the same abstract interface."""

    def test_all_subclasses_have_required_properties(self):
        """Each subclass defines screen_title, breadcrumb_text, footer_hints."""
        mgr = ConfigManager()
        screens = [
            DataSourcesScreen(config_manager=mgr),
            EntityTablesScreen(config_manager=mgr),
            RelationshipScreen(config_manager=mgr),
            TemplateBrowserScreen(config_manager=mgr),
        ]
        for screen in screens:
            assert isinstance(screen.screen_title, str), f"{type(screen).__name__} missing screen_title"
            assert len(screen.screen_title) > 0
            assert isinstance(screen.breadcrumb_text, str), f"{type(screen).__name__} missing breadcrumb_text"
            assert isinstance(screen.footer_hints, str), f"{type(screen).__name__} missing footer_hints"

    def test_all_subclasses_have_backward_compat_aliases(self):
        """Refactored screens provide backward-compatible property aliases."""
        mgr = ConfigManager()

        ds = DataSourcesScreen(config_manager=mgr)
        assert ds.source_count == ds.item_count
        assert ds.current_source == ds.current_item

        et = EntityTablesScreen(config_manager=mgr)
        assert et.entity_count == et.item_count
        assert et.current_entity == et.current_item

        rs = RelationshipScreen(config_manager=mgr)
        assert rs.relationship_count == rs.item_count
        assert rs.current_relationship == rs.current_item

        tb = TemplateBrowserScreen(config_manager=mgr)
        assert tb.template_count == tb.item_count
        assert tb.current_template == tb.current_item

    def test_all_subclasses_define_empty_list_message(self):
        """Each subclass has a non-empty empty_list_message."""
        mgr = ConfigManager()
        screens = [
            DataSourcesScreen(config_manager=mgr),
            EntityTablesScreen(config_manager=mgr),
            RelationshipScreen(config_manager=mgr),
            TemplateBrowserScreen(config_manager=mgr),
        ]
        for screen in screens:
            msg = screen.empty_list_message
            assert isinstance(msg, str) and len(msg) > 0, (
                f"{type(screen).__name__} has empty empty_list_message"
            )

    def test_all_subclasses_use_consistent_panel_ids(self):
        """All subclasses use the same panel ID conventions from the base class."""
        mgr = ConfigManager()
        screens = [
            DataSourcesScreen(config_manager=mgr),
            EntityTablesScreen(config_manager=mgr),
            RelationshipScreen(config_manager=mgr),
            TemplateBrowserScreen(config_manager=mgr),
        ]
        for screen in screens:
            assert screen.list_panel_id == "list-panel", f"{type(screen).__name__} divergent list_panel_id"
            assert screen.detail_panel_id == "detail-panel", f"{type(screen).__name__} divergent detail_panel_id"


# ---------------------------------------------------------------------------
# j/k navigation consistency across VimNavigableScreen subclasses
# ---------------------------------------------------------------------------


class TestJKNavigationConsistency:
    """j/k navigation works identically across PipelineOverview and VimNavigableScreens."""

    @pytest.mark.asyncio
    async def test_j_moves_down_on_overview(self):
        """j moves cursor down on PipelineOverviewScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            first = app.query_one(f"#{_SECTION_ID_PREFIX}data_model", SectionWidget)
            assert first.has_class(_FOCUS_CLASS)

            await pilot.press("j")
            await pilot.pause()

            assert not first.has_class(_FOCUS_CLASS)
            second = app.query_one(f"#{_SECTION_ID_PREFIX}entity_sources", SectionWidget)
            assert second.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_j_moves_down_on_data_sources(self):
        """j moves cursor down on DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[0].has_class("item-focused")

            await pilot.press("j")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert not items[0].has_class("item-focused")
            assert items[1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_k_moves_up_on_overview(self):
        """k moves cursor up on PipelineOverviewScreen after j."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()

            first = app.query_one(f"#{_SECTION_ID_PREFIX}data_model", SectionWidget)
            assert first.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_k_moves_up_on_data_sources(self):
        """k moves cursor up on DataSourcesScreen after j."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# G/gg jump consistency
# ---------------------------------------------------------------------------


class TestJumpNavigationConsistency:
    """G and gg jumps work identically across screen types."""

    @pytest.mark.asyncio
    async def test_G_jumps_to_last_on_overview(self):
        """G jumps to last section on overview."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("G")
            await pilot.pause()

            last = app.query_one(f"#{_SECTION_ID_PREFIX}outputs", SectionWidget)
            assert last.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_G_jumps_to_last_on_data_sources(self):
        """G jumps to last item on DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("G")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[-1].has_class("item-focused")
            for item in items[:-1]:
                assert not item.has_class("item-focused")

    @pytest.mark.asyncio
    async def test_gg_jumps_to_first_on_overview(self):
        """gg jumps back to first section on overview."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("G")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()

            first = app.query_one(f"#{_SECTION_ID_PREFIX}data_model", SectionWidget)
            assert first.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_gg_jumps_to_first_on_data_sources(self):
        """gg jumps back to first item on DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("G")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Boundary behavior consistency
# ---------------------------------------------------------------------------


class TestBoundaryBehaviorConsistency:
    """Cursor clamping at boundaries works identically across screens."""

    @pytest.mark.asyncio
    async def test_k_at_top_stays_on_overview(self):
        """k at top stays at first section on overview."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("k")
            await pilot.pause()

            first = app.query_one(f"#{_SECTION_ID_PREFIX}data_model", SectionWidget)
            assert first.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_k_at_top_stays_on_data_sources(self):
        """k at top stays at first item on DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            for _ in range(5):
                await pilot.press("k")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_j_at_bottom_stays_on_overview(self):
        """j past bottom stays at last section on overview."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            for _ in range(20):
                await pilot.press("j")
            await pilot.pause()

            last = app.query_one(f"#{_SECTION_ID_PREFIX}outputs", SectionWidget)
            assert last.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_j_at_bottom_stays_on_data_sources(self):
        """j past bottom stays at last item on DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            for _ in range(20):
                await pilot.press("j")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[-1].has_class("item-focused")


# ---------------------------------------------------------------------------
# Mode transitions from VimNavigableScreen subclasses
# ---------------------------------------------------------------------------


class TestModeTransitionsFromScreens:
    """Mode transitions work consistently from VimNavigableScreen subclasses.

    NOTE: PipelineOverviewScreen intentionally intercepts 'i' as 'edit' action
    and 'a' as 'add' action, so mode transitions are tested from DataSourcesScreen
    which delegates mode keys to the ModeManager through the standard flow.
    """

    @pytest.mark.asyncio
    async def test_insert_mode_from_data_sources(self):
        """i enters INSERT mode from DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("i")
            await pilot.pause()

            assert app.mode_manager.current_type == ModeType.INSERT

    @pytest.mark.asyncio
    async def test_visual_mode_from_data_sources(self):
        """v enters VISUAL mode from DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("v")
            await pilot.pause()

            assert app.mode_manager.current_type == ModeType.VISUAL

    @pytest.mark.asyncio
    async def test_command_mode_from_overview(self):
        """: enters COMMAND mode from PipelineOverviewScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("colon")
            await pilot.pause()

            assert app.mode_manager.current_type == ModeType.COMMAND

    @pytest.mark.asyncio
    async def test_command_mode_from_data_sources(self):
        """: enters COMMAND mode from DataSourcesScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("colon")
            await pilot.pause()

            assert app.mode_manager.current_type == ModeType.COMMAND

    @pytest.mark.asyncio
    async def test_escape_returns_to_normal_from_insert(self):
        """Escape returns to NORMAL from INSERT mode."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("i")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.INSERT

            await pilot.press("escape")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_escape_returns_to_normal_from_visual(self):
        """Escape returns to NORMAL from VISUAL mode."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("v")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.VISUAL

            await pilot.press("escape")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_escape_returns_to_normal_from_command(self):
        """Escape returns to NORMAL from COMMAND mode."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("colon")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.COMMAND

            await pilot.press("escape")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_overview_i_key_does_not_enter_insert(self):
        """PipelineOverviewScreen intentionally intercepts 'i' for edit, NOT insert mode.

        This is a documented intentional divergence - overview uses 'i' for
        section edit rather than VIM INSERT mode transition.
        """
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("i")
            await pilot.pause()

            # Overview intercepts 'i' — should NOT enter INSERT mode
            assert app.mode_manager.current_type == ModeType.NORMAL


# ---------------------------------------------------------------------------
# Screen transition compatibility
# ---------------------------------------------------------------------------


class TestScreenTransitionCompat:
    """Navigating between screens preserves consistent state."""

    @pytest.mark.asyncio
    async def test_overview_enter_drills_into_entity_browser(self):
        """Enter on overview entity_sources section shows EntityBrowserScreen."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            # Navigate to entity_sources (index 1, data_model is index 0)
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()

            assert len(app.query(EntityBrowserScreen)) > 0

    @pytest.mark.asyncio
    async def test_mode_stays_normal_on_screen_transition(self):
        """Mode stays NORMAL when transitioning between screens."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)
            assert app.mode_manager.current_type == ModeType.NORMAL

            # Navigate to entity_sources (index 1) then enter
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()

            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_cursor_starts_at_zero_on_fresh_screen(self):
        """When entering a screen, cursor starts at item 0."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            items = app.query(SourceListItem)
            if len(items) > 0:
                assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Arrow key parity
# ---------------------------------------------------------------------------


class TestArrowKeyParity:
    """Arrow keys work identically to VIM keys across all screens."""

    @pytest.mark.asyncio
    async def test_down_arrow_matches_j_on_overview(self):
        """down arrow moves cursor on overview same as j."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("down")
            await pilot.pause()

            second = app.query_one(f"#{_SECTION_ID_PREFIX}entity_sources", SectionWidget)
            assert second.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_down_arrow_matches_j_on_data_sources(self):
        """down arrow moves cursor on DataSourcesScreen same as j."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("down")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_up_arrow_matches_k_on_data_sources(self):
        """up arrow moves cursor same as k."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("j")
            await pilot.pause()
            await pilot.press("up")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Pending key sequence compatibility
# ---------------------------------------------------------------------------


class TestPendingKeySequenceCompat:
    """Multi-key sequences (gg, dd) work consistently across screens."""

    @pytest.mark.asyncio
    async def test_g_escape_cancels_on_overview(self):
        """g then Escape cancels pending sequence on overview without crash."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("g")
            await pilot.press("escape")
            await pilot.pause()

            assert app.is_running

    @pytest.mark.asyncio
    async def test_g_escape_cancels_on_data_sources(self):
        """g then Escape cancels pending sequence on DataSourcesScreen without crash."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("j")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("escape")
            await pilot.pause()

            assert app.is_running


# ---------------------------------------------------------------------------
# ctrl+f / ctrl+b page navigation consistency
# ---------------------------------------------------------------------------


class TestPageNavigationConsistency:
    """ctrl+f (page down) and ctrl+b (page up) work on all screens."""

    @pytest.mark.asyncio
    async def test_ctrl_f_on_overview(self):
        """ctrl+f pages down on overview."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            await pilot.press("ctrl+f")
            await pilot.pause()

            # With 4 sections, ctrl+f (page 5) from 0 should reach last (clamped)
            last = app.query_one(f"#{_SECTION_ID_PREFIX}outputs", SectionWidget)
            assert last.has_class(_FOCUS_CLASS)

    @pytest.mark.asyncio
    async def test_ctrl_b_after_G_on_data_sources(self):
        """ctrl+b pages up on DataSourcesScreen after G."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            await pilot.press("G")
            await pilot.pause()
            await pilot.press("ctrl+b")
            await pilot.pause()

            items = app.query(SourceListItem)
            assert items[0].has_class("item-focused")


# ---------------------------------------------------------------------------
# Mode indicator stays in sync across screen switches
# ---------------------------------------------------------------------------


class TestModeIndicatorSync:
    """Mode indicator reflects correct state through screen transitions."""

    @pytest.mark.asyncio
    async def test_mode_indicator_normal_after_screen_switch(self):
        """Mode indicator shows NORMAL after switching screens."""
        app = _make_app()
        async with app.run_test() as pilot:
            await _show_overview_and_wait(app, pilot)

            indicator = app.query_one("#mode-indicator", ModeIndicator)
            assert indicator.mode_name == "NORMAL"

            await app._show_data_sources()
            await pilot.pause()

            assert indicator.mode_name == "NORMAL"

    @pytest.mark.asyncio
    async def test_mode_indicator_updates_on_mode_change(self):
        """Mode indicator updates correctly regardless of which screen is active."""
        app = _make_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            indicator = app.query_one("#mode-indicator", ModeIndicator)
            assert indicator.mode_name == "NORMAL"

            await pilot.press("i")
            await pilot.pause()
            assert indicator.mode_name == "INSERT"

            await pilot.press("escape")
            await pilot.pause()
            assert indicator.mode_name == "NORMAL"
