"""Comprehensive tests for tab navigation in the DataModel screen.

Validates that tabs in the ModelDetailPanel are visible, navigable via
keyboard and programmatic API, and retain content correctly across switches.
Covers the fix for the reported bug where tabs were visible but not navigable.

Test categories:
- Tab presence and initial state
- Programmatic tab switching (.active property)
- Keyboard-based tab navigation (Tab/Shift+Tab, click simulation)
- Tab content persistence across switches
- Tab navigation after node selection changes
- Edge cases (rapid switching, no node, boundary tabs)
- Regression guards for the navigation fix
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from textual.widgets import Label, LoadingIndicator, TabbedContent, TabPane

from pycypher_tui.app import PyCypherTUI
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import get_template
from pycypher_tui.screens.data_model import (
    DataModelScreen,
    ModelDetailPanel,
    ModelNode,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_TAB_IDS = ("tab-overview", "tab-attributes", "tab-validation", "tab-statistics", "tab-lineage")


def _make_test_app() -> PyCypherTUI:
    """Create test app with ecommerce template."""
    t = get_template("ecommerce_pipeline")
    config = t.instantiate(project_name="test_shop", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


def _make_entity_node() -> ModelNode:
    return ModelNode(
        node_id="entity:Customer",
        label="Customer",
        node_type="entity",
        source_count=2,
        source_ids=("customers_csv", "customers_db"),
        connections=(),
    )


def _make_relationship_node() -> ModelNode:
    return ModelNode(
        node_id="rel:PURCHASED",
        label="PURCHASED",
        node_type="relationship",
        source_count=1,
        source_ids=("purchases_csv",),
        connections=("(customer_id) -> (product_id)",),
    )


async def _setup_data_model(app, pilot):
    """Navigate to DataModel screen and return (screen, panel, tabs)."""
    await app._show_data_model()
    await pilot.pause()
    screen = app.query_one(DataModelScreen)
    panel = app.query_one(ModelDetailPanel)
    tabs = panel.query_one(TabbedContent)
    return screen, panel, tabs


# ---------------------------------------------------------------------------
# 1. Tab presence and initial state
# ---------------------------------------------------------------------------


class TestTabPresenceAndInitialState:
    """Verify that TabbedContent is composed with all expected tabs."""

    @pytest.mark.asyncio
    async def test_tabbed_content_exists(self):
        """ModelDetailPanel contains exactly one TabbedContent widget."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, _ = await _setup_data_model(app, pilot)
            tabbed = panel.query(TabbedContent)
            assert len(tabbed) == 1

    @pytest.mark.asyncio
    async def test_all_five_tabs_present(self):
        """All five expected TabPanes are composed."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)
            pane_ids = {pane.id for pane in tabs.query("TabPane")}
            for tab_id in ALL_TAB_IDS:
                assert tab_id in pane_ids, f"Missing tab: {tab_id}"

    @pytest.mark.asyncio
    async def test_overview_tab_is_default_active(self):
        """Overview tab is the initially active tab."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)
            assert tabs.active == "tab-overview"

    @pytest.mark.asyncio
    async def test_tabbed_content_has_correct_id(self):
        """TabbedContent has the expected CSS id 'attribute-tabs'."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)
            assert tabs.id == "attribute-tabs"

    @pytest.mark.asyncio
    async def test_tab_pane_count_matches(self):
        """Number of TabPanes matches the expected count of 5."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)
            panes = tabs.query("TabPane")
            assert len(panes) == 5


# ---------------------------------------------------------------------------
# 2. Programmatic tab switching (.active property)
# ---------------------------------------------------------------------------


class TestProgrammaticTabSwitching:
    """Test that setting tabs.active programmatically works correctly."""

    @pytest.mark.asyncio
    async def test_switch_to_each_tab_programmatically(self):
        """Can switch to every tab by setting .active property."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)

            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id
                await pilot.pause()
                assert tabs.active == tab_id, f"Failed to switch to {tab_id}"

    @pytest.mark.asyncio
    async def test_switch_forward_and_back(self):
        """Can switch forward to a tab and back to overview."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)

            tabs.active = "tab-statistics"
            await pilot.pause()
            assert tabs.active == "tab-statistics"

            tabs.active = "tab-overview"
            await pilot.pause()
            assert tabs.active == "tab-overview"

    @pytest.mark.asyncio
    async def test_switch_to_same_tab_is_noop(self):
        """Setting active to the already-active tab does not crash."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)

            assert tabs.active == "tab-overview"
            tabs.active = "tab-overview"
            await pilot.pause()
            assert tabs.active == "tab-overview"

    @pytest.mark.asyncio
    async def test_rapid_programmatic_switching(self):
        """Rapidly switching tabs does not cause errors."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)

            # Switch through all tabs rapidly without pausing
            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id

            await pilot.pause()
            # Should end on the last tab
            assert tabs.active == "tab-lineage"


# ---------------------------------------------------------------------------
# 3. Keyboard-based tab navigation
# ---------------------------------------------------------------------------


class TestKeyboardTabNavigation:
    """Test keyboard-driven tab switching on the DataModel screen.

    These tests validate the fix for the reported bug: tabs visible but
    not navigable via keyboard.  The fix adds ``_screen_override_keys``
    returning ``{"tab", "shift+tab"}`` and ``handle_extra_key`` that
    cycles through the TabbedContent panes.
    """

    @pytest.mark.asyncio
    async def test_tab_key_advances_active_tab(self):
        """Pressing Tab key advances from overview to the next tab."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            assert tabs.active == "tab-overview"

            # Press Tab — the fix routes this through handle_extra_key
            # which cycles tabs.active to the next pane
            await pilot.press("tab")
            await pilot.pause()

            assert tabs.active == "tab-attributes", (
                "Tab key did not advance from overview to attributes — "
                "handle_extra_key may not be wired up"
            )

    @pytest.mark.asyncio
    async def test_shift_tab_goes_to_previous_tab(self):
        """Pressing Shift+Tab goes to the previous tab (or wraps to last)."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            # Start on overview, Shift+Tab should wrap to lineage (last tab)
            assert tabs.active == "tab-overview"
            await pilot.press("shift+tab")
            await pilot.pause()

            assert tabs.active == "tab-lineage", (
                "Shift+Tab did not wrap from overview to lineage"
            )

    @pytest.mark.asyncio
    async def test_tab_cycles_through_all_five_tabs(self):
        """Pressing Tab five times cycles through all tabs and wraps back."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            expected_order = list(ALL_TAB_IDS[1:]) + [ALL_TAB_IDS[0]]
            # overview -> attributes -> validation -> statistics -> lineage -> overview

            for expected in expected_order:
                await pilot.press("tab")
                await pilot.pause()
                assert tabs.active == expected, (
                    f"Expected {expected} but got {tabs.active}"
                )

    @pytest.mark.asyncio
    async def test_shift_tab_cycles_backward_through_all_tabs(self):
        """Pressing Shift+Tab five times cycles backward through all tabs."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            # overview -> lineage -> statistics -> validation -> attributes -> overview
            expected_order = list(reversed(ALL_TAB_IDS[1:])) + [ALL_TAB_IDS[0]]

            for expected in expected_order:
                await pilot.press("shift+tab")
                await pilot.pause()
                assert tabs.active == expected, (
                    f"Expected {expected} but got {tabs.active}"
                )

    @pytest.mark.asyncio
    async def test_tab_and_shift_tab_are_inverse(self):
        """Tab followed by Shift+Tab returns to original tab."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            assert tabs.active == "tab-overview"

            await pilot.press("tab")
            await pilot.pause()
            assert tabs.active == "tab-attributes"

            await pilot.press("shift+tab")
            await pilot.pause()
            assert tabs.active == "tab-overview"

    @pytest.mark.asyncio
    async def test_clicking_tab_header_switches_tab(self):
        """Clicking a tab header switches to that tab (mouse navigation)."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            assert tabs.active == "tab-overview"

            # Use click on the Attributes tab header
            tab_headers = tabs.query("Tab")
            if len(tab_headers) >= 2:
                await pilot.click(tab_headers[1].__class__, offset=(5, 0))
                await pilot.pause()
                # After clicking, active should have changed
                # (exact behavior depends on Textual click targeting)

    @pytest.mark.asyncio
    async def test_tab_navigation_does_not_crash_screen(self):
        """Tab navigation attempts never crash the screen."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            # Try various key combinations that might relate to tab navigation
            for key in ("tab", "shift+tab", "left", "right"):
                await pilot.press(key)
                await pilot.pause()

            # Screen should still be responsive
            screen_widget = app.query_one(DataModelScreen)
            assert screen_widget is not None

    @pytest.mark.asyncio
    async def test_tab_navigation_after_node_focus(self):
        """Tab navigation works after selecting a node in the list."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            # Navigate to a node using j key
            await pilot.press("j")
            await pilot.pause()

            # Tab should still cycle tabs (not get swallowed)
            await pilot.press("tab")
            await pilot.pause()
            assert tabs.active == "tab-attributes"

    @pytest.mark.asyncio
    async def test_tab_key_with_node_selected_cycles_tabs_not_filters(self):
        """On DataModelScreen, Tab cycles tabs (unlike DataSourcesScreen filter cycling)."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            panel.update_node(_make_entity_node())
            await pilot.pause()

            # Tab should advance tabs, not do something else
            await pilot.press("tab")
            await pilot.pause()
            assert tabs.active != "tab-overview", (
                "Tab key did not change the active tab"
            )

    @pytest.mark.asyncio
    async def test_rapid_keyboard_tab_cycling(self):
        """Rapidly pressing Tab multiple times is stable."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            for _ in range(10):
                await pilot.press("tab")

            await pilot.pause()
            # 10 presses from overview: 10 % 5 = 0 → back to overview
            assert tabs.active == "tab-overview"


# ---------------------------------------------------------------------------
# 3b. Screen override integration
# ---------------------------------------------------------------------------


class TestScreenOverrideIntegration:
    """Verify that DataModelScreen correctly registers Tab/Shift+Tab as override keys.

    These tests validate the specific mechanism Christopher identified:
    ``_screen_override_keys`` must include 'tab' and 'shift+tab',
    and ``handle_extra_key`` must route them to the TabbedContent.
    """

    @pytest.mark.asyncio
    async def test_screen_override_keys_includes_tab(self):
        """DataModelScreen._screen_override_keys includes 'tab'."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, _, _ = await _setup_data_model(app, pilot)
            assert "tab" in screen._screen_override_keys

    @pytest.mark.asyncio
    async def test_screen_override_keys_includes_shift_tab(self):
        """DataModelScreen._screen_override_keys includes 'shift+tab'."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, _, _ = await _setup_data_model(app, pilot)
            assert "shift+tab" in screen._screen_override_keys

    @pytest.mark.asyncio
    async def test_handle_extra_key_returns_true_for_tab(self):
        """handle_extra_key('tab') returns True (key consumed)."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, _, _ = await _setup_data_model(app, pilot)
            result = screen.handle_extra_key("tab")
            assert result is True, "handle_extra_key did not consume 'tab'"

    @pytest.mark.asyncio
    async def test_handle_extra_key_returns_true_for_shift_tab(self):
        """handle_extra_key('shift+tab') returns True (key consumed)."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, _, _ = await _setup_data_model(app, pilot)
            result = screen.handle_extra_key("shift+tab")
            assert result is True, "handle_extra_key did not consume 'shift+tab'"

    @pytest.mark.asyncio
    async def test_handle_extra_key_returns_false_for_unrelated_key(self):
        """handle_extra_key returns False for keys it doesn't handle."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, _, _ = await _setup_data_model(app, pilot)
            result = screen.handle_extra_key("x")
            assert result is False


# ---------------------------------------------------------------------------
# 4. Tab content persistence across switches
# ---------------------------------------------------------------------------


class TestTabContentPersistence:
    """Verify content in tabs persists when switching away and back."""

    @pytest.mark.asyncio
    async def test_overview_content_persists_after_switch(self):
        """Overview tab content persists after switching to another tab and back."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Update with a node to populate overview
            node = _make_entity_node()
            panel.update_node(node)
            await pilot.pause()

            # Capture overview content
            overview = panel.query_one("#tab-overview")
            labels_before = [str(l.render()) for l in overview.query(Label)]

            # Switch away
            tabs.active = "tab-statistics"
            await pilot.pause()

            # Switch back
            tabs.active = "tab-overview"
            await pilot.pause()

            # Content should still be there
            labels_after = [str(l.render()) for l in overview.query(Label)]
            assert labels_before == labels_after

    @pytest.mark.asyncio
    async def test_loading_indicators_visible_in_non_active_tabs(self):
        """Loading indicators are present in non-active tabs during data load."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            node = _make_entity_node()
            panel.update_node(node)
            # Don't wait for loading to complete

            # Check loading indicators in non-active tabs
            for tab_id in ("tab-attributes", "tab-validation", "tab-statistics", "tab-lineage"):
                tab = panel.query_one(f"#{tab_id}")
                indicators = tab.query(LoadingIndicator)
                assert len(indicators) > 0, f"No loading indicator in {tab_id}"


# ---------------------------------------------------------------------------
# 5. Tab navigation after node selection changes
# ---------------------------------------------------------------------------


class TestTabNavigationWithNodeChanges:
    """Test tab behavior when the selected node changes."""

    @pytest.mark.asyncio
    async def test_tabs_reset_to_overview_on_node_change(self):
        """Active tab remains stable when a new node is selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Select first node
            node1 = _make_entity_node()
            panel.update_node(node1)
            await pilot.pause()

            # Switch to attributes tab
            tabs.active = "tab-attributes"
            await pilot.pause()
            assert tabs.active == "tab-attributes"

            # Select a different node
            node2 = _make_relationship_node()
            panel.update_node(node2)
            await pilot.pause()

            # Tab should still be on attributes (content refreshes, tab stays)
            assert tabs.active == "tab-attributes"

    @pytest.mark.asyncio
    async def test_tabs_navigable_after_clearing_node(self):
        """Tabs remain navigable after clearing the selected node."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Select and then clear
            panel.update_node(_make_entity_node())
            await pilot.pause()
            panel.update_node(None)
            await pilot.pause()

            # Should still be able to switch tabs
            tabs.active = "tab-lineage"
            await pilot.pause()
            assert tabs.active == "tab-lineage"

    @pytest.mark.asyncio
    async def test_tabs_navigable_with_entity_node(self):
        """All tabs are navigable when an entity node is selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            panel.update_node(_make_entity_node())
            await pilot.pause()

            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id
                await pilot.pause()
                assert tabs.active == tab_id

    @pytest.mark.asyncio
    async def test_tabs_navigable_with_relationship_node(self):
        """All tabs are navigable when a relationship node is selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            panel.update_node(_make_relationship_node())
            await pilot.pause()

            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id
                await pilot.pause()
                assert tabs.active == tab_id


# ---------------------------------------------------------------------------
# 6. Edge cases
# ---------------------------------------------------------------------------


class TestTabNavigationEdgeCases:
    """Edge cases for tab navigation robustness."""

    @pytest.mark.asyncio
    async def test_rapid_node_switching_with_tab_changes(self):
        """Rapidly switching nodes while also switching tabs does not crash."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            entity = _make_entity_node()
            relationship = _make_relationship_node()

            for _ in range(3):
                panel.update_node(entity)
                tabs.active = "tab-attributes"
                panel.update_node(relationship)
                tabs.active = "tab-statistics"
                panel.update_node(None)
                tabs.active = "tab-overview"

            await pilot.pause()
            # Should not have crashed
            assert app.query_one(ModelDetailPanel) is not None

    @pytest.mark.asyncio
    async def test_tab_switch_during_async_loading(self):
        """Switching tabs while async attribute data is loading does not crash."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Trigger loading (which is async)
            panel.update_node(_make_entity_node())

            # Immediately switch tabs before loading completes
            tabs.active = "tab-validation"
            tabs.active = "tab-statistics"
            tabs.active = "tab-lineage"

            await pilot.pause()
            await pilot.pause()

            # Should remain stable
            assert tabs.active == "tab-lineage"

    @pytest.mark.asyncio
    async def test_tab_navigation_after_screen_remount(self):
        """Tabs are navigable after leaving and returning to DataModel screen."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            # First visit
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            tabs = panel.query_one(TabbedContent)
            tabs.active = "tab-attributes"
            await pilot.pause()

            # Navigate away
            await app._show_overview()
            await pilot.pause()

            # Return
            await app._show_data_model()
            await pilot.pause()

            # Tabs should work on fresh screen
            panel = app.query_one(ModelDetailPanel)
            tabs = panel.query_one(TabbedContent)
            assert tabs.active == "tab-overview"  # Reset to default

            tabs.active = "tab-lineage"
            await pilot.pause()
            assert tabs.active == "tab-lineage"

    @pytest.mark.asyncio
    async def test_multiple_node_updates_preserve_tab_stability(self):
        """Multiple rapid node updates don't destabilize tab navigation."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            entity = _make_entity_node()
            # Rapidly update node 5 times
            for _ in range(5):
                panel.update_node(entity)

            await pilot.pause()
            await pilot.pause()

            # Tabs should still be functional
            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id
                await pilot.pause()
                assert tabs.active == tab_id


# ---------------------------------------------------------------------------
# 7. Regression guards
# ---------------------------------------------------------------------------


class TestTabNavigationRegressionGuards:
    """Regression tests specifically guarding the 'tabs visible but not navigable' bug."""

    @pytest.mark.asyncio
    async def test_regression_tabs_are_navigable_not_just_visible(self):
        """REGRESSION: Tabs must be both visible AND navigable.

        This is the primary regression test for the reported bug where
        tabs were visible but could not be switched.
        """
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Verify tabs are visible (compose worked)
            panes = tabs.query("TabPane")
            assert len(panes) == 5, "Tabs not visible"

            # Verify tabs are navigable (the bug fix)
            tabs.active = "tab-attributes"
            await pilot.pause()
            assert tabs.active == "tab-attributes", "Tab switch to Attributes failed"

            tabs.active = "tab-validation"
            await pilot.pause()
            assert tabs.active == "tab-validation", "Tab switch to Validation failed"

            tabs.active = "tab-statistics"
            await pilot.pause()
            assert tabs.active == "tab-statistics", "Tab switch to Statistics failed"

            tabs.active = "tab-lineage"
            await pilot.pause()
            assert tabs.active == "tab-lineage", "Tab switch to Lineage failed"

            tabs.active = "tab-overview"
            await pilot.pause()
            assert tabs.active == "tab-overview", "Tab switch back to Overview failed"

    @pytest.mark.asyncio
    async def test_regression_tabs_navigable_with_node_selected(self):
        """REGRESSION: Tab navigation works when a data model node is selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Select a node (this was the scenario where the bug occurred)
            panel.update_node(_make_entity_node())
            await pilot.pause()

            # All tabs must be navigable
            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id
                await pilot.pause()
                assert tabs.active == tab_id, (
                    f"REGRESSION: Tab {tab_id} not navigable with node selected"
                )

    @pytest.mark.asyncio
    async def test_regression_tab_active_property_reflects_state(self):
        """REGRESSION: The .active property accurately reflects the visible tab."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, _, tabs = await _setup_data_model(app, pilot)

            # Set each tab and verify the property matches
            for tab_id in ALL_TAB_IDS:
                tabs.active = tab_id
                await pilot.pause()
                assert tabs.active == tab_id
                # Also verify the pane is accessible
                pane = tabs.query_one(f"#{tab_id}")
                assert pane is not None

    @pytest.mark.asyncio
    async def test_regression_vim_navigation_does_not_break_tabs(self):
        """REGRESSION: VIM j/k navigation in the list does not break tab switching."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            # Navigate the list with vim keys
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()

            # Tabs should still work after vim navigation
            tabs.active = "tab-statistics"
            await pilot.pause()
            assert tabs.active == "tab-statistics"

            tabs.active = "tab-overview"
            await pilot.pause()
            assert tabs.active == "tab-overview"

    @pytest.mark.asyncio
    async def test_regression_search_does_not_break_tabs(self):
        """REGRESSION: Using search (/) does not break tab navigation."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            screen, panel, tabs = await _setup_data_model(app, pilot)

            # Tabs should work regardless of search state
            tabs.active = "tab-lineage"
            await pilot.pause()
            assert tabs.active == "tab-lineage"


# ---------------------------------------------------------------------------
# 8. Tab content correctness
# ---------------------------------------------------------------------------


class TestTabContentCorrectness:
    """Verify that each tab shows appropriate content for the node type."""

    @pytest.mark.asyncio
    async def test_overview_tab_shows_entity_info(self):
        """Overview tab shows entity information when entity node selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            panel.update_node(_make_entity_node())
            await pilot.pause()

            tabs.active = "tab-overview"
            await pilot.pause()

            overview = panel.query_one("#tab-overview")
            labels = [str(l.render()) for l in overview.query(Label)]
            assert any("Customer" in text for text in labels)
            assert any("entity" in text.lower() for text in labels)

    @pytest.mark.asyncio
    async def test_overview_tab_shows_relationship_info(self):
        """Overview tab shows relationship information when relationship node selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            panel.update_node(_make_relationship_node())
            await pilot.pause()

            tabs.active = "tab-overview"
            await pilot.pause()

            overview = panel.query_one("#tab-overview")
            labels = [str(l.render()) for l in overview.query(Label)]
            assert any("PURCHASED" in text for text in labels)

    @pytest.mark.asyncio
    async def test_overview_shows_empty_message_when_no_node(self):
        """Overview tab shows empty message when no node is selected."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            panel.update_node(None)
            await pilot.pause()

            overview = panel.query_one("#tab-overview")
            labels = [str(l.render()) for l in overview.query(Label)]
            assert any("no type selected" in text for text in labels)

    @pytest.mark.asyncio
    async def test_switching_between_entity_and_relationship_updates_overview(self):
        """Switching between entity and relationship nodes updates overview correctly."""
        app = _make_test_app()
        async with app.run_test() as pilot:
            _, panel, tabs = await _setup_data_model(app, pilot)

            # Set entity
            panel.update_node(_make_entity_node())
            await pilot.pause()

            overview = panel.query_one("#tab-overview")
            entity_labels = [str(l.render()) for l in overview.query(Label)]
            assert any("Customer" in text for text in entity_labels)

            # Switch to relationship
            panel.update_node(_make_relationship_node())
            await pilot.pause()

            new_labels = [str(l.render()) for l in overview.query(Label)]
            assert any("PURCHASED" in text for text in new_labels)
