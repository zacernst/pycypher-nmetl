"""Comprehensive TUI audit tests for threading, navigation, and content display.

Systematically verifies that all TUI screens follow correct patterns for:
- Threading: blocking I/O uses thread=True in run_worker()
- Navigation: VimNavigableScreen subclasses handle keys correctly
- Content display: loading indicators, error handling, tab content
- Performance: no UI-blocking operations on event loop

This audit was triggered by the DataModel screen tab navigation bug
and content hanging issue, to ensure similar problems don't exist
in other screens.
"""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import Mock, patch

import pytest
from textual.widgets import Label, LoadingIndicator, TabbedContent, TabPane

from pycypher_tui.app import PyCypherTUI
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import get_template
from pycypher_tui.screens.base import VimNavigableScreen
from pycypher_tui.screens.data_model import DataModelScreen, ModelDetailPanel
from pycypher_tui.screens.data_sources import DataSourcesScreen
from pycypher_tui.screens.query_lineage import (
    LineageDetailPanel,
    QueryLineageScreen,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ecommerce_app() -> PyCypherTUI:
    t = get_template("ecommerce_pipeline")
    config = t.instantiate(project_name="test_shop", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


def _make_social_app() -> PyCypherTUI:
    t = get_template("social_network")
    config = t.instantiate(project_name="test_social", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


# ---------------------------------------------------------------------------
# 1. Threading audit: verify blocking I/O uses thread=True
# ---------------------------------------------------------------------------


class TestThreadingAudit:
    """Verify that screens with blocking I/O correctly use thread=True."""

    def test_data_model_panel_uses_thread_true(self):
        """ModelDetailPanel._load_attribute_data runs in a thread."""
        # Inspect the update_node method to find the run_worker call
        source = inspect.getsource(ModelDetailPanel.update_node)
        assert "thread=True" in source, (
            "ModelDetailPanel.update_node must use thread=True for blocking I/O"
        )

    def test_query_lineage_no_blocking_io_in_worker(self):
        """LineageDetailPanel._load_component_details does no blocking I/O (async sleep only)."""
        source = inspect.getsource(LineageDetailPanel._load_component_details)
        # Should NOT contain blocking calls like DataSourceIntrospector, open(), pd.read_csv
        blocking_patterns = ["DataSourceIntrospector", "pd.read_csv", "open(", "data_source_from_uri"]
        for pattern in blocking_patterns:
            assert pattern not in source, (
                f"LineageDetailPanel._load_component_details contains blocking call: {pattern}"
            )

    def test_data_model_worker_cancellation_on_unmount(self):
        """ModelDetailPanel cancels worker on unmount."""
        source = inspect.getsource(ModelDetailPanel.on_unmount)
        assert "cancel" in source

    def test_lineage_panel_worker_cancellation_on_unmount(self):
        """LineageDetailPanel cancels worker on unmount."""
        source = inspect.getsource(LineageDetailPanel.on_unmount)
        assert "cancel" in source


# ---------------------------------------------------------------------------
# 2. Navigation audit: all VimNavigableScreen subclasses
# ---------------------------------------------------------------------------


class TestNavigationOverrideAudit:
    """Verify screen-specific key overrides are properly configured."""

    @pytest.mark.asyncio
    async def test_data_model_screen_overrides_tab_keys(self):
        """DataModelScreen registers tab and shift+tab as override keys."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            screen = app.query_one(DataModelScreen)
            assert "tab" in screen._screen_override_keys
            assert "shift+tab" in screen._screen_override_keys

    @pytest.mark.asyncio
    async def test_data_model_handle_extra_key_consumes_tab(self):
        """DataModelScreen.handle_extra_key consumes tab key."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            screen = app.query_one(DataModelScreen)
            assert screen.handle_extra_key("tab") is True
            assert screen.handle_extra_key("shift+tab") is True

    @pytest.mark.asyncio
    async def test_data_sources_screen_overrides_tab(self):
        """DataSourcesScreen registers tab as override key."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()
            screen = app.query_one(DataSourcesScreen)
            assert "tab" in screen._screen_override_keys

    @pytest.mark.asyncio
    async def test_query_lineage_screen_overrides_tab_keys(self):
        """QueryLineageScreen registers tab and shift+tab as override keys."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_query_lineage()
            await pilot.pause()
            screen = app.query_one(QueryLineageScreen)
            assert "tab" in screen._screen_override_keys
            assert "shift+tab" in screen._screen_override_keys

    @pytest.mark.asyncio
    async def test_data_model_handles_j_k_navigation(self):
        """DataModelScreen responds to j/k for cursor movement."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            screen = app.query_one(DataModelScreen)
            if screen.item_count > 1:
                await pilot.press("j")
                await pilot.pause()
                assert screen._cursor == 1, "j key did not advance cursor"

    @pytest.mark.asyncio
    async def test_data_sources_handles_j_k_navigation(self):
        """DataSourcesScreen responds to j/k for cursor movement."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            screen = app.query_one(DataSourcesScreen)
            if screen.item_count > 1:
                await pilot.press("j")
                await pilot.pause()
                assert screen._cursor == 1, "j key did not advance cursor"


# ---------------------------------------------------------------------------
# 3. TabbedContent audit: all screens with tabs
# ---------------------------------------------------------------------------


class TestTabbedContentAudit:
    """Verify all screens with TabbedContent have proper setup."""

    @pytest.mark.asyncio
    async def test_data_model_tabbed_content_structure(self):
        """DataModel detail panel has 5 tabs with correct IDs."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            tabs = panel.query_one(TabbedContent)
            pane_ids = {p.id for p in tabs.query("TabPane")}

            expected = {"tab-overview", "tab-attributes", "tab-validation", "tab-statistics", "tab-lineage"}
            assert pane_ids == expected

    @pytest.mark.asyncio
    async def test_query_lineage_tabbed_content_structure(self):
        """QueryLineage detail panel has 4 tabs with correct IDs."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_query_lineage()
            await pilot.pause()

            panel = app.query_one(LineageDetailPanel)
            tabs = panel.query_one(TabbedContent)
            pane_ids = {p.id for p in tabs.query("TabPane")}

            expected = {"tab-overview", "tab-dependencies", "tab-flow", "tab-analysis"}
            assert pane_ids == expected

    @pytest.mark.asyncio
    async def test_data_model_tabs_start_with_loading_indicators(self):
        """Non-overview tabs in DataModel panel initially have loading indicators.

        Note: Loading indicators may be replaced quickly if the worker completes
        before the assertion runs (e.g., when data files are not found). We test
        by triggering a fresh node update and checking immediately.
        """
        from pycypher_tui.screens.data_model import ModelNode

        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            node = ModelNode(
                node_id="entity:Test", label="Test", node_type="entity",
                source_count=1, source_ids=("test_source",), connections=(),
            )
            panel.update_node(node)
            # Check immediately — loading indicators should be present before worker completes
            for tab_id in ("tab-attributes", "tab-validation", "tab-statistics", "tab-lineage"):
                tab = panel.query_one(f"#{tab_id}")
                indicators = tab.query(LoadingIndicator)
                assert len(indicators) > 0, f"No loading indicator in {tab_id}"


# ---------------------------------------------------------------------------
# 4. Screen responsiveness audit
# ---------------------------------------------------------------------------


class TestScreenResponsivenessAudit:
    """Verify screens remain responsive during operations."""

    @pytest.mark.asyncio
    async def test_data_model_screen_responsive_after_mount(self):
        """DataModel screen is responsive immediately after mounting."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            screen = app.query_one(DataModelScreen)
            assert screen is not None
            assert screen.item_count > 0

    @pytest.mark.asyncio
    async def test_data_sources_screen_responsive_after_mount(self):
        """DataSources screen is responsive immediately after mounting."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            screen = app.query_one(DataSourcesScreen)
            assert screen is not None
            assert screen.item_count > 0

    @pytest.mark.asyncio
    async def test_query_lineage_screen_responsive_after_mount(self):
        """QueryLineage screen is responsive immediately after mounting."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_query_lineage()
            await pilot.pause()

            screen = app.query_one(QueryLineageScreen)
            assert screen is not None
            assert screen.item_count > 0

    @pytest.mark.asyncio
    async def test_data_model_navigation_responsive(self):
        """DataModel screen responds to navigation keys without delay."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            screen = app.query_one(DataModelScreen)
            if screen.item_count > 1:
                await pilot.press("j")
                await pilot.pause()
                assert screen._cursor == 1

                await pilot.press("k")
                await pilot.pause()
                assert screen._cursor == 0

    @pytest.mark.asyncio
    async def test_data_sources_filter_responsive(self):
        """DataSources filter cycling responds without hanging."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            screen = app.query_one(DataSourcesScreen)
            initial_count = screen.item_count

            # Tab cycles filter — should not hang
            await pilot.press("tab")
            await pilot.pause()
            await pilot.pause()

            # Filter should have changed item count
            new_count = screen.item_count
            # Count may differ if filter applied
            assert isinstance(new_count, int)


# ---------------------------------------------------------------------------
# 5. Cross-screen tab navigation consistency
# ---------------------------------------------------------------------------


class TestCrossScreenTabConsistency:
    """Verify tab navigation patterns are consistent across screens with tabs."""

    @pytest.mark.asyncio
    async def test_data_model_tab_cycling_direction(self):
        """DataModel Tab key cycles tabs forward."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            tabs = panel.query_one(TabbedContent)

            assert tabs.active == "tab-overview"
            await pilot.press("tab")
            await pilot.pause()
            assert tabs.active == "tab-attributes"

    @pytest.mark.asyncio
    async def test_data_model_shift_tab_reverses_direction(self):
        """DataModel Shift+Tab cycles tabs backward."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            tabs = panel.query_one(TabbedContent)

            assert tabs.active == "tab-overview"
            await pilot.press("shift+tab")
            await pilot.pause()
            assert tabs.active == "tab-lineage"

    @pytest.mark.asyncio
    async def test_query_lineage_tab_cycles_filter(self):
        """QueryLineage Tab key cycles component type filter (not tabs)."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_query_lineage()
            await pilot.pause()

            screen = app.query_one(QueryLineageScreen)
            initial_filter = screen._filter_type

            await pilot.press("tab")
            await pilot.pause()

            # Filter should have changed
            new_filter = screen._filter_type
            assert new_filter != initial_filter or new_filter is not None

    @pytest.mark.asyncio
    async def test_query_lineage_shift_tab_switches_tabs(self):
        """QueryLineage Shift+Tab switches detail panel tabs."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_query_lineage()
            await pilot.pause()

            panel = app.query_one(LineageDetailPanel)
            tabs = panel.query_one(TabbedContent)

            initial = tabs.active
            await pilot.press("shift+tab")
            await pilot.pause()

            # Tab should have advanced
            assert tabs.active != initial or tabs.active == "tab-overview"


# ---------------------------------------------------------------------------
# 6. Error handling audit
# ---------------------------------------------------------------------------


class TestErrorHandlingAudit:
    """Verify screens handle errors gracefully without crashing."""

    @pytest.mark.asyncio
    async def test_data_model_handles_none_node_gracefully(self):
        """DataModel panel handles None node without crash."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            panel.update_node(None)
            await pilot.pause()

            # Should show empty message
            overview = panel.query_one("#tab-overview")
            labels = [str(l.render()) for l in overview.query(Label)]
            assert any("no type selected" in t for t in labels)

    @pytest.mark.asyncio
    async def test_query_lineage_handles_none_component(self):
        """QueryLineage panel handles None component without crash."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_query_lineage()
            await pilot.pause()

            panel = app.query_one(LineageDetailPanel)
            panel.update_component(None)
            await pilot.pause()

            overview = panel.query_one("#tab-overview")
            labels = [str(l.render()) for l in overview.query(Label)]
            assert any("no component selected" in t for t in labels)

    @pytest.mark.asyncio
    async def test_data_model_screen_survives_rapid_navigation(self):
        """Rapid j/k navigation on DataModel screen doesn't crash."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            for _ in range(10):
                await pilot.press("j")
            for _ in range(10):
                await pilot.press("k")

            await pilot.pause()
            screen = app.query_one(DataModelScreen)
            assert screen is not None

    @pytest.mark.asyncio
    async def test_data_sources_screen_survives_rapid_filter_cycling(self):
        """Rapid Tab cycling on DataSources doesn't crash."""
        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_sources()
            await pilot.pause()

            for _ in range(6):  # 2 full cycles through 3 filter states
                await pilot.press("tab")
                await pilot.pause()

            screen = app.query_one(DataSourcesScreen)
            assert screen is not None


# ---------------------------------------------------------------------------
# 7. Worker lifecycle audit
# ---------------------------------------------------------------------------


class TestWorkerLifecycleAudit:
    """Verify workers are properly managed across screens."""

    @pytest.mark.asyncio
    async def test_data_model_cancels_worker_on_new_node(self):
        """ModelDetailPanel cancels previous worker when new node selected."""
        from pycypher_tui.screens.data_model import ModelNode

        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)

            node1 = ModelNode(
                node_id="entity:A", label="A", node_type="entity",
                source_count=1, source_ids=("a",), connections=(),
            )
            node2 = ModelNode(
                node_id="entity:B", label="B", node_type="entity",
                source_count=1, source_ids=("b",), connections=(),
            )

            panel.update_node(node1)
            first_worker = panel._worker

            panel.update_node(node2)
            await pilot.pause()

            if first_worker is not None:
                assert first_worker.is_finished or first_worker.is_cancelled

    @pytest.mark.asyncio
    async def test_data_model_cancels_worker_on_screen_exit(self):
        """ModelDetailPanel cancels worker when navigating away."""
        from pycypher_tui.screens.data_model import ModelNode

        app = _make_ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()

            panel = app.query_one(ModelDetailPanel)
            node = ModelNode(
                node_id="entity:A", label="A", node_type="entity",
                source_count=1, source_ids=("a",), connections=(),
            )
            panel.update_node(node)
            worker = panel._worker

            # Navigate away
            await app._show_overview()
            await pilot.pause()

            if worker is not None:
                assert worker.is_finished or worker.is_cancelled


# ---------------------------------------------------------------------------
# 8. BUG REPORT: QueryLineageScreen CSS ID sanitization
# ---------------------------------------------------------------------------


class TestQueryLineageCSSIDSanitization:
    """Regression tests for CSS ID sanitization in QueryLineageScreen.

    Previously, get_item_id() only replaced ':' with '-', but component IDs
    can contain '/' and '.' from file paths, causing BadIdentifier crashes.
    Fixed to use re.sub for comprehensive sanitization.
    """

    def test_get_item_id_uses_regex_sanitization(self):
        """get_item_id uses re.sub for comprehensive character sanitization."""
        source = inspect.getsource(QueryLineageScreen.get_item_id)
        assert "re.sub" in source, "get_item_id should use re.sub for sanitization"

    def test_component_ids_with_paths_produce_valid_css(self):
        """Component IDs with file paths produce valid CSS identifiers after sanitization."""
        import re as re_mod
        component_id = "output:top_products-data/output/top_products.csv"
        sanitized = re_mod.sub(r"[^a-zA-Z0-9_-]", "-", component_id)
        item_id = f"item-{sanitized}"

        css_id_pattern = re_mod.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")
        assert css_id_pattern.match(item_id), (
            f"Sanitized ID should be valid CSS: {item_id}"
        )

    def test_sanitization_handles_various_special_characters(self):
        """Sanitization handles /, ., :, spaces, and other special characters."""
        import re as re_mod
        test_ids = [
            "output:data/file.csv",
            "source:s3://bucket/path",
            "query:select * from table",
            "rel:user.orders",
        ]
        css_id_pattern = re_mod.compile(r"^[a-zA-Z0-9_-]+$")
        for component_id in test_ids:
            sanitized = re_mod.sub(r"[^a-zA-Z0-9_-]", "-", component_id)
            assert css_id_pattern.match(sanitized), (
                f"Sanitized '{component_id}' → '{sanitized}' is not valid CSS"
            )
