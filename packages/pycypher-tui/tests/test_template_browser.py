"""Tests for the template browser screen."""

from __future__ import annotations

import pytest

from pycypher_tui.config.templates import (
    PipelineTemplate,
    get_template,
    list_templates,
)
from pycypher_tui.screens.template_browser import (
    TemplateBrowserScreen,
    TemplateDetailPanel,
    TemplateListItem,
    TemplateSummary,
    summarise_template,
)

# ---------------------------------------------------------------------------
# TemplateSummary dataclass tests
# ---------------------------------------------------------------------------


class TestTemplateSummary:
    """Tests for the TemplateSummary frozen dataclass."""

    def test_create_summary(self):
        s = TemplateSummary(
            name="test",
            description="A test template",
            category="testing",
            entity_count=2,
            relationship_count=1,
            query_count=3,
            output_count=1,
        )
        assert s.name == "test"
        assert s.description == "A test template"
        assert s.category == "testing"
        assert s.entity_count == 2
        assert s.relationship_count == 1
        assert s.query_count == 3
        assert s.output_count == 1

    def test_frozen_immutability(self):
        s = TemplateSummary(
            name="x", description="x", category="x",
            entity_count=0, relationship_count=0,
            query_count=0, output_count=0,
        )
        with pytest.raises(AttributeError):
            s.name = "changed"

    def test_equality(self):
        kwargs = dict(
            name="a", description="b", category="c",
            entity_count=1, relationship_count=2,
            query_count=3, output_count=4,
        )
        assert TemplateSummary(**kwargs) == TemplateSummary(**kwargs)


# ---------------------------------------------------------------------------
# summarise_template tests
# ---------------------------------------------------------------------------


class TestSummariseTemplate:
    """Tests for the summarise_template helper."""

    def test_summarise_csv_analytics(self):
        t = get_template("csv_analytics")
        s = summarise_template(t)
        assert s.name == "csv_analytics"
        assert s.entity_count >= 1
        assert s.query_count >= 1
        assert s.output_count >= 1

    def test_summarise_ecommerce(self):
        t = get_template("ecommerce_pipeline")
        s = summarise_template(t)
        assert s.entity_count >= 2
        assert s.relationship_count >= 1
        assert s.query_count >= 2

    def test_summarise_social_network(self):
        t = get_template("social_network")
        s = summarise_template(t)
        assert s.relationship_count >= 2

    def test_summarise_time_series(self):
        t = get_template("time_series")
        s = summarise_template(t)
        assert s.entity_count >= 2

    def test_summarise_broken_template_returns_zeros(self):
        t = PipelineTemplate(
            name="broken",
            description="No builder",
            category="test",
        )
        s = summarise_template(t)
        assert s.name == "broken"
        assert s.entity_count == 0
        assert s.relationship_count == 0
        assert s.query_count == 0
        assert s.output_count == 0

    def test_all_templates_summarise(self):
        for t in list_templates():
            s = summarise_template(t)
            assert isinstance(s, TemplateSummary)
            assert s.name == t.name
            assert s.category == t.category


# ---------------------------------------------------------------------------
# TemplateBrowserScreen logic tests (non-widget, using __new__)
# ---------------------------------------------------------------------------


def _make_screen():
    """Create a TemplateBrowserScreen bypassing __init__ for unit tests."""
    screen = TemplateBrowserScreen.__new__(TemplateBrowserScreen)
    screen._cursor = 0
    screen._items = []
    screen._pending_keys = []
    screen._category_filter = None
    return screen


def _sample_summaries(n=4):
    """Generate n sample TemplateSummary objects."""
    return [
        TemplateSummary(
            name=f"template_{i}",
            description=f"Description {i}",
            category="test" if i % 2 == 0 else "other",
            entity_count=i + 1,
            relationship_count=i,
            query_count=i + 1,
            output_count=1,
        )
        for i in range(n)
    ]


class TestTemplateBrowserProperties:
    """Tests for TemplateBrowserScreen properties."""

    def test_template_count_empty(self):
        screen = _make_screen()
        assert screen.template_count == 0

    def test_template_count_with_items(self):
        screen = _make_screen()
        screen._items = _sample_summaries(3)
        assert screen.template_count == 3

    def test_current_template_none_when_empty(self):
        screen = _make_screen()
        assert screen.current_template is None

    def test_current_template_returns_item(self):
        screen = _make_screen()
        items = _sample_summaries(3)
        screen._items = items
        screen._cursor = 1
        assert screen.current_template == items[1]

    def test_current_template_out_of_bounds(self):
        screen = _make_screen()
        screen._items = _sample_summaries(2)
        screen._cursor = 5
        assert screen.current_template is None

    def test_current_template_negative_cursor(self):
        screen = _make_screen()
        screen._items = _sample_summaries(2)
        screen._cursor = -1
        assert screen.current_template is None


# ---------------------------------------------------------------------------
# Cursor navigation tests
# ---------------------------------------------------------------------------


class TestCursorNavigation:
    """Tests for VIM cursor movement logic."""

    def test_move_down(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._move_cursor(1)
        assert screen._cursor == 1

    def test_move_up(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._cursor = 2
        screen._move_cursor(-1)
        assert screen._cursor == 1

    def test_clamp_at_bottom(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._cursor = 3
        screen._move_cursor(1)
        assert screen._cursor == 3

    def test_clamp_at_top(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._move_cursor(-1)
        assert screen._cursor == 0

    def test_move_on_empty(self):
        screen = _make_screen()
        screen._move_cursor(1)
        assert screen._cursor == 0

    def test_jump_to_specific(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._jump_to(2)
        assert screen._cursor == 2

    def test_jump_to_first(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._cursor = 3
        screen._jump_to(0)
        assert screen._cursor == 0

    def test_jump_to_last(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._jump_to(3)
        assert screen._cursor == 3

    def test_jump_clamps_high(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._jump_to(999)
        assert screen._cursor == 3

    def test_jump_clamps_low(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._jump_to(-10)
        assert screen._cursor == 0

    def test_jump_on_empty(self):
        screen = _make_screen()
        screen._jump_to(5)
        assert screen._cursor == 0

    def test_page_down(self):
        screen = _make_screen()
        screen._items = _sample_summaries(10)
        screen._move_cursor(5)
        assert screen._cursor == 5

    def test_page_up(self):
        screen = _make_screen()
        screen._items = _sample_summaries(10)
        screen._cursor = 7
        screen._move_cursor(-5)
        assert screen._cursor == 2


# ---------------------------------------------------------------------------
# Pending key sequence tests
# ---------------------------------------------------------------------------


class TestPendingKeySequences:
    """Tests for multi-key VIM sequences."""

    def test_gg_jumps_to_first(self):
        screen = _make_screen()
        screen._items = _sample_summaries(4)
        screen._cursor = 3
        screen._pending_keys = ["g"]
        screen._handle_pending("g")
        assert screen._cursor == 0
        assert screen._pending_keys == []

    def test_escape_clears_pending(self):
        screen = _make_screen()
        screen._pending_keys = ["g"]
        screen._handle_pending("escape")
        assert screen._pending_keys == []

    def test_unknown_sequence_clears_pending(self):
        screen = _make_screen()
        screen._pending_keys = ["g"]
        screen._handle_pending("x")
        assert screen._pending_keys == []


# ---------------------------------------------------------------------------
# Message type tests
# ---------------------------------------------------------------------------


class TestMessages:
    """Tests for screen message types."""

    def test_navigate_back_message_is_message_subclass(self):
        from textual.message import Message
        msg = TemplateBrowserScreen.NavigateBack()
        assert isinstance(msg, Message)

    def test_template_selected_message(self):
        msg = TemplateBrowserScreen.TemplateSelected("csv_analytics")
        assert msg.template_name == "csv_analytics"

    def test_template_selected_preserves_name(self):
        msg = TemplateBrowserScreen.TemplateSelected("ecommerce_pipeline")
        assert msg.template_name == "ecommerce_pipeline"


# ---------------------------------------------------------------------------
# CSS and structural tests
# ---------------------------------------------------------------------------


class TestScreenStructure:
    """Tests for screen CSS and structural constants."""

    def test_screen_has_css(self):
        assert TemplateBrowserScreen.CSS is not None
        assert len(TemplateBrowserScreen.CSS) > 0

    def test_detail_panel_has_css(self):
        assert len(TemplateDetailPanel.CSS) > 0

    def test_list_item_has_css(self):
        assert len(TemplateListItem.CSS) > 0

    def test_css_contains_layout_ids(self):
        from pycypher_tui.screens.base import VimNavigableScreen

        # Layout IDs are now provided by the VimNavigableScreen base class
        base_css = VimNavigableScreen.CSS
        assert "#screen-header" in base_css
        assert "#screen-main" in base_css
        assert "#list-panel" in base_css
        assert "#screen-footer" in base_css


# ---------------------------------------------------------------------------
# Category filtering tests
# ---------------------------------------------------------------------------


class TestCategoryFiltering:
    """Tests for category-based filtering of templates."""

    def test_no_filter_shows_all(self):
        screen = _make_screen()
        screen._category_filter = None
        # Simulate what _refresh_templates does
        all_summaries = _sample_summaries(4)
        screen._items = all_summaries
        assert screen.template_count == 4

    def test_filter_by_category(self):
        screen = _make_screen()
        all_summaries = _sample_summaries(4)
        screen._category_filter = "test"
        screen._items = [
            s for s in all_summaries if s.category == screen._category_filter
        ]
        # indices 0, 2 have category "test"
        assert screen.template_count == 2
        assert all(s.category == "test" for s in screen._items)

    def test_filter_no_match(self):
        screen = _make_screen()
        all_summaries = _sample_summaries(4)
        screen._category_filter = "nonexistent"
        screen._items = [
            s for s in all_summaries if s.category == screen._category_filter
        ]
        assert screen.template_count == 0
