"""Tests for data preview integration into entity and relationship screens.

Validates:
- `p` key binding registered in _screen_override_keys
- handle_extra_key dispatches `p` to _open_preview
- DataPreviewDialog schema tab fix (no yield in non-generator)
- Preview opens with correct source_uri and source_id
"""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from pycypher_tui.screens.entity_tables import (
    EntityTableInfo,
    EntityTablesScreen,
)
from pycypher_tui.screens.relationships import (
    RelationshipItem,
    RelationshipScreen,
)
from pycypher_tui.widgets.data_preview import DataPreviewDialog, PreviewData

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entity_screen(entities=None):
    """Create an EntityTablesScreen without full Textual lifecycle."""
    screen = EntityTablesScreen.__new__(EntityTablesScreen)
    screen._cursor = 0
    screen._items = entities or []
    screen._pending_keys = []
    return screen


def _make_relationship_screen(rels=None):
    """Create a RelationshipScreen without full Textual lifecycle."""
    screen = RelationshipScreen.__new__(RelationshipScreen)
    screen._cursor = 0
    screen._items = rels or []
    screen._pending_keys = []
    return screen


def _sample_entities():
    return [
        EntityTableInfo("customers_csv", "Customer", "data/customers.csv", "id", {}, False),
        EntityTableInfo("orders_csv", "Order", "data/orders.csv", "order_id", {}, True),
    ]


def _sample_relationships():
    return [
        RelationshipItem(
            source_id="follows_csv",
            relationship_type="FOLLOWS",
            uri="data/follows.csv",
            source_col="from_id",
            target_col="to_id",
            id_col=None,
            source_entity="Person",
            target_entity="Person",
            status="valid",
            validation_messages=[],
        ),
    ]


# ---------------------------------------------------------------------------
# EntityTablesScreen preview key binding
# ---------------------------------------------------------------------------

class TestEntityPreviewKeyBinding:
    def test_p_in_override_keys(self):
        """The 'p' key should be in the screen override keys."""
        screen = _make_entity_screen(_sample_entities())
        assert "p" in screen._screen_override_keys

    def test_handle_extra_key_p_returns_true(self):
        """handle_extra_key('p') should return True (handled)."""
        screen = _make_entity_screen(_sample_entities())
        # Mock app.push_screen since we're not in full Textual lifecycle
        mock_app = MagicMock()
        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            result = screen.handle_extra_key("p")
        assert result is True

    def test_handle_extra_key_unknown_returns_false(self):
        """handle_extra_key with unknown key should return False."""
        screen = _make_entity_screen(_sample_entities())
        result = screen.handle_extra_key("z")
        assert result is False

    def test_open_preview_calls_push_screen(self):
        """_open_preview should call app.push_screen with DataPreviewDialog."""
        screen = _make_entity_screen(_sample_entities())
        mock_app = MagicMock()

        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            screen._open_preview()

        mock_app.push_screen.assert_called_once()
        dialog = mock_app.push_screen.call_args[0][0]
        assert isinstance(dialog, DataPreviewDialog)
        assert dialog.source_uri == "data/customers.csv"
        assert dialog.source_id == "customers_csv"

    def test_open_preview_uses_current_item(self):
        """_open_preview should use the currently selected entity."""
        screen = _make_entity_screen(_sample_entities())
        screen._cursor = 1  # Select second entity
        mock_app = MagicMock()

        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            screen._open_preview()

        dialog = mock_app.push_screen.call_args[0][0]
        assert dialog.source_uri == "data/orders.csv"
        assert dialog.source_id == "orders_csv"

    def test_open_preview_noop_when_empty(self):
        """_open_preview should do nothing when no items exist."""
        screen = _make_entity_screen([])
        mock_app = MagicMock()

        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            screen._open_preview()

        mock_app.push_screen.assert_not_called()

    def test_footer_hints_includes_preview(self):
        """Footer hints should mention the p:preview binding."""
        screen = _make_entity_screen(_sample_entities())
        assert "p:preview" in screen.footer_hints


# ---------------------------------------------------------------------------
# RelationshipScreen preview key binding
# ---------------------------------------------------------------------------

class TestRelationshipPreviewKeyBinding:
    def test_p_in_override_keys(self):
        """The 'p' key should be in the screen override keys."""
        screen = _make_relationship_screen(_sample_relationships())
        assert "p" in screen._screen_override_keys

    def test_handle_extra_key_p_returns_true(self):
        """handle_extra_key('p') should return True (handled)."""
        screen = _make_relationship_screen(_sample_relationships())
        mock_app = MagicMock()
        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            result = screen.handle_extra_key("p")
        assert result is True

    def test_handle_extra_key_unknown_returns_false(self):
        """handle_extra_key with unknown key should return False."""
        screen = _make_relationship_screen(_sample_relationships())
        result = screen.handle_extra_key("z")
        assert result is False

    def test_open_preview_calls_push_screen(self):
        """_open_preview should call app.push_screen with DataPreviewDialog."""
        screen = _make_relationship_screen(_sample_relationships())
        mock_app = MagicMock()

        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            screen._open_preview()

        mock_app.push_screen.assert_called_once()
        dialog = mock_app.push_screen.call_args[0][0]
        assert isinstance(dialog, DataPreviewDialog)
        assert dialog.source_uri == "data/follows.csv"
        assert dialog.source_id == "follows_csv"

    def test_open_preview_noop_when_empty(self):
        """_open_preview should do nothing when no items exist."""
        screen = _make_relationship_screen([])
        mock_app = MagicMock()

        with patch.object(type(screen), "app", new_callable=PropertyMock, return_value=mock_app):
            screen._open_preview()

        mock_app.push_screen.assert_not_called()

    def test_footer_hints_includes_preview(self):
        """Footer hints should mention the p:preview binding."""
        screen = _make_relationship_screen(_sample_relationships())
        assert "p:preview" in screen.footer_hints


# ---------------------------------------------------------------------------
# DataPreviewDialog construction
# ---------------------------------------------------------------------------

class TestDataPreviewDialog:
    def test_dialog_stores_source_uri(self):
        """DataPreviewDialog should store source_uri and source_id."""
        dialog = DataPreviewDialog(source_uri="data/test.csv", source_id="test_csv")
        assert dialog.source_uri == "data/test.csv"
        assert dialog.source_id == "test_csv"

    def test_dialog_title_includes_source_id(self):
        """Dialog title should include the source_id when provided."""
        dialog = DataPreviewDialog(source_uri="data/test.csv", source_id="test_csv")
        assert "test_csv" in dialog.dialog_title

    def test_dialog_title_default_when_no_id(self):
        """Dialog title should be 'Data Preview' when no source_id."""
        dialog = DataPreviewDialog(source_uri="data/test.csv")
        assert dialog.dialog_title == "Data Preview"


# ---------------------------------------------------------------------------
# DataPreviewDialog._update_schema_tab fix validation
# ---------------------------------------------------------------------------

class TestSchemaTabFix:
    def test_update_schema_tab_is_not_generator(self):
        """_update_schema_tab should not be a generator (no yield statements)."""
        import inspect
        assert not inspect.isgeneratorfunction(DataPreviewDialog._update_schema_tab)

    def test_update_data_tab_is_not_generator(self):
        """_update_data_tab should not be a generator."""
        import inspect
        assert not inspect.isgeneratorfunction(DataPreviewDialog._update_data_tab)

    def test_update_stats_tab_is_not_generator(self):
        """_update_stats_tab should not be a generator."""
        import inspect
        assert not inspect.isgeneratorfunction(DataPreviewDialog._update_stats_tab)


# ---------------------------------------------------------------------------
# PreviewData container
# ---------------------------------------------------------------------------

class TestPreviewData:
    def test_default_state(self):
        """PreviewData should have None defaults."""
        data = PreviewData()
        assert data.schema_info is None
        assert data.sample_data is None
        assert data.column_stats is None
        assert data.error is None

    def test_error_state(self):
        """PreviewData can store error information."""
        data = PreviewData(error="File not found")
        assert data.error == "File not found"
        assert data.schema_info is None

    def test_populated_state(self):
        """PreviewData can store all preview results."""
        data = PreviewData(
            schema_info={"columns": [("id", "INT")], "row_count": 100},
            sample_data=[{"id": 1}, {"id": 2}],
            column_stats={"id": {"dtype": "INT", "null_count": 0}},
        )
        assert data.schema_info["row_count"] == 100
        assert len(data.sample_data) == 2
