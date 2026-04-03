"""Tests for VimEditableScreen base class."""

from __future__ import annotations

import pytest

from pycypher_tui.screens.editable_base import (
    EditableField,
    EditableFieldWidget,
    FieldValidationResult,
    ValidationPanel,
    VimEditableScreen,
)
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem
from pycypher_tui.config.pipeline import ConfigManager
from pycypher.ingestion.config import (
    EntitySourceConfig,
    PipelineConfig,
    ProjectConfig,
    SourcesConfig,
)


# -- EditableField Tests ----------------------------------------------------

class TestEditableField:
    def test_default_values(self):
        field = EditableField(name="test", label="Test")
        assert field.value == ""
        assert field.placeholder == ""
        assert field.required is False
        assert field.readonly is False
        assert field.validation_error is None

    def test_required_field(self):
        field = EditableField(name="id", label="ID", required=True)
        assert field.required is True

    def test_readonly_field(self):
        field = EditableField(
            name="type",
            label="Type",
            value="entity",
            readonly=True,
        )
        assert field.readonly is True
        assert field.value == "entity"

    def test_field_with_validation_error(self):
        field = EditableField(
            name="uri",
            label="URI",
            validation_error="File not found",
        )
        assert field.validation_error == "File not found"


# -- FieldValidationResult Tests -------------------------------------------

class TestFieldValidationResult:
    def test_valid_result(self):
        result = FieldValidationResult(valid=True)
        assert result.valid is True
        assert result.error is None

    def test_invalid_result(self):
        result = FieldValidationResult(valid=False, error="Invalid format")
        assert result.valid is False
        assert result.error == "Invalid format"


# -- Concrete test subclass for VimEditableScreen ---------------------------

def _make_config() -> PipelineConfig:
    return PipelineConfig(
        version="1.0",
        project=ProjectConfig(name="test"),
        sources=SourcesConfig(entities=[], relationships=[]),
        queries=[],
        output=[],
    )


class _SimpleItem:
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class _TestListItem(BaseListItem[_SimpleItem]):
    pass


class _TestEditableScreen(VimEditableScreen[_SimpleItem]):
    """Minimal concrete subclass for testing."""

    @property
    def screen_title(self) -> str:
        return "Test Editor"

    @property
    def breadcrumb_text(self) -> str:
        return "Test > Editor"

    @property
    def footer_hints(self) -> str:
        return "Tab:next  Escape:cancel"

    def load_items(self) -> list[_SimpleItem]:
        return [_SimpleItem("item1", "value1")]

    def create_list_item(self, item, item_id):
        return _TestListItem(id=item_id)

    def create_detail_panel(self):
        return BaseDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item):
        pass

    def get_item_id(self, item):
        return item.name

    def on_edit(self, item):
        self.start_editing(item)

    def on_add(self):
        self.start_editing(None)

    async def on_delete(self, item):
        pass

    def get_fields(self, item):
        if item is None:
            return [
                EditableField(name="name", label="Name", required=True),
                EditableField(name="value", label="Value", placeholder="Enter value"),
            ]
        return [
            EditableField(
                name="name",
                label="Name",
                value=item.name,
                readonly=True,
            ),
            EditableField(
                name="value",
                label="Value",
                value=item.value,
            ),
        ]

    def validate_field(self, name, value):
        if name == "name" and not value.strip():
            return FieldValidationResult(valid=False, error="Name is required")
        if name == "name" and " " in value:
            return FieldValidationResult(valid=False, error="No spaces allowed")
        return FieldValidationResult(valid=True)

    def apply_changes(self, item, field_values):
        if item is not None:
            item.value = field_values.get("value", item.value)


# -- VimEditableScreen Tests ------------------------------------------------

class TestVimEditableScreenInit:
    def test_creates_with_config_manager(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        assert screen._fields == []
        assert screen._field_cursor == 0
        assert screen._editing_item is None

    def test_start_editing_existing_item(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        item = _SimpleItem("test", "hello")
        screen.start_editing(item)

        assert screen._editing_item is item
        assert len(screen._fields) == 2
        assert screen._fields[0].name == "name"
        assert screen._fields[0].readonly is True
        assert screen._fields[1].name == "value"
        assert screen._fields[1].value == "hello"

    def test_start_editing_new_item(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        screen.start_editing(None)

        assert screen._editing_item is None
        assert len(screen._fields) == 2
        assert screen._fields[0].required is True
        assert screen._fields[0].readonly is False

    def test_initial_values_captured(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        item = _SimpleItem("test", "hello")
        screen.start_editing(item)

        assert screen._initial_values == {"name": "test", "value": "hello"}


class TestFieldValidation:
    def test_validate_valid_field(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        result = screen.validate_field("name", "valid_name")
        assert result.valid is True

    def test_validate_empty_required(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        result = screen.validate_field("name", "")
        assert result.valid is False
        assert result.error == "Name is required"

    def test_validate_name_with_spaces(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        result = screen.validate_field("name", "has space")
        assert result.valid is False
        assert "spaces" in result.error.lower()


class TestApplyChanges:
    def test_apply_changes_updates_item(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        item = _SimpleItem("test", "old_value")
        screen.apply_changes(item, {"name": "test", "value": "new_value"})

        assert item.value == "new_value"


class TestScreenOverrideKeys:
    def test_override_keys_include_tab(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        override = screen._screen_override_keys
        assert "tab" in override
        assert "shift+tab" in override
        assert "ctrl+u" in override
        assert "escape" in override

    def test_handle_extra_key_tab(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        # Tab with no fields is safe (no-op)
        assert screen.handle_extra_key("tab") is True

    def test_handle_extra_key_unknown(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        assert screen.handle_extra_key("x") is False


class TestDirtyState:
    def test_not_dirty_initially(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        # No field widgets, so not dirty
        assert screen.is_form_dirty is False

    def test_undo_redo_delegation(self):
        mgr = ConfigManager.from_config(_make_config())
        screen = _TestEditableScreen.__new__(_TestEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)

        # Undo/redo when nothing to undo is safe (no-op)
        screen.undo()
        screen.redo()
