"""Entity editor screen — edit entity source configuration.

Extends VimEditableScreen with entity-specific field definitions,
validation, and ConfigManager mutations. Supports editing URI,
ID column, entity type, and schema hints.
"""

from __future__ import annotations

import logging

from textual.css.query import NoMatches
from textual.message import Message

from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.adapters.view_models import EntitySourceViewModel
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem
from pycypher_tui.screens.editable_base import (
    EditableField,
    FieldValidationResult,
    VimEditableScreen,
)

logger = logging.getLogger(__name__)


class EntitySourceListItem(BaseListItem[EntitySourceViewModel]):
    """List item for an entity source in the editor."""

    CSS = """
    EntitySourceListItem {
        width: 100%;
        height: 2;
        padding: 0 2;
    }

    EntitySourceListItem.item-focused {
        background: #283457;
    }

    EntitySourceListItem .source-name {
        width: 100%;
        height: 1;
    }

    EntitySourceListItem .source-uri {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, source: EntitySourceViewModel, **kwargs) -> None:
        from textual.app import ComposeResult
        from textual.widgets import Label

        super().__init__(**kwargs)
        self.source = source

    def compose(self):
        from textual.app import ComposeResult
        from textual.widgets import Label

        id_text = f"  [ID: {self.source.id_col}]" if self.source.id_col else ""
        yield Label(
            f"  {self.source.entity_type} ({self.source.source_id}){id_text}",
            classes="source-name",
        )
        yield Label(f"    {self.source.uri}", classes="source-uri")


class EntityEditorScreen(VimEditableScreen[EntitySourceViewModel]):
    """Editor for entity source configurations.

    Displays entity sources for a specific entity type and allows
    editing their properties via the VimEditableScreen form system.

    VIM navigation:
        j/k         - Move between sources
        Enter/l     - Edit selected source (opens form)
        a           - Add new source
        dd          - Delete selected source
        Tab         - Next field (in edit mode)
        Shift+Tab   - Previous field (in edit mode)
        Escape      - Cancel edit / navigate back
        h           - Navigate back
    """

    class SourceUpdated(Message):
        """Emitted when a source has been updated."""

        def __init__(self, source_id: str) -> None:
            super().__init__()
            self.source_id = source_id

    class SourceAdded(Message):
        """Emitted when a new source has been added."""

    class SourceDeleted(Message):
        """Emitted when a source has been deleted."""

        def __init__(self, source_id: str) -> None:
            super().__init__()
            self.source_id = source_id

    def __init__(
        self,
        config_manager: ConfigManager,
        entity_type: str,
        adapter: DataModelAdapter | None = None,
        **kwargs,
    ) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._entity_type = entity_type
        self._adapter = adapter or DataModelAdapter(config_manager=config_manager)

    @property
    def entity_type(self) -> str:
        return self._entity_type

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return f"Edit: {self._entity_type}"

    @property
    def breadcrumb_text(self) -> str:
        return f"Pipeline > Entity Types > {self._entity_type}"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:edit  a:add  dd:delete  Tab:next-field  Escape:back"

    @property
    def empty_list_message(self) -> str:
        return (
            f"No sources for entity type '{self._entity_type}'.\n"
            f"Press 'a' to add a source."
        )

    # --- VimNavigableScreen abstract methods ---

    def load_items(self) -> list[EntitySourceViewModel]:
        self._adapter.refresh()
        detail = self._adapter.entity_detail(self._entity_type)
        return list(detail.sources)

    def create_list_item(
        self, item: EntitySourceViewModel, item_id: str
    ) -> BaseListItem:
        return EntitySourceListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return BaseDetailPanel(
            empty_message="(select a source to edit)", id=self.detail_panel_id
        )

    def update_detail_panel(self, item: EntitySourceViewModel | None) -> None:
        from textual.widgets import Label

        try:
            panel = self.query_one(f"#{self.detail_panel_id}", BaseDetailPanel)
            panel.remove_children()

            if item is None:
                panel.mount(Label("(no source selected)", classes="detail-title"))
                return

            panel.mount(Label(f"Source: {item.source_id}", classes="detail-title"))
            panel.mount(Label(f"  URI: {item.uri}", classes="detail-row"))
            panel.mount(Label(f"  Entity Type: {item.entity_type}", classes="detail-row"))
            if item.id_col:
                panel.mount(Label(f"  ID Column: {item.id_col}", classes="detail-row"))
            if item.query:
                panel.mount(Label(f"  Query: {item.query[:50]}...", classes="detail-row"))
            if item.schema_hints:
                panel.mount(Label("  Schema Hints:", classes="detail-section"))
                for col, dtype in item.schema_hints:
                    panel.mount(Label(f"    {col}: {dtype}", classes="detail-row"))

            panel.mount(Label("", classes="detail-row"))
            panel.mount(Label("Press Enter to edit fields", classes="detail-label"))

        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: EntitySourceViewModel) -> str:
        return item.source_id

    def get_item_search_text(self, item: EntitySourceViewModel) -> str:
        return f"{item.source_id} {item.uri} {item.entity_type}"

    def on_edit(self, item: EntitySourceViewModel) -> None:
        """Open the edit form for the selected source."""
        self.start_editing(item)
        # For now, use dialog-based editing (form integration comes with mount)
        self._edit_source_uri(item)

    def _edit_source_uri(self, item: EntitySourceViewModel) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            new_uri = response.value.strip()
            if new_uri == item.uri:
                return
            self.config_manager.update_entity_source(item.source_id, uri=new_uri)
            self._adapter.refresh()
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.SourceUpdated(item.source_id))

        self.app.push_screen(
            InputDialog(
                title=f"Edit Entity Source: {item.source_id}",
                body="URI:",
                placeholder=item.uri,
                default_value=item.uri,
            ),
            callback=_got_uri,
        )

    def on_add(self) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_id(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step2(response.value.strip())

        self.app.push_screen(
            InputDialog(
                title=f"Add Source for {self._entity_type}",
                body="Source ID:",
                placeholder="e.g. customers_api",
            ),
            callback=_got_id,
        )

    def _add_step2(self, source_id: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self.config_manager.add_entity_source(
                source_id, response.value.strip(), self._entity_type
            )
            self._adapter.refresh()
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.SourceAdded())

        self.app.push_screen(
            InputDialog(
                title=f"Add Source for {self._entity_type}",
                body="File URI:",
                placeholder="e.g. data/customers.csv",
            ),
            callback=_got_uri,
        )

    async def on_delete(self, item: EntitySourceViewModel) -> None:
        self.config_manager.remove_entity_source(item.source_id)
        self._adapter.refresh()
        await self.refresh_from_config()
        self.post_message(self.SourceDeleted(item.source_id))

    # --- VimEditableScreen field definitions ---

    def get_fields(self, item: EntitySourceViewModel | None) -> list[EditableField]:
        if item is None:
            return [
                EditableField(
                    name="source_id", label="Source ID",
                    required=True, placeholder="e.g. customers_csv",
                ),
                EditableField(
                    name="uri", label="URI",
                    required=True, placeholder="e.g. data/customers.csv",
                ),
                EditableField(
                    name="entity_type", label="Entity Type",
                    value=self._entity_type, readonly=True,
                ),
                EditableField(
                    name="id_col", label="ID Column",
                    placeholder="e.g. customer_id (optional)",
                ),
            ]
        return [
            EditableField(
                name="source_id", label="Source ID",
                value=item.source_id, readonly=True,
            ),
            EditableField(
                name="uri", label="URI",
                value=item.uri, required=True,
            ),
            EditableField(
                name="entity_type", label="Entity Type",
                value=item.entity_type, readonly=True,
            ),
            EditableField(
                name="id_col", label="ID Column",
                value=item.id_col or "",
                placeholder="e.g. customer_id (optional)",
            ),
        ]

    def validate_field(self, name: str, value: str) -> FieldValidationResult:
        match name:
            case "source_id":
                if not value.strip():
                    return FieldValidationResult(
                        valid=False, error="Source ID is required"
                    )
                if " " in value:
                    return FieldValidationResult(
                        valid=False, error="Source ID cannot contain spaces"
                    )
                return FieldValidationResult(valid=True)
            case "uri":
                if not value.strip():
                    return FieldValidationResult(
                        valid=False, error="URI is required"
                    )
                return FieldValidationResult(valid=True)
            case _:
                return FieldValidationResult(valid=True)

    def apply_changes(
        self, item: EntitySourceViewModel | None, field_values: dict[str, str]
    ) -> None:
        if item is None:
            # Create new source
            self.config_manager.add_entity_source(
                field_values["source_id"],
                field_values["uri"],
                self._entity_type,
                id_col=field_values.get("id_col") or None,
            )
        else:
            # Update existing source
            updates = {}
            if field_values.get("uri") and field_values["uri"] != item.uri:
                updates["uri"] = field_values["uri"]
            id_col_val = field_values.get("id_col", "").strip() or None
            if id_col_val != item.id_col:
                updates["id_col"] = id_col_val
            if updates:
                self.config_manager.update_entity_source(item.source_id, **updates)

        self._adapter.refresh()
