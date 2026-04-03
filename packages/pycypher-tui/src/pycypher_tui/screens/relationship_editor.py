"""Relationship editor screen — edit relationship source configuration.

Extends VimEditableScreen with relationship-specific field definitions,
validation, and ConfigManager mutations. Unified form replaces the 5
sequential input dialogs from the old RelationshipScreen.

Supports editing:
- Source ID, URI, relationship type
- Source/target column endpoints (with entity resolution)
- ID column and schema hints
"""

from __future__ import annotations

import logging

from textual.app import ComposeResult
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label

from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.adapters.view_models import RelationshipSourceViewModel
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem
from pycypher_tui.screens.editable_base import (
    EditableField,
    FieldValidationResult,
    VimEditableScreen,
)

logger = logging.getLogger(__name__)


class RelationshipSourceListItem(BaseListItem[RelationshipSourceViewModel]):
    """List item for a relationship source in the editor."""

    CSS = """
    RelationshipSourceListItem {
        width: 100%;
        height: 3;
        padding: 0 2;
    }

    RelationshipSourceListItem.item-focused {
        background: #283457;
    }

    RelationshipSourceListItem .source-name {
        width: 100%;
        height: 1;
    }

    RelationshipSourceListItem .source-mapping {
        width: 100%;
        height: 1;
        color: #9ece6a;
        padding-left: 4;
    }

    RelationshipSourceListItem .source-uri {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, source: RelationshipSourceViewModel, **kwargs) -> None:
        super().__init__(**kwargs)
        self.source = source

    def compose(self) -> ComposeResult:
        id_text = f"  [ID: {self.source.id_col}]" if self.source.id_col else ""
        yield Label(
            f"  {self.source.relationship_type} ({self.source.source_id}){id_text}",
            classes="source-name",
        )
        yield Label(
            f"      {self.source.source_col} -> {self.source.target_col}",
            classes="source-mapping",
        )
        yield Label(f"      {self.source.uri}", classes="source-uri")


class RelationshipEditorDetailPanel(BaseDetailPanel):
    """Detail panel for the relationship editor showing source details."""

    CSS = """
    RelationshipEditorDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    RelationshipEditorDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    RelationshipEditorDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    RelationshipEditorDetailPanel .detail-section {
        text-style: bold;
        color: #e0af68;
        width: 100%;
        margin-top: 1;
    }

    RelationshipEditorDetailPanel .detail-label {
        color: #565f89;
    }

    RelationshipEditorDetailPanel .mapping-display {
        color: #9ece6a;
        padding-left: 2;
    }
    """

    def update_source(self, item: RelationshipSourceViewModel | None) -> None:
        """Update detail panel with source information."""
        self.remove_children()

        if item is None:
            self.mount(Label("(no source selected)", classes="detail-title"))
            return

        self.mount(Label(f"Source: {item.source_id}", classes="detail-title"))
        self.mount(Label(f"  URI: {item.uri}", classes="detail-row"))
        self.mount(
            Label(f"  Relationship Type: {item.relationship_type}", classes="detail-row")
        )
        self.mount(Label("", classes="detail-row"))

        # Column mapping visualization
        self.mount(Label("Column Mapping:", classes="detail-section"))
        self.mount(
            Label(
                f"  Source column: {item.source_col}",
                classes="mapping-display",
            )
        )
        self.mount(
            Label(
                f"  Target column: {item.target_col}",
                classes="mapping-display",
            )
        )

        if item.id_col:
            self.mount(Label(f"  ID column: {item.id_col}", classes="detail-row"))

        if item.query:
            self.mount(Label("", classes="detail-row"))
            self.mount(Label(f"  Query: {item.query[:80]}...", classes="detail-row"))

        if item.schema_hints:
            self.mount(Label("Schema Hints:", classes="detail-section"))
            for col, dtype in item.schema_hints:
                self.mount(Label(f"    {col}: {dtype}", classes="detail-row"))

        self.mount(Label("", classes="detail-row"))
        self.mount(Label("Press Enter to edit fields", classes="detail-label"))


class RelationshipEditorScreen(VimEditableScreen[RelationshipSourceViewModel]):
    """Editor for relationship source configurations.

    Displays relationship sources for a specific relationship type and allows
    editing their properties via the VimEditableScreen form system. The unified
    form replaces the 5 sequential input dialogs from the old RelationshipScreen.

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
        relationship_type: str,
        adapter: DataModelAdapter | None = None,
        **kwargs,
    ) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._relationship_type = relationship_type
        self._adapter = adapter or DataModelAdapter(config_manager=config_manager)

    @property
    def relationship_type(self) -> str:
        return self._relationship_type

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return f"Edit: {self._relationship_type}"

    @property
    def breadcrumb_text(self) -> str:
        return f"Pipeline > Relationship Types > {self._relationship_type}"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:edit  a:add  dd:delete  Tab:next-field  Escape:back"

    @property
    def empty_list_message(self) -> str:
        return (
            f"No sources for relationship type '{self._relationship_type}'.\n"
            f"Press 'a' to add a source."
        )

    # --- VimNavigableScreen abstract methods ---

    def load_items(self) -> list[RelationshipSourceViewModel]:
        self._adapter.refresh()
        detail = self._adapter.relationship_detail(self._relationship_type)
        return list(detail.sources)

    def create_list_item(
        self, item: RelationshipSourceViewModel, item_id: str
    ) -> BaseListItem:
        return RelationshipSourceListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return RelationshipEditorDetailPanel(
            empty_message="(select a source to edit)", id=self.detail_panel_id
        )

    def update_detail_panel(self, item: RelationshipSourceViewModel | None) -> None:
        try:
            panel = self.query_one(
                f"#{self.detail_panel_id}", RelationshipEditorDetailPanel
            )
            panel.update_source(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: RelationshipSourceViewModel) -> str:
        return item.source_id

    def get_item_search_text(self, item: RelationshipSourceViewModel) -> str:
        return (
            f"{item.source_id} {item.uri} {item.relationship_type} "
            f"{item.source_col} {item.target_col}"
        )

    def on_edit(self, item: RelationshipSourceViewModel) -> None:
        """Open the edit form for the selected source."""
        self.start_editing(item)
        self._edit_source_dialog(item)

    def _edit_source_dialog(self, item: RelationshipSourceViewModel) -> None:
        """Dialog-based editing for relationship source URI."""
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            new_uri = response.value.strip()
            if new_uri == item.uri:
                return
            self.config_manager.update_relationship_source(item.source_id, uri=new_uri)
            self._adapter.refresh()
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.SourceUpdated(item.source_id))

        self.app.push_screen(
            InputDialog(
                title=f"Edit Relationship Source: {item.source_id}",
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
                title=f"Add Source for {self._relationship_type}",
                body="Source ID:",
                placeholder="e.g. follows_api",
            ),
            callback=_got_id,
        )

    def _add_step2(self, source_id: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step3(source_id, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title=f"Add Source for {self._relationship_type}",
                body="File URI:",
                placeholder="e.g. data/follows.csv",
            ),
            callback=_got_uri,
        )

    def _add_step3(self, source_id: str, uri: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_source_col(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step4(source_id, uri, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title=f"Add Source for {self._relationship_type}",
                body="Source column (from entity ID):",
                placeholder="e.g. from_id",
            ),
            callback=_got_source_col,
        )

    def _add_step4(self, source_id: str, uri: str, source_col: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_target_col(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self.config_manager.add_relationship_source(
                source_id,
                uri,
                self._relationship_type,
                source_col,
                response.value.strip(),
            )
            self._adapter.refresh()
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.SourceAdded())

        self.app.push_screen(
            InputDialog(
                title=f"Add Source for {self._relationship_type}",
                body="Target column (to entity ID):",
                placeholder="e.g. to_id",
            ),
            callback=_got_target_col,
        )

    async def on_delete(self, item: RelationshipSourceViewModel) -> None:
        self.config_manager.remove_relationship_source(item.source_id)
        self._adapter.refresh()
        await self.refresh_from_config()
        self.post_message(self.SourceDeleted(item.source_id))

    # --- VimEditableScreen field definitions ---

    def get_fields(
        self, item: RelationshipSourceViewModel | None
    ) -> list[EditableField]:
        if item is None:
            return [
                EditableField(
                    name="source_id",
                    label="Source ID",
                    required=True,
                    placeholder="e.g. follows_csv",
                ),
                EditableField(
                    name="uri",
                    label="URI",
                    required=True,
                    placeholder="e.g. data/follows.csv",
                ),
                EditableField(
                    name="relationship_type",
                    label="Relationship Type",
                    value=self._relationship_type,
                    readonly=True,
                ),
                EditableField(
                    name="source_col",
                    label="Source Column",
                    required=True,
                    placeholder="e.g. from_id",
                ),
                EditableField(
                    name="target_col",
                    label="Target Column",
                    required=True,
                    placeholder="e.g. to_id",
                ),
                EditableField(
                    name="id_col",
                    label="ID Column",
                    placeholder="e.g. edge_id (optional)",
                ),
            ]
        return [
            EditableField(
                name="source_id",
                label="Source ID",
                value=item.source_id,
                readonly=True,
            ),
            EditableField(
                name="uri",
                label="URI",
                value=item.uri,
                required=True,
            ),
            EditableField(
                name="relationship_type",
                label="Relationship Type",
                value=item.relationship_type,
                readonly=True,
            ),
            EditableField(
                name="source_col",
                label="Source Column",
                value=item.source_col,
                required=True,
                placeholder="e.g. from_id",
            ),
            EditableField(
                name="target_col",
                label="Target Column",
                value=item.target_col,
                required=True,
                placeholder="e.g. to_id",
            ),
            EditableField(
                name="id_col",
                label="ID Column",
                value=item.id_col or "",
                placeholder="e.g. edge_id (optional)",
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
            case "source_col":
                if not value.strip():
                    return FieldValidationResult(
                        valid=False, error="Source column is required"
                    )
                if " " in value.strip():
                    return FieldValidationResult(
                        valid=False, error="Column name cannot contain spaces"
                    )
                return FieldValidationResult(valid=True)
            case "target_col":
                if not value.strip():
                    return FieldValidationResult(
                        valid=False, error="Target column is required"
                    )
                if " " in value.strip():
                    return FieldValidationResult(
                        valid=False, error="Column name cannot contain spaces"
                    )
                return FieldValidationResult(valid=True)
            case _:
                return FieldValidationResult(valid=True)

    def apply_changes(
        self,
        item: RelationshipSourceViewModel | None,
        field_values: dict[str, str],
    ) -> None:
        if item is None:
            # Create new source
            self.config_manager.add_relationship_source(
                field_values["source_id"],
                field_values["uri"],
                self._relationship_type,
                field_values["source_col"],
                field_values["target_col"],
                id_col=field_values.get("id_col") or None,
            )
        else:
            # Update existing source
            updates: dict[str, str | None] = {}
            if field_values.get("uri") and field_values["uri"] != item.uri:
                updates["uri"] = field_values["uri"]
            if (
                field_values.get("source_col")
                and field_values["source_col"] != item.source_col
            ):
                updates["source_col"] = field_values["source_col"]
            if (
                field_values.get("target_col")
                and field_values["target_col"] != item.target_col
            ):
                updates["target_col"] = field_values["target_col"]
            id_col_val = field_values.get("id_col", "").strip() or None
            if id_col_val != item.id_col:
                updates["id_col"] = id_col_val
            if updates:
                self.config_manager.update_relationship_source(
                    item.source_id, **updates
                )

        self._adapter.refresh()
