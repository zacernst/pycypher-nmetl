"""Entity tables configuration screen.

Displays entity source mappings with column details, ID column
management, and type system integration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from textual.app import ComposeResult
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import (
    BaseDetailPanel,
    BaseListItem,
    VimNavigableScreen,
)
from pycypher_tui.widgets.data_preview import DataPreviewDialog
from pycypher_tui.widgets.dialog import DialogResult, InputDialog

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntityTableInfo:
    """Summary of an entity source for display."""

    source_id: str
    entity_type: str
    uri: str
    id_col: str | None
    schema_hints: dict[str, str]
    has_query: bool


@dataclass(frozen=True)
class ColumnMapping:
    """Represents a column in an entity source."""

    name: str
    mapped_type: str  # From schema_hints or "auto"
    is_id: bool
    is_property: bool


class EntityDetailPanel(BaseDetailPanel):
    """Detail panel showing entity configuration and column mappings."""

    CSS = """
    EntityDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    EntityDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    EntityDetailPanel .detail-section {
        text-style: bold;
        color: #e0af68;
        width: 100%;
        margin-top: 1;
    }

    EntityDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    EntityDetailPanel .column-id {
        color: #9ece6a;
    }

    EntityDetailPanel .column-property {
        color: #c0caf5;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(empty_message="(no entity selected)", **kwargs)

    def update_entity(self, entity: EntityTableInfo | None) -> None:
        """Update display with entity details."""
        self.remove_children()

        if entity is None:
            self.mount(Label("(no entity selected)", classes="detail-title"))
            return

        self.mount(Label(f"{entity.entity_type}", classes="detail-title"))
        self.mount(Label(f"  Source: {entity.source_id}", classes="detail-row"))
        self.mount(Label(f"  URI:    {entity.uri}", classes="detail-row"))

        if entity.id_col:
            self.mount(Label(f"  ID Col: {entity.id_col} -> __ID__", classes="column-id"))
        else:
            self.mount(Label("  ID Col: (auto-generated)", classes="detail-row"))

        if entity.has_query:
            self.mount(Label("  Query:  (custom SQL filter)", classes="detail-row"))

        if entity.schema_hints:
            self.mount(Label("Column Types:", classes="detail-section"))
            for col_name, col_type in entity.schema_hints.items():
                self.mount(
                    Label(f"  {col_name}: {col_type}", classes="column-property")
                )


class EntityListItem(BaseListItem[EntityTableInfo]):
    """Single entity entry in the list."""

    CSS = """
    EntityListItem {
        width: 100%;
        height: 2;
        padding: 0 2;
    }

    EntityListItem.item-focused {
        background: #283457;
    }

    EntityListItem .entity-name {
        width: 100%;
        height: 1;
    }

    EntityListItem .entity-uri {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, entity: EntityTableInfo, **kwargs) -> None:
        super().__init__(**kwargs)
        self.entity = entity

    def compose(self) -> ComposeResult:
        id_marker = f" [ID: {self.entity.id_col}]" if self.entity.id_col else ""
        yield Label(
            f"  {self.entity.entity_type} ({self.entity.source_id}){id_marker}",
            classes="entity-name",
        )
        yield Label(f"    {self.entity.uri}", classes="entity-uri")


class EntityTablesScreen(VimNavigableScreen[EntityTableInfo]):
    """Entity table configuration screen with list and detail panels.

    VIM navigation:
        j/k         - Move between entities
        Enter/l     - Edit selected entity
        a           - Add new entity
        dd          - Delete selected entity
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        Space       - Toggle ID column
    """

    class NavigateBack(VimNavigableScreen.NavigateBack):
        """Request to navigate back to overview."""

    class EditEntity(Message):
        """Request to edit an entity source."""

        def __init__(self, source_id: str) -> None:
            super().__init__()
            self.source_id = source_id

    class AddEntity(Message):
        """Request to add a new entity source."""

    class DeleteEntity(Message):
        """Request to delete an entity source."""

        def __init__(self, source_id: str) -> None:
            super().__init__()
            self.source_id = source_id

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Entity Tables"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Entity Tables"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  a:add  Enter:edit  dd:delete  p:preview  h:back"

    @property
    def empty_list_message(self) -> str:
        return "No entity tables configured.\nPress 'a' to add an entity."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[EntityTableInfo]:
        """Load entity sources from config."""
        config = self.config_manager.get_config()
        return self._extract_entities(config)

    def _extract_entities(self, config) -> list[EntityTableInfo]:
        """Extract entity sources from config."""
        entities = []
        for entity in config.sources.entities:
            entities.append(
                EntityTableInfo(
                    source_id=entity.id,
                    entity_type=entity.entity_type,
                    uri=entity.uri,
                    id_col=entity.id_col,
                    schema_hints=entity.schema_hints or {},
                    has_query=entity.query is not None,
                )
            )
        return entities

    def create_list_item(self, item: EntityTableInfo, item_id: str) -> BaseListItem:
        return EntityListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return EntityDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: EntityTableInfo | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", EntityDetailPanel)
            detail.update_entity(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: EntityTableInfo) -> str:
        return item.source_id

    def get_item_search_text(self, item: EntityTableInfo) -> str:
        return f"{item.source_id} {item.entity_type} {item.uri}"

    def on_edit(self, item: EntityTableInfo) -> None:
        self._edit_entity_uri(item)

    def _edit_entity_uri(self, item: EntityTableInfo) -> None:
        """Edit the URI of an existing entity source."""

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            new_uri = response.value.strip()
            if new_uri == item.uri:
                return
            self.config_manager.update_entity_source(item.source_id, uri=new_uri)
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.EditEntity(item.source_id))

        self.app.push_screen(
            InputDialog(
                title="Edit Entity Source",
                body=f"Edit URI for '{item.source_id}' ({item.entity_type}):",
                placeholder=item.uri,
                default_value=item.uri,
            ),
            callback=_got_uri,
        )

    def on_add(self) -> None:
        self._add_entity_step1()

    def _add_entity_step1(self) -> None:
        """Step 1: Ask for source ID."""

        def _got_id(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_entity_step2(response.value.strip())

        self.app.push_screen(
            InputDialog(
                title="Add Entity Source",
                body="Enter source ID:",
                placeholder="e.g. customers_csv",
            ),
            callback=_got_id,
        )

    def _add_entity_step2(self, source_id: str) -> None:
        """Step 2: Ask for URI."""

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_entity_step3(source_id, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title="Add Entity Source",
                body="Enter file URI:",
                placeholder="e.g. data/customers.csv",
            ),
            callback=_got_uri,
        )

    def _add_entity_step3(self, source_id: str, uri: str) -> None:
        """Step 3: Ask for entity type, then create."""

        def _got_type(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self.config_manager.add_entity_source(
                source_id, uri, response.value.strip(),
            )
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.AddEntity())

        self.app.push_screen(
            InputDialog(
                title="Add Entity Source",
                body="Enter entity type:",
                placeholder="e.g. Person",
            ),
            callback=_got_type,
        )

    async def on_delete(self, item: EntityTableInfo) -> None:
        self.config_manager.remove_entity_source(item.source_id)
        await self.refresh_from_config()
        self.post_message(self.DeleteEntity(item.source_id))

    # --- Screen-specific key overrides ---

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"p"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "p":
                self._open_preview()
                return True
            case _:
                return False

    def _open_preview(self) -> None:
        """Open data preview dialog for the currently selected entity source."""
        item = self.current_item
        if item is None:
            return
        self.app.push_screen(
            DataPreviewDialog(source_uri=item.uri, source_id=item.source_id)
        )

    # --- Backward compatibility ---

    @property
    def entity_count(self) -> int:
        return self.item_count

    @property
    def current_entity(self) -> EntityTableInfo | None:
        return self.current_item
