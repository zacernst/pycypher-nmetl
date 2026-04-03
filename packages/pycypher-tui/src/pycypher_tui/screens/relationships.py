"""Relationship configuration screen.

Visual relationship builder with entity selection, column mapping,
validation engine, and referential integrity checking.
VIM-style navigation with drill-down editing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from textual.app import ComposeResult
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label, Static

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem, VimNavigableScreen
from pycypher_tui.widgets.data_preview import DataPreviewDialog
from pycypher_tui.widgets.dialog import DialogResult, InputDialog

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RelationshipItem:
    """Unified representation of a relationship source for display."""

    source_id: str
    relationship_type: str
    uri: str
    source_col: str
    target_col: str
    id_col: str | None
    source_entity: str | None  # resolved source entity type
    target_entity: str | None  # resolved target entity type
    status: str  # "valid", "warning", "error"
    validation_messages: list[str]


class RelationshipDetailPanel(BaseDetailPanel):
    """Detail panel showing selected relationship properties."""

    CSS = """
    RelationshipDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    RelationshipDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    RelationshipDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    RelationshipDetailPanel .detail-mapping {
        width: 100%;
        color: #9ece6a;
        padding: 0 2;
    }

    RelationshipDetailPanel .detail-warning {
        color: #e0af68;
    }

    RelationshipDetailPanel .detail-error {
        color: #f7768e;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(empty_message="(no relationship selected)", **kwargs)

    def update_relationship(self, rel: RelationshipItem | None) -> None:
        """Update detail panel with relationship info."""
        self.remove_children()

        if rel is None:
            self.mount(Label("(no relationship selected)", classes="detail-title"))
            return

        self.mount(Label(f"{rel.relationship_type}", classes="detail-title"))
        self.mount(Label(f"  ID:  {rel.source_id}", classes="detail-row"))
        self.mount(Label(f"  URI: {rel.uri}", classes="detail-row"))
        self.mount(Label("", classes="detail-row"))

        # Column mapping visualization
        source_label = rel.source_entity or "?"
        target_label = rel.target_entity or "?"
        self.mount(
            Label(
                f"  ({source_label})-[:{rel.relationship_type}]->({target_label})",
                classes="detail-mapping",
            )
        )
        self.mount(Label(f"  Source column: {rel.source_col}", classes="detail-row"))
        self.mount(Label(f"  Target column: {rel.target_col}", classes="detail-row"))

        if rel.id_col:
            self.mount(Label(f"  ID column:     {rel.id_col}", classes="detail-row"))

        # Validation messages
        if rel.validation_messages:
            self.mount(Label("", classes="detail-row"))
            for msg in rel.validation_messages:
                css_class = "detail-error" if rel.status == "error" else "detail-warning"
                self.mount(Label(f"  {msg}", classes=css_class))


class RelationshipListItem(BaseListItem[RelationshipItem]):
    """Single relationship entry in the list."""

    CSS = """
    RelationshipListItem {
        width: 100%;
        height: 3;
        padding: 0 2;
    }

    RelationshipListItem.item-focused {
        background: #283457;
    }

    RelationshipListItem .rel-name {
        width: 100%;
        height: 1;
    }

    RelationshipListItem .rel-mapping {
        width: 100%;
        height: 1;
        color: #9ece6a;
        padding-left: 4;
    }

    RelationshipListItem .rel-uri {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, rel: RelationshipItem, **kwargs) -> None:
        super().__init__(**kwargs)
        self.rel = rel

    def compose(self) -> ComposeResult:
        status_icon = {
            "valid": "[ok]",
            "warning": "[!!]",
            "error": "[XX]",
        }.get(self.rel.status, "[ ]")

        src = self.rel.source_entity or "?"
        tgt = self.rel.target_entity or "?"

        yield Label(
            f"  {status_icon} {self.rel.relationship_type} ({self.rel.source_id})",
            classes="rel-name",
        )
        yield Label(
            f"    ({src})-[:{self.rel.relationship_type}]->({tgt})",
            classes="rel-mapping",
        )
        yield Label(f"    {self.rel.uri}", classes="rel-uri")


class RelationshipScreen(VimNavigableScreen[RelationshipItem]):
    """Relationship configuration screen with visual builder.

    VIM navigation:
        j/k         - Move between relationships
        Enter/l     - Edit selected relationship
        a           - Add new relationship
        dd          - Delete selected relationship
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        v           - Toggle validation view
    """

    CSS = """
    RelationshipScreen {
        layout: vertical;
    }

    #rel-summary {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
    }

    .summary-ok { color: #9ece6a; }
    .summary-warn { color: #e0af68; }
    .summary-error { color: #f7768e; }

    .empty-list-message {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: #565f89;
    }
    """

    class NavigateBack(VimNavigableScreen.NavigateBack):
        """Request to navigate back to overview."""

    class EditRelationship(Message):
        """Request to edit a relationship."""

        def __init__(self, source_id: str) -> None:
            super().__init__()
            self.source_id = source_id

    class AddRelationship(Message):
        """Request to add a new relationship."""

    class DeleteRelationship(Message):
        """Request to delete a relationship."""

        def __init__(self, source_id: str) -> None:
            super().__init__()
            self.source_id = source_id

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._entity_types: dict[str, str] = {}  # source_id -> entity_type

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Relationships"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Relationships"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  a:add  Enter:edit  dd:delete  p:preview  h:back"

    @property
    def empty_list_message(self) -> str:
        return "No relationships configured.\nPress 'a' to add a relationship."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[RelationshipItem]:
        config = self.config_manager.get_config()

        # Build entity type lookup for referential integrity
        self._entity_types = {}
        for entity in config.sources.entities:
            self._entity_types[entity.id] = entity.entity_type

        return self._build_relationship_list(config)

    def create_list_item(self, item: RelationshipItem, item_id: str) -> BaseListItem:
        return RelationshipListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return RelationshipDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: RelationshipItem | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", RelationshipDetailPanel)
            detail.update_relationship(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: RelationshipItem) -> str:
        return item.source_id

    def get_item_search_text(self, item: RelationshipItem) -> str:
        return f"{item.source_id} {item.relationship_type} {item.uri}"

    def on_edit(self, item: RelationshipItem) -> None:
        self._edit_relationship_uri(item)

    def _edit_relationship_uri(self, item: RelationshipItem) -> None:
        """Edit the URI of an existing relationship source."""

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            new_uri = response.value.strip()
            if new_uri == item.uri:
                return
            self.config_manager.update_relationship_source(item.source_id, uri=new_uri)
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.EditRelationship(item.source_id))

        self.app.push_screen(
            InputDialog(
                title="Edit Relationship Source",
                body=f"Edit URI for '{item.source_id}' ({item.relationship_type}):",
                placeholder=item.uri,
                default_value=item.uri,
            ),
            callback=_got_uri,
        )

    def on_add(self) -> None:
        self._add_rel_step1()

    def _add_rel_step1(self) -> None:
        def _got_id(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_rel_step2(response.value.strip())

        self.app.push_screen(
            InputDialog(title="Add Relationship", body="Source ID:", placeholder="e.g. follows_csv"),
            callback=_got_id,
        )

    def _add_rel_step2(self, source_id: str) -> None:
        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_rel_step3(source_id, response.value.strip())

        self.app.push_screen(
            InputDialog(title="Add Relationship", body="File URI:", placeholder="e.g. data/follows.csv"),
            callback=_got_uri,
        )

    def _add_rel_step3(self, source_id: str, uri: str) -> None:
        def _got_type(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_rel_step4(source_id, uri, response.value.strip())

        self.app.push_screen(
            InputDialog(title="Add Relationship", body="Relationship type:", placeholder="e.g. FOLLOWS"),
            callback=_got_type,
        )

    def _add_rel_step4(self, source_id: str, uri: str, rel_type: str) -> None:
        def _got_source_col(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_rel_step5(source_id, uri, rel_type, response.value.strip())

        self.app.push_screen(
            InputDialog(title="Add Relationship", body="Source column:", placeholder="e.g. from_id"),
            callback=_got_source_col,
        )

    def _add_rel_step5(self, source_id: str, uri: str, rel_type: str, source_col: str) -> None:
        def _got_target_col(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self.config_manager.add_relationship_source(
                source_id, uri, rel_type, source_col, response.value.strip(),
            )
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.AddRelationship())

        self.app.push_screen(
            InputDialog(title="Add Relationship", body="Target column:", placeholder="e.g. to_id"),
            callback=_got_target_col,
        )

    async def on_delete(self, item: RelationshipItem) -> None:
        self.config_manager.remove_relationship_source(item.source_id)
        await self.refresh_from_config()
        self.post_message(self.DeleteRelationship(item.source_id))

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
        """Open data preview dialog for the currently selected relationship source."""
        item = self.current_item
        if item is None:
            return
        self.app.push_screen(
            DataPreviewDialog(source_uri=item.uri, source_id=item.source_id)
        )

    # --- Custom compose to add summary ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_summary)

    def _mount_summary(self) -> None:
        try:
            self.query_one("#rel-summary")
        except NoMatches:
            try:
                self.mount(Static("", id="rel-summary"))
            except NoMatches:
                logger.debug("_mount_summary: failed to mount #rel-summary")

    async def refresh_from_config(self) -> None:
        await super().refresh_from_config()
        self._update_summary()

    # --- Screen-specific logic ---

    def _build_relationship_list(self, config) -> list[RelationshipItem]:
        """Extract relationships with validation status."""
        items: list[RelationshipItem] = []

        for rel in config.sources.relationships:
            messages: list[str] = []
            status = "valid"

            source_entity = self._resolve_entity_by_col(rel.source_col, config)
            target_entity = self._resolve_entity_by_col(rel.target_col, config)

            if not config.sources.entities:
                messages.append("No entity sources defined")
                status = "warning"

            if not rel.uri:
                messages.append("Missing URI")
                status = "error"

            items.append(
                RelationshipItem(
                    source_id=rel.id,
                    relationship_type=rel.relationship_type,
                    uri=rel.uri,
                    source_col=rel.source_col,
                    target_col=rel.target_col,
                    id_col=rel.id_col,
                    source_entity=source_entity,
                    target_entity=target_entity,
                    status=status,
                    validation_messages=messages,
                )
            )

        return items

    def _resolve_entity_by_col(self, col_name: str, config) -> str | None:
        """Try to resolve an entity type by column name heuristic."""
        for entity in config.sources.entities:
            if entity.id_col and entity.id_col == col_name:
                return entity.entity_type
        return None

    def _update_summary(self) -> None:
        """Update validation summary."""
        try:
            summary = self.query_one("#rel-summary", Static)
        except NoMatches:
            return

        total = self.item_count
        errors = sum(1 for r in self.items if r.status == "error")
        warnings = sum(1 for r in self.items if r.status == "warning")

        if total == 0:
            summary.update(" No relationships configured")
            summary.remove_class("summary-ok", "summary-warn", "summary-error")
        elif errors > 0:
            summary.update(f" {total} relationships, {errors} with errors")
            summary.remove_class("summary-ok", "summary-warn")
            summary.add_class("summary-error")
        elif warnings > 0:
            summary.update(f" {total} relationships, {warnings} with warnings")
            summary.remove_class("summary-ok", "summary-error")
            summary.add_class("summary-warn")
        else:
            summary.update(f" {total} relationships, all valid")
            summary.remove_class("summary-warn", "summary-error")
            summary.add_class("summary-ok")

    # --- Backward compatibility ---

    @property
    def relationship_count(self) -> int:
        return self.item_count

    @property
    def current_relationship(self) -> RelationshipItem | None:
        return self.current_item
