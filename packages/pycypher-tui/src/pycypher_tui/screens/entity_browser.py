"""Entity browser screen — unified entity type explorer.

Displays entity types with source counts and properties using the
DataModelAdapter. Two-column layout: entity type list (left) and
detail panel (right) with sources, properties, and validation info.

Drill-down to EntityEditorScreen for editing individual sources.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label, Static

from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.adapters.view_models import (
    EntityDetailViewModel,
    EntityViewModel,
)
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import (
    BaseDetailPanel,
    BaseListItem,
    VimNavigableScreen,
)
from pycypher_tui.widgets.data_preview import DataPreviewDialog

logger = logging.getLogger(__name__)


class EntityListItem(BaseListItem[EntityViewModel]):
    """Displays a single entity type in the browser list."""

    CSS = """
    EntityListItem {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin: 0;
    }

    EntityListItem.item-focused {
        background: #283457;
    }

    EntityListItem .entity-header {
        width: 100%;
        height: 1;
    }

    EntityListItem .entity-detail {
        width: 100%;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, entity: EntityViewModel, **kwargs) -> None:
        super().__init__(**kwargs)
        self.entity = entity

    def compose(self) -> ComposeResult:
        sources_text = (
            f"{self.entity.source_count} source"
            f"{'s' if self.entity.source_count != 1 else ''}"
        )
        id_text = f"  ID: {self.entity.id_column}" if self.entity.id_column else ""
        yield Label(
            f"  (O) {self.entity.entity_type}  [{sources_text}]{id_text}",
            classes="entity-header",
        )
        if self.entity.property_names:
            props = ", ".join(self.entity.property_names[:5])
            if len(self.entity.property_names) > 5:
                props += f" (+{len(self.entity.property_names) - 5} more)"
            yield Label(f"      Properties: {props}", classes="entity-detail")


class EntityBrowserDetailPanel(BaseDetailPanel):
    """Right-side detail panel for the entity browser."""

    CSS = """
    EntityBrowserDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    EntityBrowserDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    EntityBrowserDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    EntityBrowserDetailPanel .detail-section {
        text-style: bold;
        color: #e0af68;
        width: 100%;
        margin-top: 1;
    }

    EntityBrowserDetailPanel .source-row {
        color: #9ece6a;
        padding-left: 2;
    }

    EntityBrowserDetailPanel .property-row {
        color: #c0caf5;
        padding-left: 2;
    }

    EntityBrowserDetailPanel .validation-pass {
        color: #9ece6a;
    }

    EntityBrowserDetailPanel .validation-warn {
        color: #e0af68;
    }

    EntityBrowserDetailPanel .validation-error {
        color: #f7768e;
    }
    """

    def __init__(self, adapter: DataModelAdapter, **kwargs) -> None:
        super().__init__(empty_message="(no entity type selected)", **kwargs)
        self._adapter = adapter

    def update_entity(self, entity: EntityViewModel | None) -> None:
        """Update detail panel with entity type information."""
        self.remove_children()

        if entity is None:
            self.mount(Label("(no entity type selected)", classes="detail-title"))
            return

        detail = self._adapter.entity_detail(entity.entity_type)

        container = VerticalScroll()
        self.mount(container)

        # Header
        container.mount(Label(f"Entity: {detail.entity_type}", classes="detail-title"))

        # Summary
        container.mount(
            Label(
                f"  Sources: {len(detail.sources)}",
                classes="detail-row",
            )
        )
        if detail.row_count is not None:
            container.mount(
                Label(f"  Rows: {detail.row_count:,}", classes="detail-row")
            )

        # Sources section
        if detail.sources:
            container.mount(Label("Data Sources:", classes="detail-section"))
            for src in detail.sources:
                id_text = f"  [ID: {src.id_col}]" if src.id_col else ""
                container.mount(
                    Label(f"  {src.source_id}: {src.uri}{id_text}", classes="source-row")
                )
                if src.query:
                    container.mount(
                        Label("    (custom SQL query)", classes="detail-row")
                    )

        # Properties section
        if detail.properties:
            container.mount(Label("Properties:", classes="detail-section"))
            for prop in detail.properties:
                source_note = f" (from {prop.source_id})" if prop.source_id else ""
                container.mount(
                    Label(
                        f"  {prop.name}: {prop.dtype}{source_note}",
                        classes="property-row",
                    )
                )

        # Validation section
        if detail.validation_issues:
            container.mount(Label("Validation:", classes="detail-section"))
            for issue in detail.validation_issues:
                css_class = f"validation-{issue.level}"
                icon = {"error": "[XX]", "warning": "[!!]", "info": "[ii]"}.get(
                    issue.level, "[ ]"
                )
                container.mount(
                    Label(f"  {icon} {issue.message}", classes=css_class)
                )
                if issue.fix_hint:
                    container.mount(
                        Label(f"      Hint: {issue.fix_hint}", classes="detail-row")
                    )
        else:
            container.mount(Label("Validation:", classes="detail-section"))
            container.mount(Label("  [ok] No issues found", classes="validation-pass"))


class EntityBrowserScreen(VimNavigableScreen[EntityViewModel]):
    """Entity type browser with list/detail layout.

    Displays all entity types from the data model with source counts,
    properties, and validation status. Drill-down to edit individual
    entity sources.

    VIM navigation:
        j/k         - Move between entity types
        Enter/l     - Drill down to entity sources / edit
        a           - Add new entity source
        dd          - Delete entity type (all sources)
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        p           - Preview first source's data
        /           - Search entity types
    """

    CSS = """
    EntityBrowserScreen {
        layout: vertical;
    }

    #entity-summary {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
        color: #9ece6a;
    }
    """

    class DrillDown(Message):
        """Request to drill into a specific entity type's sources."""

        def __init__(self, entity_type: str) -> None:
            super().__init__()
            self.entity_type = entity_type

    class AddEntitySource(Message):
        """Request to add a new entity source."""

    class DeleteEntityType(Message):
        """Request to delete an entity type."""

        def __init__(self, entity_type: str) -> None:
            super().__init__()
            self.entity_type = entity_type

    def __init__(
        self,
        config_manager: ConfigManager,
        adapter: DataModelAdapter | None = None,
        **kwargs,
    ) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._adapter = adapter or DataModelAdapter(config_manager=config_manager)

    @property
    def adapter(self) -> DataModelAdapter:
        return self._adapter

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Entity Types"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Entity Types"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:drill-down  a:add  dd:delete  p:preview  h:back  /:search"

    @property
    def empty_list_message(self) -> str:
        return "No entity types defined.\nAdd entity data sources first."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[EntityViewModel]:
        self._adapter.refresh()
        return self._adapter.entity_types()

    def create_list_item(self, item: EntityViewModel, item_id: str) -> BaseListItem:
        return EntityListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return EntityBrowserDetailPanel(
            adapter=self._adapter, id=self.detail_panel_id
        )

    def update_detail_panel(self, item: EntityViewModel | None) -> None:
        try:
            detail = self.query_one(
                f"#{self.detail_panel_id}", EntityBrowserDetailPanel
            )
            detail.update_entity(item)
        except (NoMatches, AttributeError):
            logger.debug(
                "update_detail_panel: #%s not found", self.detail_panel_id
            )

    def get_item_id(self, item: EntityViewModel) -> str:
        return item.entity_type.replace(" ", "-")

    def get_item_search_text(self, item: EntityViewModel) -> str:
        props = " ".join(item.property_names)
        return f"{item.entity_type} {props}"

    def on_edit(self, item: EntityViewModel) -> None:
        self.post_message(self.DrillDown(item.entity_type))

    def on_add(self) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_id(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step2(response.value.strip())

        self.app.push_screen(
            InputDialog(
                title="Add Entity Source",
                body="Source ID:",
                placeholder="e.g. customers_csv",
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
                title="Add Entity Source",
                body="File URI:",
                placeholder="e.g. data/customers.csv",
            ),
            callback=_got_uri,
        )

    def _add_step3(self, source_id: str, uri: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_type(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self.config_manager.add_entity_source(
                source_id, uri, response.value.strip()
            )
            self._adapter.refresh()
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.AddEntitySource())

        self.app.push_screen(
            InputDialog(
                title="Add Entity Source",
                body="Entity type:",
                placeholder="e.g. Person",
            ),
            callback=_got_type,
        )

    async def on_delete(self, item: EntityViewModel) -> None:
        """Delete all sources for this entity type."""
        detail = self._adapter.entity_detail(item.entity_type)
        for src in detail.sources:
            self.config_manager.remove_entity_source(src.source_id)
        self._adapter.refresh()
        await self.refresh_from_config()
        self.post_message(self.DeleteEntityType(item.entity_type))

    # --- Screen-specific key overrides ---

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"p"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "p":
                self._preview_first_source()
                return True
            case _:
                return False

    def _preview_first_source(self) -> None:
        """Preview data from the first source of the selected entity type."""
        item = self.current_item
        if item is None:
            return

        detail = self._adapter.entity_detail(item.entity_type)
        if detail.sources:
            src = detail.sources[0]
            self.app.push_screen(
                DataPreviewDialog(source_uri=src.uri, source_id=src.source_id)
            )

    # --- Layout: add summary bar ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_summary)

    def _mount_summary(self) -> None:
        try:
            self.query_one("#entity-summary")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer")
                stats = self._adapter.model_statistics()
                summary = (
                    f" {stats.entity_type_count} entity type"
                    f"{'s' if stats.entity_type_count != 1 else ''}"
                    f"  {stats.total_source_count} total source"
                    f"{'s' if stats.total_source_count != 1 else ''}"
                )
                self.mount(Label(summary, id="entity-summary"), before=footer)
            except (NoMatches, AttributeError):
                logger.debug("_mount_summary: #screen-footer not found")
