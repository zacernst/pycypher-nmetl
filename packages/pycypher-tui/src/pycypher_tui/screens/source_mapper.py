"""Data source mapper screen — source-to-model mapping visualization.

Split panel showing all data sources by type (left) and
source-to-entity/relationship mapping visualization (right).
Uses DataModelAdapter.source_mappings() for data.

Provides overview of how data files connect to the graph model,
with validation status and drill-down to schema inspection.
"""

from __future__ import annotations

import logging

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label, Static

from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.adapters.view_models import SourceMappingViewModel
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem, VimNavigableScreen
from pycypher_tui.widgets.data_preview import DataPreviewDialog

logger = logging.getLogger(__name__)


class SourceMappingListItem(BaseListItem[SourceMappingViewModel]):
    """Displays a single data source mapping in the list."""

    CSS = """
    SourceMappingListItem {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin: 0;
    }

    SourceMappingListItem.item-focused {
        background: #283457;
    }

    SourceMappingListItem .source-header {
        width: 100%;
        height: 1;
    }

    SourceMappingListItem .source-detail {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }

    SourceMappingListItem .source-mapping-entity {
        color: #9ece6a;
        padding-left: 4;
    }

    SourceMappingListItem .source-mapping-rel {
        color: #bb9af7;
        padding-left: 4;
    }
    """

    def __init__(self, mapping: SourceMappingViewModel, **kwargs) -> None:
        super().__init__(**kwargs)
        self.mapping = mapping

    def compose(self) -> ComposeResult:
        status_icon = {
            "connected": "[ok]",
            "orphaned": "[!!]",
            "error": "[XX]",
        }.get(self.mapping.status, "[ ]")

        yield Label(
            f"  {status_icon} {self.mapping.source_id}",
            classes="source-header",
        )

        mapping_class = (
            "source-mapping-entity"
            if self.mapping.mapping_type == "entity"
            else "source-mapping-rel"
        )
        type_icon = "(O)" if self.mapping.mapping_type == "entity" else "[->]"
        yield Label(
            f"      {type_icon} {self.mapping.maps_to}",
            classes=mapping_class,
        )
        yield Label(f"      {self.mapping.uri}", classes="source-detail")


class SourceMappingDetailPanel(BaseDetailPanel):
    """Right-side detail panel showing source mapping details."""

    CSS = """
    SourceMappingDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    SourceMappingDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    SourceMappingDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    SourceMappingDetailPanel .detail-section {
        text-style: bold;
        color: #e0af68;
        width: 100%;
        margin-top: 1;
    }

    SourceMappingDetailPanel .mapping-visual {
        color: #9ece6a;
        padding: 1 2;
    }

    SourceMappingDetailPanel .status-connected {
        color: #9ece6a;
    }

    SourceMappingDetailPanel .status-orphaned {
        color: #e0af68;
    }

    SourceMappingDetailPanel .status-error {
        color: #f7768e;
    }
    """

    def __init__(self, adapter: DataModelAdapter, **kwargs) -> None:
        super().__init__(empty_message="(no source selected)", **kwargs)
        self._adapter = adapter

    def update_mapping(self, mapping: SourceMappingViewModel | None) -> None:
        """Update detail panel with source mapping information."""
        self.remove_children()

        if mapping is None:
            self.mount(Label("(no source selected)", classes="detail-title"))
            return

        container = VerticalScroll()
        self.mount(container)

        # Header
        container.mount(
            Label(f"Source: {mapping.source_id}", classes="detail-title")
        )

        # Connection status
        status_class = f"status-{mapping.status}"
        container.mount(
            Label(f"  Status: {mapping.status.upper()}", classes=status_class)
        )

        # URI
        container.mount(Label(f"  URI: {mapping.uri}", classes="detail-row"))

        # Mapping type
        type_label = "Entity Type" if mapping.mapping_type == "entity" else "Relationship Type"
        container.mount(
            Label(f"  {type_label}: {mapping.maps_to}", classes="detail-row")
        )

        # Visual mapping
        container.mount(Label("Mapping:", classes="detail-section"))
        if mapping.mapping_type == "entity":
            container.mount(
                Label(
                    f"  {mapping.source_id}\n"
                    f"    |\n"
                    f"    +---> (O) {mapping.maps_to}",
                    classes="mapping-visual",
                )
            )
        else:
            container.mount(
                Label(
                    f"  {mapping.source_id}\n"
                    f"    |\n"
                    f"    +---> [->] :{mapping.maps_to}",
                    classes="mapping-visual",
                )
            )

        # Statistics from adapter
        stats = self._adapter.model_statistics()
        container.mount(Label("Model Overview:", classes="detail-section"))
        container.mount(
            Label(f"  Entity types: {stats.entity_type_count}", classes="detail-row")
        )
        container.mount(
            Label(
                f"  Relationship types: {stats.relationship_type_count}",
                classes="detail-row",
            )
        )
        container.mount(
            Label(f"  Total sources: {stats.total_source_count}", classes="detail-row")
        )

        container.mount(Label("", classes="detail-row"))
        container.mount(Label("Press 'p' to preview data", classes="detail-row"))


class DataSourceMapperScreen(VimNavigableScreen[SourceMappingViewModel]):
    """Data source mapper showing source-to-model mappings.

    Split panel: all data sources (left) with source-to-entity/relationship
    mapping visualization (right). Shows connection status and allows
    drill-down to schema inspection.

    VIM navigation:
        j/k         - Move between sources
        Enter/l     - Drill down to source detail / inspect schema
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        p           - Preview source data
        /           - Search sources
    """

    CSS = """
    DataSourceMapperScreen {
        layout: vertical;
    }

    #mapper-summary {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
    }

    .summary-ok { color: #9ece6a; }
    .summary-warn { color: #e0af68; }
    .summary-error { color: #f7768e; }
    """

    class DrillDown(Message):
        """Request to drill down into source details."""

        def __init__(self, source_id: str, mapping_type: str) -> None:
            super().__init__()
            self.source_id = source_id
            self.mapping_type = mapping_type

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
        return "Data Source Mappings"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Source Mappings"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:inspect  p:preview  h:back  /:search"

    @property
    def empty_list_message(self) -> str:
        return "No data sources configured.\nAdd entity or relationship sources first."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[SourceMappingViewModel]:
        self._adapter.refresh()
        return self._adapter.source_mappings()

    def create_list_item(
        self, item: SourceMappingViewModel, item_id: str
    ) -> BaseListItem:
        return SourceMappingListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return SourceMappingDetailPanel(
            adapter=self._adapter, id=self.detail_panel_id
        )

    def update_detail_panel(self, item: SourceMappingViewModel | None) -> None:
        try:
            detail = self.query_one(
                f"#{self.detail_panel_id}", SourceMappingDetailPanel
            )
            detail.update_mapping(item)
        except (NoMatches, AttributeError):
            logger.debug(
                "update_detail_panel: #%s not found", self.detail_panel_id
            )

    def get_item_id(self, item: SourceMappingViewModel) -> str:
        return item.source_id.replace(" ", "-")

    def get_item_search_text(self, item: SourceMappingViewModel) -> str:
        return f"{item.source_id} {item.uri} {item.maps_to} {item.mapping_type}"

    def on_edit(self, item: SourceMappingViewModel) -> None:
        self.post_message(
            self.DrillDown(item.source_id, item.mapping_type)
        )

    def on_add(self) -> None:
        pass  # read-only mapping view

    async def on_delete(self, item: SourceMappingViewModel) -> None:
        pass  # read-only mapping view

    # --- Screen-specific key overrides ---

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"p"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "p":
                self._preview_source()
                return True
            case _:
                return False

    def _preview_source(self) -> None:
        """Preview data from the selected source."""
        item = self.current_item
        if item is None:
            return
        self.app.push_screen(
            DataPreviewDialog(source_uri=item.uri, source_id=item.source_id)
        )

    # --- Layout: add summary bar ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_summary)

    def _mount_summary(self) -> None:
        try:
            self.query_one("#mapper-summary")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer")
                stats = self._adapter.model_statistics()

                entity_count = sum(
                    1 for m in self._items if m.mapping_type == "entity"
                )
                rel_count = sum(
                    1 for m in self._items if m.mapping_type == "relationship"
                )
                orphaned = sum(
                    1 for m in self._items if m.status == "orphaned"
                )

                summary = (
                    f" {len(self._items)} source"
                    f"{'s' if len(self._items) != 1 else ''}"
                    f" ({entity_count} entity, {rel_count} relationship)"
                )
                if orphaned:
                    summary += f"  {orphaned} orphaned"
                    label = Label(summary, id="mapper-summary")
                    label.add_class("summary-warn")
                else:
                    label = Label(summary, id="mapper-summary")
                    label.add_class("summary-ok")

                self.mount(label, before=footer)
            except (NoMatches, AttributeError):
                logger.debug("_mount_summary: #screen-footer not found")
