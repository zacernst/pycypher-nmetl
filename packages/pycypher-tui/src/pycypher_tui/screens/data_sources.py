"""Data sources configuration screen.

Displays and manages entity and relationship data sources with
VIM-style navigation, CRUD operations, and source detail display.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Label, Static

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
class SourceItem:
    """Unified representation of a data source for display."""

    source_id: str
    uri: str
    source_type: str  # "entity" or "relationship"
    label: str  # entity_type or relationship_type
    id_col: str | None
    extra: dict[str, str]  # Additional info (source_col, target_col, etc.)


class SourceDetailPanel(BaseDetailPanel):
    """Right-side detail panel showing selected source properties."""

    def __init__(self, **kwargs) -> None:
        super().__init__(empty_message="(no source selected)", **kwargs)
        self._source: SourceItem | None = None

    def update_source(self, source: SourceItem | None) -> None:
        """Update the detail panel with source information."""
        self._source = source
        self.remove_children()

        if source is None:
            self.mount(Label("(no source selected)", classes="detail-title"))
            return

        self.mount(Label(f"{source.label}", classes="detail-title"))
        self.mount(Label(f"  Type: {source.source_type}", classes="detail-row"))
        self.mount(Label(f"  ID:   {source.source_id}", classes="detail-row"))
        self.mount(Label(f"  URI:  {source.uri}", classes="detail-row"))

        if source.id_col:
            self.mount(Label(f"  ID Column: {source.id_col}", classes="detail-row"))

        for key, value in source.extra.items():
            self.mount(Label(f"  {key}: {value}", classes="detail-row"))

        # Add preview hint
        self.mount(Label("", classes="detail-row"))  # spacer
        self.mount(Label("Press 'p' to preview data", classes="detail-label"))


class SourceListItem(BaseListItem[SourceItem]):
    """Single source entry in the list."""

    CSS = """
    SourceListItem {
        width: 100%;
        height: 2;
        padding: 0 2;
    }

    SourceListItem.item-focused {
        background: #283457;
    }

    SourceListItem .source-name {
        width: 100%;
        height: 1;
    }

    SourceListItem .source-uri {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, source: SourceItem, **kwargs) -> None:
        super().__init__(**kwargs)
        self.source = source

    def compose(self) -> ComposeResult:
        type_indicator = "[E]" if self.source.source_type == "entity" else "[R]"
        yield Label(
            f"  {type_indicator} {self.source.label} ({self.source.source_id})",
            classes="source-name",
        )
        yield Label(f"    {self.source.uri}", classes="source-uri")


class DataSourcesScreen(VimNavigableScreen[SourceItem]):
    """Data source management screen with list and detail panels.

    VIM navigation:
        j/k         - Move between sources
        Enter/l     - Edit selected source
        p           - Preview data source
        a           - Add new source
        dd          - Delete selected source
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        /           - Search sources
        Tab         - Switch between entity/relationship filter
    """

    CSS = """
    DataSourcesScreen {
        layout: vertical;
    }

    #filter-indicator {
        dock: top;
        height: 1;
        width: 100%;
        padding: 0 2;
        color: #e0af68;
    }

    .empty-list-message {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: #565f89;
    }
    """

    class EditSource(Message):
        """Request to edit a source."""

        def __init__(self, source_id: str, source_type: str) -> None:
            super().__init__()
            self.source_id = source_id
            self.source_type = source_type

    class AddSource(Message):
        """Request to add a new source."""

        def __init__(self, source_type: str) -> None:
            super().__init__()
            self.source_type = source_type

    class DeleteSource(Message):
        """Request to delete a source."""

        def __init__(self, source_id: str, source_type: str) -> None:
            super().__init__()
            self.source_id = source_id
            self.source_type = source_type

    FILTER_MODES = ["all", "entity", "relationship"]

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._filter_mode: str = "all"
        self._search_pattern: str = ""

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        """Add 'p' for preview and 'tab' for filter toggle."""
        return frozenset({"tab", "p"})

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Data Sources"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Data Sources"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  a:add  p:preview  Enter:edit  dd:delete  Tab:filter  h:back"

    @property
    def empty_list_message(self) -> str:
        return "No data sources configured.\nPress 'a' to add a source."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[SourceItem]:
        """Load sources from config, applying filter and search."""
        config = self.config_manager.get_config()
        all_sources = self._extract_sources(config)

        # Apply filter
        if self._filter_mode == "entity":
            all_sources = [s for s in all_sources if s.source_type == "entity"]
        elif self._filter_mode == "relationship":
            all_sources = [s for s in all_sources if s.source_type == "relationship"]

        # Apply search
        if self._search_pattern:
            pattern = self._search_pattern.lower()
            all_sources = [
                s
                for s in all_sources
                if pattern in s.source_id.lower()
                or pattern in s.label.lower()
                or pattern in s.uri.lower()
            ]

        return all_sources

    def create_list_item(self, item: SourceItem, item_id: str) -> BaseListItem:
        return SourceListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return SourceDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: SourceItem | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", SourceDetailPanel)
            detail.update_source(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: SourceItem) -> str:
        return item.source_id

    def get_item_search_text(self, item: SourceItem) -> str:
        return f"{item.source_id} {item.label} {item.uri} {item.source_type}"

    def on_edit(self, item: SourceItem) -> None:
        self._edit_source_uri(item)

    def _edit_source_uri(self, item: SourceItem) -> None:
        """Edit the URI of an existing source."""

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            new_uri = response.value.strip()
            if new_uri == item.uri:
                return
            if item.source_type == "entity":
                self.config_manager.update_entity_source(item.source_id, uri=new_uri)
            else:
                self.config_manager.update_relationship_source(item.source_id, uri=new_uri)
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.EditSource(item.source_id, item.source_type))

        self.app.push_screen(
            InputDialog(
                title=f"Edit {item.source_type.title()} Source",
                body=f"Edit URI for '{item.source_id}':",
                placeholder=item.uri,
                default_value=item.uri,
            ),
            callback=_got_uri,
        )

    def on_add(self) -> None:
        source_type = "entity" if self._filter_mode != "relationship" else "relationship"
        self._add_source_step1(source_type)

    def _add_source_step1(self, source_type: str) -> None:
        """Step 1: Ask for source ID."""

        def _got_id(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_source_step2(source_type, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title=f"Add {source_type.title()} Source",
                body="Enter source ID:",
                placeholder="e.g. customers_csv",
            ),
            callback=_got_id,
        )

    def _add_source_step2(self, source_type: str, source_id: str) -> None:
        """Step 2: Ask for URI."""

        def _got_uri(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_source_step3(source_type, source_id, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title=f"Add {source_type.title()} Source",
                body="Enter file URI:",
                placeholder="e.g. data/customers.csv",
            ),
            callback=_got_uri,
        )

    def _add_source_step3(self, source_type: str, source_id: str, uri: str) -> None:
        """Step 3: Ask for entity/relationship type, then create."""

        def _got_type(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            type_name = response.value.strip()
            if source_type == "entity":
                self.config_manager.add_entity_source(source_id, uri, type_name)
            else:
                # For relationships, use type_name as rel type with placeholder columns
                self.config_manager.add_relationship_source(
                    source_id, uri, type_name, "source_id", "target_id",
                )
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.AddSource(source_type))

        label = "entity type" if source_type == "entity" else "relationship type"
        self.app.push_screen(
            InputDialog(
                title=f"Add {source_type.title()} Source",
                body=f"Enter {label}:",
                placeholder="e.g. Person" if source_type == "entity" else "e.g. KNOWS",
            ),
            callback=_got_type,
        )

    async def on_delete(self, item: SourceItem) -> None:
        if item.source_type == "entity":
            self.config_manager.remove_entity_source(item.source_id)
        else:
            self.config_manager.remove_relationship_source(item.source_id)
        await self.refresh_from_config()
        self.post_message(self.DeleteSource(item.source_id, item.source_type))

    def handle_extra_key(self, key: str) -> bool:
        """Handle filter toggle via Tab and preview via 'p'."""
        if key == "tab":
            self.run_worker(self._cycle_filter(), exclusive=True)
            return True
        elif key == "p":
            self._preview_current_source()
            return True
        return False

    # --- Custom compose to add filter indicator ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        # Insert filter indicator (will be positioned by CSS dock)
        self.call_after_refresh(self._mount_filter_indicator)

    def _mount_filter_indicator(self) -> None:
        """Mount filter indicator after initial compose."""
        try:
            # Only mount if not already present
            self.query_one("#filter-indicator")
        except NoMatches:
            try:
                header = self.query_one("#screen-header")
                header.mount(Label("", id="filter-indicator"))
            except NoMatches:
                logger.debug("_mount_filter_indicator: #screen-header not found")

    # --- Screen-specific logic ---

    def _extract_sources(self, config) -> list[SourceItem]:
        """Extract all sources from config into unified list."""
        sources: list[SourceItem] = []

        for entity in config.sources.entities:
            sources.append(
                SourceItem(
                    source_id=entity.id,
                    uri=entity.uri,
                    source_type="entity",
                    label=entity.entity_type,
                    id_col=entity.id_col,
                    extra={
                        k: v
                        for k, v in [
                            ("query", entity.query),
                            ("on_error", entity.on_error.value if entity.on_error else None),
                        ]
                        if v is not None
                    },
                )
            )

        for rel in config.sources.relationships:
            sources.append(
                SourceItem(
                    source_id=rel.id,
                    uri=rel.uri,
                    source_type="relationship",
                    label=rel.relationship_type,
                    id_col=rel.id_col,
                    extra={
                        "source_col": rel.source_col,
                        "target_col": rel.target_col,
                        **({"query": rel.query} if rel.query else {}),
                    },
                )
            )

        return sources

    async def _cycle_filter(self) -> None:
        """Cycle through filter modes: all -> entity -> relationship -> all."""
        idx = self.FILTER_MODES.index(self._filter_mode)
        self._filter_mode = self.FILTER_MODES[(idx + 1) % len(self.FILTER_MODES)]
        await self.refresh_from_config()
        self._update_filter_indicator()

    def _update_filter_indicator(self) -> None:
        """Update filter mode display."""
        try:
            indicator = self.query_one("#filter-indicator", Label)
            if self._filter_mode == "all":
                indicator.update("")
            else:
                indicator.update(f" Filter: {self._filter_mode} sources only")
        except NoMatches:
            logger.debug("_update_filter_indicator: #filter-indicator not found")

    def _preview_current_source(self) -> None:
        """Open data preview dialog for the current source."""
        source = self.current_item
        if source is None:
            return

        # Show preview dialog
        preview_dialog = DataPreviewDialog(
            source_uri=source.uri,
            source_id=source.source_id,
        )
        self.app.push_screen(preview_dialog)

    @property
    def source_count(self) -> int:
        """Backward compatibility alias for item_count."""
        return self.item_count

    @property
    def current_source(self) -> SourceItem | None:
        """Backward compatibility alias for current_item."""
        return self.current_item
