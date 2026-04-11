"""Relationship browser screen — unified relationship type explorer.

Displays relationship types with endpoints, column mappings, and validation
status using the DataModelAdapter. Two-column layout: relationship type list
(left) and detail panel (right) with sources, mappings, and validation info.

Drill-down to RelationshipEditorScreen for editing individual sources.
"""

from __future__ import annotations

import logging

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label, Static

from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.adapters.view_models import (
    RelationshipDetailViewModel,
    RelationshipViewModel,
)
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import (
    BaseDetailPanel,
    BaseListItem,
    VimNavigableScreen,
)
from pycypher_tui.widgets.data_preview import DataPreviewDialog

logger = logging.getLogger(__name__)


class RelationshipBrowserListItem(BaseListItem[RelationshipViewModel]):
    """Displays a single relationship type in the browser list."""

    CSS = """
    RelationshipBrowserListItem {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin: 0;
    }

    RelationshipBrowserListItem.item-focused {
        background: #283457;
    }

    RelationshipBrowserListItem .rel-header {
        width: 100%;
        height: 1;
    }

    RelationshipBrowserListItem .rel-mapping {
        width: 100%;
        height: 1;
        color: #9ece6a;
        padding-left: 4;
    }

    RelationshipBrowserListItem .rel-status {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, rel: RelationshipViewModel, **kwargs) -> None:
        super().__init__(**kwargs)
        self.rel = rel

    def compose(self) -> ComposeResult:
        status_icon = {
            "valid": "[ok]",
            "warning": "[!!]",
            "error": "[XX]",
        }.get(self.rel.validation_status, "[ ]")

        sources_text = (
            f"{self.rel.source_count} source"
            f"{'s' if self.rel.source_count != 1 else ''}"
        )

        yield Label(
            f"  {status_icon} {self.rel.relationship_type}  [{sources_text}]",
            classes="rel-header",
        )

        src = self.rel.source_entity or "?"
        tgt = self.rel.target_entity or "?"
        yield Label(
            f"      ({src})-[:{self.rel.relationship_type}]->({tgt})",
            classes="rel-mapping",
        )


class RelationshipBrowserDetailPanel(BaseDetailPanel):
    """Right-side detail panel for the relationship browser."""

    CSS = """
    RelationshipBrowserDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    RelationshipBrowserDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    RelationshipBrowserDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    RelationshipBrowserDetailPanel .detail-section {
        text-style: bold;
        color: #e0af68;
        width: 100%;
        margin-top: 1;
    }

    RelationshipBrowserDetailPanel .source-row {
        color: #9ece6a;
        padding-left: 2;
    }

    RelationshipBrowserDetailPanel .mapping-row {
        color: #bb9af7;
        padding-left: 2;
    }

    RelationshipBrowserDetailPanel .validation-pass {
        color: #9ece6a;
    }

    RelationshipBrowserDetailPanel .validation-warn {
        color: #e0af68;
    }

    RelationshipBrowserDetailPanel .validation-error {
        color: #f7768e;
    }
    """

    def __init__(self, adapter: DataModelAdapter, **kwargs) -> None:
        super().__init__(empty_message="(no relationship type selected)", **kwargs)
        self._adapter = adapter

    def update_relationship(self, rel: RelationshipViewModel | None) -> None:
        """Update detail panel with relationship type information."""
        self.remove_children()

        if rel is None:
            self.mount(Label("(no relationship type selected)", classes="detail-title"))
            return

        detail = self._adapter.relationship_detail(rel.relationship_type)

        container = VerticalScroll()
        self.mount(container)

        # Header
        container.mount(
            Label(f"Relationship: {detail.relationship_type}", classes="detail-title")
        )

        # Endpoint summary
        src_entity = rel.source_entity or "?"
        tgt_entity = rel.target_entity or "?"
        container.mount(
            Label(
                f"  ({src_entity})-[:{detail.relationship_type}]->({tgt_entity})",
                classes="source-row",
            )
        )
        container.mount(
            Label(f"  Sources: {len(detail.sources)}", classes="detail-row")
        )

        # Column mappings section
        if detail.column_mappings:
            container.mount(Label("Column Mappings:", classes="detail-section"))
            for mapping in detail.column_mappings:
                src_label = mapping.source_entity or "?"
                tgt_label = mapping.target_entity or "?"
                container.mount(
                    Label(
                        f"  {mapping.source_col} ({src_label}) -> "
                        f"{mapping.target_col} ({tgt_label})",
                        classes="mapping-row",
                    )
                )

        # Data sources section
        if detail.sources:
            container.mount(Label("Data Sources:", classes="detail-section"))
            for src in detail.sources:
                id_text = f"  [ID: {src.id_col}]" if src.id_col else ""
                container.mount(
                    Label(
                        f"  {src.source_id}: {src.uri}{id_text}",
                        classes="source-row",
                    )
                )
                if src.query:
                    container.mount(
                        Label("    (custom SQL query)", classes="detail-row")
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


class RelationshipBrowserScreen(VimNavigableScreen[RelationshipViewModel]):
    """Relationship type browser with list/detail layout.

    Displays all relationship types from the data model with endpoints,
    column mappings, and validation status. Drill-down to edit individual
    relationship sources.

    VIM navigation:
        j/k         - Move between relationship types
        Enter/l     - Drill down to relationship sources / edit
        a           - Add new relationship source
        dd          - Delete relationship type (all sources)
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        p           - Preview first source's data
        /           - Search relationship types
    """

    CSS = """
    RelationshipBrowserScreen {
        layout: vertical;
    }

    #rel-browser-summary {
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
        """Request to drill into a specific relationship type's sources."""

        def __init__(self, relationship_type: str) -> None:
            super().__init__()
            self.relationship_type = relationship_type

    class AddRelationshipSource(Message):
        """Request to add a new relationship source."""

    class DeleteRelationshipType(Message):
        """Request to delete a relationship type."""

        def __init__(self, relationship_type: str) -> None:
            super().__init__()
            self.relationship_type = relationship_type

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
        return "Relationship Types"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Relationship Types"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:drill-down  a:add  dd:delete  p:preview  h:back  /:search"

    @property
    def empty_list_message(self) -> str:
        return "No relationship types defined.\nAdd relationship data sources first."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[RelationshipViewModel]:
        self._adapter.refresh()
        return self._adapter.relationship_types()

    def create_list_item(
        self, item: RelationshipViewModel, item_id: str
    ) -> BaseListItem:
        return RelationshipBrowserListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return RelationshipBrowserDetailPanel(
            adapter=self._adapter, id=self.detail_panel_id
        )

    def update_detail_panel(self, item: RelationshipViewModel | None) -> None:
        try:
            detail = self.query_one(
                f"#{self.detail_panel_id}", RelationshipBrowserDetailPanel
            )
            detail.update_relationship(item)
        except (NoMatches, AttributeError):
            logger.debug(
                "update_detail_panel: #%s not found", self.detail_panel_id
            )

    def get_item_id(self, item: RelationshipViewModel) -> str:
        return item.relationship_type.replace(" ", "-")

    def get_item_search_text(self, item: RelationshipViewModel) -> str:
        parts = [item.relationship_type]
        if item.source_entity:
            parts.append(item.source_entity)
        if item.target_entity:
            parts.append(item.target_entity)
        return " ".join(parts)

    def on_edit(self, item: RelationshipViewModel) -> None:
        self.post_message(self.DrillDown(item.relationship_type))

    def on_add(self) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_id(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step2(response.value.strip())

        self.app.push_screen(
            InputDialog(
                title="Add Relationship Source",
                body="Source ID:",
                placeholder="e.g. follows_csv",
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
                title="Add Relationship Source",
                body="File URI:",
                placeholder="e.g. data/follows.csv",
            ),
            callback=_got_uri,
        )

    def _add_step3(self, source_id: str, uri: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_type(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step4(source_id, uri, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title="Add Relationship Source",
                body="Relationship type:",
                placeholder="e.g. FOLLOWS",
            ),
            callback=_got_type,
        )

    def _add_step4(self, source_id: str, uri: str, rel_type: str) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_source_col(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self._add_step5(source_id, uri, rel_type, response.value.strip())

        self.app.push_screen(
            InputDialog(
                title="Add Relationship Source",
                body="Source column:",
                placeholder="e.g. from_id",
            ),
            callback=_got_source_col,
        )

    def _add_step5(
        self, source_id: str, uri: str, rel_type: str, source_col: str
    ) -> None:
        from pycypher_tui.widgets.dialog import DialogResult, InputDialog

        def _got_target_col(response):
            if response.result != DialogResult.CONFIRMED or not response.value:
                return
            self.config_manager.add_relationship_source(
                source_id, uri, rel_type, source_col, response.value.strip(),
            )
            self._adapter.refresh()
            self.run_worker(self.refresh_from_config(), exclusive=True)
            self.post_message(self.AddRelationshipSource())

        self.app.push_screen(
            InputDialog(
                title="Add Relationship Source",
                body="Target column:",
                placeholder="e.g. to_id",
            ),
            callback=_got_target_col,
        )

    async def on_delete(self, item: RelationshipViewModel) -> None:
        """Delete all sources for this relationship type."""
        detail = self._adapter.relationship_detail(item.relationship_type)
        for src in detail.sources:
            self.config_manager.remove_relationship_source(src.source_id)
        self._adapter.refresh()
        await self.refresh_from_config()
        self.post_message(self.DeleteRelationshipType(item.relationship_type))

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
        """Preview data from the first source of the selected relationship type."""
        item = self.current_item
        if item is None:
            return

        detail = self._adapter.relationship_detail(item.relationship_type)
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
            self.query_one("#rel-browser-summary")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer")
                self._update_summary_content(footer)
            except (NoMatches, AttributeError):
                logger.debug("_mount_summary: #screen-footer not found")

    def _update_summary_content(self, footer) -> None:
        stats = self._adapter.model_statistics()
        summary = (
            f" {stats.relationship_type_count} relationship type"
            f"{'s' if stats.relationship_type_count != 1 else ''}"
            f"  {stats.total_source_count} total source"
            f"{'s' if stats.total_source_count != 1 else ''}"
        )

        # Determine validation summary
        items = self._items
        errors = sum(1 for r in items if r.validation_status == "error")
        warnings = sum(1 for r in items if r.validation_status == "warning")

        if errors > 0:
            summary += f"  ({errors} with errors)"
            css_class = "summary-error"
        elif warnings > 0:
            summary += f"  ({warnings} with warnings)"
            css_class = "summary-warn"
        else:
            css_class = "summary-ok"

        label = Label(summary, id="rel-browser-summary")
        label.add_class(css_class)
        self.mount(label, before=footer)

    async def refresh_from_config(self) -> None:
        await super().refresh_from_config()
        self._update_summary()

    def _update_summary(self) -> None:
        """Update validation summary."""
        try:
            summary_widget = self.query_one("#rel-browser-summary", Label)
        except NoMatches:
            return

        total = self.item_count
        errors = sum(1 for r in self.items if r.validation_status == "error")
        warnings = sum(1 for r in self.items if r.validation_status == "warning")

        if total == 0:
            summary_widget.update(" No relationship types configured")
            summary_widget.remove_class("summary-ok", "summary-warn", "summary-error")
        elif errors > 0:
            summary_widget.update(f" {total} relationship types, {errors} with errors")
            summary_widget.remove_class("summary-ok", "summary-warn")
            summary_widget.add_class("summary-error")
        elif warnings > 0:
            summary_widget.update(
                f" {total} relationship types, {warnings} with warnings"
            )
            summary_widget.remove_class("summary-ok", "summary-error")
            summary_widget.add_class("summary-warn")
        else:
            summary_widget.update(f" {total} relationship types, all valid")
            summary_widget.remove_class("summary-warn", "summary-error")
            summary_widget.add_class("summary-ok")
