"""Main pipeline overview screen - central dashboard for pipeline management.

Displays all pipeline sections (sources, entities, relationships, queries,
outputs) with status indicators and VIM-style navigation for drill-down.

Refactored to use VimNavigableScreen for consistent VIM navigation,
ModeManager integration, search, and register/clipboard support.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SectionInfo:
    """Summary info for a pipeline section."""

    key: str
    label: str
    icon: str
    item_count: int
    status: str  # "empty", "configured", "error"
    details: list[str]


class SectionWidget(BaseListItem[SectionInfo]):
    """Displays a single pipeline section with status and item count."""

    CSS = """
    SectionWidget {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin: 0;
    }

    SectionWidget.item-focused {
        background: #283457;
    }

    SectionWidget .section-header {
        width: 100%;
        height: 1;
    }

    SectionWidget .section-details {
        width: 100%;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, info: SectionInfo, **kwargs) -> None:
        super().__init__(**kwargs)
        self.info = info

    def compose(self) -> ComposeResult:
        status_color = {
            "empty": "#565f89",
            "configured": "#9ece6a",
            "error": "#f7768e",
        }.get(self.info.status, "#565f89")

        count_text = (
            f"{self.info.item_count} item{'s' if self.info.item_count != 1 else ''}"
            if self.info.item_count > 0
            else "none"
        )

        yield Label(
            f"  {self.info.icon}  {self.info.label}  [{count_text}]",
            classes="section-header",
        )
        for detail in self.info.details[:3]:
            yield Label(f"    {detail}", classes="section-details")


class SectionDetailPanel(BaseDetailPanel):
    """Right-side detail panel showing selected section properties."""

    def __init__(self, **kwargs) -> None:
        super().__init__(empty_message="(no section selected)", **kwargs)

    def update_section(self, section: SectionInfo | None) -> None:
        """Update the detail panel with section information."""
        self.remove_children()

        if section is None:
            self.mount(Label("(no section selected)", classes="detail-title"))
            return

        self.mount(Label(f"{section.icon}  {section.label}", classes="detail-title"))
        self.mount(Label(f"  Status: {section.status}", classes="detail-row"))

        count_text = (
            f"{section.item_count} item{'s' if section.item_count != 1 else ''}"
            if section.item_count > 0
            else "none configured"
        )
        self.mount(Label(f"  Items: {count_text}", classes="detail-row"))

        if section.details:
            self.mount(Label("  Contents", classes="detail-section"))
            for detail in section.details:
                self.mount(Label(f"    {detail}", classes="detail-row"))


class PipelineOverviewScreen(VimNavigableScreen[SectionInfo]):
    """Central dashboard showing all pipeline sections.

    VIM navigation (via VimNavigableScreen + ModeManager):
        j/k         - Move between sections
        Enter/l     - Drill into selected section
        gg/G        - Jump to first/last section
        dd          - Delete action on section
        /pattern    - Search sections
        n/N         - Next/previous search match
        1-4         - Jump to section by number
        u/Ctrl+r    - Undo/redo config changes
        i           - Edit action on section
        ?           - Show help
    """

    CSS = """
    PipelineOverviewScreen {
        layout: vertical;
    }

    #validation-summary {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
    }

    .validation-ok {
        color: #9ece6a;
    }

    .validation-error {
        color: #f7768e;
    }

    .validation-warn {
        color: #e0af68;
    }
    """

    class SectionSelected(Message):
        """Posted when a section is activated for drill-down."""

        def __init__(self, section_key: str) -> None:
            super().__init__()
            self.section_key = section_key

    class ActionRequested(Message):
        """Posted when user requests an action on the current section."""

        def __init__(self, section_key: str, action: str) -> None:
            super().__init__()
            self.section_key = section_key
            self.action = action  # "edit", "add", "delete"

    SECTION_KEYS = [
        "data_model",
        "entity_sources",
        "relationship_sources",
        "queries",
        "query_lineage",
        "outputs",
    ]

    def __init__(
        self,
        config_manager: ConfigManager | None = None,
        **kwargs,
    ) -> None:
        cm = config_manager or ConfigManager()
        super().__init__(config_manager=cm, **kwargs)

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        config = self._config_manager.get_config()
        return config.project.name if config.project else "Untitled Pipeline"

    @property
    def breadcrumb_text(self) -> str:
        config = self._config_manager.get_config()
        desc = config.project.description if config.project else ""
        return desc or "Pipeline Overview"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:open  i:edit  :w:save  :q:quit  ?:help"

    @property
    def empty_list_message(self) -> str:
        return "No pipeline sections available."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[SectionInfo]:
        config = self._config_manager.get_config()
        return self._build_section_list(config)

    def create_list_item(self, item: SectionInfo, item_id: str) -> BaseListItem:
        return SectionWidget(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return SectionDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: SectionInfo | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", SectionDetailPanel)
            detail.update_section(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: SectionInfo) -> str:
        return item.key

    def get_item_search_text(self, item: SectionInfo) -> str:
        return f"{item.key} {item.label} {' '.join(item.details)}"

    def on_edit(self, item: SectionInfo) -> None:
        self.post_message(self.SectionSelected(item.key))

    def on_add(self) -> None:
        self._request_action("add")

    def on_delete(self, item: SectionInfo) -> None:
        self._request_action("delete")

    # --- Screen-specific key overrides ---

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"i", "u", "ctrl+r", "1", "2", "3", "4", "5"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "i":
                self._request_action("edit")
                return True
            case "u":
                self.run_worker(self._undo(), exclusive=True)
                return True
            case "ctrl+r":
                self.run_worker(self._redo(), exclusive=True)
                return True
            case "1" | "2" | "3" | "4" | "5":
                idx = int(key) - 1
                if idx < self.item_count:
                    self._jump_to(idx)
                    section = self.current_item
                    if section:
                        self.post_message(self.SectionSelected(section.key))
                return True
            case _:
                return False

    # --- Layout override: add validation summary ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_validation_summary)

    def _mount_validation_summary(self) -> None:
        """Mount validation summary widget after initial compose."""
        try:
            self.query_one("#validation-summary")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer", Static)
                self.mount(
                    Static("", id="validation-summary"),
                    before=footer,
                )
            except (NoMatches, AttributeError):
                logger.debug("_mount_validation_summary: #screen-footer not found")

    def on_mount(self) -> None:
        # NOTE: Do NOT call super().on_mount() here.  Textual 1.0 dispatches
        # Mount to every class in the MRO that defines on_mount, so
        # VimNavigableScreen.on_mount is already invoked automatically.
        self._update_validation()

    # --- Section building ---

    def _build_section_list(self, config) -> list[SectionInfo]:
        """Extract section summaries from config."""
        sections = []

        # Data model overview
        entity_types = {e.entity_type for e in config.sources.entities}
        rel_types = {r.relationship_type for r in config.sources.relationships}
        type_count = len(entity_types) + len(rel_types)
        model_details = []
        if entity_types:
            model_details.append(f"Entities: {', '.join(sorted(entity_types)[:3])}")
        if rel_types:
            model_details.append(f"Relationships: {', '.join(sorted(rel_types)[:3])}")
        sections.append(
            SectionInfo(
                key="data_model",
                label="Data Model",
                icon="[G]",
                item_count=type_count,
                status="configured" if type_count > 0 else "empty",
                details=model_details,
            )
        )

        # Entity sources
        entities = config.sources.entities
        entity_details = [
            f"{e.id}: {e.entity_type} ({e.uri})" for e in entities[:3]
        ]
        if len(entities) > 3:
            entity_details.append(f"  ... and {len(entities) - 3} more")
        sections.append(
            SectionInfo(
                key="entity_sources",
                label="Entity Sources",
                icon="[E]",
                item_count=len(entities),
                status="configured" if entities else "empty",
                details=entity_details,
            )
        )

        # Relationship sources
        rels = config.sources.relationships
        rel_details = [
            f"{r.id}: {r.relationship_type} ({r.source_col} -> {r.target_col})"
            for r in rels[:3]
        ]
        if len(rels) > 3:
            rel_details.append(f"  ... and {len(rels) - 3} more")
        sections.append(
            SectionInfo(
                key="relationship_sources",
                label="Relationship Sources",
                icon="[R]",
                item_count=len(rels),
                status="configured" if rels else "empty",
                details=rel_details,
            )
        )

        # Queries
        queries = config.queries
        query_details = [
            f"{q.id}: {q.description or '(no description)'}"
            for q in queries[:3]
        ]
        if len(queries) > 3:
            query_details.append(f"  ... and {len(queries) - 3} more")
        sections.append(
            SectionInfo(
                key="queries",
                label="Cypher Queries",
                icon="[Q]",
                item_count=len(queries),
                status="configured" if queries else "empty",
                details=query_details,
            )
        )

        # Outputs (define early so lineage section can reference it)
        outputs = config.output

        # Query lineage & data flow
        total_components = len(entities) + len(rels) + len(queries) + len(outputs)
        lineage_details = []
        if total_components > 0:
            lineage_details.append(f"Pipeline components: {total_components}")
            if entities and queries:
                lineage_details.append(f"Data flow: {len(entities)} sources → {len(queries)} queries")
            if queries and outputs:
                lineage_details.append(f"Output flow: {len(queries)} queries → {len(outputs)} sinks")
        sections.append(
            SectionInfo(
                key="query_lineage",
                label="Query Lineage & Data Flow",
                icon="[L]",
                item_count=total_components,
                status="configured" if total_components > 0 else "empty",
                details=lineage_details,
            )
        )

        # Outputs
        output_details = [
            f"{o.query_id} -> {o.uri}" for o in outputs[:3]
        ]
        if len(outputs) > 3:
            output_details.append(f"  ... and {len(outputs) - 3} more")
        sections.append(
            SectionInfo(
                key="outputs",
                label="Output Sinks",
                icon="[O]",
                item_count=len(outputs),
                status="configured" if outputs else "empty",
                details=output_details,
            )
        )

        return sections

    # --- Validation ---

    def _update_validation(self) -> None:
        """Update validation summary display."""
        try:
            summary_widget = self.query_one("#validation-summary", Static)
        except NoMatches:
            return

        result = self._config_manager.validate()

        if result.is_valid:
            summary_widget.update(" Pipeline configuration is valid")
            summary_widget.remove_class("validation-error", "validation-warn")
            summary_widget.add_class("validation-ok")
        else:
            error_count = len(result.errors)
            warning_count = len(result.warnings)
            parts = []
            if error_count:
                parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
            if warning_count:
                parts.append(f"{warning_count} warning{'s' if warning_count != 1 else ''}")
            summary_widget.update(f" Validation: {', '.join(parts)}")
            if error_count:
                summary_widget.remove_class("validation-ok", "validation-warn")
                summary_widget.add_class("validation-error")
            else:
                summary_widget.remove_class("validation-ok", "validation-error")
                summary_widget.add_class("validation-warn")

    # --- Actions ---

    def _request_action(self, action: str) -> None:
        """Post an action request for the current section."""
        section = self.current_item
        if section:
            self.post_message(self.ActionRequested(section.key, action))

    async def _undo(self) -> None:
        """Undo last config change and refresh."""
        if self._config_manager.can_undo():
            self._config_manager.undo()
            await self.refresh_from_config()
            self._update_validation()

    async def _redo(self) -> None:
        """Redo last undone config change and refresh."""
        if self._config_manager.can_redo():
            self._config_manager.redo()
            await self.refresh_from_config()
            self._update_validation()

    # --- Backward compatibility ---

    @property
    def section_count(self) -> int:
        """Backward compatibility alias for item_count."""
        return self.item_count

    @property
    def current_section(self) -> SectionInfo | None:
        """Backward compatibility alias for current_item."""
        return self.current_item
