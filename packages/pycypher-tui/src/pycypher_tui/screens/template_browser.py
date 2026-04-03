"""Template browser screen.

Displays available pipeline templates grouped by category,
with preview and parameterised instantiation.
VIM-style navigation with Enter to select.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from textual.app import ComposeResult
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import Label, Static

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import (
    PipelineTemplate,
    list_templates,
)
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem, VimNavigableScreen

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TemplateSummary:
    """Display-friendly representation of a template."""

    name: str
    description: str
    category: str
    entity_count: int
    relationship_count: int
    query_count: int
    output_count: int


def summarise_template(template: PipelineTemplate) -> TemplateSummary:
    """Build a summary by instantiating the template with defaults."""
    try:
        config = template.instantiate()
    except (ValueError, KeyError, TypeError, AttributeError, RuntimeError):
        return TemplateSummary(
            name=template.name,
            description=template.description,
            category=template.category,
            entity_count=0,
            relationship_count=0,
            query_count=0,
            output_count=0,
        )

    return TemplateSummary(
        name=template.name,
        description=template.description,
        category=template.category,
        entity_count=len(config.sources.entities),
        relationship_count=len(config.sources.relationships),
        query_count=len(config.queries),
        output_count=len(config.output),
    )


class TemplateDetailPanel(BaseDetailPanel):
    """Right panel showing selected template details."""

    CSS = """
    TemplateDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    TemplateDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    TemplateDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    TemplateDetailPanel .detail-category {
        color: #e0af68;
    }

    TemplateDetailPanel .detail-stat {
        color: #9ece6a;
        padding: 0 2;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(empty_message="(no template selected)", **kwargs)

    def update_template(self, summary: TemplateSummary | None) -> None:
        """Update detail panel with template info."""
        self.remove_children()

        if summary is None:
            self.mount(Label("(no template selected)", classes="detail-title"))
            return

        self.mount(Label(f"{summary.name}", classes="detail-title"))
        self.mount(Label(f"  {summary.description}", classes="detail-row"))
        self.mount(Label(f"  Category: {summary.category}", classes="detail-category"))
        self.mount(Label("", classes="detail-row"))
        self.mount(Label("  Includes:", classes="detail-row"))
        self.mount(Label(f"    {summary.entity_count} entity sources", classes="detail-stat"))
        self.mount(
            Label(f"    {summary.relationship_count} relationship sources", classes="detail-stat")
        )
        self.mount(Label(f"    {summary.query_count} queries", classes="detail-stat"))
        self.mount(Label(f"    {summary.output_count} outputs", classes="detail-stat"))


class TemplateListItem(BaseListItem[TemplateSummary]):
    """Single template entry in the list."""

    CSS = """
    TemplateListItem {
        width: 100%;
        height: 3;
        padding: 0 2;
    }

    TemplateListItem.item-focused {
        background: #283457;
    }

    TemplateListItem .tmpl-name {
        width: 100%;
        height: 1;
    }

    TemplateListItem .tmpl-desc {
        width: 100%;
        height: 1;
        color: #565f89;
        padding-left: 4;
    }

    TemplateListItem .tmpl-category {
        width: 100%;
        height: 1;
        color: #e0af68;
        padding-left: 4;
    }
    """

    def __init__(self, summary: TemplateSummary, **kwargs) -> None:
        super().__init__(**kwargs)
        self.summary = summary

    def compose(self) -> ComposeResult:
        yield Label(f"  {self.summary.name}", classes="tmpl-name")
        yield Label(f"    {self.summary.description}", classes="tmpl-desc")
        yield Label(f"    [{self.summary.category}]", classes="tmpl-category")


class TemplateBrowserScreen(VimNavigableScreen[TemplateSummary]):
    """Template browser with VIM navigation.

    VIM navigation:
        j/k         - Move between templates
        Enter/l     - Select template for instantiation
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        /           - Filter by category
    """

    class NavigateBack(VimNavigableScreen.NavigateBack):
        """Request to navigate back to overview."""

    class TemplateSelected(Message):
        """User selected a template for instantiation."""

        def __init__(self, template_name: str) -> None:
            super().__init__()
            self.template_name = template_name

    def __init__(self, config_manager: ConfigManager | None = None, **kwargs) -> None:
        # TemplateBrowserScreen doesn't need ConfigManager for loading,
        # but accepts it for interface consistency with VimNavigableScreen
        super().__init__(config_manager=config_manager or ConfigManager(), **kwargs)
        self._category_filter: str | None = None

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Template Browser"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Templates"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:select  h:back"

    @property
    def empty_list_message(self) -> str:
        return "No templates available."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[TemplateSummary]:
        all_summaries = [summarise_template(t) for t in list_templates()]
        if self._category_filter:
            return [s for s in all_summaries if s.category == self._category_filter]
        return all_summaries

    def create_list_item(self, item: TemplateSummary, item_id: str) -> BaseListItem:
        return TemplateListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return TemplateDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: TemplateSummary | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", TemplateDetailPanel)
            detail.update_template(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: TemplateSummary) -> str:
        return item.name

    def get_item_search_text(self, item: TemplateSummary) -> str:
        return f"{item.name} {item.description} {item.category}"

    def on_edit(self, item: TemplateSummary) -> None:
        self.post_message(self.TemplateSelected(item.name))

    def on_add(self) -> None:
        pass  # Templates are read-only

    def on_delete(self, item: TemplateSummary) -> None:
        pass  # Templates are read-only

    # --- Backward compatibility ---

    @property
    def template_count(self) -> int:
        return self.item_count

    @property
    def current_template(self) -> TemplateSummary | None:
        return self.current_item
