"""Data model overview screen - graph visualization of entity types and relationships.

Displays entity types as nodes and relationship types as edges in an ASCII
graph view, with VIM-style navigation and drill-down to existing entity/
relationship screens.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field

from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.ingestion.introspector import DataSourceIntrospector
from textual.app import ComposeResult
from textual.containers import Container, VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import (
    DataTable,
    Label,
    LoadingIndicator,
    TabbedContent,
    TabPane,
    Tabs,
)
from textual.worker import Worker

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import (
    BaseDetailPanel,
    BaseListItem,
    VimNavigableScreen,
)
from pycypher_tui.widgets.column_mapping import ColumnMappingWidget

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelNode:
    """A node in the data model graph — either an entity type or relationship type."""

    node_id: str  # unique key: "entity:Person" or "rel:KNOWS"
    label: str  # display label: "Person", "KNOWS"
    node_type: str  # "entity" or "relationship"
    source_count: int  # how many data sources feed this type
    source_ids: tuple[str, ...]  # IDs of the backing data sources
    connections: tuple[str, ...]  # formatted connection strings


@dataclass(frozen=True)
class ModelEdge:
    """A relationship edge connecting entity types."""

    relationship_type: str
    source_entity: str | None  # inferred from column names or None
    target_entity: str | None
    source_col: str
    target_col: str


@dataclass
class AttributeData:
    """Container for attribute inspection results."""

    schema_info: dict | None = None
    column_stats: dict | None = None
    validation_results: dict | None = None
    sample_data: list[dict] | None = None
    error: str | None = None


def _build_model(config) -> tuple[list[ModelNode], list[ModelEdge]]:
    """Extract the data model graph from a PipelineConfig.

    Returns a list of ModelNodes (entity types first, then relationship types)
    and a list of ModelEdges for the relationship connections.
    """
    # Collect entity types and their sources
    entity_types: dict[str, list[str]] = {}
    for e in config.sources.entities:
        entity_types.setdefault(e.entity_type, []).append(e.id)

    # Collect relationship types with connection info
    rel_types: dict[str, list[str]] = {}
    edges: list[ModelEdge] = []
    for r in config.sources.relationships:
        rel_types.setdefault(r.relationship_type, []).append(r.id)
        edges.append(
            ModelEdge(
                relationship_type=r.relationship_type,
                source_entity=None,  # not available from config
                target_entity=None,
                source_col=r.source_col,
                target_col=r.target_col,
            )
        )

    # Build entity nodes
    nodes: list[ModelNode] = []
    entity_names = sorted(entity_types.keys())
    for etype in entity_names:
        src_ids = entity_types[etype]
        # Find relationships that reference this entity type
        connections = []
        for edge in edges:
            connections.append(
                f"-[:{edge.relationship_type}]-> (via {edge.source_col}->{edge.target_col})"
            )
        # Only show unique connections relevant to this entity
        nodes.append(
            ModelNode(
                node_id=f"entity:{etype}",
                label=etype,
                node_type="entity",
                source_count=len(src_ids),
                source_ids=tuple(src_ids),
                connections=(),  # populated below
            )
        )

    # Build relationship nodes with connection info
    for rtype in sorted(rel_types.keys()):
        src_ids = rel_types[rtype]
        related_edges = [e for e in edges if e.relationship_type == rtype]
        conn_strs = []
        for edge in related_edges:
            conn_strs.append(f"({edge.source_col}) -> ({edge.target_col})")
        nodes.append(
            ModelNode(
                node_id=f"rel:{rtype}",
                label=rtype,
                node_type="relationship",
                source_count=len(src_ids),
                source_ids=tuple(src_ids),
                connections=tuple(conn_strs),
            )
        )

    return nodes, edges


class ModelNodeWidget(BaseListItem[ModelNode]):
    """Displays a single data model node (entity type or relationship type)."""

    CSS = """
    ModelNodeWidget {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin: 0;
    }

    ModelNodeWidget.item-focused {
        background: #283457;
    }

    ModelNodeWidget .node-header {
        width: 100%;
        height: 1;
    }

    ModelNodeWidget .node-detail {
        width: 100%;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, node: ModelNode, **kwargs) -> None:
        super().__init__(**kwargs)
        self.node = node

    def compose(self) -> ComposeResult:
        if self.node.node_type == "entity":
            icon = "(\u25CB)"  # circle for entities
            sources_text = (
                f"{self.node.source_count} source{'s' if self.node.source_count != 1 else ''}"
            )
            yield Label(
                f"  {icon}  {self.node.label}  [{sources_text}]",
                classes="node-header",
            )
        else:
            icon = "[\u2192]"  # arrow for relationships
            sources_text = (
                f"{self.node.source_count} source{'s' if self.node.source_count != 1 else ''}"
            )
            yield Label(
                f"  {icon}  :{self.node.label}  [{sources_text}]",
                classes="node-header",
            )
            for conn in self.node.connections[:3]:
                yield Label(f"      {conn}", classes="node-detail")


class ModelDetailPanel(BaseDetailPanel):
    """Right-side detail panel showing selected model node properties with tabbed attribute inspector."""

    CSS = """
    ModelDetailPanel {
        width: 100%;
        height: 100%;
    }

    #detail-content {
        width: 100%;
        height: 1fr;
    }

    .loading-container {
        width: 100%;
        height: 100%;
        content-align: center middle;
    }

    .error-container {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: #f7768e;
    }

    .attributes-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .validation-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .stats-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .lineage-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .info-row {
        width: 100%;
        color: #a9b1d6;
        margin-bottom: 1;
    }

    .info-label {
        color: #7aa2f7;
        text-style: bold;
    }

    .section-header {
        color: #e0af68;
        text-style: bold;
        margin-top: 1;
        margin-bottom: 1;
    }

    .attribute-table {
        width: 100%;
        height: 100%;
    }

    .validation-pass {
        color: #9ece6a;
    }

    .validation-fail {
        color: #f7768e;
    }

    .validation-warn {
        color: #e0af68;
    }
    """

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(empty_message="(no type selected)", **kwargs)
        self.config_manager = config_manager
        self._current_node: ModelNode | None = None
        self._attribute_data: AttributeData | None = None
        self._worker: Worker | None = None

    def compose(self) -> ComposeResult:
        with Container(id="detail-content"):
            with TabbedContent(id="attribute-tabs"):
                with TabPane("Overview", id="tab-overview"):
                    yield Label("(no type selected)", classes="info-row")
                with TabPane("Attributes", id="tab-attributes"):
                    yield LoadingIndicator(id="attributes-loading")
                with TabPane("Validation", id="tab-validation"):
                    yield LoadingIndicator(id="validation-loading")
                with TabPane("Statistics", id="tab-statistics"):
                    yield LoadingIndicator(id="statistics-loading")
                with TabPane("Lineage", id="tab-lineage"):
                    yield LoadingIndicator(id="lineage-loading")

    def update_node(self, node: ModelNode | None) -> None:
        """Update detail panel with model node information."""
        self._current_node = node

        # Cancel any existing worker
        if self._worker is not None and not self._worker.is_finished:
            self._worker.cancel()

        # Update overview tab immediately
        self._update_overview_tab(node)

        if node is None:
            self._clear_attribute_tabs()
            return

        # Show loading indicators in attribute tabs
        self._show_loading_indicators()

        # Start attribute loading in background thread (blocking I/O).
        # Must use a closure — run_worker(thread=True) expects a no-arg callable.
        _node = node  # capture for closure

        def _load() -> None:
            self._load_attribute_data(_node)

        self._worker = self.run_worker(_load, thread=True, exclusive=True)

    def _update_overview_tab(self, node: ModelNode | None) -> None:
        """Update the overview tab with basic node information."""
        try:
            tab = self.query_one("#tab-overview")
            tab.remove_children()

            if node is None:
                tab.mount(Label("(no type selected)", classes="info-row"))
                return

            # Basic node information
            container = VerticalScroll(classes="attributes-info")
            tab.mount(container)

            if node.node_type == "entity":
                container.mount(Label(f"Entity: {node.label}", classes="section-header"))
            else:
                container.mount(Label(f"Relationship: :{node.label}", classes="section-header"))

            container.mount(Label(f"Type: {node.node_type}", classes="info-row"))

            sources_text = (
                f"{node.source_count} data source{'s' if node.source_count != 1 else ''}"
            )
            container.mount(Label(f"Sources: {sources_text}", classes="info-row"))

            if node.source_ids:
                container.mount(Label("Source IDs:", classes="info-label"))
                for sid in node.source_ids:
                    container.mount(Label(f"  {sid}", classes="info-row"))

            if node.connections:
                container.mount(Label("Connections:", classes="info-label"))
                for conn in node.connections:
                    container.mount(Label(f"  {conn}", classes="info-row"))

        except Exception as exc:
            logger.exception("Failed to update overview tab: %s", exc)

    def _clear_attribute_tabs(self) -> None:
        """Clear all attribute tabs and show loading indicators."""
        for tab_id in ["tab-attributes", "tab-validation", "tab-statistics", "tab-lineage"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(LoadingIndicator())
            except Exception:
                logger.debug("Failed to clear tab %s", tab_id)

    def _show_loading_indicators(self) -> None:
        """Show loading indicators in attribute tabs."""
        for tab_id in ["tab-attributes", "tab-validation", "tab-statistics", "tab-lineage"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(LoadingIndicator())
            except Exception:
                logger.debug("Failed to show loading indicator in tab %s", tab_id)

    def _load_attribute_data(self, node: ModelNode) -> None:
        """Load attribute data in a background thread (blocking I/O)."""
        try:
            config = self.config_manager.get_config()

            # Collect all sources for this node type
            all_schema_info = {}
            all_column_stats = {}
            validation_results = {}
            sample_data = []

            # Process entity or relationship sources
            if node.node_type == "entity":
                sources = [e for e in config.sources.entities if e.entity_type == node.label]
            else:
                sources = [r for r in config.sources.relationships if r.relationship_type == node.label]

            for source in sources:
                try:
                    # Use DataSourceIntrospector for analysis
                    introspector = DataSourceIntrospector(source.uri)

                    # Get schema info
                    schema = introspector.get_schema()
                    all_schema_info[source.id] = {
                        "columns": schema.columns,
                        "row_count": schema.row_count,
                    }

                    # Get column statistics
                    stats = introspector.get_column_stats()
                    all_column_stats[source.id] = stats

                    # Sample data (first few rows)
                    sample_df = introspector.sample(n=50)
                    sample_data.append({
                        "source_id": source.id,
                        "data": sample_df.to_dict("records")
                    })

                    # Basic validation
                    validation_results[source.id] = self._validate_source_mapping(source, schema, node)

                except Exception as exc:
                    logger.warning("Failed to introspect source %s: %s", source.id, exc)
                    validation_results[source.id] = {
                        "status": "error",
                        "message": f"Failed to load: {exc}"
                    }

            self._attribute_data = AttributeData(
                schema_info=all_schema_info,
                column_stats=all_column_stats,
                validation_results=validation_results,
                sample_data=sample_data,
            )

            # Update UI on main thread
            self.call_after_refresh(self._update_attribute_display)

        except Exception as exc:
            logger.exception("Failed to load attribute data for %s", node.label)
            self._attribute_data = AttributeData(error=str(exc))
            self.call_after_refresh(self._update_attribute_display)

    def _validate_source_mapping(self, source, schema, node: ModelNode) -> dict:
        """Validate source column mapping against expected attributes."""
        validation = {"status": "pass", "issues": []}

        try:
            column_names = [col["name"] for col in schema.columns]

            if node.node_type == "entity":
                # Check if entity has an ID column
                id_candidates = [col for col in column_names if "id" in col.lower()]
                if not id_candidates:
                    validation["issues"].append({
                        "type": "warning",
                        "message": "No obvious ID column found (recommended: 'id', 'entity_id', etc.)"
                    })

                # Check for common attribute patterns
                if len(column_names) == 1:
                    validation["issues"].append({
                        "type": "warning",
                        "message": "Only one column found - consider adding more entity attributes"
                    })

            else:  # relationship
                # Check for source/target columns
                if hasattr(source, "source_col") and source.source_col not in column_names:
                    validation["issues"].append({
                        "type": "error",
                        "message": f"Source column '{source.source_col}' not found in data"
                    })

                if hasattr(source, "target_col") and source.target_col not in column_names:
                    validation["issues"].append({
                        "type": "error",
                        "message": f"Target column '{source.target_col}' not found in data"
                    })

            # Set overall status
            if any(issue["type"] == "error" for issue in validation["issues"]):
                validation["status"] = "error"
            elif any(issue["type"] == "warning" for issue in validation["issues"]):
                validation["status"] = "warning"

        except Exception as exc:
            validation = {
                "status": "error",
                "issues": [{"type": "error", "message": f"Validation failed: {exc}"}]
            }

        return validation

    def _update_attribute_display(self) -> None:
        """Update all attribute tabs with loaded data."""
        if self._attribute_data is None:
            return

        if self._attribute_data.error:
            self._show_error(self._attribute_data.error)
            return

        # Update each tab
        self._update_attributes_tab()
        self._update_validation_tab()
        self._update_statistics_tab()
        self._update_lineage_tab()

    def _show_error(self, error: str) -> None:
        """Show error message in all tabs."""
        error_widget = Container(
            Label("Error loading attribute data:", classes="info-label"),
            Label(error, classes="info-row"),
            classes="error-container"
        )

        for tab_id in ["tab-attributes", "tab-validation", "tab-statistics", "tab-lineage"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(error_widget)
            except Exception:
                logger.debug("Failed to update tab %s with error", tab_id)

    def _update_attributes_tab(self) -> None:
        """Update the attributes tab with schema information."""
        if not self._attribute_data or not self._attribute_data.schema_info:
            return

        try:
            tab = self.query_one("#tab-attributes")
            tab.remove_children()

            container = VerticalScroll(classes="attributes-info")
            tab.mount(container)

            for source_id, schema in self._attribute_data.schema_info.items():
                container.mount(Label(f"Source: {source_id}", classes="section-header"))
                container.mount(Label(f"Rows: {schema['row_count']:,}", classes="info-row"))
                container.mount(Label(f"Columns: {len(schema['columns'])}", classes="info-row"))
                container.mount(Label("", classes="info-row"))  # spacer

                # Create table for columns
                table = DataTable(classes="attribute-table")
                table.add_columns("Column", "Type")

                for col in schema["columns"]:
                    table.add_row(col["name"], col["type"])

                container.mount(table)
                container.mount(Label("", classes="info-row"))  # spacer

        except Exception as exc:
            logger.exception("Failed to update attributes tab: %s", exc)

    def _update_validation_tab(self) -> None:
        """Update the validation tab with mapping validation results."""
        if not self._attribute_data or not self._attribute_data.validation_results:
            return

        try:
            tab = self.query_one("#tab-validation")
            tab.remove_children()

            # Check if this is a relationship node for column mapping validation
            if self._current_node and self._current_node.node_type == "relationship":
                self._update_relationship_validation_tab(tab)
            else:
                self._update_entity_validation_tab(tab)

        except Exception as exc:
            logger.exception("Failed to update validation tab: %s", exc)

    def _update_entity_validation_tab(self, tab) -> None:
        """Update validation tab for entity nodes."""
        container = VerticalScroll(classes="validation-info")
        tab.mount(container)

        for source_id, validation in self._attribute_data.validation_results.items():
            container.mount(Label(f"Source: {source_id}", classes="section-header"))

            # Overall status
            status_class = f"validation-{validation['status']}"
            container.mount(Label(f"Status: {validation['status'].upper()}", classes=status_class))

            # Issues
            if "issues" in validation and validation["issues"]:
                container.mount(Label("Issues:", classes="info-label"))
                for issue in validation["issues"]:
                    issue_class = f"validation-{issue['type']}"
                    container.mount(Label(f"  {issue['type'].upper()}: {issue['message']}", classes=issue_class))
            else:
                container.mount(Label("No issues found", classes="validation-pass"))

            container.mount(Label("", classes="info-row"))  # spacer

    def _update_relationship_validation_tab(self, tab) -> None:
        """Update validation tab for relationship nodes with column mapping visualization."""
        # Create column mapping widget for relationship validation
        mapping_widget = ColumnMappingWidget(id="column-mapping")
        tab.mount(mapping_widget)

        # Load relationship sources for this node type
        config = self.config_manager.get_config()
        relationship_sources = [
            r for r in config.sources.relationships
            if r.relationship_type == self._current_node.label
        ]

        # Store sources for mapping change handling
        self._current_relationship_sources = relationship_sources

        # Update the mapping widget in background thread (blocking I/O)
        def update_mappings():
            mapping_widget.update_relationship_sources(relationship_sources)

        self.run_worker(update_mappings, thread=True, exclusive=True)

    def on_column_mapping_widget_mapping_changed(self, message) -> None:
        """Handle column mapping changes and persist to configuration."""
        try:
            if not hasattr(self, "_current_relationship_sources"):
                return

            # Find the source to update based on message
            # For simplicity, we'll update the first source for this relationship type
            # In a full implementation, we'd need to track which specific source is being edited
            if self._current_relationship_sources:
                source = self._current_relationship_sources[0]
                new_mapping = message.new_mapping

                # Update the configuration
                self.config_manager.update_relationship_source(
                    source.id,
                    source_col=new_mapping.get("source_col", source.source_col),
                    target_col=new_mapping.get("target_col", source.target_col)
                )

                # Refresh the display to show updated validation
                self.update_node(self._current_node)

                logger.info(
                    "Updated relationship source %s column mapping: %s",
                    source.id, new_mapping
                )

        except Exception as exc:
            logger.exception("Failed to handle mapping change: %s", exc)

    def _update_statistics_tab(self) -> None:
        """Update the statistics tab with column statistics."""
        if not self._attribute_data or not self._attribute_data.column_stats:
            return

        try:
            tab = self.query_one("#tab-statistics")
            tab.remove_children()

            container = VerticalScroll(classes="stats-info")
            tab.mount(container)

            for source_id, stats in self._attribute_data.column_stats.items():
                container.mount(Label(f"Source: {source_id}", classes="section-header"))

                for col_name, col_stats in stats.items():
                    container.mount(Label(f"{col_name}:", classes="info-label"))
                    container.mount(Label(f"  Type: {col_stats.dtype}", classes="info-row"))
                    container.mount(Label(f"  Null Count: {col_stats.null_count:,}", classes="info-row"))
                    container.mount(Label(f"  Unique Count: {col_stats.unique_count:,}", classes="info-row"))

                    if col_stats.min_value is not None:
                        container.mount(Label(f"  Min: {col_stats.min_value}", classes="info-row"))
                    if col_stats.max_value is not None:
                        container.mount(Label(f"  Max: {col_stats.max_value}", classes="info-row"))

                    container.mount(Label("", classes="info-row"))  # spacer

                container.mount(Label("", classes="info-row"))  # spacer between sources

        except Exception as exc:
            logger.exception("Failed to update statistics tab: %s", exc)

    def _update_lineage_tab(self) -> None:
        """Update the lineage tab with data flow information."""
        try:
            tab = self.query_one("#tab-lineage")
            tab.remove_children()

            container = VerticalScroll(classes="lineage-info")
            tab.mount(container)

            if self._current_node:
                container.mount(Label("Data Flow:", classes="section-header"))
                container.mount(Label(f"Node Type: {self._current_node.node_type}", classes="info-row"))
                container.mount(Label(f"Source Count: {self._current_node.source_count}", classes="info-row"))

                if self._current_node.source_ids:
                    container.mount(Label("Data Sources:", classes="info-label"))
                    for source_id in self._current_node.source_ids:
                        container.mount(Label(f"  → {source_id}", classes="info-row"))

                if self._current_node.connections:
                    container.mount(Label("Graph Connections:", classes="info-label"))
                    for conn in self._current_node.connections:
                        container.mount(Label(f"  {conn}", classes="info-row"))

                # Future: Add actual lineage tracking when available
                container.mount(Label("", classes="info-row"))
                container.mount(Label("Advanced lineage tracking coming soon...", classes="info-row"))

        except Exception as exc:
            logger.exception("Failed to update lineage tab: %s", exc)

    def on_unmount(self) -> None:
        """Cancel loading worker when panel is unmounted."""
        if self._worker is not None and not self._worker.is_finished:
            self._worker.cancel()


class DataModelScreen(VimNavigableScreen[ModelNode]):
    """Data model overview showing entity types and relationship types.

    Displays entity types as graph nodes and relationship types as edges,
    providing a high-level view of the entire data model.

    VIM navigation:
        j/k         - Move between types
        Enter/l     - Drill down to entity/relationship sources
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        /           - Search types
    """

    CSS = """
    DataModelScreen {
        layout: vertical;
    }

    #graph-summary {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
        color: #9ece6a;
    }
    """

    class DrillDown(Message):
        """Request to drill down into a specific type's sources."""

        def __init__(self, node_type: str, label: str) -> None:
            super().__init__()
            self.node_type = node_type  # "entity" or "relationship"
            self.label = label  # the type label

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._edges: list[ModelEdge] = []

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Data Model"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Data Model"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Tab/Shift+Tab:switch tab  Enter:drill-down  h:back  /:search"

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"tab", "shift+tab"})

    @property
    def empty_list_message(self) -> str:
        return "No entity or relationship types defined.\nAdd data sources first."

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[ModelNode]:
        config = self.config_manager.get_config()
        nodes, edges = _build_model(config)
        self._edges = edges
        return nodes

    def create_list_item(self, item: ModelNode, item_id: str) -> BaseListItem:
        return ModelNodeWidget(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return ModelDetailPanel(config_manager=self.config_manager, id=self.detail_panel_id)

    def update_detail_panel(self, item: ModelNode | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", ModelDetailPanel)
            detail.update_node(item)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: ModelNode) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "-", item.node_id)

    def get_item_search_text(self, item: ModelNode) -> str:
        return f"{item.label} {item.node_type} {' '.join(item.source_ids)}"

    def on_edit(self, item: ModelNode) -> None:
        self.post_message(self.DrillDown(item.node_type, item.label))

    def on_add(self) -> None:
        pass  # read-only screen

    def handle_extra_key(self, key: str) -> bool:
        """Handle tab switching in the detail panel."""
        if key in ("tab", "shift+tab"):
            try:
                detail = self.query_one(f"#{self.detail_panel_id}", ModelDetailPanel)
                tabs_widget = detail.query_one(Tabs)
                if key == "tab":
                    tabs_widget.action_next_tab()
                else:
                    tabs_widget.action_previous_tab()
                return True
            except (NoMatches, AttributeError):
                logger.debug("handle_extra_key: could not find tabs widget")
                return False
        return super().handle_extra_key(key)

    async def on_delete(self, item: ModelNode) -> None:
        pass  # read-only screen

    # --- Layout override: add graph summary ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_graph_summary)

    def _mount_graph_summary(self) -> None:
        """Mount graph summary widget after initial compose."""
        try:
            self.query_one("#graph-summary")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer")
                entity_count = sum(
                    1 for n in self._items if n.node_type == "entity"
                )
                rel_count = sum(
                    1 for n in self._items if n.node_type == "relationship"
                )
                edge_count = len(self._edges)
                summary = (
                    f" {entity_count} entity type{'s' if entity_count != 1 else ''}"
                    f"  {rel_count} relationship type{'s' if rel_count != 1 else ''}"
                    f"  {edge_count} connection{'s' if edge_count != 1 else ''}"
                )
                self.mount(
                    Label(summary, id="graph-summary"),
                    before=footer,
                )
            except (NoMatches, AttributeError):
                logger.debug("_mount_graph_summary: #screen-footer not found")
