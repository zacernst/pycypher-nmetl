"""Query lineage and data flow visualization screen.

Provides an ASCII-based visualization of the ETL pipeline data flow,
showing how data flows from sources through queries to outputs.
Supports interactive navigation through pipeline components and
dependency analysis.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.widgets import (
    DataTable,
    Label,
    LoadingIndicator,
    Static,
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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineComponent:
    """A component in the ETL pipeline - source, query, or output."""

    component_id: str
    component_type: str  # "source", "query", "output"
    display_name: str
    description: str | None = None
    dependencies: tuple[str, ...] = ()  # IDs this component depends on
    dependents: tuple[str, ...] = ()    # IDs that depend on this component
    metadata: dict[str, Any] | None = None


@dataclass
class LineageAnalysis:
    """Results of pipeline lineage analysis."""

    components: list[PipelineComponent]
    flow_diagram: str
    dependency_graph: dict[str, list[str]]
    critical_path: list[str]
    orphaned_components: list[str]
    error: str | None = None


def _analyze_pipeline_lineage(config) -> LineageAnalysis:
    """Analyze pipeline configuration to build lineage graph."""
    try:
        components = []
        dependency_graph = {}

        # Extract entity sources
        entity_sources = {}
        for entity in config.sources.entities:
            component_id = f"entity:{entity.id}"
            entity_sources[entity.entity_type] = component_id
            components.append(PipelineComponent(
                component_id=component_id,
                component_type="source",
                display_name=f"Entity: {entity.entity_type}",
                description=f"Source: {entity.uri}",
                metadata={
                    "source_type": "entity",
                    "entity_type": entity.entity_type,
                    "uri": entity.uri,
                    "id_col": getattr(entity, "id_col", None)
                }
            ))
            dependency_graph[component_id] = []

        # Extract relationship sources
        relationship_sources = {}
        for rel in config.sources.relationships:
            component_id = f"relationship:{rel.id}"
            relationship_sources[rel.relationship_type] = component_id
            # Relationships depend on entity sources (if we can infer them)
            deps = []
            components.append(PipelineComponent(
                component_id=component_id,
                component_type="source",
                display_name=f"Relationship: {rel.relationship_type}",
                description=f"Source: {rel.uri}",
                dependencies=tuple(deps),
                metadata={
                    "source_type": "relationship",
                    "relationship_type": rel.relationship_type,
                    "uri": rel.uri,
                    "source_col": rel.source_col,
                    "target_col": rel.target_col
                }
            ))
            dependency_graph[component_id] = deps

        # Extract queries and build dependencies
        query_components = {}
        for query in config.queries:
            component_id = f"query:{query.id}"
            query_components[query.id] = component_id

            # Try to infer dependencies from query content
            query_text = query.inline or ""
            if query.source:
                # Would need to read file - for now just note it
                query_text = f"<from file: {query.source}>"

            # Simple dependency inference - looks for entity/relationship type names
            deps = []
            for entity_type, source_id in entity_sources.items():
                if entity_type.lower() in query_text.lower():
                    deps.append(source_id)
            for rel_type, source_id in relationship_sources.items():
                if rel_type.lower() in query_text.lower():
                    deps.append(source_id)

            components.append(PipelineComponent(
                component_id=component_id,
                component_type="query",
                display_name=f"Query: {query.id}",
                description=query.description or "Cypher query",
                dependencies=tuple(deps),
                metadata={
                    "query_id": query.id,
                    "source": query.source,
                    "inline": query.inline,
                    "description": query.description
                }
            ))
            dependency_graph[component_id] = deps

        # Extract outputs
        for output in config.output:
            component_id = f"output:{output.query_id}:{output.uri}"
            query_comp_id = f"query:{output.query_id}"

            components.append(PipelineComponent(
                component_id=component_id,
                component_type="output",
                display_name=f"Output: {output.uri}",
                description=f"Format: {output.format or 'inferred'}",
                dependencies=(query_comp_id,),
                metadata={
                    "query_id": output.query_id,
                    "uri": output.uri,
                    "format": output.format
                }
            ))
            dependency_graph[component_id] = [query_comp_id]

        # Calculate reverse dependencies (dependents)
        dependents_map = {}
        for comp_id, deps in dependency_graph.items():
            for dep in deps:
                dependents_map.setdefault(dep, []).append(comp_id)

        # Update components with dependents
        components_with_dependents = []
        for comp in components:
            dependents = tuple(dependents_map.get(comp.component_id, []))
            components_with_dependents.append(PipelineComponent(
                component_id=comp.component_id,
                component_type=comp.component_type,
                display_name=comp.display_name,
                description=comp.description,
                dependencies=comp.dependencies,
                dependents=dependents,
                metadata=comp.metadata
            ))

        # Generate flow diagram
        flow_diagram = _generate_flow_diagram(components_with_dependents, dependency_graph)

        # Find critical path (longest dependency chain)
        critical_path = _find_critical_path(dependency_graph)

        # Find orphaned components (no dependencies or dependents)
        orphaned = []
        for comp in components_with_dependents:
            if not comp.dependencies and not comp.dependents:
                orphaned.append(comp.component_id)

        return LineageAnalysis(
            components=components_with_dependents,
            flow_diagram=flow_diagram,
            dependency_graph=dependency_graph,
            critical_path=critical_path,
            orphaned_components=orphaned
        )

    except Exception as exc:
        logger.exception("Failed to analyze pipeline lineage")
        return LineageAnalysis(
            components=[],
            flow_diagram="Error analyzing pipeline",
            dependency_graph={},
            critical_path=[],
            orphaned_components=[],
            error=str(exc)
        )


def _generate_flow_diagram(components: list[PipelineComponent],
                          dependency_graph: dict[str, list[str]]) -> str:
    """Generate ASCII flow diagram of the pipeline."""
    if not components:
        return "No pipeline components found"

    # Group by component type
    sources = [c for c in components if c.component_type == "source"]
    queries = [c for c in components if c.component_type == "query"]
    outputs = [c for c in components if c.component_type == "output"]

    diagram_lines = []
    diagram_lines.append("ETL Pipeline Data Flow:")
    diagram_lines.append("=" * 50)
    diagram_lines.append("")

    # Sources section
    if sources:
        diagram_lines.append("📊 DATA SOURCES")
        diagram_lines.append("─" * 20)
        for source in sources:
            name = source.display_name.replace("Entity: ", "").replace("Relationship: ", "")
            diagram_lines.append(f"  ┌─ {name}")
            if source.dependents:
                diagram_lines.append("  │")
                for i, dep in enumerate(source.dependents):
                    dep_comp = next((c for c in components if c.component_id == dep), None)
                    if dep_comp:
                        dep_name = dep_comp.display_name.replace("Query: ", "")
                        connector = "└──>" if i == len(source.dependents) - 1 else "├──>"
                        diagram_lines.append(f"  {connector} {dep_name}")
            else:
                diagram_lines.append("  └── (no consumers)")
        diagram_lines.append("")

    # Queries section
    if queries:
        diagram_lines.append("🔄 TRANSFORMATIONS")
        diagram_lines.append("─" * 20)
        for query in queries:
            name = query.display_name.replace("Query: ", "")
            diagram_lines.append(f"  ┌─ {name}")

            # Show inputs
            if query.dependencies:
                diagram_lines.append("  │  Inputs:")
                for dep in query.dependencies:
                    dep_comp = next((c for c in components if c.component_id == dep), None)
                    if dep_comp:
                        dep_name = dep_comp.display_name
                        diagram_lines.append(f"  │    ← {dep_name}")

            # Show outputs
            if query.dependents:
                diagram_lines.append("  │  Outputs:")
                for dep in query.dependents:
                    dep_comp = next((c for c in components if c.component_id == dep), None)
                    if dep_comp:
                        dep_name = dep_comp.display_name.replace("Output: ", "")
                        diagram_lines.append(f"  │    → {dep_name}")

            diagram_lines.append("  └─")
        diagram_lines.append("")

    # Outputs section
    if outputs:
        diagram_lines.append("💾 OUTPUTS")
        diagram_lines.append("─" * 20)
        for output in outputs:
            name = output.display_name.replace("Output: ", "")
            diagram_lines.append(f"  ┌─ {name}")
            if output.dependencies:
                dep_comp = next((c for c in components if c.component_id == output.dependencies[0]), None)
                if dep_comp:
                    dep_name = dep_comp.display_name.replace("Query: ", "")
                    diagram_lines.append(f"  └── from {dep_name}")
        diagram_lines.append("")

    # Summary
    diagram_lines.append("📈 PIPELINE SUMMARY")
    diagram_lines.append("─" * 20)
    diagram_lines.append(f"  Sources: {len(sources)}")
    diagram_lines.append(f"  Queries: {len(queries)}")
    diagram_lines.append(f"  Outputs: {len(outputs)}")

    return "\n".join(diagram_lines)


def _find_critical_path(dependency_graph: dict[str, list[str]]) -> list[str]:
    """Find the longest dependency chain in the pipeline."""
    # Simple DFS to find longest path
    def dfs(node: str, visited: set[str]) -> list[str]:
        if node in visited:
            return [node]  # Cycle detection

        visited.add(node)
        max_path = [node]

        # Find all nodes that depend on this node
        for comp_id, deps in dependency_graph.items():
            if node in deps:
                sub_path = dfs(comp_id, visited.copy())
                if len(sub_path) > len(max_path) - 1:
                    max_path = [node] + sub_path

        return max_path

    # Try from all nodes with no dependencies (sources)
    sources = [comp_id for comp_id, deps in dependency_graph.items() if not deps]
    longest_path = []

    for source in sources:
        path = dfs(source, set())
        if len(path) > len(longest_path):
            longest_path = path

    return longest_path


class LineageComponentWidget(BaseListItem[PipelineComponent]):
    """Widget displaying a single pipeline component."""

    CSS = """
    LineageComponentWidget {
        width: 100%;
        height: auto;
        padding: 0 2;
        margin: 0;
    }

    LineageComponentWidget.item-focused {
        background: #283457;
    }

    LineageComponentWidget .component-header {
        width: 100%;
        height: 1;
    }

    LineageComponentWidget .component-detail {
        width: 100%;
        color: #565f89;
        padding-left: 4;
    }
    """

    def __init__(self, component: PipelineComponent, **kwargs) -> None:
        super().__init__(**kwargs)
        self.component = component

    def compose(self) -> ComposeResult:
        # Choose icon based on component type
        if self.component.component_type == "source":
            icon = "📊"
        elif self.component.component_type == "query":
            icon = "🔄"
        elif self.component.component_type == "output":
            icon = "💾"
        else:
            icon = "❓"

        yield Label(
            f"  {icon}  {self.component.display_name}",
            classes="component-header"
        )

        if self.component.description:
            yield Label(f"      {self.component.description}", classes="component-detail")

        # Show dependency count
        dep_count = len(self.component.dependencies)
        dependent_count = len(self.component.dependents)
        if dep_count > 0 or dependent_count > 0:
            yield Label(
                f"      ← {dep_count} inputs  → {dependent_count} outputs",
                classes="component-detail"
            )


class LineageDetailPanel(BaseDetailPanel):
    """Detail panel showing component information and dependencies."""

    CSS = """
    LineageDetailPanel {
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

    .component-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .dependencies-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .flow-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .analysis-info {
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

    .dependency-table {
        width: 100%;
        height: 100%;
    }

    .critical-path {
        color: #f7768e;
        text-style: bold;
    }

    .orphaned {
        color: #e0af68;
    }

    .flow-diagram {
        width: 100%;
        font-family: monospace;
        color: #c0caf5;
        white-space: pre;
    }
    """

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(empty_message="(no component selected)", **kwargs)
        self.config_manager = config_manager
        self._current_component: PipelineComponent | None = None
        self._lineage_analysis: LineageAnalysis | None = None
        self._worker: Worker | None = None

    def compose(self) -> ComposeResult:
        with Container(id="detail-content"):
            with TabbedContent(id="lineage-tabs"):
                with TabPane("Overview", id="tab-overview"):
                    yield Label("(no component selected)", classes="info-row")
                with TabPane("Dependencies", id="tab-dependencies"):
                    yield LoadingIndicator(id="dependencies-loading")
                with TabPane("Flow Diagram", id="tab-flow"):
                    yield LoadingIndicator(id="flow-loading")
                with TabPane("Analysis", id="tab-analysis"):
                    yield LoadingIndicator(id="analysis-loading")

    def update_component(self, component: PipelineComponent | None,
                        lineage_analysis: LineageAnalysis | None = None) -> None:
        """Update detail panel with component information."""
        self._current_component = component
        self._lineage_analysis = lineage_analysis

        # Cancel any existing worker
        if self._worker is not None and not self._worker.is_finished:
            self._worker.cancel()

        # Update overview tab immediately
        self._update_overview_tab(component)

        if component is None or lineage_analysis is None:
            self._clear_detail_tabs()
            return

        # Show loading indicators
        self._show_loading_indicators()

        # Start async detail loading
        self._worker = self.run_worker(self._load_component_details(), exclusive=True)

    def _update_overview_tab(self, component: PipelineComponent | None) -> None:
        """Update the overview tab with basic component information."""
        try:
            tab = self.query_one("#tab-overview")
            tab.remove_children()

            if component is None:
                tab.mount(Label("(no component selected)", classes="info-row"))
                return

            container = VerticalScroll(classes="component-info")
            tab.mount(container)

            # Component header
            container.mount(Label(f"{component.display_name}", classes="section-header"))

            # Basic info
            container.mount(Label(f"Type: {component.component_type}", classes="info-row"))
            container.mount(Label(f"ID: {component.component_id}", classes="info-row"))

            if component.description:
                container.mount(Label(f"Description: {component.description}", classes="info-row"))

            # Dependencies summary
            container.mount(Label("", classes="info-row"))  # spacer
            container.mount(Label("Dependencies:", classes="info-label"))
            container.mount(Label(f"  Inputs: {len(component.dependencies)}", classes="info-row"))
            container.mount(Label(f"  Outputs: {len(component.dependents)}", classes="info-row"))

            # Metadata
            if component.metadata:
                container.mount(Label("", classes="info-row"))  # spacer
                container.mount(Label("Metadata:", classes="info-label"))
                for key, value in component.metadata.items():
                    if value is not None:
                        container.mount(Label(f"  {key}: {value}", classes="info-row"))

        except Exception as exc:
            logger.exception("Failed to update overview tab: %s", exc)

    def _clear_detail_tabs(self) -> None:
        """Clear all detail tabs and show loading indicators."""
        for tab_id in ["tab-dependencies", "tab-flow", "tab-analysis"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(LoadingIndicator())
            except Exception:
                logger.debug("Failed to clear tab %s", tab_id)

    def _show_loading_indicators(self) -> None:
        """Show loading indicators in detail tabs."""
        for tab_id in ["tab-dependencies", "tab-flow", "tab-analysis"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(LoadingIndicator())
            except Exception:
                logger.debug("Failed to show loading indicator in tab %s", tab_id)

    async def _load_component_details(self) -> None:
        """Load detailed component information asynchronously."""
        try:
            # Simulate some processing time for complex analysis
            await asyncio.sleep(0.1)

            # Update UI on main thread
            self.call_after_refresh(self._update_detail_display)

        except Exception as exc:
            logger.exception("Failed to load component details")
            self.call_after_refresh(self._show_error, str(exc))

    def _update_detail_display(self) -> None:
        """Update all detail tabs with loaded data."""
        if self._current_component is None or self._lineage_analysis is None:
            return

        self._update_dependencies_tab()
        self._update_flow_tab()
        self._update_analysis_tab()

    def _show_error(self, error: str) -> None:
        """Show error message in all tabs."""
        error_widget = Container(
            Label("Error loading component details:", classes="info-label"),
            Label(error, classes="info-row"),
            classes="error-container"
        )

        for tab_id in ["tab-dependencies", "tab-flow", "tab-analysis"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(error_widget)
            except Exception:
                logger.debug("Failed to update tab %s with error", tab_id)

    def _update_dependencies_tab(self) -> None:
        """Update the dependencies tab with dependency information."""
        if not self._current_component or not self._lineage_analysis:
            return

        try:
            tab = self.query_one("#tab-dependencies")
            tab.remove_children()

            container = VerticalScroll(classes="dependencies-info")
            tab.mount(container)

            component = self._current_component

            # Input dependencies
            container.mount(Label("Input Dependencies:", classes="section-header"))
            if component.dependencies:
                table = DataTable(classes="dependency-table")
                table.add_columns("Component", "Type", "Description")

                for dep_id in component.dependencies:
                    dep_comp = next(
                        (c for c in self._lineage_analysis.components if c.component_id == dep_id),
                        None
                    )
                    if dep_comp:
                        table.add_row(
                            dep_comp.display_name,
                            dep_comp.component_type,
                            dep_comp.description or ""
                        )

                container.mount(table)
            else:
                container.mount(Label("  No input dependencies", classes="info-row"))

            # Output dependents
            container.mount(Label("", classes="info-row"))  # spacer
            container.mount(Label("Output Dependents:", classes="section-header"))
            if component.dependents:
                table = DataTable(classes="dependency-table")
                table.add_columns("Component", "Type", "Description")

                for dep_id in component.dependents:
                    dep_comp = next(
                        (c for c in self._lineage_analysis.components if c.component_id == dep_id),
                        None
                    )
                    if dep_comp:
                        table.add_row(
                            dep_comp.display_name,
                            dep_comp.component_type,
                            dep_comp.description or ""
                        )

                container.mount(table)
            else:
                container.mount(Label("  No output dependents", classes="info-row"))

        except Exception as exc:
            logger.exception("Failed to update dependencies tab: %s", exc)

    def _update_flow_tab(self) -> None:
        """Update the flow tab with pipeline diagram."""
        if not self._lineage_analysis:
            return

        try:
            tab = self.query_one("#tab-flow")
            tab.remove_children()

            container = VerticalScroll(classes="flow-info")
            tab.mount(container)

            container.mount(Label("Pipeline Flow Diagram:", classes="section-header"))
            container.mount(Static(self._lineage_analysis.flow_diagram, classes="flow-diagram"))

        except Exception as exc:
            logger.exception("Failed to update flow tab: %s", exc)

    def _update_analysis_tab(self) -> None:
        """Update the analysis tab with pipeline analysis."""
        if not self._lineage_analysis:
            return

        try:
            tab = self.query_one("#tab-analysis")
            tab.remove_children()

            container = VerticalScroll(classes="analysis-info")
            tab.mount(container)

            # Critical path
            container.mount(Label("Critical Path Analysis:", classes="section-header"))
            if self._lineage_analysis.critical_path:
                container.mount(Label("Longest dependency chain:", classes="info-label"))
                for i, comp_id in enumerate(self._lineage_analysis.critical_path):
                    comp = next(
                        (c for c in self._lineage_analysis.components if c.component_id == comp_id),
                        None
                    )
                    if comp:
                        arrow = " → " if i < len(self._lineage_analysis.critical_path) - 1 else ""
                        container.mount(Label(f"  {comp.display_name}{arrow}", classes="critical-path"))
            else:
                container.mount(Label("  No critical path found", classes="info-row"))

            # Orphaned components
            container.mount(Label("", classes="info-row"))  # spacer
            container.mount(Label("Orphaned Components:", classes="section-header"))
            if self._lineage_analysis.orphaned_components:
                container.mount(Label("Components with no dependencies or dependents:", classes="info-label"))
                for comp_id in self._lineage_analysis.orphaned_components:
                    comp = next(
                        (c for c in self._lineage_analysis.components if c.component_id == comp_id),
                        None
                    )
                    if comp:
                        container.mount(Label(f"  {comp.display_name}", classes="orphaned"))
            else:
                container.mount(Label("  No orphaned components", classes="info-row"))

            # Pipeline statistics
            container.mount(Label("", classes="info-row"))  # spacer
            container.mount(Label("Pipeline Statistics:", classes="section-header"))
            sources = sum(1 for c in self._lineage_analysis.components if c.component_type == "source")
            queries = sum(1 for c in self._lineage_analysis.components if c.component_type == "query")
            outputs = sum(1 for c in self._lineage_analysis.components if c.component_type == "output")

            container.mount(Label(f"  Total components: {len(self._lineage_analysis.components)}", classes="info-row"))
            container.mount(Label(f"  Data sources: {sources}", classes="info-row"))
            container.mount(Label(f"  Queries: {queries}", classes="info-row"))
            container.mount(Label(f"  Outputs: {outputs}", classes="info-row"))

            # Complexity metrics
            total_deps = sum(len(c.dependencies) for c in self._lineage_analysis.components)
            avg_deps = total_deps / len(self._lineage_analysis.components) if self._lineage_analysis.components else 0

            container.mount(Label(f"  Total dependencies: {total_deps}", classes="info-row"))
            container.mount(Label(f"  Average dependencies per component: {avg_deps:.1f}", classes="info-row"))

        except Exception as exc:
            logger.exception("Failed to update analysis tab: %s", exc)

    def on_unmount(self) -> None:
        """Cancel loading worker when panel is unmounted."""
        if self._worker is not None and not self._worker.is_finished:
            self._worker.cancel()


class QueryLineageScreen(VimNavigableScreen[PipelineComponent]):
    """Query lineage and data flow visualization screen.

    Provides an interactive view of the ETL pipeline showing how data
    flows from sources through queries to outputs. Supports navigation
    through pipeline components and dependency analysis.

    VIM navigation:
        j/k         - Move between components
        Enter/l     - View component details
        gg/G        - Jump to first/last
        h/Escape    - Back to overview
        /           - Search components
        Tab         - Filter by component type
    """

    CSS = """
    QueryLineageScreen {
        layout: vertical;
    }

    #pipeline-summary {
        dock: bottom;
        height: 1;
        width: 100%;
        padding: 0 2;
        color: #9ece6a;
    }
    """

    class DrillDown(Message):
        """Request to drill down into a specific component."""

        def __init__(self, component: PipelineComponent) -> None:
            super().__init__()
            self.component = component

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(config_manager=config_manager, **kwargs)
        self._lineage_analysis: LineageAnalysis | None = None
        self._filter_type: str | None = None  # "source", "query", "output", or None for all
        self._all_components: list[PipelineComponent] = []

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Query Lineage & Data Flow"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Query Lineage"

    @property
    def footer_hints(self) -> str:
        filter_hint = f" [{self._filter_type}]" if self._filter_type else " [all]"
        return f" j/k:navigate  Enter:details  Tab:filter{filter_hint}  Shift+Tab:switch tab  h:back  /:search"

    @property
    def empty_list_message(self) -> str:
        return "No pipeline components found.\nAdd data sources and queries first."

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        """Add Tab key for filtering and Shift+Tab for tab switching."""
        return frozenset({"tab", "shift+tab"})

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[PipelineComponent]:
        config = self.config_manager.get_config()
        self._lineage_analysis = _analyze_pipeline_lineage(config)
        self._all_components = self._lineage_analysis.components

        # Apply current filter
        return self._apply_filter(self._all_components)

    def _apply_filter(self, components: list[PipelineComponent]) -> list[PipelineComponent]:
        """Apply current component type filter."""
        if self._filter_type is None:
            return components
        return [c for c in components if c.component_type == self._filter_type]

    def create_list_item(self, item: PipelineComponent, item_id: str) -> BaseListItem:
        return LineageComponentWidget(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return LineageDetailPanel(config_manager=self.config_manager, id=self.detail_panel_id)

    def update_detail_panel(self, item: PipelineComponent | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", LineageDetailPanel)
            detail.update_component(item, self._lineage_analysis)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: PipelineComponent) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "-", item.component_id)

    def get_item_search_text(self, item: PipelineComponent) -> str:
        return f"{item.display_name} {item.component_type} {item.description or ''}"

    def on_edit(self, item: PipelineComponent) -> None:
        self.post_message(self.DrillDown(item))

    def on_add(self) -> None:
        pass  # read-only screen

    async def on_delete(self, item: PipelineComponent) -> None:
        pass  # read-only screen

    def handle_extra_key(self, key: str) -> bool:
        """Handle Tab key for filtering and Shift+Tab for tab switching."""
        if key == "tab":
            self._cycle_filter()
            return True
        if key == "shift+tab":
            try:
                detail = self.query_one(f"#{self.detail_panel_id}", LineageDetailPanel)
                tabs_widget = detail.query_one(Tabs)
                tabs_widget.action_next_tab()
                return True
            except (NoMatches, AttributeError):
                logger.debug("handle_extra_key: could not find tabs widget")
                return False
        return False

    def _cycle_filter(self) -> None:
        """Cycle through component type filters."""
        filters = [None, "source", "query", "output"]
        try:
            current_index = filters.index(self._filter_type)
            next_index = (current_index + 1) % len(filters)
            self._filter_type = filters[next_index]
        except ValueError:
            self._filter_type = None

        # Refresh the list with new filter
        self.run_worker(self._refresh_with_filter(), exclusive=True)

    async def _refresh_with_filter(self) -> None:
        """Refresh the component list with current filter."""
        # Update items with filter applied
        self._items = self._apply_filter(self._all_components)

        # Reset cursor if needed
        if self._cursor >= self.item_count:
            self._cursor = max(0, self.item_count - 1)

        # Re-render the list
        await self._render_list()
        self.update_detail_panel(self.current_item)

        # Update footer to show new filter
        try:
            footer = self.query_one("#screen-footer", Static)
            footer.update(self.footer_hints)
        except NoMatches:
            pass

    # --- Layout override: add pipeline summary ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_pipeline_summary)

    def _mount_pipeline_summary(self) -> None:
        """Mount pipeline summary widget after initial compose."""
        try:
            self.query_one("#pipeline-summary")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer")

                if self._lineage_analysis:
                    source_count = sum(1 for c in self._lineage_analysis.components if c.component_type == "source")
                    query_count = sum(1 for c in self._lineage_analysis.components if c.component_type == "query")
                    output_count = sum(1 for c in self._lineage_analysis.components if c.component_type == "output")

                    summary = (
                        f" {source_count} source{'s' if source_count != 1 else ''}"
                        f"  {query_count} quer{'ies' if query_count != 1 else 'y'}"
                        f"  {output_count} output{'s' if output_count != 1 else ''}"
                    )

                    if self._lineage_analysis.critical_path:
                        summary += f"  •  Critical path: {len(self._lineage_analysis.critical_path)} steps"
                else:
                    summary = " Pipeline analysis in progress..."

                self.mount(
                    Label(summary, id="pipeline-summary"),
                    before=footer,
                )
            except (NoMatches, AttributeError):
                logger.debug("_mount_pipeline_summary: #screen-footer not found")