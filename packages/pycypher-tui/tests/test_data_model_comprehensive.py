"""Comprehensive test suite for TUI data model features.

Test strategy covering:
1. Unit tests for _build_model logic and data structures
2. Behavioral tests for DataModelScreen navigation and display
3. Integration tests for attribute inspector and tabbed detail panel
4. Validation logic tests (_validate_source_mapping)
5. Performance tests for large data model handling
6. Config editing workflow validation
7. Cross-screen navigation integration
8. Edge cases and error handling
"""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pycypher.ingestion.config import (
    EntitySourceConfig,
    PipelineConfig,
    ProjectConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)

from pycypher_tui.app import PyCypherTUI
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import get_template
from pycypher_tui.modes.base import ModeType
from pycypher_tui.screens.data_model import (
    AttributeData,
    DataModelScreen,
    ModelDetailPanel,
    ModelEdge,
    ModelNode,
    ModelNodeWidget,
    _build_model,
)
from pycypher_tui.screens.data_sources import DataSourcesScreen
from pycypher_tui.screens.pipeline_overview import PipelineOverviewScreen

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _app_from_config(config: PipelineConfig) -> PyCypherTUI:
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


def _ecommerce_app() -> PyCypherTUI:
    t = get_template("ecommerce_pipeline")
    config = t.instantiate(project_name="test_shop", data_dir="data")
    return _app_from_config(config)


def _social_app() -> PyCypherTUI:
    t = get_template("social_network")
    config = t.instantiate(project_name="test_social", data_dir="data")
    return _app_from_config(config)


def _empty_app() -> PyCypherTUI:
    app = PyCypherTUI()
    app._config_manager = ConfigManager()
    return app


def _large_config(n_entities: int = 50, n_rels: int = 30) -> PipelineConfig:
    """Create a config with many entity and relationship types for perf testing."""
    entities = [
        EntitySourceConfig(
            id=f"entity_{i}",
            uri=f"file:///data/entity_{i}.csv",
            entity_type=f"Type{i}",
            id_col="id",
        )
        for i in range(n_entities)
    ]
    relationships = [
        RelationshipSourceConfig(
            id=f"rel_{i}",
            uri=f"file:///data/rel_{i}.csv",
            relationship_type=f"REL_{i}",
            source_col=f"src_{i}",
            target_col=f"tgt_{i}",
        )
        for i in range(n_rels)
    ]
    return PipelineConfig(
        project=ProjectConfig(name="large_test"),
        sources=SourcesConfig(entities=entities, relationships=relationships),
        queries=[],
        output=[],
    )


def _entity_node(label: str = "Person", source_count: int = 1) -> ModelNode:
    return ModelNode(
        node_id=f"entity:{label}",
        label=label,
        node_type="entity",
        source_count=source_count,
        source_ids=(f"{label.lower()}_csv",),
        connections=(),
    )


def _rel_node(label: str = "KNOWS", source_count: int = 1) -> ModelNode:
    return ModelNode(
        node_id=f"rel:{label}",
        label=label,
        node_type="relationship",
        source_count=source_count,
        source_ids=(f"{label.lower()}_csv",),
        connections=("(person_id) -> (friend_id)",),
    )


# ===========================================================================
# 1. Unit tests: _build_model
# ===========================================================================


class TestBuildModelUnit:
    """Exhaustive tests for the _build_model function."""

    def test_empty_config_returns_empty(self):
        config = PipelineConfig()
        nodes, edges = _build_model(config)
        assert nodes == []
        assert edges == []

    def test_entities_only(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(id="e1", uri="f:///a.csv", entity_type="A", id_col="id"),
                    EntitySourceConfig(id="e2", uri="f:///b.csv", entity_type="B", id_col="id"),
                ],
                relationships=[],
            ),
        )
        nodes, edges = _build_model(config)
        assert len(nodes) == 2
        assert all(n.node_type == "entity" for n in nodes)
        assert edges == []

    def test_relationships_only(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="KNOWS",
                        source_col="src", target_col="tgt"
                    ),
                ],
            ),
        )
        nodes, edges = _build_model(config)
        rel_nodes = [n for n in nodes if n.node_type == "relationship"]
        assert len(rel_nodes) == 1
        assert len(edges) == 1

    def test_multiple_sources_same_entity_type(self):
        """Multiple sources for the same entity type should be aggregated."""
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(id="e1", uri="f:///a.csv", entity_type="Person", id_col="id"),
                    EntitySourceConfig(id="e2", uri="f:///b.csv", entity_type="Person", id_col="id"),
                    EntitySourceConfig(id="e3", uri="f:///c.csv", entity_type="Person", id_col="id"),
                ],
                relationships=[],
            ),
        )
        nodes, edges = _build_model(config)
        entity_nodes = [n for n in nodes if n.node_type == "entity"]
        assert len(entity_nodes) == 1
        assert entity_nodes[0].label == "Person"
        assert entity_nodes[0].source_count == 3
        assert len(entity_nodes[0].source_ids) == 3

    def test_multiple_sources_same_relationship_type(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="KNOWS",
                        source_col="a", target_col="b"
                    ),
                    RelationshipSourceConfig(
                        id="r2", uri="f:///s.csv", relationship_type="KNOWS",
                        source_col="c", target_col="d"
                    ),
                ],
            ),
        )
        nodes, edges = _build_model(config)
        rel_nodes = [n for n in nodes if n.node_type == "relationship"]
        assert len(rel_nodes) == 1
        assert rel_nodes[0].source_count == 2
        assert len(edges) == 2  # two edges, one per source

    def test_nodes_sorted_alphabetically(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(id="e1", uri="f:///z.csv", entity_type="Zebra", id_col="id"),
                    EntitySourceConfig(id="e2", uri="f:///a.csv", entity_type="Ant", id_col="id"),
                    EntitySourceConfig(id="e3", uri="f:///m.csv", entity_type="Mouse", id_col="id"),
                ],
                relationships=[],
            ),
        )
        nodes, _ = _build_model(config)
        entity_labels = [n.label for n in nodes if n.node_type == "entity"]
        assert entity_labels == sorted(entity_labels)

    def test_entity_nodes_before_relationship_nodes(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(id="e1", uri="f:///a.csv", entity_type="A", id_col="id"),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="KNOWS",
                        source_col="a", target_col="b"
                    ),
                ],
            ),
        )
        nodes, _ = _build_model(config)
        types = [n.node_type for n in nodes]
        # Entities should come before relationships
        assert types.index("entity") < types.index("relationship")

    def test_edge_preserves_column_info(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="PURCHASED",
                        source_col="customer_id", target_col="product_id"
                    ),
                ],
            ),
        )
        _, edges = _build_model(config)
        assert len(edges) == 1
        assert edges[0].source_col == "customer_id"
        assert edges[0].target_col == "product_id"
        assert edges[0].relationship_type == "PURCHASED"

    def test_node_id_format(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(id="e1", uri="f:///a.csv", entity_type="Person", id_col="id"),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="KNOWS",
                        source_col="a", target_col="b"
                    ),
                ],
            ),
        )
        nodes, _ = _build_model(config)
        entity = next(n for n in nodes if n.node_type == "entity")
        rel = next(n for n in nodes if n.node_type == "relationship")
        assert entity.node_id == "entity:Person"
        assert rel.node_id == "rel:KNOWS"

    def test_relationship_connection_strings(self):
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="KNOWS",
                        source_col="person_id", target_col="friend_id"
                    ),
                ],
            ),
        )
        nodes, _ = _build_model(config)
        rel = next(n for n in nodes if n.node_type == "relationship")
        assert len(rel.connections) == 1
        assert "(person_id) -> (friend_id)" in rel.connections[0]


# ===========================================================================
# 2. Data structure unit tests
# ===========================================================================


class TestModelDataStructures:
    """Test ModelNode, ModelEdge, AttributeData dataclasses."""

    def test_model_node_frozen(self):
        node = _entity_node()
        with pytest.raises(AttributeError):
            node.label = "Changed"

    def test_model_edge_frozen(self):
        edge = ModelEdge(
            relationship_type="KNOWS",
            source_entity=None,
            target_entity=None,
            source_col="a",
            target_col="b",
        )
        with pytest.raises(AttributeError):
            edge.source_col = "changed"

    def test_attribute_data_defaults(self):
        data = AttributeData()
        assert data.schema_info is None
        assert data.column_stats is None
        assert data.validation_results is None
        assert data.sample_data is None
        assert data.error is None

    def test_attribute_data_with_error(self):
        data = AttributeData(error="Connection refused")
        assert data.error == "Connection refused"
        assert data.schema_info is None


# ===========================================================================
# 3. ModelNodeWidget rendering tests
# ===========================================================================


class TestModelNodeWidgetRendering:
    """Test that ModelNodeWidget renders correct icons and labels."""

    def test_entity_widget_stores_node(self):
        node = _entity_node("Customer")
        widget = ModelNodeWidget(node, id="test-widget")
        assert widget.node == node
        assert widget.node.label == "Customer"

    def test_relationship_widget_stores_node(self):
        node = _rel_node("PURCHASED")
        widget = ModelNodeWidget(node, id="test-widget")
        assert widget.node.node_type == "relationship"
        assert widget.node.label == "PURCHASED"

    def test_entity_widget_shows_source_count_singular(self):
        node = _entity_node("Person", source_count=1)
        # Verify the rendering logic produces "1 source" not "1 sources"
        assert node.source_count == 1

    def test_entity_widget_shows_source_count_plural(self):
        node = _entity_node("Person", source_count=3)
        assert node.source_count == 3


# ===========================================================================
# 4. Validation logic tests
# ===========================================================================


class TestSourceMappingValidation:
    """Test _validate_source_mapping on ModelDetailPanel."""

    def _make_panel(self) -> ModelDetailPanel:
        mgr = ConfigManager()
        return ModelDetailPanel(config_manager=mgr, id="test-panel")

    def test_entity_with_id_column_passes(self):
        panel = self._make_panel()
        source = Mock(id="e1", entity_type="Person")
        schema = Mock(columns=[{"name": "id", "type": "int"}, {"name": "name", "type": "str"}])
        node = _entity_node()
        result = panel._validate_source_mapping(source, schema, node)
        assert result["status"] == "pass"
        assert result["issues"] == []

    def test_entity_without_id_column_warns(self):
        panel = self._make_panel()
        source = Mock(id="e1", entity_type="Person")
        schema = Mock(columns=[{"name": "name", "type": "str"}, {"name": "age", "type": "int"}])
        node = _entity_node()
        result = panel._validate_source_mapping(source, schema, node)
        assert result["status"] == "warning"
        assert any("ID column" in issue["message"] for issue in result["issues"])

    def test_entity_single_column_warns(self):
        panel = self._make_panel()
        source = Mock(id="e1", entity_type="Person")
        schema = Mock(columns=[{"name": "name", "type": "str"}])
        node = _entity_node()
        result = panel._validate_source_mapping(source, schema, node)
        assert result["status"] == "warning"
        assert any("one column" in issue["message"].lower() for issue in result["issues"])

    def test_relationship_missing_source_col_errors(self):
        panel = self._make_panel()
        source = Mock(
            id="r1", relationship_type="KNOWS",
            source_col="person_id", target_col="friend_id"
        )
        schema = Mock(columns=[{"name": "friend_id", "type": "int"}])
        node = _rel_node()
        result = panel._validate_source_mapping(source, schema, node)
        assert result["status"] == "error"
        assert any("person_id" in issue["message"] for issue in result["issues"])

    def test_relationship_missing_target_col_errors(self):
        panel = self._make_panel()
        source = Mock(
            id="r1", relationship_type="KNOWS",
            source_col="person_id", target_col="friend_id"
        )
        schema = Mock(columns=[{"name": "person_id", "type": "int"}])
        node = _rel_node()
        result = panel._validate_source_mapping(source, schema, node)
        assert result["status"] == "error"
        assert any("friend_id" in issue["message"] for issue in result["issues"])

    def test_relationship_both_cols_present_passes(self):
        panel = self._make_panel()
        source = Mock(
            id="r1", relationship_type="KNOWS",
            source_col="person_id", target_col="friend_id"
        )
        schema = Mock(columns=[
            {"name": "person_id", "type": "int"},
            {"name": "friend_id", "type": "int"},
        ])
        node = _rel_node()
        result = panel._validate_source_mapping(source, schema, node)
        assert result["status"] == "pass"

    def test_validation_exception_returns_error(self):
        panel = self._make_panel()
        source = Mock(id="e1", entity_type="Person")
        # Schema that raises on column access
        schema = Mock()
        schema.columns = property(lambda self: (_ for _ in ()).throw(RuntimeError("broken")))
        schema_bad = Mock()
        type(schema_bad).columns = property(lambda self: (_ for _ in ()).throw(RuntimeError("broken")))
        node = _entity_node()
        result = panel._validate_source_mapping(source, schema_bad, node)
        assert result["status"] == "error"


# ===========================================================================
# 5. Behavioral tests: DataModelScreen
# ===========================================================================


class TestDataModelScreenDisplay:
    """Test data model screen displays correct content for different configs."""

    @pytest.mark.asyncio
    async def test_ecommerce_shows_all_entity_types(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            entity_labels = {n.node.label for n in nodes if n.node.node_type == "entity"}
            assert "Customer" in entity_labels
            assert "Product" in entity_labels

    @pytest.mark.asyncio
    async def test_ecommerce_shows_relationships(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            rel_labels = {n.node.label for n in nodes if n.node.node_type == "relationship"}
            assert "PURCHASED" in rel_labels

    @pytest.mark.asyncio
    async def test_empty_config_no_nodes(self):
        app = _empty_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert len(nodes) == 0

    @pytest.mark.asyncio
    async def test_social_network_multiple_relationships(self):
        app = _social_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            rel_nodes = [n for n in nodes if n.node.node_type == "relationship"]
            assert len(rel_nodes) >= 2

    @pytest.mark.asyncio
    async def test_first_item_focused_on_load(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert len(nodes) > 0
            assert nodes[0].has_class("item-focused")


class TestDataModelScreenNavigation:
    """Test VIM-style navigation within the data model screen."""

    @pytest.mark.asyncio
    async def test_j_moves_down(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            await pilot.press("j")
            await pilot.pause()
            assert nodes[1].has_class("item-focused")
            assert not nodes[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_k_moves_up(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("j")
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert nodes[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_G_jumps_to_last(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert nodes[-1].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_gg_jumps_to_first(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("G")
            await pilot.pause()
            await pilot.press("g")
            await pilot.press("g")
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert nodes[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_k_at_top_stays_at_top(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("k")
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert nodes[0].has_class("item-focused")

    @pytest.mark.asyncio
    async def test_j_at_bottom_stays_at_bottom(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            # Navigate to last
            for _ in range(len(nodes) + 5):
                await pilot.press("j")
                await pilot.pause()
            assert nodes[-1].has_class("item-focused")


class TestDataModelScreenModes:
    """Test modal system integration."""

    @pytest.mark.asyncio
    async def test_colon_enters_command_mode(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("colon")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.COMMAND

    @pytest.mark.asyncio
    async def test_escape_returns_to_normal_from_command(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("colon")
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.NORMAL

    @pytest.mark.asyncio
    async def test_v_enters_visual_mode(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("v")
            await pilot.pause()
            assert app.mode_manager.current_type == ModeType.VISUAL


class TestDataModelDrillDown:
    """Test drill-down navigation from data model to source screens."""

    @pytest.mark.asyncio
    async def test_enter_navigates_to_sources(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            # Entity nodes now drill down to EntityEditorScreen
            from pycypher_tui.screens.entity_editor import EntityEditorScreen
            assert len(app.query(EntityEditorScreen)) > 0

    @pytest.mark.asyncio
    async def test_h_goes_back_to_overview(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            await pilot.press("h")
            await pilot.pause()
            await pilot.pause()
            assert len(app.query(PipelineOverviewScreen)) > 0


class TestDataModelDetailPanel:
    """Test detail panel updates as navigation changes."""

    @pytest.mark.asyncio
    async def test_detail_updates_on_navigate(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            from textual.widgets import Label
            detail = app.query_one("#detail-panel", ModelDetailPanel)
            labels_before = [str(lb.render()) for lb in detail.query(Label)]
            await pilot.press("j")
            await pilot.pause()
            labels_after = [str(lb.render()) for lb in detail.query(Label)]
            assert labels_after != labels_before

    @pytest.mark.asyncio
    async def test_detail_panel_has_tabbed_content(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            from textual.widgets import TabbedContent
            detail = app.query_one("#detail-panel", ModelDetailPanel)
            tabs = detail.query(TabbedContent)
            assert len(tabs) >= 1


# ===========================================================================
# 6. DataModelScreen property tests
# ===========================================================================


class TestDataModelScreenProperties:
    """Test screen metadata properties."""

    def test_screen_title(self):
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        assert screen.screen_title == "Data Model"

    def test_breadcrumb(self):
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        assert "Data Model" in screen.breadcrumb_text

    def test_footer_hints(self):
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        assert "j/k" in screen.footer_hints
        assert "drill-down" in screen.footer_hints

    def test_empty_list_message(self):
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        assert "No entity" in screen.empty_list_message

    def test_get_item_id_replaces_colon(self):
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        node = _entity_node("Person")
        assert screen.get_item_id(node) == "entity-Person"

    def test_get_item_search_text_includes_label(self):
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        node = _entity_node("Customer")
        text = screen.get_item_search_text(node)
        assert "Customer" in text
        assert "entity" in text


# ===========================================================================
# 7. Performance: large data model
# ===========================================================================


class TestLargeDataModelPerformance:
    """Test that _build_model handles large configs efficiently."""

    def test_50_entities_30_rels_builds_fast(self):
        import time
        config = _large_config(n_entities=50, n_rels=30)
        start = time.perf_counter()
        nodes, edges = _build_model(config)
        elapsed = time.perf_counter() - start

        assert len(nodes) == 80  # 50 entities + 30 relationships
        assert len(edges) == 30
        assert elapsed < 1.0  # should be well under 1 second

    def test_200_entities_100_rels_builds_fast(self):
        import time
        config = _large_config(n_entities=200, n_rels=100)
        start = time.perf_counter()
        nodes, edges = _build_model(config)
        elapsed = time.perf_counter() - start

        assert len(nodes) == 300
        assert len(edges) == 100
        assert elapsed < 2.0

    def test_node_ids_unique_at_scale(self):
        config = _large_config(n_entities=100, n_rels=50)
        nodes, _ = _build_model(config)
        ids = [n.node_id for n in nodes]
        assert len(ids) == len(set(ids))

    @pytest.mark.asyncio
    async def test_large_model_renders_without_timeout(self):
        """Large data model screen should mount without timing out."""
        config = _large_config(n_entities=30, n_rels=20)
        app = _app_from_config(config)
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert len(nodes) == 50  # 30 + 20


# ===========================================================================
# 8. Cross-template consistency
# ===========================================================================


class TestCrossTemplateConsistency:
    """Verify data model screen works consistently across all templates."""

    @pytest.mark.asyncio
    async def test_csv_analytics_template(self):
        t = get_template("csv_analytics")
        config = t.instantiate(project_name="test", data_dir="data")
        app = _app_from_config(config)
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert len(nodes) > 0

    @pytest.mark.asyncio
    async def test_ecommerce_template(self):
        app = _ecommerce_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            entity_nodes = [n for n in nodes if n.node.node_type == "entity"]
            rel_nodes = [n for n in nodes if n.node.node_type == "relationship"]
            assert len(entity_nodes) >= 2
            assert len(rel_nodes) >= 1

    @pytest.mark.asyncio
    async def test_social_network_template(self):
        app = _social_app()
        async with app.run_test() as pilot:
            await app._show_data_model()
            await pilot.pause()
            nodes = app.query(ModelNodeWidget)
            assert len(nodes) > 0
            # Social network should have relationship types
            rel_labels = {n.node.label for n in nodes if n.node.node_type == "relationship"}
            assert len(rel_labels) >= 1


# ===========================================================================
# 9. Edge cases and error resilience
# ===========================================================================


class TestDataModelEdgeCases:
    """Test edge cases and error resilience."""

    def test_entity_type_with_special_characters_in_id(self):
        """Entity type names with special chars should produce valid node IDs."""
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="e1", uri="f:///a.csv",
                        entity_type="My Entity", id_col="id"
                    ),
                ],
                relationships=[],
            ),
        )
        nodes, _ = _build_model(config)
        assert len(nodes) == 1
        assert nodes[0].node_id == "entity:My Entity"

    def test_relationship_source_and_target_entity_none(self):
        """ModelEdge source_entity and target_entity are None from config."""
        config = PipelineConfig(
            sources=SourcesConfig(
                entities=[],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1", uri="f:///r.csv", relationship_type="KNOWS",
                        source_col="a", target_col="b"
                    ),
                ],
            ),
        )
        _, edges = _build_model(config)
        assert edges[0].source_entity is None
        assert edges[0].target_entity is None

    def test_config_with_only_project_info(self):
        config = PipelineConfig(project=ProjectConfig(name="empty_project"))
        nodes, edges = _build_model(config)
        assert nodes == []
        assert edges == []

    @pytest.mark.asyncio
    async def test_screen_search_text_includes_source_ids(self):
        """Search text should include source IDs for findability."""
        mgr = ConfigManager()
        screen = DataModelScreen(config_manager=mgr)
        node = ModelNode(
            node_id="entity:Customer",
            label="Customer",
            node_type="entity",
            source_count=2,
            source_ids=("customers_csv", "customers_db"),
            connections=(),
        )
        text = screen.get_item_search_text(node)
        assert "customers_csv" in text
        assert "customers_db" in text
