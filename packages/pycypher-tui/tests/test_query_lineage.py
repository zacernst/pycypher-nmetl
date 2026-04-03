"""Tests for query lineage and data flow visualization screen."""

import pytest
from unittest.mock import Mock

from pycypher.ingestion.config import PipelineConfig, SourcesConfig, EntitySourceConfig, RelationshipSourceConfig, QueryConfig, OutputConfig
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.query_lineage import (
    QueryLineageScreen,
    _analyze_pipeline_lineage,
    _generate_flow_diagram,
    _find_critical_path,
    PipelineComponent
)


@pytest.fixture
def sample_config():
    """Sample pipeline configuration for testing."""
    return PipelineConfig(
        version="1.0",
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="users",
                    uri="data/users.csv",
                    entity_type="Person",
                    id_col="user_id"
                ),
                EntitySourceConfig(
                    id="products",
                    uri="data/products.csv",
                    entity_type="Product",
                    id_col="product_id"
                )
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="purchases",
                    uri="data/purchases.csv",
                    relationship_type="PURCHASED",
                    source_col="user_id",
                    target_col="product_id"
                )
            ]
        ),
        queries=[
            QueryConfig(
                id="user_stats",
                description="User purchase statistics",
                inline="MATCH (p:Person)-[r:PURCHASED]->(prod:Product) RETURN p.user_id, COUNT(r) as purchases"
            )
        ],
        output=[
            OutputConfig(
                query_id="user_stats",
                uri="output/user_stats.parquet",
                format="parquet"
            )
        ]
    )


@pytest.fixture
def config_manager(sample_config):
    """ConfigManager with sample configuration."""
    return ConfigManager.from_config(sample_config)


def test_analyze_pipeline_lineage(sample_config):
    """Test pipeline lineage analysis."""
    analysis = _analyze_pipeline_lineage(sample_config)

    assert analysis.error is None
    assert len(analysis.components) == 5  # 2 entities + 1 relationship + 1 query + 1 output

    # Check component types
    component_types = [c.component_type for c in analysis.components]
    assert component_types.count("source") == 3  # 2 entities + 1 relationship
    assert component_types.count("query") == 1
    assert component_types.count("output") == 1

    # Check dependency graph structure
    assert len(analysis.dependency_graph) == 5

    # Output should depend on query
    output_comp = next(c for c in analysis.components if c.component_type == "output")
    assert len(output_comp.dependencies) == 1
    assert output_comp.dependencies[0].startswith("query:")


def test_generate_flow_diagram(sample_config):
    """Test flow diagram generation."""
    analysis = _analyze_pipeline_lineage(sample_config)
    diagram = _generate_flow_diagram(analysis.components, analysis.dependency_graph)

    assert "ETL Pipeline Data Flow:" in diagram
    assert "DATA SOURCES" in diagram
    assert "TRANSFORMATIONS" in diagram
    assert "OUTPUTS" in diagram
    assert "Person" in diagram
    assert "Product" in diagram
    assert "PURCHASED" in diagram


def test_find_critical_path():
    """Test critical path finding."""
    dependency_graph = {
        "source1": [],
        "source2": [],
        "query1": ["source1", "source2"],
        "query2": ["query1"],
        "output1": ["query2"]
    }

    critical_path = _find_critical_path(dependency_graph)

    # Should find the longest path
    assert len(critical_path) >= 3
    assert critical_path[0] in ["source1", "source2"]  # Starts with a source
    assert critical_path[-1] == "output1"  # Ends with output


def test_query_lineage_screen_initialization(config_manager):
    """Test QueryLineageScreen can be initialized."""
    screen = QueryLineageScreen(config_manager=config_manager)

    assert screen.screen_title == "Query Lineage & Data Flow"
    assert "Pipeline > Query Lineage" in screen.breadcrumb_text
    assert "navigate" in screen.footer_hints


def test_query_lineage_screen_load_items(config_manager):
    """Test loading items in QueryLineageScreen."""
    screen = QueryLineageScreen(config_manager=config_manager)

    items = screen.load_items()

    assert len(items) == 5  # 2 entities + 1 relationship + 1 query + 1 output
    assert screen._lineage_analysis is not None
    assert len(screen._lineage_analysis.components) == 5


def test_pipeline_component_filtering(config_manager):
    """Test component type filtering."""
    screen = QueryLineageScreen(config_manager=config_manager)
    screen.load_items()

    # Test source filter
    screen._filter_type = "source"
    filtered = screen._apply_filter(screen._all_components)
    assert all(c.component_type == "source" for c in filtered)
    assert len(filtered) == 3  # 2 entities + 1 relationship

    # Test query filter
    screen._filter_type = "query"
    filtered = screen._apply_filter(screen._all_components)
    assert all(c.component_type == "query" for c in filtered)
    assert len(filtered) == 1

    # Test output filter
    screen._filter_type = "output"
    filtered = screen._apply_filter(screen._all_components)
    assert all(c.component_type == "output" for c in filtered)
    assert len(filtered) == 1

    # Test no filter (all components)
    screen._filter_type = None
    filtered = screen._apply_filter(screen._all_components)
    assert len(filtered) == 5


def test_pipeline_component_search_text(config_manager):
    """Test search text generation for components."""
    screen = QueryLineageScreen(config_manager=config_manager)
    items = screen.load_items()

    # Find a component and test search text
    entity_comp = next(c for c in items if c.component_type == "source" and "Entity:" in c.display_name)
    search_text = screen.get_item_search_text(entity_comp)

    assert "Entity:" in search_text
    assert "source" in search_text
    assert "Person" in search_text or "Product" in search_text


def test_empty_pipeline_analysis():
    """Test analysis with empty pipeline."""
    empty_config = PipelineConfig(version="1.0", sources=SourcesConfig(entities=[], relationships=[]), queries=[], output=[])

    analysis = _analyze_pipeline_lineage(empty_config)

    assert analysis.error is None
    assert len(analysis.components) == 0
    assert analysis.flow_diagram == "No pipeline components found"
    assert len(analysis.critical_path) == 0
    assert len(analysis.orphaned_components) == 0


def test_component_metadata_extraction(sample_config):
    """Test that component metadata is properly extracted."""
    analysis = _analyze_pipeline_lineage(sample_config)

    # Check entity component metadata
    entity_comp = next(c for c in analysis.components if c.display_name == "Entity: Person")
    assert entity_comp.metadata["source_type"] == "entity"
    assert entity_comp.metadata["entity_type"] == "Person"
    assert entity_comp.metadata["uri"] == "data/users.csv"
    assert entity_comp.metadata["id_col"] == "user_id"

    # Check relationship component metadata
    rel_comp = next(c for c in analysis.components if c.display_name == "Relationship: PURCHASED")
    assert rel_comp.metadata["source_type"] == "relationship"
    assert rel_comp.metadata["relationship_type"] == "PURCHASED"
    assert rel_comp.metadata["source_col"] == "user_id"
    assert rel_comp.metadata["target_col"] == "product_id"

    # Check query component metadata
    query_comp = next(c for c in analysis.components if c.display_name == "Query: user_stats")
    assert query_comp.metadata["query_id"] == "user_stats"
    assert query_comp.metadata["description"] == "User purchase statistics"
    assert "Person" in query_comp.metadata["inline"]

    # Check output component metadata
    output_comp = next(c for c in analysis.components if c.display_name.startswith("Output:"))
    assert output_comp.metadata["query_id"] == "user_stats"
    assert output_comp.metadata["format"] == "parquet"
    assert "user_stats.parquet" in output_comp.metadata["uri"]


def test_dependency_relationships(sample_config):
    """Test that dependencies and dependents are correctly calculated."""
    analysis = _analyze_pipeline_lineage(sample_config)

    # Query should have dependencies on sources (if inference works)
    query_comp = next(c for c in analysis.components if c.component_type == "query")
    # Note: dependency inference is basic and may not catch all relationships

    # Output should depend on query
    output_comp = next(c for c in analysis.components if c.component_type == "output")
    assert len(output_comp.dependencies) == 1
    assert output_comp.dependencies[0] == "query:user_stats"

    # Query should have output as dependent
    query_comp = next(c for c in analysis.components if c.component_id == "query:user_stats")
    assert len(query_comp.dependents) == 1
    assert query_comp.dependents[0].startswith("output:")