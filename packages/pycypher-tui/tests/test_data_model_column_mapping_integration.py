"""Integration tests for column mapping in data model screen."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pycypher.ingestion.config import (
    PipelineConfig,
    ProjectConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.data_model import (
    DataModelScreen,
    ModelDetailPanel,
    ModelNode,
)


@pytest.fixture
def mock_config_manager():
    """Create a mock config manager with relationship sources."""
    config = PipelineConfig(
        project=ProjectConfig(name="test_project"),
        sources=SourcesConfig(
            entities=[],
            relationships=[
                RelationshipSourceConfig(
                    id="test_rel_1",
                    uri="data/relationships.csv",
                    relationship_type="KNOWS",
                    source_col="person_id",
                    target_col="friend_id"
                ),
                RelationshipSourceConfig(
                    id="test_rel_2",
                    uri="data/works_for.csv",
                    relationship_type="WORKS_FOR",
                    source_col="employee_id",
                    target_col="company_id"
                )
            ]
        ),
        queries=[],
        output=[]
    )

    manager = Mock(spec=ConfigManager)
    manager.get_config.return_value = config
    manager.update_relationship_source = Mock()
    return manager


@pytest.fixture
def relationship_node():
    """Create a relationship model node for testing."""
    return ModelNode(
        node_id="rel:KNOWS",
        label="KNOWS",
        node_type="relationship",
        source_count=1,
        source_ids=("test_rel_1",),
        connections=("(person_id) -> (friend_id)",)
    )


class TestDataModelColumnMappingIntegration:
    """Test integration of column mapping with data model screen."""

    def test_model_detail_panel_creation(self, mock_config_manager):
        """Test creating ModelDetailPanel with column mapping capability."""
        panel = ModelDetailPanel(config_manager=mock_config_manager)
        assert panel is not None
        assert panel.config_manager == mock_config_manager

    def test_relationship_validation_tab_setup(self, mock_config_manager, relationship_node):
        """Test that relationship nodes get column mapping validation."""
        panel = ModelDetailPanel(config_manager=mock_config_manager)

        # Mock the tab mounting and widget creation
        with patch.object(panel, "query_one") as mock_query, \
             patch.object(panel, "run_worker") as mock_worker:

            # Mock tab object
            mock_tab = Mock()
            mock_tab.mount = Mock()
            mock_tab.remove_children = Mock()
            mock_query.return_value = mock_tab

            # Set current node and call the validation tab update
            panel._current_node = relationship_node
            panel._attribute_data = Mock()
            panel._attribute_data.validation_results = {"test": "data"}

            # This should trigger the relationship validation path
            panel._update_validation_tab()

            # Verify that tab.mount was called (should mount ColumnMappingWidget)
            assert mock_tab.mount.called
            assert mock_worker.called

    def test_mapping_change_handling(self, mock_config_manager, relationship_node):
        """Test handling of column mapping changes."""
        panel = ModelDetailPanel(config_manager=mock_config_manager)
        panel._current_node = relationship_node

        # Set up mock relationship sources
        config = mock_config_manager.get_config()
        panel._current_relationship_sources = config.sources.relationships

        # Create a mock mapping change message
        mock_message = Mock()
        mock_message.new_mapping = {
            "source_col": "new_person_id",
            "target_col": "new_friend_id"
        }

        # Call the mapping change handler
        panel.on_column_mapping_widget_mapping_changed(mock_message)

        # Verify that config manager was called to update the relationship source
        mock_config_manager.update_relationship_source.assert_called_once_with(
            "test_rel_1",
            source_col="new_person_id",
            target_col="new_friend_id"
        )

    def test_data_model_screen_integration(self, mock_config_manager):
        """Test that DataModelScreen properly integrates with column mapping."""
        # This is a basic integration test - in a full test environment we would
        # test the complete UI interaction
        screen = DataModelScreen(config_manager=mock_config_manager)
        assert screen.config_manager == mock_config_manager

        # Test that the screen can load items (including relationships)
        items = screen.load_items()
        assert len(items) == 2  # Two relationship types from mock config

        # Verify relationship nodes are created correctly
        relationship_nodes = [item for item in items if item.node_type == "relationship"]
        assert len(relationship_nodes) == 2
        assert any(node.label == "KNOWS" for node in relationship_nodes)
        assert any(node.label == "WORKS_FOR" for node in relationship_nodes)

    def test_detail_panel_creation_and_update(self, mock_config_manager, relationship_node):
        """Test detail panel creation and node updates."""
        screen = DataModelScreen(config_manager=mock_config_manager)
        detail_panel = screen.create_detail_panel()

        assert isinstance(detail_panel, ModelDetailPanel)
        assert detail_panel.config_manager == mock_config_manager

        # Test updating with a relationship node
        with patch.object(detail_panel, "update_node") as mock_update:
            screen.update_detail_panel(relationship_node)
            # The actual update_node call happens through query_one, so we test the pattern


if __name__ == "__main__":
    pytest.main([__file__])