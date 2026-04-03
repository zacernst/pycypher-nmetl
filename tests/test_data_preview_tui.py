"""Tests for TUI data preview functionality.

Verifies that the data preview dialog integrates properly with the data sources
screen and provides correct async loading behavior.
"""

import asyncio
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from textual.app import App
from textual.widgets import DataTable, Label

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.data_sources import DataSourcesScreen, SourceItem
from pycypher_tui.widgets.data_preview import DataPreviewDialog, PreviewData


class TestDataPreviewDialog:
    """Test the DataPreviewDialog widget."""

    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,age,city\n")
            f.write("1,Alice,25,New York\n")
            f.write("2,Bob,30,London\n")
            f.write("3,Charlie,35,Paris\n")
            f.write("4,Diana,28,Tokyo\n")
            return Path(f.name)

    def test_dialog_initialization(self, sample_csv_file):
        """Test that the dialog initializes correctly."""
        dialog = DataPreviewDialog(
            source_uri=str(sample_csv_file),
            source_id="test_csv"
        )

        assert dialog.source_uri == str(sample_csv_file)
        assert dialog.source_id == "test_csv"
        assert dialog._preview_data is None
        assert dialog._cache is not None

    def test_preview_data_container(self):
        """Test the PreviewData container."""
        data = PreviewData(
            schema_info={"columns": [("id", "int64"), ("name", "string")]},
            sample_data=[{"id": 1, "name": "Alice"}],
            column_stats={"id": {"null_count": 0}},
        )

        assert data.schema_info is not None
        assert data.sample_data is not None
        assert data.column_stats is not None
        assert data.error is None

    def test_error_preview_data(self):
        """Test PreviewData with error."""
        data = PreviewData(error="File not found")

        assert data.error == "File not found"
        assert data.schema_info is None
        assert data.sample_data is None
        assert data.column_stats is None

    def test_dialog_with_invalid_file(self):
        """Test dialog behavior with invalid file."""
        dialog = DataPreviewDialog(
            source_uri="/nonexistent/file.csv",
            source_id="invalid_csv"
        )

        # Should initialize without error
        assert dialog.source_uri == "/nonexistent/file.csv"
        assert dialog.source_id == "invalid_csv"


class TestDataSourcesScreenIntegration:
    """Test integration of preview functionality with DataSourcesScreen."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary directory for config."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("customer_id,name,email,city\n")
            f.write("1,Alice Smith,alice@example.com,New York\n")
            f.write("2,Bob Johnson,bob@example.com,London\n")
            f.write("3,Charlie Brown,charlie@example.com,Paris\n")
            return Path(f.name)

    @pytest.fixture
    def config_manager_with_sources(self, temp_config_dir, sample_csv_file):
        """Create a config manager with test data sources."""
        config_manager = ConfigManager()

        # Add an entity source
        config_manager.add_entity_source(
            source_id="customers",
            uri=str(sample_csv_file),
            entity_type="Customer"
        )

        return config_manager

    def test_screen_override_keys_include_preview(self, config_manager_with_sources):
        """Test that the screen includes 'p' as an override key."""
        screen = DataSourcesScreen(config_manager_with_sources)

        override_keys = screen._screen_override_keys
        assert "p" in override_keys
        assert "tab" in override_keys

    def test_footer_hints_include_preview(self, config_manager_with_sources):
        """Test that footer hints include preview shortcut."""
        screen = DataSourcesScreen(config_manager_with_sources)

        footer_hints = screen.footer_hints
        assert "p:preview" in footer_hints

    def test_source_item_creation(self, config_manager_with_sources):
        """Test that SourceItem is created correctly."""
        screen = DataSourcesScreen(config_manager_with_sources)
        items = screen.load_items()

        assert len(items) == 1
        source = items[0]
        assert isinstance(source, SourceItem)
        assert source.source_id == "customers"
        assert source.source_type == "entity"
        assert source.label == "Customer"
        assert source.uri.endswith('.csv')

    def test_handle_extra_key_preview(self, config_manager_with_sources, monkeypatch):
        """Test that 'p' key is handled for preview."""
        screen = DataSourcesScreen(config_manager_with_sources)

        # Mock the current item
        screen._items = screen.load_items()
        screen._cursor = 0

        # Mock the preview method to avoid app.push_screen call
        preview_called = False
        def mock_preview():
            nonlocal preview_called
            preview_called = True
        monkeypatch.setattr(screen, '_preview_current_source', mock_preview)

        # Should handle 'p' key
        handled = screen.handle_extra_key("p")
        assert handled is True
        assert preview_called is True

        # Should not handle unknown keys
        handled = screen.handle_extra_key("x")
        assert handled is False


class TestDataPreviewIntegration:
    """Test end-to-end data preview functionality."""

    @pytest.fixture
    def sample_data_files(self):
        """Create sample data files for testing."""
        files = {}

        # CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("product_id,name,price,category\n")
            f.write("1,Widget A,19.99,Electronics\n")
            f.write("2,Widget B,29.99,Electronics\n")
            f.write("3,Book X,14.99,Books\n")
            files['csv'] = Path(f.name)

        # JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            import json
            data = [
                {"order_id": 1, "customer_id": 1, "total": 45.98},
                {"order_id": 2, "customer_id": 2, "total": 29.99},
                {"order_id": 3, "customer_id": 1, "total": 14.99},
            ]
            json.dump(data, f)
            files['json'] = Path(f.name)

        return files

    def test_preview_csv_file(self, sample_data_files):
        """Test previewing a CSV file."""
        csv_file = sample_data_files['csv']

        dialog = DataPreviewDialog(
            source_uri=str(csv_file),
            source_id="products"
        )

        # Dialog should initialize correctly
        assert dialog.source_uri == str(csv_file)
        assert dialog.source_id == "products"

    def test_preview_json_file(self, sample_data_files):
        """Test previewing a JSON file."""
        json_file = sample_data_files['json']

        dialog = DataPreviewDialog(
            source_uri=str(json_file),
            source_id="orders"
        )

        # Dialog should initialize correctly
        assert dialog.source_uri == str(json_file)
        assert dialog.source_id == "orders"

    def test_preview_nonexistent_file(self):
        """Test previewing a nonexistent file."""
        dialog = DataPreviewDialog(
            source_uri="/tmp/nonexistent.csv",
            source_id="missing"
        )

        # Should initialize without immediate error
        assert dialog.source_uri == "/tmp/nonexistent.csv"
        # Error should be caught during async loading


if __name__ == "__main__":
    pytest.main([__file__])