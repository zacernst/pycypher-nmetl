"""Tests for visual column mapping validation widget."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from dataclasses import dataclass

from pycypher_tui.widgets.column_mapping import (
    ColumnMappingWidget,
    ColumnMapping,
    MappingValidationResult,
)


@dataclass
class MockRelationshipSource:
    """Mock relationship source for testing."""
    id: str
    uri: str
    relationship_type: str
    source_col: str
    target_col: str


@dataclass
class MockSchemaInfo:
    """Mock schema info for testing."""
    columns: list[dict[str, str]]
    row_count: int = 100


class MockIntrospector:
    """Mock DataSourceIntrospector for testing."""

    def __init__(self, uri: str):
        self.uri = uri

    def get_schema(self):
        # Return different schemas based on URI for testing
        if "people" in self.uri:
            return MockSchemaInfo(
                columns=[
                    {"name": "person_id", "type": "integer"},
                    {"name": "name", "type": "varchar"},
                    {"name": "age", "type": "integer"},
                ]
            )
        elif "relationships" in self.uri:
            return MockSchemaInfo(
                columns=[
                    {"name": "from_id", "type": "integer"},
                    {"name": "to_id", "type": "integer"},
                    {"name": "relationship_type", "type": "varchar"},
                ]
            )
        else:
            return MockSchemaInfo(columns=[])


@pytest.fixture
def column_mapping_widget():
    """Create a column mapping widget for testing."""
    return ColumnMappingWidget()


@pytest.fixture
def sample_relationship_sources():
    """Sample relationship sources for testing."""
    return [
        MockRelationshipSource(
            id="friends_rel",
            uri="data/friends.csv",
            relationship_type="FRIENDS",
            source_col="from_id",
            target_col="to_id"
        ),
        MockRelationshipSource(
            id="works_for_rel",
            uri="data/employment.csv",
            relationship_type="WORKS_FOR",
            source_col="employee_id",  # This column doesn't exist - should cause error
            target_col="company_id"    # This column doesn't exist - should cause error
        )
    ]


class TestColumnMapping:
    """Test ColumnMapping data class."""

    def test_column_mapping_creation(self):
        """Test creating a column mapping."""
        mapping = ColumnMapping(
            source_col="from_id",
            target_col="to_id",
            source_type="integer",
            target_type="integer",
            validation_status="valid",
            validation_message="Mapping is valid"
        )

        assert mapping.source_col == "from_id"
        assert mapping.target_col == "to_id"
        assert mapping.source_type == "integer"
        assert mapping.target_type == "integer"
        assert mapping.validation_status == "valid"
        assert mapping.validation_message == "Mapping is valid"

    def test_column_mapping_defaults(self):
        """Test column mapping with default values."""
        mapping = ColumnMapping(
            source_col="from_id",
            target_col="to_id"
        )

        assert mapping.source_col == "from_id"
        assert mapping.target_col == "to_id"
        assert mapping.source_type is None
        assert mapping.target_type is None
        assert mapping.validation_status == "unknown"
        assert mapping.validation_message == ""


class TestMappingValidationResult:
    """Test MappingValidationResult data class."""

    def test_validation_result_creation(self):
        """Test creating a validation result."""
        mappings = [
            ColumnMapping(source_col="from_id", target_col="to_id", validation_status="valid")
        ]
        result = MappingValidationResult(
            mappings=mappings,
            source_schema={"from_id": "integer", "to_id": "integer"},
            target_entities=["Person"],
            overall_status="valid",
            issues=[]
        )

        assert len(result.mappings) == 1
        assert result.mappings[0].source_col == "from_id"
        assert result.source_schema == {"from_id": "integer", "to_id": "integer"}
        assert result.target_entities == ["Person"]
        assert result.overall_status == "valid"
        assert result.issues == []

    def test_validation_result_defaults(self):
        """Test validation result with default values."""
        result = MappingValidationResult()

        assert result.mappings == []
        assert result.source_schema == {}
        assert result.target_entities == []
        assert result.overall_status == "unknown"
        assert result.issues == []


class TestColumnMappingWidget:
    """Test ColumnMappingWidget functionality."""

    @patch('pycypher_tui.widgets.column_mapping.DataSourceIntrospector', MockIntrospector)
    async def test_update_relationship_sources_valid(self, column_mapping_widget, sample_relationship_sources):
        """Test updating widget with valid relationship sources."""
        # Use only the first source which has valid columns
        valid_sources = [sample_relationship_sources[0]]

        column_mapping_widget.update_relationship_sources(valid_sources)

        result = column_mapping_widget._validation_result
        assert result is not None
        assert len(result.mappings) == 1
        assert result.mappings[0].source_col == "from_id"
        assert result.mappings[0].target_col == "to_id"
        assert result.overall_status == "error"  # Columns don't exist in mock schema

    @patch('pycypher_tui.widgets.column_mapping.DataSourceIntrospector', MockIntrospector)
    async def test_update_relationship_sources_invalid(self, column_mapping_widget, sample_relationship_sources):
        """Test updating widget with invalid relationship sources."""
        # Use the second source which has invalid column names
        invalid_sources = [sample_relationship_sources[1]]

        column_mapping_widget.update_relationship_sources(invalid_sources)

        result = column_mapping_widget._validation_result
        assert result is not None
        assert len(result.mappings) == 1
        assert result.overall_status == "error"
        assert len(result.issues) > 0
        assert result.issues[0]["type"] == "error"

    async def test_update_empty_sources(self, column_mapping_widget):
        """Test updating widget with empty sources list."""
        column_mapping_widget.update_relationship_sources([])

        # Should show empty state
        result = column_mapping_widget._validation_result
        assert result is None

    def test_types_compatible(self, column_mapping_widget):
        """Test type compatibility checking."""
        # Same types should be compatible
        assert column_mapping_widget._types_compatible("integer", "integer")
        assert column_mapping_widget._types_compatible("varchar", "varchar")

        # Numeric types should be compatible
        assert column_mapping_widget._types_compatible("integer", "bigint")
        assert column_mapping_widget._types_compatible("float", "double")
        assert column_mapping_widget._types_compatible("int", "number")

        # String types should be compatible
        assert column_mapping_widget._types_compatible("varchar", "text")
        assert column_mapping_widget._types_compatible("string", "char")

        # Incompatible types
        assert not column_mapping_widget._types_compatible("integer", "varchar")
        assert not column_mapping_widget._types_compatible("float", "text")

    def test_validate_mapping_missing_columns(self, column_mapping_widget):
        """Test validation with missing columns."""
        mapping = ColumnMapping(source_col="missing_col", target_col="to_id")
        source_columns = {"from_id": "integer", "to_id": "integer"}

        status, message = column_mapping_widget._validate_mapping(mapping, source_columns)

        assert status == "error"
        assert "not found" in message
        assert "missing_col" in message

    def test_validate_mapping_type_mismatch(self, column_mapping_widget):
        """Test validation with type mismatch."""
        mapping = ColumnMapping(source_col="from_id", target_col="to_name")
        source_columns = {"from_id": "integer", "to_name": "varchar"}

        status, message = column_mapping_widget._validate_mapping(mapping, source_columns)

        assert status == "warning"
        assert "Type mismatch" in message

    def test_validate_mapping_same_column(self, column_mapping_widget):
        """Test validation with same column for source and target."""
        mapping = ColumnMapping(source_col="id", target_col="id")
        source_columns = {"id": "integer"}

        status, message = column_mapping_widget._validate_mapping(mapping, source_columns)

        assert status == "warning"
        assert "Source and target columns are the same" in message

    def test_validate_mapping_valid(self, column_mapping_widget):
        """Test validation with valid mapping."""
        mapping = ColumnMapping(source_col="from_id", target_col="to_id")
        source_columns = {"from_id": "integer", "to_id": "integer"}

        status, message = column_mapping_widget._validate_mapping(mapping, source_columns)

        assert status == "valid"
        assert "Mapping is valid" in message

    def test_navigation_key_handling(self, column_mapping_widget):
        """Test VIM-style navigation key handling."""
        # Set up validation result with multiple mappings
        column_mapping_widget._validation_result = MappingValidationResult(
            mappings=[
                ColumnMapping(source_col="col1", target_col="col2"),
                ColumnMapping(source_col="col3", target_col="col4"),
                ColumnMapping(source_col="col5", target_col="col6"),
            ]
        )

        # Test moving down
        initial_index = column_mapping_widget.selected_index
        column_mapping_widget.selected_index = min(
            column_mapping_widget.selected_index + 1,
            len(column_mapping_widget._validation_result.mappings) - 1
        )
        assert column_mapping_widget.selected_index == initial_index + 1

        # Test moving up
        column_mapping_widget.selected_index = max(column_mapping_widget.selected_index - 1, 0)
        assert column_mapping_widget.selected_index == initial_index

    def test_message_handling(self, column_mapping_widget):
        """Test mapping change message creation."""
        message = column_mapping_widget.MappingChanged(
            source_id="test_source",
            new_mapping={"source_col": "new_source", "target_col": "new_target"}
        )

        assert message.source_id == "test_source"
        assert message.new_mapping["source_col"] == "new_source"
        assert message.new_mapping["target_col"] == "new_target"


if __name__ == "__main__":
    pytest.main([__file__])