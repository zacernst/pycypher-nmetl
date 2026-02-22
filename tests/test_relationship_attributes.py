"""
Unit tests for relationship attributes using inductive approach.

Tests the translation of RelationshipPattern objects with various numbers of
attributes to ensure the inductive implementation works correctly.
"""

import pandas as pd
import pytest
from pycypher.ast_models import (
    NodePattern,
    PatternPath,
    RelationshipDirection,
    RelationshipPattern,
    Variable,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    AttributeEqualsValue,
    Context,
    EntityMapping,
    EntityTable,
    FilterRows,
    Projection,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def test_context():
    """Create a test context with sample data."""
    # Entity data
    entity_df_person = pd.DataFrame(
        data={
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 40, 50],
        }
    )

    entity_df_city = pd.DataFrame(
        data={
            ID_COLUMN: [4, 5],
            "name": ["New York", "Los Angeles"],
            "population": [8000000, 4000000],
        }
    )

    # Relationship data with attributes
    relationship_df_knows = pd.DataFrame(
        data={
            ID_COLUMN: [10, 11, 12],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 1],
            "since": [2020, 2019, 2021],
            "strength": [0.9, 0.7, 0.8],
            "verified": [True, False, True],
        }
    )

    relationship_df_lives_in = pd.DataFrame(
        data={
            ID_COLUMN: [20, 21, 22],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [4, 5, 4],
            "since": [2015, 2018, 2020],
            "rent": [2000, 1500, 2200],
        }
    )

    # Create entity tables
    entity_table_person = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[
            ID_COLUMN,
            "name",
            "age",
        ],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={
            "name": "name",
            "age": "age",
        },
        source_obj=entity_df_person,
    )

    entity_table_city = EntityTable(
        entity_type="City",
        identifier="City",
        column_names=[
            ID_COLUMN,
            "name",
            "population",
        ],
        source_obj_attribute_map={"name": "name", "population": "population"},
        attribute_map={
            "name": "name",
            "population": "population",
        },
        source_obj=entity_df_city,
    )

    # Create relationship tables
    knows_id = "test_knows_rel"
    relationship_table_knows = RelationshipTable(
        relationship_type="KNOWS",
        identifier=knows_id,
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
            "since",
            "strength",
            "verified",
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
            "strength": "strength",
            "verified": "verified",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
            "strength": "strength",
            "verified": "verified",
        },
        source_obj=relationship_df_knows,
    )

    lives_in_id = "test_lives_in_rel"
    relationship_table_lives_in = RelationshipTable(
        relationship_type="LIVES_IN",
        identifier=lives_in_id,
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
            "since",
            "rent",
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
            "rent": "rent",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
            "rent": "rent",
        },
        source_obj=relationship_df_lives_in,
    )

    # Create context
    context = Context(
        entity_mapping=EntityMapping(
            mapping={"Person": entity_table_person, "City": entity_table_city}
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": relationship_table_knows,
                "LIVES_IN": relationship_table_lives_in,
            }
        ),
    )

    return context


class TestRelationshipAttributesInduction:
    """Test inductive approach for relationships with attributes."""

    def test_base_case_no_attributes(self, test_context):
        """Test base case: relationship with no attributes."""
        star = Star(context=test_context)

        relationship = RelationshipPattern(
            variable=Variable(name="r"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={},
        )

        result = star.to_relation(obj=relationship)

        # With current architecture, returns Projection wrapping RelationshipTable
        assert isinstance(result, Projection)
        assert isinstance(result.relation, RelationshipTable)
        assert result.relation.relationship_type == "KNOWS"
        assert Variable(name="r") in result.variable_map
        # Should have ID, SOURCE, TARGET columns (prefixed)
        assert len(result.column_names) == 3  # KNOWS____ID__, KNOWS____SOURCE__, KNOWS____TARGET__

    def test_single_attribute(self, test_context):
        """Test relationship with ONE attribute."""
        star = Star(context=test_context)

        relationship = RelationshipPattern(
            variable=Variable(name="r1"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={"since": 2020},
        )

        result = star.to_relation(obj=relationship)

        # Should return FilterRows
        assert isinstance(result, FilterRows)
        assert isinstance(result.condition, AttributeEqualsValue)
        assert result.condition.left == "since"
        assert result.condition.right == 2020
        # Base relation should be Projection wrapping RelationshipTable
        assert isinstance(result.relation, Projection)
        assert isinstance(result.relation.relation, RelationshipTable)
        # Variable mapping should be preserved
        assert Variable(name="r1") in result.variable_map

    def test_two_attributes(self, test_context):
        """Test relationship with TWO attributes."""
        star = Star(context=test_context)

        relationship = RelationshipPattern(
            variable=Variable(name="r2"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={"since": 2020, "strength": 0.9},
        )

        result = star.to_relation(obj=relationship)

        # Should return FilterRows
        assert isinstance(result, FilterRows)
        # Should have nested FilterRows
        assert isinstance(result.relation, FilterRows)
        # Innermost should be Projection wrapping RelationshipTable
        assert isinstance(result.relation.relation, Projection)
        assert isinstance(result.relation.relation.relation, RelationshipTable)
        # Variable mapping should be preserved
        assert Variable(name="r2") in result.variable_map

    def test_three_attributes(self, test_context):
        """Test relationship with THREE attributes."""
        star = Star(context=test_context)

        relationship = RelationshipPattern(
            variable=Variable(name="r3"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={"since": 2020, "strength": 0.9, "verified": True},
        )

        result = star.to_relation(obj=relationship)

        # Should return FilterRows
        assert isinstance(result, FilterRows)
        # Should have triple-nested FilterRows
        assert isinstance(result.relation, FilterRows)
        assert isinstance(result.relation.relation, FilterRows)
        # Innermost should be Projection wrapping RelationshipTable
        assert isinstance(result.relation.relation.relation, Projection)
        assert isinstance(result.relation.relation.relation.relation, RelationshipTable)
        # Variable mapping should be preserved through all levels
        assert Variable(name="r3") in result.variable_map

    def test_different_relationship_type(self, test_context):
        """Test different relationship type with attributes."""
        star = Star(context=test_context)

        relationship = RelationshipPattern(
            variable=Variable(name="r4"),
            labels=["LIVES_IN"],
            direction=RelationshipDirection.RIGHT,
            properties={"since": 2015, "rent": 2000},
        )

        result = star.to_relation(obj=relationship)

        # Should return FilterRows
        assert isinstance(result, FilterRows)
        # Should have nested FilterRows
        assert isinstance(result.relation, FilterRows)
        # Innermost should be Projection wrapping RelationshipTable
        assert isinstance(result.relation.relation, Projection)
        assert isinstance(result.relation.relation.relation, RelationshipTable)
        assert result.relation.relation.relation.relationship_type == "LIVES_IN"

    def test_direction_preserved(self, test_context):
        """Test that relationship direction is preserved through recursion."""
        star = Star(context=test_context)

        # Test LEFT direction
        relationship_left = RelationshipPattern(
            variable=Variable(name="r_left"),
            labels=["KNOWS"],
            direction=RelationshipDirection.LEFT,
            properties={"since": 2020},
        )

        result_left = star.to_relation(obj=relationship_left)

        # Direction should be preserved in the FilterRows source_algebraizable
        assert isinstance(result_left, FilterRows)
        # The source_algebraizable should have the correct direction
        assert result_left.source_algebraizable is not None
        assert isinstance(result_left.source_algebraizable, RelationshipPattern)
        assert (
            result_left.source_algebraizable.direction
            == RelationshipDirection.LEFT
        )

    def test_column_names_preserved(self, test_context):
        """Test that variable columns are preserved through filtering."""
        star = Star(context=test_context)

        relationship = RelationshipPattern(
            variable=Variable(name="r"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={"since": 2020, "strength": 0.9},
        )

        result = star.to_relation(obj=relationship)

        # Variable mapping should be preserved
        assert Variable(name="r") in result.variable_map
        # Column names should contain the variable's column
        assert len(result.column_names) >= 1
        # Verify the variable column is present
        assert result.variable_map[Variable(name="r")] in result.column_names


class TestRelationshipAttributesRegression:
    """Regression tests to ensure existing functionality still works."""

    def test_node_pattern_no_attributes(self, test_context):
        """Regression: NodePattern with no attributes should still work."""
        star = Star(context=test_context)

        node = NodePattern(
            variable=Variable(name="n"),
            labels=["Person"],
            properties={},
        )

        result = star.to_relation(obj=node)

        # With current architecture, returns Projection wrapping EntityTable
        assert isinstance(result, Projection)
        assert isinstance(result.relation, EntityTable)
        assert result.relation.entity_type == "Person"

    def test_node_pattern_with_attributes(self, test_context):
        """Regression: NodePattern with attributes should still work."""
        star = Star(context=test_context)

        node = NodePattern(
            variable=Variable(name="p"),
            labels=["Person"],
            properties={"name": "Alice", "age": 30},
        )

        result = star.to_relation(obj=node)

        assert isinstance(result, FilterRows)
        assert isinstance(result.relation, FilterRows)
        # After filtering both properties, base is Projection wrapping EntityTable
        assert isinstance(result.relation.relation, Projection)
        assert isinstance(result.relation.relation.relation, EntityTable)

    def test_pattern_path_basic(self, test_context):
        """Regression: PatternPath without relationship attributes should still work."""
        star = Star(context=test_context)

        node1 = NodePattern(
            variable=Variable(name="p1"),
            labels=["Person"],
            properties={"name": "Alice"},
        )

        relationship = RelationshipPattern(
            variable=Variable(name="r"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={},  # No attributes
        )

        node2 = NodePattern(
            variable=Variable(name="p2"),
            labels=["Person"],
            properties={"name": "Bob"},
        )

        path = PatternPath(elements=[node1, relationship, node2])

        result = star.to_relation(obj=path)

        # Should successfully create a join
        assert result is not None
        # Variable mappings should include all three variables
        assert Variable(name="p1") in result.variable_map
        assert Variable(name="r") in result.variable_map
        assert Variable(name="p2") in result.variable_map

    def test_pattern_path_left_direction(self, test_context):
        """Regression: PatternPath with LEFT direction should now work."""
        star = Star(context=test_context)

        node1 = NodePattern(
            variable=Variable(name="p1"),
            labels=["Person"],
            properties={"name": "Alice"},
        )

        relationship = RelationshipPattern(
            variable=Variable(name="r"),
            labels=["KNOWS"],
            direction=RelationshipDirection.LEFT,
            properties={},
        )
        
        node2 = NodePattern(
            variable=Variable(name="p2"),
            labels=["Person"],
            properties={"name": "Bob"},
        )

        path = PatternPath(elements=[node1, relationship, node2])

        result = star.to_relation(obj=path)

        assert result is not None
        assert Variable(name="p1") in result.variable_map
        assert Variable(name="r") in result.variable_map
        assert Variable(name="p2") in result.variable_map


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
