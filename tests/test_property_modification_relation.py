"""
Unit tests for PropertyModification relational model.
Tests the relation that applies property changes to DataFrames.
"""

import pytest
import pandas as pd
from pycypher.relational_models import (
    PropertyModification, EntityTable, Context, EntityMapping, RelationshipMapping
)
from pycypher.property_change import PropertyChange, PropertyChangeType
from pycypher.ast_models import Variable, Literal, PropertyLookup, Arithmetic


class TestPropertyModificationBasic:
    """Test basic PropertyModification relation functionality."""

    @pytest.fixture
    def base_entity_table(self):
        """Create a base EntityTable for testing."""
        df = pd.DataFrame({
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [25, 30, 35],
            "salary": [50000, 60000, 70000]
        })

        return EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=["__ID__", "name", "age", "salary"],
            source_obj_attribute_map={"name": "name", "age": "age", "salary": "salary"},
            attribute_map={"name": "name", "age": "age", "salary": "salary"},
            source_obj=df
        )

    @pytest.fixture
    def sample_context(self, base_entity_table):
        """Create a Context with the base entity table."""
        return Context(
            entity_mapping=EntityMapping(mapping={"Person": base_entity_table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_create_property_modification(self, base_entity_table):
        """Test creating PropertyModification relation."""
        modifications = [
            PropertyChange(
                variable_type="Person",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="newAge",
                value_expression=Literal(value=99)
            )
        ]

        prop_mod = PropertyModification(
            base_relation=base_entity_table,
            modifications=modifications,
            variable_map={Variable(name="p"): "__ID__"},
            variable_type_map={Variable(name="p"): "Person"},
            column_names=["__ID__", "name", "age", "salary", "newAge"]
        )

        assert prop_mod.base_relation == base_entity_table
        assert len(prop_mod.modifications) == 1
        assert prop_mod.modifications[0].property_name == "newAge"

    def test_property_modification_to_pandas_add_column(self, base_entity_table, sample_context):
        """Test PropertyModification.to_pandas() adding new column."""
        modifications = [
            PropertyChange(
                variable_type="Person",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="category",
                value_expression=Literal(value="employee")
            )
        ]

        prop_mod = PropertyModification(
            base_relation=base_entity_table,
            modifications=modifications,
            variable_map={Variable(name="p"): "__ID__"},
            variable_type_map={Variable(name="p"): "Person"},
            column_names=["__ID__", "name", "age", "salary", "category"]
        )

        result_df = prop_mod.to_pandas(context=sample_context)

        # Check that new column was added
        assert "category" in result_df.columns
        assert len(result_df.columns) == 5  # Original 4 + 1 new
        assert (result_df["category"] == "employee").all()

        # Check original columns are preserved
        assert result_df["name"].tolist() == ["Alice", "Bob", "Carol"]
        assert result_df["age"].tolist() == [25, 30, 35]

    def test_property_modification_modify_existing_column(self, base_entity_table, sample_context):
        """Test PropertyModification modifying existing column."""
        modifications = [
            PropertyChange(
                variable_type="Person",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="age",  # Modify existing column
                value_expression=Literal(value=40)
            )
        ]

        prop_mod = PropertyModification(
            base_relation=base_entity_table,
            modifications=modifications,
            variable_map={Variable(name="p"): "__ID__"},
            variable_type_map={Variable(name="p"): "Person"},
            column_names=["__ID__", "name", "age", "salary"]
        )

        result_df = prop_mod.to_pandas(context=sample_context)

        # Check that age column was modified
        assert (result_df["age"] == 40).all()
        assert len(result_df.columns) == 4  # Same number of columns

        # Check other columns are preserved
        assert result_df["name"].tolist() == ["Alice", "Bob", "Carol"]
        assert result_df["salary"].tolist() == [50000, 60000, 70000]

    def test_property_modification_with_expression(self, base_entity_table, sample_context):
        """Test PropertyModification with computed expression."""
        modifications = [
            PropertyChange(
                variable_type="Person",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="doubled_age",
                value_expression=Arithmetic(
                    left=PropertyLookup(variable=Variable(name="p"), property="age"),
                    operator="*",
                    right=Literal(value=2)
                )
            )
        ]

        prop_mod = PropertyModification(
            base_relation=base_entity_table,
            modifications=modifications,
            variable_map={Variable(name="p"): "__ID__"},
            variable_type_map={Variable(name="p"): "Person"},
            column_names=["__ID__", "name", "age", "salary", "doubled_age"]
        )

        result_df = prop_mod.to_pandas(context=sample_context)

        # Check that expression was evaluated correctly
        assert "doubled_age" in result_df.columns
        assert result_df["doubled_age"].tolist() == [50, 60, 70]  # 25*2, 30*2, 35*2

    def test_property_modification_multiple_changes(self, base_entity_table, sample_context):
        """Test PropertyModification with multiple property changes."""
        modifications = [
            PropertyChange(
                variable_type="Person",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="category",
                value_expression=Literal(value="staff")
            ),
            PropertyChange(
                variable_type="Person",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="bonus",
                value_expression=Arithmetic(
                    left=PropertyLookup(variable=Variable(name="p"), property="salary"),
                    operator="*",
                    right=Literal(value=0.1)
                )
            )
        ]

        prop_mod = PropertyModification(
            base_relation=base_entity_table,
            modifications=modifications,
            variable_map={Variable(name="p"): "__ID__"},
            variable_type_map={Variable(name="p"): "Person"},
            column_names=["__ID__", "name", "age", "salary", "category", "bonus"]
        )

        result_df = prop_mod.to_pandas(context=sample_context)

        # Check both modifications applied
        assert "category" in result_df.columns
        assert "bonus" in result_df.columns
        assert (result_df["category"] == "staff").all()
        assert result_df["bonus"].tolist() == [5000.0, 6000.0, 7000.0]  # 10% of salary


class TestPropertyModificationAdvanced:
    """Test advanced PropertyModification functionality."""

    @pytest.fixture
    def complex_entity_table(self):
        """Create a more complex EntityTable for advanced testing."""
        df = pd.DataFrame({
            "__ID__": [1, 2, 3, 4],
            "firstName": ["Alice", "Bob", "Carol", "Dave"],
            "lastName": ["Smith", "Jones", "Brown", "Wilson"],
            "age": [25, 30, 35, 28],
            "department": ["Engineering", "Sales", "Engineering", "Marketing"],
            "baseSalary": [70000, 60000, 80000, 65000],
            "active": [True, True, False, True]
        })

        return EntityTable(
            entity_type="Employee",
            identifier="Employee",
            column_names=["__ID__", "firstName", "lastName", "age", "department", "baseSalary", "active"],
            source_obj_attribute_map={
                "firstName": "firstName", "lastName": "lastName", "age": "age",
                "department": "department", "baseSalary": "baseSalary", "active": "active"
            },
            attribute_map={
                "firstName": "firstName", "lastName": "lastName", "age": "age",
                "department": "department", "baseSalary": "baseSalary", "active": "active"
            },
            source_obj=df
        )

    @pytest.fixture
    def complex_context(self, complex_entity_table):
        return Context(
            entity_mapping=EntityMapping(mapping={"Employee": complex_entity_table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_set_all_properties_modification(self, complex_entity_table, complex_context):
        """Test SET n = {map} modification."""
        modifications = [
            PropertyChange(
                variable_type="Employee",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_ALL_PROPERTIES,
                properties_map={
                    "fullName": Literal(value="New Name"),
                    "status": Literal(value="updated"),
                    "level": Literal(value=5)
                }
            )
        ]

        prop_mod = PropertyModification(
            base_relation=complex_entity_table,
            modifications=modifications,
            variable_map={Variable(name="e"): "__ID__"},
            variable_type_map={Variable(name="e"): "Employee"},
            column_names=["__ID__", "firstName", "lastName", "age", "department", "baseSalary", "active",
                         "fullName", "status", "level"]
        )

        result_df = prop_mod.to_pandas(context=complex_context)

        # Check that all properties in map were set
        assert "fullName" in result_df.columns
        assert "status" in result_df.columns
        assert "level" in result_df.columns

        assert (result_df["fullName"] == "New Name").all()
        assert (result_df["status"] == "updated").all()
        assert (result_df["level"] == 5).all()

    def test_add_all_properties_modification(self, complex_entity_table, complex_context):
        """Test SET n += {map} modification."""
        modifications = [
            PropertyChange(
                variable_type="Employee",
                variable_column="__ID__",
                change_type=PropertyChangeType.ADD_ALL_PROPERTIES,
                properties_map={
                    "bonus": Arithmetic(
                        left=PropertyLookup(variable=Variable(name="e"), property="baseSalary"),
                        operator="*",
                        right=Literal(value=0.15)
                    ),
                    "yearsService": Literal(value=3),
                    "eligible": Literal(value=True)
                }
            )
        ]

        prop_mod = PropertyModification(
            base_relation=complex_entity_table,
            modifications=modifications,
            variable_map={Variable(name="e"): "__ID__"},
            variable_type_map={Variable(name="e"): "Employee"},
            column_names=["__ID__", "firstName", "lastName", "age", "department", "baseSalary", "active",
                         "bonus", "yearsService", "eligible"]
        )

        result_df = prop_mod.to_pandas(context=complex_context)

        # Check that properties were added (not replacing existing ones)
        assert "bonus" in result_df.columns
        assert "yearsService" in result_df.columns
        assert "eligible" in result_df.columns

        # Original columns should still exist
        assert "firstName" in result_df.columns
        assert "baseSalary" in result_df.columns

        # Check computed values
        expected_bonuses = [10500.0, 9000.0, 12000.0, 9750.0]  # 15% of base salary
        assert result_df["bonus"].tolist() == expected_bonuses
        assert (result_df["yearsService"] == 3).all()

    def test_set_labels_modification(self, complex_entity_table, complex_context):
        """Test SET n:Label modification."""
        modifications = [
            PropertyChange(
                variable_type="Employee",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_LABELS,
                labels=["Person", "Worker", "TeamMember"]
            )
        ]

        prop_mod = PropertyModification(
            base_relation=complex_entity_table,
            modifications=modifications,
            variable_map={Variable(name="e"): "__ID__"},
            variable_type_map={Variable(name="e"): "Employee"},
            column_names=["__ID__", "firstName", "lastName", "age", "department", "baseSalary", "active", "__labels__"]
        )

        result_df = prop_mod.to_pandas(context=complex_context)

        # Check that labels column was added
        assert "__labels__" in result_df.columns

        # Check that all rows have the same labels
        for _, row in result_df.iterrows():
            labels = row["__labels__"]
            assert "Person" in labels
            assert "Worker" in labels
            assert "TeamMember" in labels
            assert len(labels) == 3

    def test_mixed_modification_types(self, complex_entity_table, complex_context):
        """Test mixing different types of modifications."""
        modifications = [
            # SET e.status = 'active'
            PropertyChange(
                variable_type="Employee",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="status",
                value_expression=Literal(value="active")
            ),
            # SET e:Manager
            PropertyChange(
                variable_type="Employee",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_LABELS,
                labels=["Manager"]
            ),
            # SET e += {rating: 5}
            PropertyChange(
                variable_type="Employee",
                variable_column="__ID__",
                change_type=PropertyChangeType.ADD_ALL_PROPERTIES,
                properties_map={
                    "rating": Literal(value=5)
                }
            )
        ]

        prop_mod = PropertyModification(
            base_relation=complex_entity_table,
            modifications=modifications,
            variable_map={Variable(name="e"): "__ID__"},
            variable_type_map={Variable(name="e"): "Employee"},
            column_names=["__ID__", "firstName", "lastName", "age", "department", "baseSalary", "active",
                         "status", "__labels__", "rating"]
        )

        result_df = prop_mod.to_pandas(context=complex_context)

        # Check all modifications applied
        assert "status" in result_df.columns
        assert "__labels__" in result_df.columns
        assert "rating" in result_df.columns

        assert (result_df["status"] == "active").all()
        assert (result_df["rating"] == 5).all()

        # Check labels
        for _, row in result_df.iterrows():
            assert "Manager" in row["__labels__"]


class TestPropertyModificationEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def minimal_entity_table(self):
        df = pd.DataFrame({
            "__ID__": [1],
            "value": [42]
        })

        return EntityTable(
            entity_type="Test",
            identifier="Test",
            column_names=["__ID__", "value"],
            source_obj_attribute_map={"value": "value"},
            attribute_map={"value": "value"},
            source_obj=df
        )

    @pytest.fixture
    def minimal_context(self, minimal_entity_table):
        return Context(
            entity_mapping=EntityMapping(mapping={"Test": minimal_entity_table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_property_modification_empty_modifications(self, minimal_entity_table, minimal_context):
        """Test PropertyModification with no modifications."""
        prop_mod = PropertyModification(
            base_relation=minimal_entity_table,
            modifications=[],  # No modifications
            variable_map={Variable(name="n"): "__ID__"},
            variable_type_map={Variable(name="n"): "Test"},
            column_names=["__ID__", "value"]
        )

        result_df = prop_mod.to_pandas(context=minimal_context)

        # Should return unchanged DataFrame
        assert len(result_df) == 1
        assert list(result_df.columns) == ["__ID__", "value"]
        assert result_df["value"].iloc[0] == 42

    def test_property_modification_with_null_values(self, minimal_entity_table, minimal_context):
        """Test PropertyModification setting null values."""
        modifications = [
            PropertyChange(
                variable_type="Test",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="nullable",
                value_expression=Literal(value=None)
            )
        ]

        prop_mod = PropertyModification(
            base_relation=minimal_entity_table,
            modifications=modifications,
            variable_map={Variable(name="n"): "__ID__"},
            variable_type_map={Variable(name="n"): "Test"},
            column_names=["__ID__", "value", "nullable"]
        )

        result_df = prop_mod.to_pandas(context=minimal_context)

        assert "nullable" in result_df.columns
        assert pd.isna(result_df["nullable"].iloc[0])

    def test_property_modification_type_preservation(self, minimal_entity_table, minimal_context):
        """Test that PropertyModification preserves data types."""
        modifications = [
            PropertyChange(
                variable_type="Test",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="int_val",
                value_expression=Literal(value=42)
            ),
            PropertyChange(
                variable_type="Test",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="float_val",
                value_expression=Literal(value=3.14)
            ),
            PropertyChange(
                variable_type="Test",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="str_val",
                value_expression=Literal(value="hello")
            ),
            PropertyChange(
                variable_type="Test",
                variable_column="__ID__",
                change_type=PropertyChangeType.SET_PROPERTY,
                property_name="bool_val",
                value_expression=Literal(value=True)
            )
        ]

        prop_mod = PropertyModification(
            base_relation=minimal_entity_table,
            modifications=modifications,
            variable_map={Variable(name="n"): "__ID__"},
            variable_type_map={Variable(name="n"): "Test"},
            column_names=["__ID__", "value", "int_val", "float_val", "str_val", "bool_val"]
        )

        result_df = prop_mod.to_pandas(context=minimal_context)

        # Check types are preserved
        assert isinstance(result_df["int_val"].iloc[0], int)
        assert isinstance(result_df["float_val"].iloc[0], float)
        assert isinstance(result_df["str_val"].iloc[0], str)
        assert isinstance(result_df["bool_val"].iloc[0], bool)