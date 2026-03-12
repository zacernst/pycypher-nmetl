"""
Unit tests for PropertyChange data model.
Tests the data structures that represent individual property modifications.
"""

import pytest
from enum import Enum
from pycypher.property_change import (
    PropertyChange, PropertyChangeType, PropertyModificationEvaluator
)
from pycypher.ast_models import Variable, Literal, PropertyLookup, Arithmetic
import pandas as pd


class TestPropertyChangeType:
    """Test PropertyChangeType enumeration."""

    def test_property_change_type_values(self):
        """Test that all expected PropertyChangeType values exist."""
        assert PropertyChangeType.SET_PROPERTY.value == "set_property"
        assert PropertyChangeType.SET_ALL_PROPERTIES.value == "set_all"
        assert PropertyChangeType.ADD_ALL_PROPERTIES.value == "add_all"
        assert PropertyChangeType.SET_LABELS.value == "set_labels"

    def test_property_change_type_enumeration(self):
        """Test PropertyChangeType enumeration behavior."""
        all_types = list(PropertyChangeType)
        assert len(all_types) == 4
        assert PropertyChangeType.SET_PROPERTY in all_types
        assert PropertyChangeType.SET_ALL_PROPERTIES in all_types
        assert PropertyChangeType.ADD_ALL_PROPERTIES in all_types
        assert PropertyChangeType.SET_LABELS in all_types


class TestPropertyChange:
    """Test PropertyChange data class."""

    def test_create_set_property_change(self):
        """Test creating PropertyChange for SET n.prop = value."""
        change = PropertyChange(
            variable_type="Person",
            variable_column="person_id_col",
            change_type=PropertyChangeType.SET_PROPERTY,
            property_name="age",
            value_expression=Literal(value=30)
        )

        assert change.variable_type == "Person"
        assert change.variable_column == "person_id_col"
        assert change.change_type == PropertyChangeType.SET_PROPERTY
        assert change.property_name == "age"
        assert change.value_expression.value == 30
        assert change.properties_map is None
        assert change.labels is None

    def test_create_set_labels_change(self):
        """Test creating PropertyChange for SET n:Label."""
        change = PropertyChange(
            variable_type="Person",
            variable_column="person_id_col",
            change_type=PropertyChangeType.SET_LABELS,
            labels=["Employee", "Manager"]
        )

        assert change.variable_type == "Person"
        assert change.change_type == PropertyChangeType.SET_LABELS
        assert change.labels == ["Employee", "Manager"]
        assert change.property_name is None
        assert change.value_expression is None
        assert change.properties_map is None

    def test_create_set_all_properties_change(self):
        """Test creating PropertyChange for SET n = {map}."""
        props_map = {
            "name": Literal(value="John"),
            "age": Literal(value=30),
            "active": Literal(value=True)
        }

        change = PropertyChange(
            variable_type="Person",
            variable_column="person_id_col",
            change_type=PropertyChangeType.SET_ALL_PROPERTIES,
            properties_map=props_map
        )

        assert change.variable_type == "Person"
        assert change.change_type == PropertyChangeType.SET_ALL_PROPERTIES
        assert change.properties_map == props_map
        assert len(change.properties_map) == 3
        assert change.property_name is None
        assert change.value_expression is None
        assert change.labels is None

    def test_create_add_all_properties_change(self):
        """Test creating PropertyChange for SET n += {map}."""
        props_map = {
            "newProp": Literal(value="newValue"),
            "timestamp": PropertyLookup(variable=Variable(name="system"), property="now")
        }

        change = PropertyChange(
            variable_type="Person",
            variable_column="person_id_col",
            change_type=PropertyChangeType.ADD_ALL_PROPERTIES,
            properties_map=props_map
        )

        assert change.variable_type == "Person"
        assert change.change_type == PropertyChangeType.ADD_ALL_PROPERTIES
        assert change.properties_map == props_map
        assert "newProp" in change.properties_map
        assert "timestamp" in change.properties_map

    def test_property_change_validation(self):
        """Test PropertyChange field validation."""
        # Valid change should not raise
        PropertyChange(
            variable_type="Person",
            variable_column="id_col",
            change_type=PropertyChangeType.SET_PROPERTY,
            property_name="age",
            value_expression=Literal(value=25)
        )

        # Test required fields
        with pytest.raises(TypeError):
            PropertyChange()  # Missing required fields

    def test_property_change_with_complex_expression(self):
        """Test PropertyChange with complex expression."""
        # SET n.totalSalary = n.baseSalary + n.bonus
        expr = Arithmetic(
            left=PropertyLookup(variable=Variable(name="n"), property="baseSalary"),
            operator="+",
            right=PropertyLookup(variable=Variable(name="n"), property="bonus")
        )

        change = PropertyChange(
            variable_type="Employee",
            variable_column="emp_id_col",
            change_type=PropertyChangeType.SET_PROPERTY,
            property_name="totalSalary",
            value_expression=expr
        )

        assert change.property_name == "totalSalary"
        assert isinstance(change.value_expression, Arithmetic)
        assert change.value_expression.operator == "+"


class TestPropertyModificationEvaluator:
    """Test PropertyModificationEvaluator for evaluating SET expressions."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing expression evaluation."""
        return pd.DataFrame({
            "person_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [25, 30, 35],
            "baseSalary": [50000, 60000, 70000],
            "bonus": [5000, 6000, 7000],
            "active": [True, True, False]
        })

    def test_evaluate_literal_expression(self, sample_dataframe):
        """Test evaluating literal expression for property value."""
        evaluator = PropertyModificationEvaluator()

        result = evaluator.evaluate_property_value(
            expr=Literal(value=42),
            df=sample_dataframe,
            row_index=0
        )

        assert result == 42

    def test_evaluate_property_lookup_expression(self, sample_dataframe):
        """Test evaluating property lookup expression."""
        evaluator = PropertyModificationEvaluator()

        # age property from row 1 (Bob, age=30)
        result = evaluator.evaluate_property_value(
            expr=PropertyLookup(variable=Variable(name="p"), property="age"),
            df=sample_dataframe,
            row_index=1
        )

        assert result == 30

    def test_evaluate_arithmetic_expression(self, sample_dataframe):
        """Test evaluating arithmetic expression."""
        evaluator = PropertyModificationEvaluator()

        # baseSalary + bonus for row 0 (Alice: 50000 + 5000 = 55000)
        expr = Arithmetic(
            left=PropertyLookup(variable=Variable(name="p"), property="baseSalary"),
            operator="+",
            right=PropertyLookup(variable=Variable(name="p"), property="bonus")
        )

        result = evaluator.evaluate_property_value(
            expr=expr,
            df=sample_dataframe,
            row_index=0
        )

        assert result == 55000

    def test_evaluate_expression_all_rows(self, sample_dataframe):
        """Test evaluating expression across all rows."""
        evaluator = PropertyModificationEvaluator()

        # Test that different rows produce different results
        expr = PropertyLookup(variable=Variable(name="p"), property="name")

        results = []
        for i in range(len(sample_dataframe)):
            result = evaluator.evaluate_property_value(expr, sample_dataframe, i)
            results.append(result)

        assert results == ["Alice", "Bob", "Carol"]

    def test_evaluate_properties_map(self, sample_dataframe):
        """Test evaluating a properties map for SET n = {map}."""
        evaluator = PropertyModificationEvaluator()

        properties_map = {
            "newName": Literal(value="Updated"),
            "totalComp": Arithmetic(
                left=PropertyLookup(variable=Variable(name="p"), property="baseSalary"),
                operator="+",
                right=PropertyLookup(variable=Variable(name="p"), property="bonus")
            )
        }

        results = evaluator.evaluate_properties_map(
            properties_map=properties_map,
            df=sample_dataframe,
            row_index=0
        )

        assert results["newName"] == "Updated"
        assert results["totalComp"] == 55000  # 50000 + 5000

    def test_evaluate_properties_map_multiple_rows(self, sample_dataframe):
        """Test evaluating properties map across multiple rows."""
        evaluator = PropertyModificationEvaluator()

        properties_map = {
            "doubled_age": Arithmetic(
                left=PropertyLookup(variable=Variable(name="p"), property="age"),
                operator="*",
                right=Literal(value=2)
            )
        }

        # Test all rows
        all_results = []
        for i in range(len(sample_dataframe)):
            results = evaluator.evaluate_properties_map(
                properties_map=properties_map,
                df=sample_dataframe,
                row_index=i
            )
            all_results.append(results["doubled_age"])

        assert all_results == [50, 60, 70]  # 25*2, 30*2, 35*2

    def test_evaluate_with_missing_property(self, sample_dataframe):
        """Test evaluating expression with missing property reference."""
        evaluator = PropertyModificationEvaluator()

        # Reference non-existent property
        expr = PropertyLookup(variable=Variable(name="p"), property="nonexistent")

        with pytest.raises(KeyError):
            evaluator.evaluate_property_value(expr, sample_dataframe, 0)

    def test_evaluate_with_invalid_row_index(self, sample_dataframe):
        """Test evaluating expression with invalid row index."""
        evaluator = PropertyModificationEvaluator()

        expr = Literal(value=42)

        with pytest.raises(IndexError):
            evaluator.evaluate_property_value(expr, sample_dataframe, 999)

    def test_evaluation_type_preservation(self, sample_dataframe):
        """Test that evaluation preserves Python types correctly."""
        evaluator = PropertyModificationEvaluator()

        # Test different types
        test_cases = [
            (Literal(value=42), int),
            (Literal(value=3.14), float),
            (Literal(value="string"), str),
            (Literal(value=True), bool),
            (PropertyLookup(variable=Variable(name="p"), property="age"), int),
            (PropertyLookup(variable=Variable(name="p"), property="name"), str),
        ]

        for expr, expected_type in test_cases:
            result = evaluator.evaluate_property_value(expr, sample_dataframe, 0)
            assert isinstance(result, expected_type)


class TestPropertyChangeIntegration:
    """Integration tests for PropertyChange with evaluation."""

    @pytest.fixture
    def sample_dataframe(self):
        return pd.DataFrame({
            "person_id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [25, 30],
            "salary": [50000, 60000]
        })

    def test_property_change_with_evaluation(self, sample_dataframe):
        """Test PropertyChange integrated with expression evaluation."""
        change = PropertyChange(
            variable_type="Person",
            variable_column="person_id",
            change_type=PropertyChangeType.SET_PROPERTY,
            property_name="newAge",
            value_expression=Arithmetic(
                left=PropertyLookup(variable=Variable(name="p"), property="age"),
                operator="+",
                right=Literal(value=1)
            )
        )

        evaluator = PropertyModificationEvaluator()

        # Evaluate for first row (Alice, age=25)
        new_value = evaluator.evaluate_property_value(
            change.value_expression,
            sample_dataframe,
            0
        )

        assert new_value == 26  # 25 + 1
        assert change.property_name == "newAge"
        assert change.variable_type == "Person"