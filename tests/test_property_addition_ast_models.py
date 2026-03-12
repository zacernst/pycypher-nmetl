"""
Unit tests for enhanced PropertyAddition AST models.
Tests the type-safe AST representations for all SET variants.
"""

import pytest
from pydantic import ValidationError
from pycypher.ast_models import (
    Set, SetItem, SetPropertyItem, SetLabelsItem,
    SetAllPropertiesItem, AddAllPropertiesItem,
    Variable, Literal, PropertyLookup
)


class TestSetPropertyItem:
    """Test SET n.prop = value AST model."""

    def test_create_set_property_item(self):
        """Test creating a SetPropertyItem with all required fields."""
        item = SetPropertyItem(
            variable=Variable(name="n"),
            property="age",
            value=Literal(value=30)
        )

        assert item.variable.name == "n"
        assert item.property == "age"
        assert item.value.value == 30
        assert isinstance(item, SetItem)  # Inheritance

    def test_set_property_item_with_expression(self):
        """Test SetPropertyItem with complex expression as value."""
        item = SetPropertyItem(
            variable=Variable(name="person"),
            property="fullName",
            value=PropertyLookup(
                variable=Variable(name="person"),
                property="firstName"
            )
        )

        assert item.variable.name == "person"
        assert item.property == "fullName"
        assert isinstance(item.value, PropertyLookup)
        assert item.value.property == "firstName"

    def test_set_property_item_validation(self):
        """Test that SetPropertyItem requires all fields."""
        with pytest.raises(ValidationError):
            SetPropertyItem()  # Missing required fields

        with pytest.raises(ValidationError):
            SetPropertyItem(variable=Variable(name="n"))  # Missing property and value


class TestSetLabelsItem:
    """Test SET n:Label AST model."""

    def test_create_set_labels_item(self):
        """Test creating a SetLabelsItem with labels."""
        item = SetLabelsItem(
            variable=Variable(name="n"),
            labels=["Person", "Employee"]
        )

        assert item.variable.name == "n"
        assert item.labels == ["Person", "Employee"]
        assert isinstance(item, SetItem)

    def test_set_labels_single_label(self):
        """Test SetLabelsItem with single label."""
        item = SetLabelsItem(
            variable=Variable(name="node"),
            labels=["NewLabel"]
        )

        assert item.variable.name == "node"
        assert item.labels == ["NewLabel"]
        assert len(item.labels) == 1

    def test_set_labels_empty_list(self):
        """Test SetLabelsItem with empty labels list."""
        item = SetLabelsItem(
            variable=Variable(name="n"),
            labels=[]
        )

        assert item.variable.name == "n"
        assert item.labels == []


class TestSetAllPropertiesItem:
    """Test SET n = {map} AST model."""

    def test_create_set_all_properties_item(self):
        """Test creating SetAllPropertiesItem with map literal."""
        item = SetAllPropertiesItem(
            variable=Variable(name="n"),
            properties=Literal(value={"name": "John", "age": 30})
        )

        assert item.variable.name == "n"
        assert item.properties.value == {"name": "John", "age": 30}
        assert isinstance(item, SetItem)

    def test_set_all_properties_with_variable(self):
        """Test SetAllPropertiesItem with variable reference."""
        item = SetAllPropertiesItem(
            variable=Variable(name="target"),
            properties=Variable(name="sourceProps")
        )

        assert item.variable.name == "target"
        assert isinstance(item.properties, Variable)
        assert item.properties.name == "sourceProps"


class TestAddAllPropertiesItem:
    """Test SET n += {map} AST model."""

    def test_create_add_all_properties_item(self):
        """Test creating AddAllPropertiesItem."""
        item = AddAllPropertiesItem(
            variable=Variable(name="n"),
            properties=Literal(value={"newProp": "newValue"})
        )

        assert item.variable.name == "n"
        assert item.properties.value == {"newProp": "newValue"}
        assert isinstance(item, SetItem)

    def test_add_properties_distinction_from_set_all(self):
        """Test that AddAllPropertiesItem is distinct from SetAllPropertiesItem."""
        add_item = AddAllPropertiesItem(
            variable=Variable(name="n"),
            properties=Literal(value={"a": 1})
        )

        set_item = SetAllPropertiesItem(
            variable=Variable(name="n"),
            properties=Literal(value={"a": 1})
        )

        assert type(add_item) != type(set_item)
        assert isinstance(add_item, AddAllPropertiesItem)
        assert isinstance(set_item, SetAllPropertiesItem)


class TestSetClause:
    """Test SET clause AST model."""

    def test_create_set_clause_single_item(self):
        """Test creating SET clause with single item."""
        set_clause = Set(items=[
            SetPropertyItem(
                variable=Variable(name="n"),
                property="age",
                value=Literal(value=25)
            )
        ])

        assert len(set_clause.items) == 1
        assert isinstance(set_clause.items[0], SetPropertyItem)
        assert set_clause.items[0].property == "age"

    def test_create_set_clause_multiple_items(self):
        """Test creating SET clause with multiple different items."""
        set_clause = Set(items=[
            SetPropertyItem(
                variable=Variable(name="n"),
                property="name",
                value=Literal(value="John")
            ),
            SetPropertyItem(
                variable=Variable(name="n"),
                property="age",
                value=Literal(value=30)
            ),
            SetLabelsItem(
                variable=Variable(name="n"),
                labels=["Person"]
            )
        ])

        assert len(set_clause.items) == 3
        assert isinstance(set_clause.items[0], SetPropertyItem)
        assert isinstance(set_clause.items[1], SetPropertyItem)
        assert isinstance(set_clause.items[2], SetLabelsItem)

    def test_set_clause_mixed_variables(self):
        """Test SET clause with items affecting different variables."""
        set_clause = Set(items=[
            SetPropertyItem(
                variable=Variable(name="person"),
                property="age",
                value=Literal(value=25)
            ),
            SetPropertyItem(
                variable=Variable(name="company"),
                property="revenue",
                value=Literal(value=1000000)
            )
        ])

        assert len(set_clause.items) == 2
        assert set_clause.items[0].variable.name == "person"
        assert set_clause.items[1].variable.name == "company"

    def test_empty_set_clause(self):
        """Test creating empty SET clause."""
        set_clause = Set(items=[])

        assert len(set_clause.items) == 0
        assert set_clause.items == []


class TestSetItemPolymorphism:
    """Test polymorphic behavior of SetItem subclasses."""

    def test_set_item_base_class_behavior(self):
        """Test that all SetItem subclasses can be treated as SetItem."""
        items = [
            SetPropertyItem(
                variable=Variable(name="n"),
                property="prop",
                value=Literal(value="val")
            ),
            SetLabelsItem(
                variable=Variable(name="n"),
                labels=["Label"]
            ),
            SetAllPropertiesItem(
                variable=Variable(name="n"),
                properties=Literal(value={})
            ),
            AddAllPropertiesItem(
                variable=Variable(name="n"),
                properties=Literal(value={})
            )
        ]

        for item in items:
            assert isinstance(item, SetItem)
            assert hasattr(item, 'variable')
            assert item.variable.name == "n"

    def test_set_item_type_discrimination(self):
        """Test that SetItem subclasses can be discriminated by type."""
        prop_item = SetPropertyItem(
            variable=Variable(name="n"),
            property="age",
            value=Literal(value=30)
        )

        labels_item = SetLabelsItem(
            variable=Variable(name="n"),
            labels=["Person"]
        )

        # Type checking should work
        assert isinstance(prop_item, SetPropertyItem)
        assert not isinstance(prop_item, SetLabelsItem)
        assert isinstance(labels_item, SetLabelsItem)
        assert not isinstance(labels_item, SetPropertyItem)

        # Both should be SetItems
        assert isinstance(prop_item, SetItem)
        assert isinstance(labels_item, SetItem)