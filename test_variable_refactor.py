#!/usr/bin/env python3
"""Quick test to verify Variable class refactoring works correctly."""

from packages.pycypher.src.pycypher.ast_models import (
    ASTConverter,
    IntegerLiteral,
    ListComprehension,
    NodePattern,
    Pattern,
    PatternPath,
    Quantifier,
    RemoveItem,
    SetItem,
    Variable,
)


def test_node_pattern_with_variable():
    """Test that NodePattern.variable is now a Variable instance."""
    var = Variable(name="n")
    node = NodePattern(variable=var, labels=["Person"])

    assert node.variable is not None
    assert isinstance(node.variable, Variable)
    assert node.variable.name == "n"
    print("✓ NodePattern correctly uses Variable instance")


def test_pattern_path_with_variable():
    """Test that PatternPath.variable is now a Variable instance."""
    var = Variable(name="p")
    node = NodePattern(variable=Variable(name="n"), labels=["Person"])
    path = PatternPath(variable=var, elements=[node])

    assert path.variable is not None
    assert isinstance(path.variable, Variable)
    assert path.variable.name == "p"
    print("✓ PatternPath correctly uses Variable instance")


def test_list_comprehension_with_variable():
    """Test that ListComprehension.variable is now a Variable instance."""
    var = Variable(name="x")
    list_comp = ListComprehension(
        variable=var,
        list_expr=Variable(name="items"),
        map_expr=Variable(name="x"),
    )

    assert list_comp.variable is not None
    assert isinstance(list_comp.variable, Variable)
    assert list_comp.variable.name == "x"
    print("✓ ListComprehension correctly uses Variable instance")


def test_set_item_with_variable():
    """Test that SetItem.variable is now a Variable instance."""
    var = Variable(name="n")
    set_item = SetItem(
        variable=var, property="name", expression=Variable(name="newName")
    )

    assert set_item.variable is not None
    assert isinstance(set_item.variable, Variable)
    assert set_item.variable.name == "n"
    print("✓ SetItem correctly uses Variable instance")


def test_remove_item_with_variable():
    """Test that RemoveItem.variable is now a Variable instance."""
    var = Variable(name="n")
    remove_item = RemoveItem(variable=var, property="age")

    assert remove_item.variable is not None
    assert isinstance(remove_item.variable, Variable)
    assert remove_item.variable.name == "n"
    print("✓ RemoveItem correctly uses Variable instance")


def test_quantifier_with_variable():
    """Test that Quantifier.variable is now a Variable instance."""
    var = Variable(name="x")
    quantifier = Quantifier(
        quantifier="ALL", variable=var, list_expr=Variable(name="items")
    )

    assert quantifier.variable is not None
    assert isinstance(quantifier.variable, Variable)
    assert quantifier.variable.name == "x"
    print("✓ Quantifier correctly uses Variable instance")


def test_converter_creates_variable_instances():
    """Test that the converter creates Variable instances from strings."""
    converter = ASTConverter()

    # Test NodePattern conversion
    node_dict = {"type": "NodePattern", "variable": "n", "labels": ["Person"]}
    node = converter.convert(node_dict)

    assert isinstance(node, NodePattern)
    assert node.variable is not None
    assert isinstance(node.variable, Variable)
    assert node.variable.name == "n"
    print("✓ Converter correctly creates Variable instances from strings")


def test_none_variable():
    """Test that None values for variables work correctly."""
    node = NodePattern(variable=None, labels=["Person"])
    assert node.variable is None
    print("✓ None values for variables work correctly")


if __name__ == "__main__":
    print("Testing Variable class refactoring...")
    print()

    test_node_pattern_with_variable()
    test_pattern_path_with_variable()
    test_list_comprehension_with_variable()
    test_set_item_with_variable()
    test_remove_item_with_variable()
    test_quantifier_with_variable()
    test_converter_creates_variable_instances()
    test_none_variable()

    print()
    print("✅ All tests passed! Variable refactoring is working correctly.")
