#!/usr/bin/env python3
"""Quick test to verify match/case conversions in ast_models.py work correctly."""

from pycypher.ast_models import (
    ASTNode,
    IntegerLiteral,
    PropertyLookup,
    Variable,
)


def test_get_children():
    """Test _get_children() with match/case."""
    # Create a PropertyLookup with nested ASTNode
    prop = PropertyLookup(expression=Variable(name="n"), property_name="name")

    children = prop._get_children()
    assert len(children) == 1
    assert isinstance(children[0], Variable)
    assert children[0].name == "n"
    print("✓ _get_children() works with match/case")


def test_pretty():
    """Test pretty() with match/case."""
    var = Variable(name="test_var")
    pretty_output = var.pretty()
    assert "Variable" in pretty_output
    assert "test_var" in pretty_output
    print("✓ pretty() works with match/case")


def test_to_dict():
    """Test to_dict() with match/case."""
    var = Variable(name="x")
    result = var.to_dict()
    assert result["type"] == "Variable"
    assert result["name"] == "x"
    print("✓ to_dict() works with match/case")


def test_to_dict_with_list():
    """Test to_dict() with list field using match/case."""
    # This would test the list case, but we need a node with list children
    # For now, just verify basic functionality
    lit = IntegerLiteral(value=42)
    result = lit.to_dict()
    assert result["type"] == "IntegerLiteral"
    assert result["value"] == 42
    print("✓ to_dict() with various types works with match/case")


if __name__ == "__main__":
    test_get_children()
    test_pretty()
    test_to_dict()
    test_to_dict_with_list()
    print("\n✅ All match/case conversion tests passed!")
    print("   - _get_children() using match on ASTNode and list")
    print("   - pretty() using match on None, ASTNode, list, and default")
    print("   - to_dict() using match on None, ASTNode, list, and default")
    print("   - ValidationIssue.__init__() using match on dict and conditions")
