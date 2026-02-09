"""Comprehensive unit tests for ast_models module.

This module provides complete test coverage for the Pydantic-based AST models,
including node creation, conversion, traversal, and utility methods.
"""

import pytest
from pycypher.ast_models import (
    And,
    Arithmetic,
    ASTConverter,
    ASTNode,
    BooleanLiteral,
    Call,
    CaseExpression,
    Comparison,
    CountStar,
    Create,
    Delete,
    FloatLiteral,
    FunctionInvocation,
    IntegerLiteral,
    ListComprehension,
    ListLiteral,
    MapLiteral,
    Match,
    Merge,
    NodePattern,
    Not,
    NullLiteral,
    Or,
    OrderByItem,
    Parameter,
    PathLength,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipPattern,
    Remove,
    RemoveItem,
    Return,
    ReturnItem,
    Set,
    SetItem,
    StringLiteral,
    Unwind,
    Variable,
    WhenClause,
    With,
    YieldItem,
)
from pycypher.grammar_parser import GrammarParser


@pytest.fixture
def parser():
    """Create a GrammarParser instance."""
    return GrammarParser()


@pytest.fixture
def converter():
    """Create an ASTConverter instance."""
    return ASTConverter()


# =============================================================================
# Test Primitive Conversion
# =============================================================================


class TestPrimitiveConversion:
    """Test _convert_primitive method behavior."""

    def test_convert_boolean_true(self, converter):
        """Test that True is returned as-is."""
        result = converter._convert_primitive(True)
        assert result is True

    def test_convert_boolean_false(self, converter):
        """Test that False is returned as-is."""
        result = converter._convert_primitive(False)
        assert result is False

    def test_convert_integer(self, converter):
        """Test that integers are returned as-is."""
        result = converter._convert_primitive(42)
        assert result == 42
        assert isinstance(result, int)

    def test_convert_float(self, converter):
        """Test that floats are returned as-is."""
        result = converter._convert_primitive(3.14)
        assert result == 3.14
        assert isinstance(result, float)

    def test_convert_string(self, converter):
        """Test that strings are returned as-is."""
        result = converter._convert_primitive("hello")
        assert result == "hello"
        assert isinstance(result, str)

    def test_convert_none(self, converter):
        """Test that None is returned as-is."""
        result = converter._convert_primitive(None)
        assert result is None

    def test_convert_empty_list(self, converter):
        """Test that empty lists are returned as-is."""
        result = converter._convert_primitive([])
        assert result == []
        assert isinstance(result, list)

    def test_convert_empty_dict(self, converter):
        """Test that empty dicts are returned as-is."""
        result = converter._convert_primitive({})
        assert result == {}
        assert isinstance(result, dict)


# =============================================================================
# Test Node Creation
# =============================================================================


class TestNodeCreation:
    """Test direct creation of AST nodes."""

    def test_create_variable(self):
        """Test creating a Variable node."""
        var = Variable(name="x")
        assert var.name == "x"
        assert isinstance(var, Variable)

    def test_create_integer_literal(self):
        """Test creating an IntegerLiteral node."""
        lit = IntegerLiteral(value=42)
        assert lit.value == 42

    def test_create_boolean_literal(self):
        """Test creating a BooleanLiteral node."""
        lit_true = BooleanLiteral(value=True)
        lit_false = BooleanLiteral(value=False)
        assert lit_true.value is True
        assert lit_false.value is False

    def test_create_property_lookup(self):
        """Test creating a PropertyLookup node."""
        var = Variable(name="n")
        prop = PropertyLookup(expression=var, property="name")
        assert prop.property == "name"
        assert isinstance(prop.expression, Variable)


# =============================================================================
# Test AST Traversal
# =============================================================================


class TestASTTraversal:
    """Test AST traversal methods."""

    def test_traverse_single_node(self):
        """Test traversing a single node."""
        var = Variable(name="x")
        nodes = list(var.traverse())
        assert len(nodes) == 1
        assert nodes[0] is var

    def test_traverse_nested_nodes(self):
        """Test traversing nested nodes."""
        var = Variable(name="n")
        prop = PropertyLookup(expression=var, property="name")
        nodes = list(prop.traverse())
        assert len(nodes) >= 2
        assert any(isinstance(n, PropertyLookup) for n in nodes)
        assert any(isinstance(n, Variable) for n in nodes)

    def test_find_first_by_type(self):
        """Test finding first node by type."""
        var = Variable(name="n")
        prop = PropertyLookup(expression=var, property="name")
        found = prop.find_first(Variable)
        assert found is not None
        assert isinstance(found, Variable)
        assert found.name == "n"

    def test_find_first_by_predicate(self):
        """Test finding first node by predicate function."""
        var = Variable(name="test_var")
        prop = PropertyLookup(expression=var, property="test_prop")
        found = prop.find_first(
            lambda n: isinstance(n, Variable) and n.name == "test_var"
        )
        assert found is not None
        assert found.name == "test_var"

    def test_find_all_by_type(self):
        """Test finding all nodes by type."""
        var1 = Variable(name="a")
        var2 = Variable(name="b")
        comp = Comparison(left=var1, operator="=", right=var2)
        found = comp.find_all(Variable)
        assert len(found) == 2

    def test_find_all_by_predicate(self):
        """Test finding all nodes by predicate function."""
        var = Variable(name="x")
        prop = PropertyLookup(expression=var, property="y")
        # Find all nodes (should return all traversable nodes)
        found = prop.find_all(lambda n: True)
        assert len(found) >= 2


# =============================================================================
# Test Query Parsing and Conversion
# =============================================================================


class TestQueryConversion:
    """Test converting parsed queries to AST models."""

    def test_simple_match_return(self, parser, converter):
        """Test parsing and converting simple MATCH RETURN query."""
        query = "MATCH (n) RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None
        assert isinstance(typed_ast, Query)

    def test_match_with_label(self, parser, converter):
        """Test MATCH with node label."""
        query = "MATCH (n:Person) RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None
        assert isinstance(typed_ast, Query)

    def test_match_with_property(self, parser, converter):
        """Test MATCH with property filter."""
        query = "MATCH (n:Person {name: 'Alice'}) RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_match_with_relationship(self, parser, converter):
        """Test MATCH with relationship pattern."""
        query = "MATCH (a)-[r:KNOWS]->(b) RETURN a, b"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_create_statement(self, parser, converter):
        """Test CREATE statement."""
        query = "CREATE (n:Person {name: 'Bob'})"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_return_with_alias(self, parser, converter):
        """Test RETURN with alias."""
        query = "MATCH (n) RETURN n.name AS name"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None


# =============================================================================
# Test Expression Handling
# =============================================================================


class TestExpressions:
    """Test expression parsing and conversion."""

    def test_arithmetic_expression(self, parser, converter):
        """Test arithmetic expression."""
        query = "RETURN 1 + 2"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_comparison_expression(self, parser, converter):
        """Test comparison expression."""
        query = "MATCH (n) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_boolean_expression(self, parser, converter):
        """Test boolean expression."""
        query = "MATCH (n) WHERE n.age > 30 AND n.active = true RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_function_call(self, parser, converter):
        """Test function invocation."""
        query = "RETURN count(*)"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_greater_than_operator_in_where(self, parser, converter):
        """Test that greater-than operator is correctly parsed in WHERE clause."""
        query = "MATCH (n) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        # Verify basic structure
        assert isinstance(typed_ast, Query)
        assert len(typed_ast.clauses) == 2

        # Verify MATCH clause
        match_clause = typed_ast.clauses[0]
        assert isinstance(match_clause, Match)
        assert match_clause.where is not None

        # Verify WHERE condition is a Comparison
        where_condition = match_clause.where
        assert isinstance(where_condition, Comparison)
        assert where_condition.operator == ">"

        # Verify left operand is property lookup (n.age)
        assert isinstance(where_condition.left, PropertyLookup)
        assert where_condition.left.property == "age"
        assert isinstance(where_condition.left.expression, Variable)
        assert where_condition.left.expression.name == "n"

        # Verify right operand is integer literal (30)
        assert isinstance(where_condition.right, IntegerLiteral)
        assert where_condition.right.value == 30


# =============================================================================
# Test Literal Values
# =============================================================================


class TestLiterals:
    """Test literal value handling."""

    def test_integer_literal_in_query(self, parser, converter):
        """Test integer literal in query."""
        query = "RETURN 42"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_float_literal_in_query(self, parser, converter):
        """Test float literal in query."""
        query = "RETURN 3.14"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_string_literal_in_query(self, parser, converter):
        """Test string literal in query."""
        query = "RETURN 'hello'"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_boolean_literal_in_query(self, parser, converter):
        """Test boolean literal in query."""
        query = "RETURN true, false"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_null_literal_in_query(self, parser, converter):
        """Test null literal in query."""
        query = "RETURN null"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None


# =============================================================================
# Test Advanced Features
# =============================================================================


class TestAdvancedFeatures:
    """Test advanced Cypher features."""

    def test_with_clause(self, parser, converter):
        """Test WITH clause."""
        query = "MATCH (n) WITH n.name AS name RETURN name"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_order_by(self, parser, converter):
        """Test ORDER BY clause."""
        query = "MATCH (n) RETURN n ORDER BY n.name"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_limit_skip(self, parser, converter):
        """Test LIMIT and SKIP."""
        query = "MATCH (n) RETURN n SKIP 10 LIMIT 5"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_unwind(self, parser, converter):
        """Test UNWIND statement."""
        query = "UNWIND [1, 2, 3] AS x RETURN x"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None

    def test_count_star(self, parser, converter):
        """Test count(*) function."""
        query = "MATCH (n) RETURN count(*)"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_convert_none_value(self, converter):
        """Test converting None returns None."""
        result = converter.convert(None)
        assert result is None

    def test_convert_empty_dict(self, converter):
        """Test converting empty dict."""
        result = converter.convert({})
        assert result is None

    def test_convert_dict_without_type(self, converter):
        """Test converting dict without 'type' field."""
        result = converter.convert({"foo": "bar"})
        # Should return None or handle gracefully
        assert result is None or isinstance(result, MapLiteral)

    def test_multiple_statements(self, parser, converter):
        """Test multiple statements in one query."""
        query = """
        MATCH (n) RETURN n
        UNION
        MATCH (m) RETURN m
        """
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        typed_ast = converter.convert(ast_dict)

        assert typed_ast is not None


# =============================================================================
# Test Converter Methods
# =============================================================================


class TestConverterMethods:
    """Test specific converter methods."""

    def test_convert_returns_correct_type_for_query(self, parser, converter):
        """Test that convert returns Query for valid queries."""
        query = "MATCH (n) RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        result = converter.convert(ast_dict)

        assert isinstance(result, Query)

    def test_converter_handles_variables_in_context(self, converter):
        """Test that converter properly wraps strings as Variables in AST context."""
        # When converting a string in AST context, it should become a Variable
        node_dict = {
            "type": "PropertyAccess",
            "object": "n",
            "property": "name",
        }
        result = converter.convert(node_dict)

        assert result is not None
        # The 'object' field should be converted to a Variable
        if hasattr(result, "expression"):
            assert isinstance(result.expression, Variable)


# =============================================================================
# Test Pretty Printing
# =============================================================================


class TestPrettyPrinting:
    """Test pretty printing functionality."""

    def test_pretty_print_variable(self):
        """Test pretty printing a variable."""
        var = Variable(name="x")
        output = var.pretty()
        assert isinstance(output, str)
        assert "Variable" in output or "x" in output

    def test_pretty_print_complex_expression(self):
        """Test pretty printing complex expression."""
        var1 = Variable(name="a")
        var2 = Variable(name="b")
        comp = Comparison(left=var1, operator="=", right=var2)
        output = comp.pretty()
        assert isinstance(output, str)


# =============================================================================
# Test Pattern Matching
# =============================================================================


class TestPatterns:
    """Test pattern matching functionality."""

    def test_node_pattern_creation(self):
        """Test creating a node pattern."""
        node = NodePattern(variable=Variable(name="n"), labels=["Person"])
        assert node.variable.name == "n"
        assert node.labels == ["Person"]

    def test_relationship_pattern_creation(self):
        """Test creating a relationship pattern."""
        rel = RelationshipPattern(
            variable=Variable(name="r"), types=["KNOWS"], direction="outgoing"
        )
        assert rel.variable.name == "r"
        assert rel.types == ["KNOWS"]
        assert rel.direction == "outgoing"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
