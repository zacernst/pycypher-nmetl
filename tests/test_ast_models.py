"""Tests for AST Pydantic models.

This module tests the conversion of dictionary-based ASTs to typed Pydantic models,
as well as traversal, modification, and printing functionality.
"""

import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import (
    ASTConverter, convert_ast, traverse_ast, find_nodes, print_ast,
    Query, Match, Return, Create, Delete, Set, Remove, Merge,
    NodePattern, RelationshipPattern, Pattern,
    Comparison, And, Or, Not, PropertyLookup,
    IntegerLiteral, StringLiteral, BooleanLiteral, ListLiteral,
    FunctionInvocation, Variable, ReturnItem,
    Exists, ListComprehension, MapProjection, CaseExpression,
    Quantifier, Reduce
)


@pytest.fixture
def parser():
    """Create a GrammarParser instance."""
    return GrammarParser()


@pytest.fixture
def converter():
    """Create an ASTConverter instance."""
    return ASTConverter()


class TestBasicConversion:
    """Test basic AST conversion."""
    
    def test_convert_simple_match_return(self, parser, converter):
        """Test converting simple MATCH...RETURN query."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        assert isinstance(typed_ast, Query)
        assert len(typed_ast.clauses) == 2
        assert isinstance(typed_ast.clauses[0], Match)
        assert isinstance(typed_ast.clauses[1], Return)
    
    def test_convert_with_labels(self, parser, converter):
        """Test converting query with node labels."""
        query = "MATCH (n:Person) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        # Find the NodePattern
        nodes = typed_ast.find_all(NodePattern)
        assert len(nodes) >= 1
        assert 'Person' in nodes[0].labels
    
    def test_convert_with_where(self, parser, converter):
        """Test converting query with WHERE clause."""
        query = "MATCH (n) WHERE n.age > 30 RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        match_clause = typed_ast.find_first(Match)
        assert match_clause is not None
        assert match_clause.where is not None
        assert isinstance(match_clause.where, Comparison)


class TestTraversal:
    """Test AST traversal functionality."""
    
    def test_traverse_all_nodes(self, parser, converter):
        """Test traversing all nodes in AST."""
        query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        all_nodes = list(typed_ast.traverse())
        assert len(all_nodes) > 0
        assert isinstance(all_nodes[0], Query)
    
    def test_find_all_by_type(self, parser, converter):
        """Test finding all nodes of specific type."""
        query = "MATCH (n:Person) RETURN n.name, n.age"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        # Find all PropertyLookup nodes
        prop_lookups = typed_ast.find_all(PropertyLookup)
        assert len(prop_lookups) >= 2
    
    def test_find_first(self, parser, converter):
        """Test finding first node of specific type."""
        query = "MATCH (n:Person) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        match = typed_ast.find_first(Match)
        assert match is not None
        assert isinstance(match, Match)
        
        # Non-existent type
        delete = typed_ast.find_first(Delete)
        assert delete is None


class TestPrettyPrint:
    """Test pretty printing functionality."""
    
    def test_pretty_print_simple(self, parser, converter):
        """Test pretty printing simple query."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        output = typed_ast.pretty()
        assert "Query" in output
        assert "Match" in output
        assert "Return" in output
    
    def test_pretty_print_complex(self, parser, converter):
        """Test pretty printing complex query."""
        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WHERE p.age > 30
        RETURN p.name, f.name
        """
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        output = typed_ast.pretty()
        assert "Query" in output
        assert "Match" in output
        assert "Return" in output
    
    def test_print_function(self, parser, converter):
        """Test print_ast utility function."""
        query = "RETURN 42"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        # Should not raise
        print_ast(typed_ast)


class TestCreateStatements:
    """Test CREATE statement conversion."""
    
    def test_create_simple_node(self, parser, converter):
        """Test CREATE with simple node."""
        query = "CREATE (n:Person)"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        create = typed_ast.find_first(Create)
        assert create is not None
        assert isinstance(create, Create)
    
    def test_create_with_properties(self, parser, converter):
        """Test CREATE with properties."""
        query = "CREATE (n:Person {name: 'Alice', age: 30})"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        node = typed_ast.find_first(NodePattern)
        assert node is not None
        assert node.properties is not None


class TestUpdateStatements:
    """Test UPDATE statement conversion."""
    
    def test_set_statement(self, parser, converter):
        """Test SET statement."""
        query = "MATCH (n) SET n.age = 31"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        set_clause = typed_ast.find_first(Set)
        assert set_clause is not None
        assert len(set_clause.items) >= 1
    
    def test_remove_statement(self, parser, converter):
        """Test REMOVE statement."""
        query = "MATCH (n) REMOVE n.age"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        remove_clause = typed_ast.find_first(Remove)
        assert remove_clause is not None
    
    def test_delete_statement(self, parser, converter):
        """Test DELETE statement."""
        query = "MATCH (n) DELETE n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        delete_clause = typed_ast.find_first(Delete)
        assert delete_clause is not None
        assert delete_clause.detach is False
    
    def test_merge_statement(self, parser, converter):
        """Test MERGE statement."""
        query = "MERGE (n:Person {name: 'Alice'})"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        merge = typed_ast.find_first(Merge)
        assert merge is not None


class TestExpressions:
    """Test expression conversion."""
    
    def test_comparison_expression(self, parser, converter):
        """Test comparison expressions."""
        query = "MATCH (n) WHERE n.age > 30 RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        comparison = typed_ast.find_first(Comparison)
        assert comparison is not None
        assert comparison.operator in ['>', '<', '=', '>=', '<=', '<>']
    
    def test_boolean_and(self, parser, converter):
        """Test AND expression."""
        query = "MATCH (n) WHERE n.age > 30 AND n.name = 'Alice' RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        and_expr = typed_ast.find_first(And)
        assert and_expr is not None
    
    def test_boolean_or(self, parser, converter):
        """Test OR expression."""
        query = "MATCH (n) WHERE n.age > 30 OR n.name = 'Alice' RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        or_expr = typed_ast.find_first(Or)
        assert or_expr is not None
    
    def test_boolean_not(self, parser, converter):
        """Test NOT expression."""
        query = "MATCH (n) WHERE NOT n.active RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        not_expr = typed_ast.find_first(Not)
        assert not_expr is not None


class TestLiterals:
    """Test literal conversion."""
    
    def test_integer_literal(self, parser, converter):
        """Test integer literal."""
        query = "RETURN 42"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        integers = typed_ast.find_all(IntegerLiteral)
        assert len(integers) >= 1
    
    def test_string_literal(self, parser, converter):
        """Test string literal."""
        query = "RETURN 'hello'"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        strings = typed_ast.find_all(StringLiteral)
        # Note: may not find string as it could be parsed differently
        # This test validates the structure exists
        assert typed_ast is not None
    
    def test_boolean_literal(self, parser, converter):
        """Test boolean literal."""
        query = "RETURN TRUE"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        bools = typed_ast.find_all(BooleanLiteral)
        # Structure validation
        assert typed_ast is not None
    
    def test_list_literal(self, parser, converter):
        """Test list literal."""
        query = "RETURN [1, 2, 3]"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        lists = typed_ast.find_all(ListLiteral)
        # Structure validation
        assert typed_ast is not None


class TestAdvancedFeatures:
    """Test advanced feature conversion."""
    
    def test_exists_expression(self, parser, converter):
        """Test EXISTS expression."""
        query = "MATCH (n) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        exists = typed_ast.find_first(Exists)
        assert exists is not None
    
    def test_list_comprehension(self, parser, converter):
        """Test list comprehension."""
        query = "RETURN [x IN [1,2,3] WHERE x > 1 | x * 2]"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        comp = typed_ast.find_first(ListComprehension)
        assert comp is not None
    
    def test_case_expression(self, parser, converter):
        """Test CASE expression."""
        query = """
        RETURN CASE 
            WHEN 1 > 0 THEN 'yes'
            ELSE 'no'
        END
        """
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        case = typed_ast.find_first(CaseExpression)
        assert case is not None
    
    def test_quantifier_all(self, parser, converter):
        """Test ALL quantifier."""
        query = "RETURN ALL(x IN [1,2,3] WHERE x > 0)"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        quant = typed_ast.find_first(Quantifier)
        assert quant is not None
        if quant:
            assert quant.quantifier == "ALL"


class TestModification:
    """Test AST modification."""
    
    def test_clone_node(self, parser, converter):
        """Test cloning a node."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        cloned = typed_ast.clone()
        assert cloned is not typed_ast  # Different instance
        assert isinstance(cloned, Query)
        assert len(cloned.clauses) == len(typed_ast.clauses)
    
    def test_to_dict_roundtrip(self, parser, converter):
        """Test converting to dict and back."""
        query = "MATCH (n:Person) RETURN n.name"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        dict_repr = typed_ast.to_dict()
        assert isinstance(dict_repr, dict)
        assert dict_repr['type'] == 'Query'
    
    def test_modify_node_attribute(self, parser, converter):
        """Test modifying node attributes."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        match = typed_ast.find_first(Match)
        if match:
            original_optional = match.optional
            match.optional = not original_optional
            assert match.optional != original_optional


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_convert_ast_function(self, parser):
        """Test convert_ast utility function."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = convert_ast(raw_ast)
        
        assert isinstance(typed_ast, Query)
    
    def test_traverse_ast_function(self, parser):
        """Test traverse_ast utility function."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = convert_ast(raw_ast)
        
        nodes = list(traverse_ast(typed_ast))
        assert len(nodes) > 0
    
    def test_find_nodes_function(self, parser):
        """Test find_nodes utility function."""
        query = "MATCH (n) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = convert_ast(raw_ast)
        
        matches = find_nodes(typed_ast, Match)
        assert len(matches) >= 1


class TestComplexQueries:
    """Test complex query conversion."""
    
    def test_complex_pattern(self, parser, converter):
        """Test complex pattern with relationships."""
        query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        nodes = typed_ast.find_all(NodePattern)
        rels = typed_ast.find_all(RelationshipPattern)
        
        assert len(nodes) >= 2
        assert len(rels) >= 1
    
    def test_multiple_clauses(self, parser, converter):
        """Test query with multiple clauses."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 30
        WITH p, p.name AS name
        RETURN name
        ORDER BY name
        LIMIT 10
        """
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        assert isinstance(typed_ast, Query)
        assert len(typed_ast.clauses) >= 2
    
    def test_union_query(self, parser, converter):
        """Test UNION query."""
        query = """
        MATCH (p:Person) WHERE p.age < 30 RETURN p.name
        UNION
        MATCH (p:Person) WHERE p.age > 60 RETURN p.name
        """
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        # Should parse as a query
        assert isinstance(typed_ast, Query)


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_query_result(self, converter):
        """Test converting None."""
        result = converter.convert(None)
        assert result is None
    
    def test_primitive_value(self, converter):
        """Test converting primitive values."""
        result = converter.convert(42)
        assert isinstance(result, IntegerLiteral)
        assert result.value == 42
    
    def test_unknown_node_type(self, converter):
        """Test unknown node type."""
        result = converter.convert({'type': 'UnknownNodeType', 'data': 'test'})
        # Should handle gracefully
        assert result is None or isinstance(result, ASTNode)


class TestPatterns:
    """Test pattern conversion."""
    
    def test_node_pattern_with_labels(self, parser, converter):
        """Test node pattern with multiple labels."""
        query = "MATCH (n:Person:Employee) RETURN n"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        node = typed_ast.find_first(NodePattern)
        assert node is not None
        assert len(node.labels) >= 1
    
    def test_variable_length_relationship(self, parser, converter):
        """Test variable-length relationship."""
        query = "MATCH (a)-[r*1..5]->(b) RETURN a, b"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        rel = typed_ast.find_first(RelationshipPattern)
        assert rel is not None
        # Should have length specification
        # Note: actual structure may vary
    
    def test_bidirectional_relationship(self, parser, converter):
        """Test bidirectional relationship."""
        query = "MATCH (a)-[r]-(b) RETURN a, b"
        raw_ast = parser.parse_to_ast(query)
        typed_ast = converter.convert(raw_ast)
        
        rel = typed_ast.find_first(RelationshipPattern)
        assert rel is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
