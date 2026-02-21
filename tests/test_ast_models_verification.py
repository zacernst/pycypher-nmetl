"""
Verification tests for AST models.

These tests rigorously check the structure and properties of the generated AST
to ensure that the Cypher-to-AST translation is correct.
"""

import pytest
from pycypher.ast_models import (
    ASTConverter,
    Arithmetic,
    BooleanLiteral,
    Call,
    Comparison,
    Create,
    Delete,
    IntegerLiteral,
    ListComprehension,
    ListLiteral,
    Match,
    Merge,
    NodePattern,
    OrderByItem,
    PathLength,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Set,
    SetItem,
    StringLiteral,
    StringPredicate,
    Unwind,
    Variable,
    With,
    YieldItem,
)
from pycypher.grammar_parser import GrammarParser


@pytest.fixture
def parse_to_ast():
    """Helper fixture to parse Cypher directly to typed AST."""
    parser = GrammarParser()
    converter = ASTConverter()

    def _parse(query):
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)
        return converter.convert(ast_dict)

    return _parse


class TestMatchClauseVerification:
    """Verify MATCH clause translation."""

    def test_match_node_structure(self, parse_to_ast):
        """Test simple MATCH (n:Person) structure."""
        query = "MATCH (n:Person) RETURN n"
        ast = parse_to_ast(query)

        assert isinstance(ast, Query)
        assert len(ast.clauses) == 2
        
        match_clause = ast.clauses[0]
        assert isinstance(match_clause, Match)
        
        # Verify Pattern
        assert len(match_clause.pattern.paths) == 1
        path = match_clause.pattern.paths[0]
        assert len(path.elements) == 1
        
        node = path.elements[0]
        assert isinstance(node, NodePattern)
        assert isinstance(node.variable, Variable)
        assert node.variable.name == "n"
        assert node.labels == ["Person"]
        assert node.properties == {}

    def test_match_node_with_properties(self, parse_to_ast):
        """Test MATCH with properties."""
        query = "MATCH (n:Person {name: 'Alice', age: 30}) RETURN n"
        ast = parse_to_ast(query)
        
        match_clause = ast.clauses[0]
        node = match_clause.pattern.paths[0].elements[0]
        
        assert set(node.properties.keys()) == {"name", "age"}

        name_value = node.properties["name"]
        if isinstance(name_value, StringLiteral):
            assert name_value.value == "Alice"
        else:
            assert name_value == "Alice"

        age_value = node.properties["age"]
        if isinstance(age_value, IntegerLiteral):
            assert age_value.value == 30
        else:
            assert age_value == 30

    def test_match_relationship(self, parse_to_ast):
        """Test MATCH (a)-[r:KNOWS]->(b)."""
        query = "MATCH (a)-[r:KNOWS]->(b) RETURN r"
        ast = parse_to_ast(query)
        
        path = ast.clauses[0].pattern.paths[0]
        assert len(path.elements) == 3 # Node, Rel, Node
        
        start_node = path.elements[0]
        rel = path.elements[1]
        end_node = path.elements[2]
        
        assert isinstance(start_node, NodePattern)
        assert start_node.variable.name == "a"
        
        assert isinstance(rel, RelationshipPattern)
        assert rel.variable.name == "r"
        assert rel.labels == ["KNOWS"]
        # Direction might be string or Enum based on AST model definition
        # The AST model has RelationshipDirection enum.
        assert rel.direction == RelationshipDirection.RIGHT
        
        assert isinstance(end_node, NodePattern)
        assert end_node.variable.name == "b"


class TestAdvancedMatchFeatures:
    """Verify advanced MATCH semantics."""

    def test_optional_match_with_path_length(self, parse_to_ast):
        """Test OPTIONAL MATCH with bound path and length range."""
        query = (
            "OPTIONAL MATCH p = (a:Person)-[r:KNOWS*1..3]->(b:Person) "
            "WHERE r.weight > 0 RETURN p"
        )
        ast = parse_to_ast(query)

        match_clause = ast.clauses[0]
        assert isinstance(match_clause, Match)
        assert match_clause.optional is True
        assert isinstance(match_clause.pattern.paths[0], PatternPath)

        path = match_clause.pattern.paths[0]
        assert isinstance(path.variable, Variable)
        assert path.variable.name == "p"
        assert len(path.elements) == 3

        start_node, relationship, end_node = path.elements
        assert isinstance(start_node, NodePattern)
        assert start_node.labels == ["Person"]
        assert isinstance(end_node, NodePattern)
        assert end_node.labels == ["Person"]

        assert isinstance(relationship, RelationshipPattern)
        assert relationship.labels == ["KNOWS"]
        assert isinstance(relationship.length, PathLength)
        assert relationship.length.min == 1
        assert relationship.length.max == 3
        assert relationship.length.unbounded is False

        where_condition = match_clause.where
        assert isinstance(where_condition, Comparison)
        assert where_condition.operator == ">"
        assert isinstance(where_condition.left, PropertyLookup)
        assert where_condition.left.property == "weight"
        assert isinstance(where_condition.left.expression, Variable)
        assert where_condition.left.expression.name == "r"


class TestWhereClauseVerification:
    """Verify WHERE clause structure."""

    def test_where_comparison(self, parse_to_ast):
        """Test WHERE n.age > 30."""
        query = "MATCH (n) WHERE n.age > 30 RETURN n"
        ast = parse_to_ast(query)
        
        match_clause = ast.clauses[0]
        assert match_clause.where is not None
        where = match_clause.where
        
        assert isinstance(where, Comparison)
        assert where.operator == ">"
        
        assert isinstance(where.left, PropertyLookup)
        assert where.left.property == "age"
        assert isinstance(where.left.expression, Variable)
        assert where.left.expression.name == "n"
        
        assert isinstance(where.right, IntegerLiteral)
        assert where.right.value == 30

    def test_where_arithmetic(self, parse_to_ast):
        """Test WHERE n.age + 5 = 40."""
        query = "MATCH (n) WHERE n.age + 5 = 40 RETURN n"
        ast = parse_to_ast(query)
        
        where = ast.clauses[0].where
        assert isinstance(where, Comparison)
        assert where.operator == "="
        
        # Left side should be Arithmetic
        assert isinstance(where.left, Arithmetic)
        assert where.left.operator == "+"
        assert isinstance(where.left.left, PropertyLookup)
        assert isinstance(where.left.right, IntegerLiteral)
        assert where.left.right.value == 5


class TestReturnClauseVerification:
    """Verify RETURN clause structure."""

    def test_return_alias(self, parse_to_ast):
        """Test RETURN n.name AS name."""
        query = "MATCH (n) RETURN n.name AS name"
        ast = parse_to_ast(query)
        
        return_clause = ast.clauses[1]
        assert isinstance(return_clause, Return)
        
        assert len(return_clause.items) == 1
        item = return_clause.items[0]
        assert isinstance(item, ReturnItem)
        assert item.alias == "name"
        
        assert isinstance(item.expression, PropertyLookup)
        assert item.expression.property == "name"

    def test_return_distinct(self, parse_to_ast):
        """Test RETURN DISTINCT n."""
        query = "MATCH (n) RETURN DISTINCT n"
        ast = parse_to_ast(query)
        
        return_clause = ast.clauses[1]
        assert isinstance(return_clause, Return)
        assert return_clause.distinct is True


class TestCreateClauseVerification:
    """Verify CREATE clause structure."""

    def test_create_node(self, parse_to_ast):
        """Test CREATE (n:Person {id: 1})."""
        query = "CREATE (n:Person {id: 1})"
        ast = parse_to_ast(query)
        
        assert isinstance(ast.clauses[0], Create)
        create_clause = ast.clauses[0]
        
        path = create_clause.pattern.paths[0]
        node = path.elements[0]
        
        assert isinstance(node, NodePattern)
        assert node.labels == ["Person"]
        assert "id" in node.properties


class TestDeleteClauseVerification:
    """Verify DELETE clause structure."""

    def test_detach_delete(self, parse_to_ast):
        """Test DETACH DELETE n."""
        query = "MATCH (n) DETACH DELETE n"
        ast = parse_to_ast(query)
        
        delete_clause = ast.clauses[1]
        assert isinstance(delete_clause, Delete)
        assert delete_clause.detach is True
        
        assert len(delete_clause.expressions) == 1
        expr = delete_clause.expressions[0]
        assert isinstance(expr, Variable)
        assert expr.name == "n"

class TestSetClauseVerification:
    """Verify SET clause structure."""

    def test_set_property(self, parse_to_ast):
        """Test SET n.age = 30."""
        query = "MATCH (n) SET n.age = 30 RETURN n"
        ast = parse_to_ast(query)

        set_clause = ast.clauses[1]
        assert isinstance(set_clause, Set)
        assert len(set_clause.items) == 1
        
        item = set_clause.items[0]
        assert isinstance(item, SetItem)
        assert isinstance(item.variable, Variable)
        assert item.variable.name == "n"
        assert item.property == "age"
        assert isinstance(item.expression, IntegerLiteral)
        assert item.expression.value == 30


class TestUnwindClauseVerification:
    """Verify UNWIND clause translation."""

    def test_unwind_list_literal(self, parse_to_ast):
        """Test UNWIND [1, 2, 3] AS x."""
        query = "UNWIND [1, 2, 3] AS x RETURN x"
        ast = parse_to_ast(query)

        unwind_clause = ast.clauses[0]
        assert isinstance(unwind_clause, Unwind)
        assert unwind_clause.alias == "x"
        assert isinstance(unwind_clause.expression, ListLiteral)
        assert [elem.value for elem in unwind_clause.expression.elements] == [1, 2, 3]

        return_clause = ast.clauses[1]
        assert isinstance(return_clause, Return)
        assert isinstance(return_clause.items[0].expression, Variable)
        assert return_clause.items[0].expression.name == "x"


class TestWithClauseVerification:
    """Verify WITH clause translation."""

    def test_with_distinct_and_ordering(self, parse_to_ast):
        """Test WITH DISTINCT with ordering, skip, and limit."""
        query = (
            "MATCH (n) WITH DISTINCT n ORDER BY n.name DESC SKIP 5 LIMIT 10 RETURN n"
        )
        ast = parse_to_ast(query)

        with_clause = ast.clauses[1]
        assert isinstance(with_clause, With)
        assert with_clause.distinct is True
        assert len(with_clause.items) == 1
        assert isinstance(with_clause.items[0], ReturnItem)

        assert with_clause.skip == 5
        assert with_clause.limit == 10

        assert with_clause.order_by is not None
        assert len(with_clause.order_by) == 1
        order_item = with_clause.order_by[0]
        assert isinstance(order_item, OrderByItem)
        assert order_item.ascending is False
        assert isinstance(order_item.expression, PropertyLookup)
        assert order_item.expression.property == "name"
        assert isinstance(order_item.expression.expression, Variable)
        assert order_item.expression.expression.name == "n"

        return_clause = ast.clauses[2]
        assert isinstance(return_clause, Return)
        assert isinstance(return_clause.items[0].expression, Variable)
        assert return_clause.items[0].expression.name == "n"


class TestMergeClauseVerification:
    """Verify MERGE clause translation."""

    def test_merge_with_actions(self, parse_to_ast):
        """Test MERGE with ON MATCH and ON CREATE SET clauses."""
        query = (
            "MERGE (n:Person {id: 1}) "
            "ON MATCH SET n.seen = true "
            "ON CREATE SET n.createdAt = 123 "
            "RETURN n"
        )
        ast = parse_to_ast(query)

        merge_clause = ast.clauses[0]
        assert isinstance(merge_clause, Merge)
        assert isinstance(merge_clause.pattern.paths[0].elements[0], NodePattern)
        node = merge_clause.pattern.paths[0].elements[0]
        assert node.labels == ["Person"]
        id_value = node.properties["id"]
        if isinstance(id_value, IntegerLiteral):
            assert id_value.value == 1
        else:
            assert id_value == 1

        assert merge_clause.on_match is not None
        assert len(merge_clause.on_match) == 1
        match_set = merge_clause.on_match[0]
        assert isinstance(match_set, SetItem)
        assert match_set.property == "seen"
        if isinstance(match_set.expression, BooleanLiteral):
            assert match_set.expression.value is True
        else:
            assert match_set.expression is True

        assert merge_clause.on_create is not None
        assert len(merge_clause.on_create) == 1
        create_set = merge_clause.on_create[0]
        assert isinstance(create_set.expression, IntegerLiteral)
        assert create_set.expression.value == 123

        return_clause = ast.clauses[1]
        assert isinstance(return_clause, Return)
        assert isinstance(return_clause.items[0].expression, Variable)
        assert return_clause.items[0].expression.name == "n"


class TestCallClauseVerification:
    """Verify CALL clause translation."""

    def test_call_with_yield_and_filter(self, parse_to_ast):
        """Test CALL db.labels() YIELD label WHERE filter."""
        query = "CALL db.labels() YIELD label WHERE label CONTAINS 'User' RETURN label"
        ast = parse_to_ast(query)

        call_clause = ast.clauses[0]
        assert isinstance(call_clause, Call)
        assert call_clause.procedure_name == "db.labels"
        assert call_clause.arguments == []
        assert len(call_clause.yield_items) == 1

        yield_item = call_clause.yield_items[0]
        assert isinstance(yield_item, YieldItem)
        assert isinstance(yield_item.variable, Variable)
        assert yield_item.variable.name == "label"

        assert isinstance(call_clause.where, StringPredicate)
        assert call_clause.where.operator == "CONTAINS"
        assert isinstance(call_clause.where.left, Variable)
        assert call_clause.where.left.name == "label"
        if isinstance(call_clause.where.right, StringLiteral):
            assert call_clause.where.right.value == "User"
        else:
            assert call_clause.where.right == "User"

        return_clause = ast.clauses[1]
        assert isinstance(return_clause, Return)
        assert len(return_clause.items) == 1
        assert isinstance(return_clause.items[0].expression, Variable)
        assert return_clause.items[0].expression.name == "label"


class TestComprehensionExpressionVerification:
    """Verify list comprehension expressions."""

    def test_list_comprehension_structure(self, parse_to_ast):
        """Test RETURN with list comprehension transformation."""
        query = "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS doubled"
        ast = parse_to_ast(query)

        return_clause = ast.clauses[0]
        assert isinstance(return_clause, Return)
        item = return_clause.items[0]
        assert item.alias == "doubled"

        expression = item.expression
        assert isinstance(expression, ListComprehension)
        assert isinstance(expression.variable, Variable)
        assert expression.variable.name == "x"

        assert isinstance(expression.list_expr, ListLiteral)
        assert [elem.value for elem in expression.list_expr.elements] == [1, 2, 3]

        assert isinstance(expression.where, Comparison)
        assert isinstance(expression.where.left, Variable)
        assert expression.where.left.name == "x"
        assert isinstance(expression.where.right, IntegerLiteral)
        assert expression.where.right.value == 1

        assert isinstance(expression.map_expr, Arithmetic)
        assert expression.map_expr.operator == "*"
