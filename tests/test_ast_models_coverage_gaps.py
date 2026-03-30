"""Unit tests targeting coverage gaps in ast_models.py.

This test file specifically addresses untested conversion methods and edge cases
identified in the coverage gap analysis. Focus areas:
- Quantifier expressions (ALL, ANY, SINGLE, NONE)
- REDUCE expressions
- Map projections with complex properties
- Pattern comprehensions with WHERE clauses
- Complex CASE expressions (simple and searched)
- Variable-length paths with WHERE conditions
- Edge cases in conversion methods
- Validation framework edge cases
"""

from pycypher.ast_models import (
    And,
    Arithmetic,
    ASTConverter,
    BooleanLiteral,
    CaseExpression,
    Comparison,
    IntegerLiteral,
    ListComprehension,
    ListLiteral,
    MapElement,
    MapLiteral,
    MapProjection,
    NodePattern,
    Not,
    Or,
    Parameter,
    PatternComprehension,
    PropertyLookup,
    Quantifier,
    Reduce,
    RelationshipDirection,
    RelationshipPattern,
    StringLiteral,
    Unary,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
    Variable,
    WhenClause,
    Xor,
)
from pycypher.grammar_parser import GrammarParser


class TestQuantifierConversions:
    """Test conversion of quantifier expressions (ALL, ANY, SINGLE, NONE)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()
        self.parser = GrammarParser()

    def test_convert_all_quantifier(self):
        """Test conversion of ALL quantifier expression."""
        node = {
            "type": "All",
            "variable": "x",
            "list": {"type": "Variable", "name": "items"},
            "where": {
                "type": "Comparison",
                "operator": ">",
                "left": {"type": "Variable", "name": "x"},
                "right": {"type": "IntegerLiteral", "value": 0},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Quantifier)
        assert result.quantifier == "ALL"
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "x"
        assert isinstance(result.list_expr, Variable)
        assert result.list_expr.name == "items"
        assert isinstance(result.where, Comparison)

    def test_convert_any_quantifier(self):
        """Test conversion of ANY quantifier expression."""
        node = {
            "type": "Any",
            "variable": "x",
            "list": {"type": "ListLiteral", "value": [1, 2, 3]},
            "where": {
                "type": "Comparison",
                "operator": "=",
                "left": {"type": "Variable", "name": "x"},
                "right": {"type": "IntegerLiteral", "value": 2},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Quantifier)
        assert result.quantifier == "ANY"
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "x"
        assert isinstance(result.list_expr, ListLiteral)
        assert isinstance(result.where, Comparison)

    def test_convert_single_quantifier(self):
        """Test conversion of SINGLE quantifier expression."""
        node = {
            "type": "Single",
            "variable": "item",
            "list": {"type": "Variable", "name": "collection"},
            "predicate": {
                "type": "PropertyLookup",
                "expression": {"type": "Variable", "name": "item"},
                "property": "active",
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Quantifier)
        assert result.quantifier == "SINGLE"
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "item"

    def test_convert_none_quantifier(self):
        """Test conversion of NONE quantifier expression."""
        node = {
            "type": "None",
            "variable": "n",
            "list": {"type": "Variable", "name": "nodes"},
            "where": {
                "type": "Comparison",
                "operator": "=",
                "left": {
                    "type": "PropertyLookup",
                    "expression": {"type": "Variable", "name": "n"},
                    "property": "blocked",
                },
                "right": {"type": "BooleanLiteral", "value": True},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Quantifier)
        assert result.quantifier == "NONE"
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "n"
        assert isinstance(result.where, Comparison)

    def test_quantifier_without_predicate(self):
        """Test quantifier expression without predicate (edge case)."""
        node = {
            "type": "All",
            "variable": "x",
            "list": {"type": "Variable", "name": "items"},
        }

        result = self.converter.convert(node)

        assert isinstance(result, Quantifier)
        assert result.quantifier == "ALL"
        assert result.where is None


class TestReduceConversions:
    """Test conversion of REDUCE expressions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_simple_reduce(self):
        """Test conversion of simple REDUCE expression."""
        node = {
            "type": "Reduce",
            "accumulator": "sum",
            "initial": {"type": "IntegerLiteral", "value": 0},
            "variable": "x",
            "list": {"type": "ListLiteral", "value": [1, 2, 3]},
            "map": {
                "type": "Arithmetic",
                "operator": "+",
                "left": {"type": "Variable", "name": "sum"},
                "right": {"type": "Variable", "name": "x"},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Reduce)
        assert isinstance(result.accumulator, Variable)
        assert result.accumulator.name == "sum"
        assert isinstance(result.initial, IntegerLiteral)
        assert result.initial.value == 0
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "x"
        assert isinstance(result.map_expr, Arithmetic)

    def test_convert_complex_reduce(self):
        """Test REDUCE with nested expression."""
        node = {
            "type": "Reduce",
            "accumulator": "result",
            "initial": {"type": "ListLiteral", "value": []},
            "variable": "item",
            "list": {"type": "Variable", "name": "collection"},
            "map": {
                "type": "Arithmetic",
                "operator": "+",
                "left": {"type": "Variable", "name": "result"},
                "right": {
                    "type": "ListLiteral",
                    "elements": [
                        {
                            "type": "Arithmetic",
                            "operator": "*",
                            "left": {"type": "Variable", "name": "item"},
                            "right": {"type": "IntegerLiteral", "value": 2},
                        },
                    ],
                },
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Reduce)
        assert isinstance(result.accumulator, Variable)
        assert result.accumulator.name == "result"
        assert isinstance(result.initial, ListLiteral)
        assert isinstance(result.map_expr, Arithmetic)


class TestMapProjectionConversions:
    """Test conversion of map projection expressions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_simple_map_projection(self):
        """Test conversion of simple map projection."""
        node = {
            "type": "MapProjection",
            "variable": "person",
            "elements": [
                {"type": "MapElement", "property": "name"},
                {"type": "MapElement", "property": "age"},
            ],
        }

        result = self.converter.convert(node)

        assert isinstance(result, MapProjection)
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "person"
        assert len(result.elements) == 2
        assert all(isinstance(e, MapElement) for e in result.elements)

    def test_convert_map_projection_with_computed_properties(self):
        """Test map projection with computed properties."""
        node = {
            "type": "MapProjection",
            "variable": "n",
            "elements": [
                {"type": "MapElement", "property": "name"},
                {
                    "type": "MapElement",
                    "property": "isAdult",
                    "expression": {
                        "type": "Comparison",
                        "operator": ">=",
                        "left": {
                            "type": "PropertyLookup",
                            "expression": {"type": "Variable", "name": "n"},
                            "property": "age",
                        },
                        "right": {"type": "IntegerLiteral", "value": 18},
                    },
                },
            ],
        }

        result = self.converter.convert(node)

        assert isinstance(result, MapProjection)
        assert len(result.elements) == 2
        assert result.elements[0].property == "name"
        assert result.elements[1].property == "isAdult"
        assert isinstance(result.elements[1].expression, Comparison)

    def test_convert_map_projection_with_all_properties(self):
        """Test map projection with .* (all properties)."""
        node = {
            "type": "MapProjection",
            "variable": "node",
            "elements": [{"type": "MapElement", "all_properties": True}],
            "include_all": True,
        }

        result = self.converter.convert(node)

        assert isinstance(result, MapProjection)
        assert result.include_all is True
        assert len(result.elements) == 1
        assert result.elements[0].all_properties is True


class TestPatternComprehensionConversions:
    """Test conversion of pattern comprehension expressions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_simple_pattern_comprehension(self):
        """Test conversion of pattern comprehension without WHERE."""
        node = {
            "type": "PatternComprehension",
            "variable": "path",
            "pattern": {
                "type": "Pattern",
                "paths": [
                    {
                        "type": "PatternPath",
                        "elements": [
                            {
                                "type": "NodePattern",
                                "variable": "a",
                                "labels": ["Person"],
                            },
                            {
                                "type": "RelationshipPattern",
                                "labels": ["KNOWS"],
                                "direction": "->",
                            },
                            {
                                "type": "NodePattern",
                                "variable": "b",
                                "labels": ["Person"],
                            },
                        ],
                    },
                ],
            },
            "map": {
                "type": "PropertyLookup",
                "expression": {"type": "Variable", "name": "b"},
                "property": "name",
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, PatternComprehension)
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "path"
        assert result.pattern is not None
        assert isinstance(result.map_expr, PropertyLookup)

    def test_convert_pattern_comprehension_with_where(self):
        """Test pattern comprehension with WHERE clause."""
        node = {
            "type": "PatternComprehension",
            "variable": "p",
            "pattern": {
                "type": "Pattern",
                "paths": [
                    {
                        "type": "PatternPath",
                        "elements": [
                            {
                                "type": "NodePattern",
                                "variable": "n",
                                "labels": ["Person"],
                            },
                        ],
                    },
                ],
            },
            "where": {
                "type": "Comparison",
                "operator": ">",
                "left": {
                    "type": "PropertyLookup",
                    "expression": {"type": "Variable", "name": "n"},
                    "property": "age",
                },
                "right": {"type": "IntegerLiteral", "value": 30},
            },
            "map": {"type": "Variable", "name": "n"},
        }

        result = self.converter.convert(node)

        assert isinstance(result, PatternComprehension)
        assert result.where is not None
        assert isinstance(result.where, Comparison)
        assert isinstance(result.map_expr, Variable)


class TestCaseExpressionConversions:
    """Test conversion of CASE expressions (simple and searched)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_simple_case(self):
        """Test conversion of simple CASE expression."""
        node = {
            "type": "SimpleCase",
            "operand": {"type": "Variable", "name": "status"},
            "when": [
                {
                    "type": "SimpleWhen",
                    "value": {"type": "StringLiteral", "value": "active"},
                    "result": {"type": "IntegerLiteral", "value": 1},
                },
                {
                    "type": "SimpleWhen",
                    "value": {"type": "StringLiteral", "value": "pending"},
                    "result": {"type": "IntegerLiteral", "value": 0},
                },
            ],
            "else": {"type": "IntegerLiteral", "value": -1},
        }

        result = self.converter.convert(node)

        assert isinstance(result, CaseExpression)
        assert isinstance(result.expression, Variable)
        assert result.expression.name == "status"
        assert len(result.when_clauses) == 2
        assert all(isinstance(w, WhenClause) for w in result.when_clauses)
        assert isinstance(result.else_expr, IntegerLiteral)

    def test_convert_searched_case(self):
        """Test conversion of searched CASE expression."""
        node = {
            "type": "SearchedCase",
            "when": [
                {
                    "type": "SearchedWhen",
                    "condition": {
                        "type": "Comparison",
                        "operator": "<",
                        "left": {"type": "Variable", "name": "age"},
                        "right": {"type": "IntegerLiteral", "value": 18},
                    },
                    "result": {"type": "StringLiteral", "value": "minor"},
                },
                {
                    "type": "SearchedWhen",
                    "condition": {
                        "type": "Comparison",
                        "operator": "<",
                        "left": {"type": "Variable", "name": "age"},
                        "right": {"type": "IntegerLiteral", "value": 65},
                    },
                    "result": {"type": "StringLiteral", "value": "adult"},
                },
            ],
            "else": {"type": "StringLiteral", "value": "senior"},
        }

        result = self.converter.convert(node)

        assert isinstance(result, CaseExpression)
        assert result.expression is None  # Searched case has no test expression
        assert len(result.when_clauses) == 2
        assert all(w.condition is not None for w in result.when_clauses)
        assert isinstance(result.else_expr, StringLiteral)

    def test_convert_case_without_else(self):
        """Test CASE expression without ELSE clause."""
        node = {
            "type": "SearchedCase",
            "when": [
                {
                    "type": "SearchedWhen",
                    "condition": {
                        "type": "Comparison",
                        "operator": "=",
                        "left": {"type": "Variable", "name": "x"},
                        "right": {"type": "IntegerLiteral", "value": 1},
                    },
                    "result": {"type": "StringLiteral", "value": "one"},
                },
            ],
        }

        result = self.converter.convert(node)

        assert isinstance(result, CaseExpression)
        assert result.else_expr is None

    def test_convert_nested_case(self):
        """Test nested CASE expressions."""
        node = {
            "type": "SearchedCase",
            "when": [
                {
                    "type": "SearchedWhen",
                    "condition": {
                        "type": "Comparison",
                        "operator": ">",
                        "left": {"type": "Variable", "name": "score"},
                        "right": {"type": "IntegerLiteral", "value": 90},
                    },
                    "result": {
                        "type": "SearchedCase",
                        "when": [
                            {
                                "type": "SearchedWhen",
                                "condition": {
                                    "type": "Comparison",
                                    "operator": ">=",
                                    "left": {
                                        "type": "Variable",
                                        "name": "score",
                                    },
                                    "right": {
                                        "type": "IntegerLiteral",
                                        "value": 95,
                                    },
                                },
                                "result": {
                                    "type": "StringLiteral",
                                    "value": "A+",
                                },
                            },
                        ],
                        "else": {"type": "StringLiteral", "value": "A"},
                    },
                },
            ],
            "else": {"type": "StringLiteral", "value": "B"},
        }

        result = self.converter.convert(node)

        assert isinstance(result, CaseExpression)
        assert len(result.when_clauses) == 1
        # The result is a nested CASE expression
        assert isinstance(result.when_clauses[0].result, CaseExpression)


class TestListComprehensionConversions:
    """Test conversion of list comprehension expressions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_list_comprehension_with_filter_and_map(self):
        """Test list comprehension with both WHERE and map expression."""
        node = {
            "type": "ListComprehension",
            "variable": "x",
            "in": {"type": "Variable", "name": "numbers"},
            "where": {
                "type": "Comparison",
                "operator": ">",
                "left": {"type": "Variable", "name": "x"},
                "right": {"type": "IntegerLiteral", "value": 5},
            },
            "projection": {
                "type": "Arithmetic",
                "operator": "*",
                "left": {"type": "Variable", "name": "x"},
                "right": {"type": "IntegerLiteral", "value": 2},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, ListComprehension)
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "x"
        assert isinstance(result.where, Comparison)
        assert isinstance(result.map_expr, Arithmetic)

    def test_convert_list_comprehension_filter_only(self):
        """Test list comprehension with only WHERE clause."""
        node = {
            "type": "ListComprehension",
            "variable": "item",
            "in": {"type": "Variable", "name": "items"},
            "where": {
                "type": "PropertyLookup",
                "expression": {"type": "Variable", "name": "item"},
                "property": "active",
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, ListComprehension)
        assert result.where is not None
        assert result.map_expr is None


class TestValidationFramework:
    """Test validation framework components."""

    def test_validation_issue_creation(self):
        """Test creating ValidationIssue instances."""
        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Test error",
            node_type="NodePattern",
            suggestion="Fix this",
        )

        assert issue.severity == ValidationSeverity.ERROR
        assert issue.message == "Test error"
        assert issue.node_type == "NodePattern"
        assert issue.suggestion == "Fix this"

    def test_validation_issue_str_representation(self):
        """Test string representation of ValidationIssue."""
        issue = ValidationIssue(
            severity=ValidationSeverity.WARNING,
            message="Potential issue",
            node_type="Match",
            suggestion="Consider adding label",
        )

        result = str(issue)
        assert "WARNING" in result
        assert "Potential issue" in result
        assert "Match" in result
        assert "Consider adding label" in result

    def test_validation_result_add_methods(self):
        """Test ValidationResult add_* convenience methods."""
        result = ValidationResult()

        result.add_error("Error message", node_type="Query")
        result.add_warning("Warning message", node_type="Match")
        result.add_info("Info message", node_type="Return")

        assert len(result.issues) == 3
        assert result.has_errors
        assert result.has_warnings
        assert not result.is_valid

    def test_validation_result_properties(self):
        """Test ValidationResult property filtering."""
        result = ValidationResult()

        result.add_error("Error 1")
        result.add_error("Error 2")
        result.add_warning("Warning 1")
        result.add_info("Info 1")

        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert len(result.infos) == 1
        assert result.has_errors
        assert result.has_warnings
        assert not result.is_valid

    def test_validation_result_bool_conversion(self):
        """Test ValidationResult boolean conversion."""
        result = ValidationResult()
        assert bool(result) is True  # No errors = valid

        result.add_warning("Warning")
        assert bool(result) is True  # Warnings don't invalidate

        result.add_error("Error")
        assert bool(result) is False  # Errors invalidate


class TestBinaryExpressionConversions:
    """Test conversion of binary expressions (Or, Xor, And)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_or_expression(self):
        """Test conversion of OR expression."""
        node = {
            "type": "Or",
            "operands": [
                {
                    "type": "Comparison",
                    "operator": "=",
                    "left": {"type": "Variable", "name": "x"},
                    "right": {"type": "IntegerLiteral", "value": 1},
                },
                {
                    "type": "Comparison",
                    "operator": "=",
                    "left": {"type": "Variable", "name": "x"},
                    "right": {"type": "IntegerLiteral", "value": 2},
                },
            ],
        }

        result = self.converter.convert(node)

        assert isinstance(result, Or)
        assert len(result.operands) == 2
        assert all(isinstance(op, Comparison) for op in result.operands)

    def test_convert_xor_expression(self):
        """Test conversion of XOR expression."""
        node = {
            "type": "Xor",
            "operands": [
                {"type": "BooleanLiteral", "value": True},
                {"type": "BooleanLiteral", "value": False},
            ],
        }

        result = self.converter.convert(node)

        assert isinstance(result, Xor)
        assert len(result.operands) == 2
        assert result.operator == "XOR"

    def test_convert_and_expression(self):
        """Test conversion of AND expression."""
        node = {
            "type": "And",
            "operands": [
                {
                    "type": "Comparison",
                    "operator": ">",
                    "left": {"type": "Variable", "name": "age"},
                    "right": {"type": "IntegerLiteral", "value": 18},
                },
                {
                    "type": "Comparison",
                    "operator": "<",
                    "left": {"type": "Variable", "name": "age"},
                    "right": {"type": "IntegerLiteral", "value": 65},
                },
            ],
        }

        result = self.converter.convert(node)

        assert isinstance(result, And)
        assert len(result.operands) == 2
        assert result.operator == "AND"

    def test_convert_not_expression(self):
        """Test conversion of NOT expression."""
        node = {
            "type": "Not",
            "operand": {
                "type": "Comparison",
                "operator": "=",
                "left": {"type": "Variable", "name": "active"},
                "right": {"type": "BooleanLiteral", "value": True},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, Not)
        assert isinstance(result.operand, Comparison)


class TestUnaryExpression:
    """Test conversion and handling of unary expressions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_unary_plus(self):
        """Test conversion of unary + expression."""
        node = {
            "type": "Unary",
            "operator": "+",
            "operand": {"type": "IntegerLiteral", "value": 5},
        }

        result = self.converter.convert(node)

        assert isinstance(result, Unary)
        assert result.operator == "+"
        assert isinstance(result.operand, IntegerLiteral)

    def test_convert_unary_minus(self):
        """Test conversion of unary - expression."""
        node = {
            "type": "Unary",
            "operator": "-",
            "operand": {"type": "Variable", "name": "x"},
        }

        result = self.converter.convert(node)

        assert isinstance(result, Unary)
        assert result.operator == "-"
        assert isinstance(result.operand, Variable)


class TestParameterConversion:
    """Test conversion of query parameters."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_parameter(self):
        """Test conversion of parameter reference."""
        node = {"type": "Parameter", "name": "userId"}

        result = self.converter.convert(node)

        assert isinstance(result, Parameter)
        assert result.name == "userId"


class TestMapLiteralConversion:
    """Test conversion of map literals."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_simple_map_literal(self):
        """Test conversion of simple map literal."""
        # MapLiteral is created from plain dict without 'type' field
        node = {
            "name": {"type": "StringLiteral", "value": "Alice"},
            "age": {"type": "IntegerLiteral", "value": 30},
        }

        result = self.converter.convert(node)

        assert isinstance(result, MapLiteral)
        assert "name" in result.entries
        assert "age" in result.entries
        assert isinstance(result.entries["name"], StringLiteral)
        assert isinstance(result.entries["age"], IntegerLiteral)

    def test_convert_nested_map_literal(self):
        """Test conversion of nested map literal."""
        # MapLiteral is created from plain dict without 'type' field
        node = {
            "person": {
                "name": {"type": "StringLiteral", "value": "Bob"},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, MapLiteral)
        assert isinstance(result.entries["person"], MapLiteral)


class TestComplexNestedConversions:
    """Test conversion of complex nested structures."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_nested_arithmetic_in_list_comprehension(self):
        """Test list comprehension with nested arithmetic."""
        node = {
            "type": "ListComprehension",
            "variable": "x",
            "in": {"type": "Variable", "name": "numbers"},
            "projection": {
                "type": "Arithmetic",
                "operator": "+",
                "left": {
                    "type": "Arithmetic",
                    "operator": "*",
                    "left": {"type": "Variable", "name": "x"},
                    "right": {"type": "IntegerLiteral", "value": 2},
                },
                "right": {"type": "IntegerLiteral", "value": 1},
            },
        }

        result = self.converter.convert(node)

        assert isinstance(result, ListComprehension)
        assert isinstance(result.map_expr, Arithmetic)
        # Nested arithmetic
        assert isinstance(result.map_expr.left, Arithmetic)

    def test_convert_property_lookup_chain(self):
        """Test nested property lookups."""
        node = {
            "type": "PropertyLookup",
            "expression": {
                "type": "PropertyLookup",
                "expression": {"type": "Variable", "name": "person"},
                "property": "address",
            },
            "property": "city",
        }

        result = self.converter.convert(node)

        assert isinstance(result, PropertyLookup)
        assert result.property == "city"
        assert isinstance(result.expression, PropertyLookup)
        assert result.expression.property == "address"


class TestEdgeCases:
    """Test edge cases in AST conversion and handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = ASTConverter()

    def test_convert_empty_list_literal(self):
        """Test conversion of empty list literal."""
        node = {"type": "ListLiteral", "value": []}

        result = self.converter.convert(node)

        assert isinstance(result, ListLiteral)
        assert result.value == []
        assert result.elements == []

    def test_convert_relationship_without_variable(self):
        """Test conversion of anonymous relationship pattern."""
        node = {
            "type": "RelationshipPattern",
            "labels": ["KNOWS"],
            "direction": "->",
        }

        result = self.converter.convert(node)

        assert isinstance(result, RelationshipPattern)
        assert result.variable is None
        assert result.labels == ["KNOWS"]
        assert result.direction == RelationshipDirection.RIGHT

    def test_convert_node_without_variable(self):
        """Test conversion of anonymous node pattern."""
        node = {"type": "NodePattern", "labels": ["Person"]}

        result = self.converter.convert(node)

        assert isinstance(result, NodePattern)
        assert result.variable is None
        assert result.labels == ["Person"]

    def test_convert_node_without_labels(self):
        """Test conversion of node pattern without labels."""
        node = {"type": "NodePattern", "variable": "n"}

        result = self.converter.convert(node)

        assert isinstance(result, NodePattern)
        assert isinstance(result.variable, Variable)
        assert result.variable.name == "n"
        assert result.labels == []

    def test_convert_relationship_with_properties(self):
        """Test conversion of relationship with properties."""
        node = {
            "type": "RelationshipPattern",
            "variable": "r",
            "labels": ["KNOWS"],
            "direction": "->",
            "properties": {"since": {"type": "IntegerLiteral", "value": 2020}},
        }

        result = self.converter.convert(node)

        assert isinstance(result, RelationshipPattern)
        assert result.properties is not None
        assert "since" in result.properties


class TestASTTraversalMethods:
    """Test AST traversal and utility methods."""

    def test_find_all_with_type_predicate(self):
        """Test find_all with type predicate."""
        # Create a small AST
        node = NodePattern(
            variable=Variable(name="n"),
            labels=["Person"],
            properties={
                "age": IntegerLiteral(value=30),
                "active": BooleanLiteral(value=True),
            },
        )

        # Find all IntegerLiteral nodes
        results = node.find_all(IntegerLiteral)

        assert len(results) == 1
        assert isinstance(results[0], IntegerLiteral)
        assert results[0].value == 30

    def test_find_all_with_callable_predicate(self):
        """Test find_all with callable predicate."""
        # Create AST with multiple variables
        comp = Comparison(
            operator="=",
            left=Variable(name="x"),
            right=Variable(name="y"),
        )

        # Find all Variable nodes with name starting with 'x'
        results = comp.find_all(
            lambda n: isinstance(n, Variable) and n.name.startswith("x"),
        )

        assert isinstance(results[0], Variable)
        assert len(results) == 1
        assert results[0].name == "x"

    def test_find_first_returns_none_when_not_found(self):
        """Test find_first returns None when no match found."""
        node = NodePattern(variable=Variable(name="n"), labels=["Person"])

        result = node.find_first(IntegerLiteral)

        assert result is None

    def test_traverse_yields_all_nodes(self):
        """Test traverse yields all nodes in tree."""
        # Create nested structure
        arithmetic = Arithmetic(
            operator="+",
            left=IntegerLiteral(value=1),
            right=IntegerLiteral(value=2),
        )

        nodes = list(arithmetic.traverse())

        # Should have: Arithmetic, IntegerLiteral, IntegerLiteral
        assert len(nodes) >= 3
        assert any(isinstance(n, Arithmetic) for n in nodes)
        assert sum(1 for n in nodes if isinstance(n, IntegerLiteral)) == 2

    def test_to_dict_roundtrip(self):
        """Test to_dict and back conversion."""
        original = NodePattern(
            variable=Variable(name="person"),
            labels=["Person", "Employee"],
            properties={},
        )

        dict_repr = original.to_dict()

        assert dict_repr["type"] == "NodePattern"
        assert "variable" in dict_repr
        assert dict_repr["labels"] == ["Person", "Employee"]


class TestValidationIssueEdgeCases:
    """Test edge cases in ValidationIssue construction."""

    def test_validation_issue_with_dict_construction(self):
        """Test ValidationIssue construction from dict."""
        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Test error",
            node_type="Query",
        )

        assert issue.severity == ValidationSeverity.ERROR
        assert issue.message == "Test error"
        assert issue.node_type == "Query"

    def test_validation_issue_repr(self):
        """Test ValidationIssue __repr__ method."""
        issue = ValidationIssue(
            severity=ValidationSeverity.WARNING,
            message="Warning message",
            code="W001",
        )

        repr_str = repr(issue)

        assert "WARNING" in repr_str
        assert "Warning message" in repr_str
        assert "W001" in repr_str

    def test_validation_issue_with_node_object(self):
        """Test ValidationIssue with node object reference."""
        node = NodePattern(variable=Variable(name="n"))
        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Invalid node",
            node=node,
        )

        assert issue.node == node
        repr_str = repr(issue)
        assert "NodePattern" in repr_str


class TestLiteralEvaluate:
    """Test evaluate() method on Literal subclasses."""

    def test_integer_literal_evaluate(self):
        """Test IntegerLiteral.evaluate()."""
        lit = IntegerLiteral(value=42)
        assert lit.evaluate() == 42

    def test_string_literal_evaluate(self):
        """Test StringLiteral.evaluate()."""
        lit = StringLiteral(value="hello")
        assert lit.evaluate() == "hello"

    def test_boolean_literal_evaluate(self):
        """Test BooleanLiteral.evaluate()."""
        lit = BooleanLiteral(value=True)
        assert lit.evaluate() is True

    def test_list_literal_evaluate(self):
        """Test ListLiteral.evaluate()."""
        lit = ListLiteral(value=[1, 2, 3])
        assert lit.evaluate() == [1, 2, 3]


class TestVariableHashing:
    """Test Variable __hash__ method for dict/set usage."""

    def test_variable_hash_consistency(self):
        """Test that Variables with same name hash to same value."""
        var1 = Variable(name="x")
        var2 = Variable(name="x")

        assert hash(var1) == hash(var2)

    def test_variable_hash_difference(self):
        """Test that Variables with different names hash differently."""
        var1 = Variable(name="x")
        var2 = Variable(name="y")

        assert hash(var1) != hash(var2)

    def test_variable_in_set(self):
        """Test Variable can be used in sets."""
        var1 = Variable(name="x")
        var2 = Variable(name="x")
        var3 = Variable(name="y")

        var_set = {var1, var2, var3}

        # var1 and var2 should be treated as duplicates
        assert len(var_set) == 2

    def test_variable_as_dict_key(self):
        """Test Variable can be used as dict key."""
        var1 = Variable(name="x")
        var2 = Variable(name="x")

        mapping = {}
        mapping[var1] = "value1"
        mapping[var2] = "value2"  # Should overwrite

        assert len(mapping) == 1
        assert mapping[var1] == "value2"
