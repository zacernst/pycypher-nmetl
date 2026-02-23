"""Unit tests for arithmetic operations in ExpressionEvaluator.

Tests arithmetic (+, -, *, /, %, ^) and unary (+, -) operations in
expression evaluation, including combinations with property lookups
and literals.
"""

import pytest
import pandas as pd
import numpy as np
from pycypher.ast_models import (
    Variable,
    PropertyLookup,
    IntegerLiteral,
    FloatLiteral,
    Arithmetic,
    Unary,
)
from pycypher.expression_evaluator import ExpressionEvaluator
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    Projection,
)


@pytest.fixture
def person_context():
    """Create a test context with Person entities."""
    # Create Person data (without entity type prefix in source data)
    person_data = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 40],
        "salary": [50000.0, 60000.0, 70000.0, 80000.0],
        "bonus": [5000.0, 6000.0, 7000.0, 8000.0],
    })
    
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "salary", "bonus"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "bonus": "bonus",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "bonus": "bonus",
        },
        source_obj=person_data,
    )
    
    entity_mapping = EntityMapping(mapping={"Person": person_table})
    return Context(entity_mapping=entity_mapping)


@pytest.fixture
def person_relation(person_context):
    """Create a simple relation with Person variable."""
    # Create a projection that represents MATCH (p:Person)
    person_table = person_context.entity_mapping["Person"]
    
    # This simulates what Star._from_node_pattern would create
    relation = Projection(
        relation=person_table,
        projected_column_names={
            f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}",
        },
    )
    
    # Add variable mapping
    relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
    relation.variable_type_map = {Variable(name="p"): "Person"}
    
    return relation


class TestArithmeticOperations:
    """Test arithmetic operations (+, -, *, /, %, ^)."""
    
    def test_addition_literals(self, person_context, person_relation):
        """Test addition with literal values: 2 + 3."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: 2 + 3
        expr = Arithmetic(
            operator="+",
            left=IntegerLiteral(value=2),
            right=IntegerLiteral(value=3),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # All values should be 5
        assert len(result_series) == 4
        assert all(result_series == 5)
    
    def test_addition_property_and_literal(self, person_context, person_relation):
        """Test addition with property and literal: p.age + 5."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age + 5
        expr = Arithmetic(
            operator="+",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [30, 35, 40, 45]
        expected = [30, 35, 40, 45]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_addition_two_properties(self, person_context, person_relation):
        """Test addition of two properties: p.salary + p.bonus."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.salary + p.bonus
        expr = Arithmetic(
            operator="+",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="salary"
            ),
            right=PropertyLookup(
                expression=Variable(name="p"),
                property="bonus"
            ),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [55000, 66000, 77000, 88000]
        expected = [55000.0, 66000.0, 77000.0, 88000.0]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_subtraction(self, person_context, person_relation):
        """Test subtraction: p.age - 5."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age - 5
        expr = Arithmetic(
            operator="-",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [20, 25, 30, 35]
        expected = [20, 25, 30, 35]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_multiplication(self, person_context, person_relation):
        """Test multiplication: p.age * 2."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age * 2
        expr = Arithmetic(
            operator="*",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=2),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [50, 60, 70, 80]
        expected = [50, 60, 70, 80]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_division(self, person_context, person_relation):
        """Test division: p.age / 5."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age / 5
        expr = Arithmetic(
            operator="/",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [5.0, 6.0, 7.0, 8.0]
        expected = [5.0, 6.0, 7.0, 8.0]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_division_by_zero(self, person_context, person_relation):
        """Test division by zero produces inf."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age / 0
        expr = Arithmetic(
            operator="/",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=0),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be all inf
        assert len(result_series) == 4
        assert all(np.isinf(result_series))
    
    def test_modulo(self, person_context, person_relation):
        """Test modulo: p.age % 10."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age % 10
        expr = Arithmetic(
            operator="%",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=10),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [5, 0, 5, 0] (25%10=5, 30%10=0, 35%10=5, 40%10=0)
        expected = [5, 0, 5, 0]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_power(self, person_context, person_relation):
        """Test power: 2 ^ 3."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: 2 ^ 3
        expr = Arithmetic(
            operator="^",
            left=IntegerLiteral(value=2),
            right=IntegerLiteral(value=3),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [8, 8, 8, 8]
        assert len(result_series) == 4
        assert all(result_series == 8)
    
    def test_nested_arithmetic(self, person_context, person_relation):
        """Test nested arithmetic: (p.age + 5) * 2."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: (p.age + 5) * 2
        inner_expr = Arithmetic(
            operator="+",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=5),
        )
        
        outer_expr = Arithmetic(
            operator="*",
            left=inner_expr,
            right=IntegerLiteral(value=2),
        )
        
        result_series, _ = evaluator.evaluate(outer_expr, df)
        
        # Should be [(25+5)*2, (30+5)*2, (35+5)*2, (40+5)*2] = [60, 70, 80, 90]
        expected = [60, 70, 80, 90]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_complex_nested_arithmetic(self, person_context, person_relation):
        """Test complex nested arithmetic: (p.salary + p.bonus) / 12."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: (p.salary + p.bonus) / 12
        # This calculates monthly total compensation
        sum_expr = Arithmetic(
            operator="+",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="salary"
            ),
            right=PropertyLookup(
                expression=Variable(name="p"),
                property="bonus"
            ),
        )
        
        monthly_expr = Arithmetic(
            operator="/",
            left=sum_expr,
            right=IntegerLiteral(value=12),
        )
        
        result_series, _ = evaluator.evaluate(monthly_expr, df)
        
        # Should be [55000/12, 66000/12, 77000/12, 88000/12]
        expected = [55000.0/12, 66000.0/12, 77000.0/12, 88000.0/12]
        assert len(result_series) == 4
        for actual, exp in zip(result_series, expected):
            assert abs(actual - exp) < 0.01  # Floating point comparison
    
    def test_mixed_integer_float(self, person_context, person_relation):
        """Test arithmetic with mixed integer and float literals."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: 10 + 2.5
        expr = Arithmetic(
            operator="+",
            left=IntegerLiteral(value=10),
            right=FloatLiteral(value=2.5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [12.5, 12.5, 12.5, 12.5]
        assert len(result_series) == 4
        assert all(result_series == 12.5)
    
    def test_unsupported_operator(self, person_context, person_relation):
        """Test that unsupported operators raise ValueError."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression with invalid operator
        expr = Arithmetic(
            operator="&",  # Invalid operator
            left=IntegerLiteral(value=2),
            right=IntegerLiteral(value=3),
        )
        
        with pytest.raises(ValueError, match="Unsupported arithmetic operator"):
            evaluator.evaluate(expr, df)


class TestUnaryOperations:
    """Test unary operations (+, -)."""
    
    def test_unary_plus_literal(self, person_context, person_relation):
        """Test unary plus with literal: +5."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: +5
        expr = Unary(
            operator="+",
            operand=IntegerLiteral(value=5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [5, 5, 5, 5]
        assert len(result_series) == 4
        assert all(result_series == 5)
    
    def test_unary_minus_literal(self, person_context, person_relation):
        """Test unary minus with literal: -5."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: -5
        expr = Unary(
            operator="-",
            operand=IntegerLiteral(value=5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [-5, -5, -5, -5]
        assert len(result_series) == 4
        assert all(result_series == -5)
    
    def test_unary_minus_property(self, person_context, person_relation):
        """Test unary minus with property: -p.age."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: -p.age
        expr = Unary(
            operator="-",
            operand=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [-25, -30, -35, -40]
        expected = [-25, -30, -35, -40]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_unary_with_arithmetic(self, person_context, person_relation):
        """Test unary in arithmetic expression: -(p.age + 5)."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: -(p.age + 5)
        inner_expr = Arithmetic(
            operator="+",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=5),
        )
        
        expr = Unary(
            operator="-",
            operand=inner_expr,
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [-(25+5), -(30+5), -(35+5), -(40+5)] = [-30, -35, -40, -45]
        expected = [-30, -35, -40, -45]
        assert len(result_series) == 4
        assert list(result_series) == expected
    
    def test_double_negation(self, person_context, person_relation):
        """Test double negation: --5."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: --5
        inner_expr = Unary(
            operator="-",
            operand=IntegerLiteral(value=5),
        )
        
        expr = Unary(
            operator="-",
            operand=inner_expr,
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [5, 5, 5, 5]
        assert len(result_series) == 4
        assert all(result_series == 5)
    
    def test_unsupported_unary_operator(self, person_context, person_relation):
        """Test that unsupported unary operators raise ValueError."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression with invalid operator
        expr = Unary(
            operator="!",  # Invalid operator
            operand=IntegerLiteral(value=5),
        )
        
        with pytest.raises(ValueError, match="Unsupported unary operator"):
            evaluator.evaluate(expr, df)


class TestArithmeticEdgeCases:
    """Test edge cases for arithmetic operations."""
    
    def test_arithmetic_with_float_results(self, person_context, person_relation):
        """Test arithmetic that produces float results."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: p.age / 3
        expr = Arithmetic(
            operator="/",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="age"
            ),
            right=IntegerLiteral(value=3),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [8.333..., 10.0, 11.666..., 13.333...]
        expected = [25/3, 30/3, 35/3, 40/3]
        assert len(result_series) == 4
        for actual, exp in zip(result_series, expected):
            assert abs(actual - exp) < 0.01
    
    def test_power_with_fractional_exponent(self, person_context, person_relation):
        """Test power with fractional exponent (square root)."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: 16 ^ 0.5 (square root of 16)
        expr = Arithmetic(
            operator="^",
            left=IntegerLiteral(value=16),
            right=FloatLiteral(value=0.5),
        )
        
        result_series, _ = evaluator.evaluate(expr, df)
        
        # Should be [4.0, 4.0, 4.0, 4.0]
        assert len(result_series) == 4
        assert all(abs(result_series - 4.0) < 0.01)
    
    def test_operator_precedence_via_nesting(self, person_context, person_relation):
        """Test that operator precedence is handled via AST nesting.
        
        Note: The parser handles precedence by constructing the AST correctly.
        This test verifies that the evaluator respects the AST structure.
        """
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        df = person_relation.to_pandas(context=person_context)
        
        # Create expression: 2 + 3 * 4
        # Parser would construct this as: 2 + (3 * 4)
        multiply_expr = Arithmetic(
            operator="*",
            left=IntegerLiteral(value=3),
            right=IntegerLiteral(value=4),
        )
        
        add_expr = Arithmetic(
            operator="+",
            left=IntegerLiteral(value=2),
            right=multiply_expr,
        )
        
        result_series, _ = evaluator.evaluate(add_expr, df)
        
        # Should be [14, 14, 14, 14] (2 + 12 = 14, not 20)
        assert len(result_series) == 4
        assert all(result_series == 14)
