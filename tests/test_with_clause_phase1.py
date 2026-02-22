"""Unit tests for WITH clause expression projection (Phase 1).

Tests basic expression evaluation and projection without aggregations.
"""

import pandas as pd
import pytest

from pycypher.ast_models import (
    Variable,
    PropertyLookup,
    IntegerLiteral,
    FloatLiteral,
    StringLiteral,
    BooleanLiteral,
    NullLiteral,
    ReturnItem,
    With,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    Projection,
    ExpressionProjection,
)
from pycypher.expression_evaluator import ExpressionEvaluator
from pycypher.star import Star


@pytest.fixture
def person_context():
    """Create a test context with Person entities."""
    # Create Person data
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [30, 40, 25],
        "city": ["NYC", "LA", "SF"],
    })
    
    # Create Person entity table
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "city"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        source_obj=person_df,
    )
    
    # Create context
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table})
    )
    
    return context


@pytest.fixture
def person_relation(person_context):
    """Create a simple relation with Person variable."""
    # Create a basic projection that represents MATCH (p:Person)
    person_table = person_context.entity_mapping["Person"]
    
    # This simulates what Star._from_node_pattern would create
    relation = Projection(
        relation=person_table,
        projected_column_names={
            f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}",
        },
        variable_map={Variable(name="p"): f"Person__{ID_COLUMN}"},
        variable_type_map={Variable(name="p"): "Person"},
        column_names=[f"Person__{ID_COLUMN}"],
        identifier="test_person_relation",
    )
    
    return relation


class TestExpressionEvaluator:
    """Test ExpressionEvaluator class."""
    
    def test_evaluate_property_lookup(self, person_context, person_relation):
        """Test evaluating property access like p.name."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        # Create a property lookup expression: p.name
        expr = PropertyLookup(
            expression=Variable(name="p"),
            property="name"
        )
        
        # Get DataFrame from relation
        df = person_relation.to_pandas(context=person_context)
        
        # Evaluate expression
        result_series, column_name = evaluator.evaluate(expr, df)
        
        # Check results
        assert len(result_series) == 3
        assert list(result_series) == ["Alice", "Bob", "Carol"]
        assert column_name == "Person__name"
    
    def test_evaluate_multiple_properties(self, person_context, person_relation):
        """Test evaluating multiple property accesses."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Evaluate p.age
        age_expr = PropertyLookup(
            expression=Variable(name="p"),
            property="age"
        )
        age_series, _ = evaluator.evaluate(age_expr, df)
        assert list(age_series) == [30, 40, 25]
        
        # Evaluate p.city
        city_expr = PropertyLookup(
            expression=Variable(name="p"),
            property="city"
        )
        city_series, _ = evaluator.evaluate(city_expr, df)
        assert list(city_series) == ["NYC", "LA", "SF"]
    
    def test_evaluate_variable(self, person_context, person_relation):
        """Test evaluating variable reference."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Evaluate p (should return ID column)
        var_expr = Variable(name="p")
        result_series, column_name = evaluator.evaluate(var_expr, df)
        
        assert len(result_series) == 3
        assert list(result_series) == [1, 2, 3]
        assert column_name == "Person____ID__"
    
    def test_evaluate_integer_literal(self, person_context, person_relation):
        """Test evaluating integer literal."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Evaluate literal 42
        literal_expr = IntegerLiteral(value=42)
        result_series, column_name = evaluator.evaluate(literal_expr, df)
        
        assert len(result_series) == 3
        assert all(result_series == 42)
        assert column_name == "literal"
    
    def test_evaluate_string_literal(self, person_context, person_relation):
        """Test evaluating string literal."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Evaluate literal "test"
        literal_expr = StringLiteral(value="test")
        result_series, _ = evaluator.evaluate(literal_expr, df)
        
        assert len(result_series) == 3
        assert all(result_series == "test")
    
    def test_evaluate_null_literal(self, person_context, person_relation):
        """Test evaluating NULL literal."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Evaluate NULL
        null_expr = NullLiteral()
        result_series, _ = evaluator.evaluate(null_expr, df)
        
        assert len(result_series) == 3
        assert all(pd.isna(result_series))
    
    def test_evaluate_unknown_variable(self, person_context, person_relation):
        """Test that evaluating unknown variable raises error."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Try to evaluate unknown variable
        var_expr = Variable(name="unknown")
        
        with pytest.raises(ValueError, match="Variable unknown not found"):
            evaluator.evaluate(var_expr, df)
    
    def test_evaluate_unknown_property(self, person_context, person_relation):
        """Test that evaluating unknown property raises error."""
        evaluator = ExpressionEvaluator(
            context=person_context,
            relation=person_relation
        )
        
        df = person_relation.to_pandas(context=person_context)
        
        # Try to evaluate p.unknown
        prop_expr = PropertyLookup(
            expression=Variable(name="p"),
            property="unknown"
        )
        
        with pytest.raises(ValueError, match="Property unknown not found"):
            evaluator.evaluate(prop_expr, df)


class TestExpressionProjection:
    """Test ExpressionProjection relational model."""
    
    def test_simple_property_projection(self, person_context, person_relation):
        """Test projecting a single property."""
        # Create expression projection: p.name AS person_name
        projection = ExpressionProjection(
            relation=person_relation,
            expressions={
                "person_name": PropertyLookup(
                    expression=Variable(name="p"),
                    property="name"
                )
            },
            variable_map={Variable(name="person_name"): "person_name"},
            variable_type_map={Variable(name="person_name"): "Person"},
            column_names=["person_name"],
            identifier="test_projection",
        )
        
        # Convert to DataFrame
        result_df = projection.to_pandas(context=person_context)
        
        # Check results
        assert len(result_df) == 3
        assert list(result_df.columns) == ["person_name"]
        assert list(result_df["person_name"]) == ["Alice", "Bob", "Carol"]
    
    def test_multiple_property_projection(self, person_context, person_relation):
        """Test projecting multiple properties."""
        # Create: p.name AS name, p.age AS age, p.city AS city
        projection = ExpressionProjection(
            relation=person_relation,
            expressions={
                "name": PropertyLookup(
                    expression=Variable(name="p"),
                    property="name"
                ),
                "age": PropertyLookup(
                    expression=Variable(name="p"),
                    property="age"
                ),
                "city": PropertyLookup( 
                    expression=Variable(name="p"),
                    property="city"
                ),
            },
            variable_map={
                Variable(name="name"): "name",
                Variable(name="age"): "age",
                Variable(name="city"): "city",
            },
            variable_type_map={},
            column_names=["name", "age", "city"],
            identifier="test_projection",
        )
        
        result_df = projection.to_pandas(context=person_context)
        
        assert len(result_df) == 3
        assert list(result_df.columns) == ["name", "age", "city"]
        assert list(result_df["name"]) == ["Alice", "Bob", "Carol"]
        assert list(result_df["age"]) == [30, 40, 25]
        assert list(result_df["city"]) == ["NYC", "LA", "SF"]
    
    def test_mixed_expression_projection(self, person_context, person_relation):
        """Test projecting mix of properties and literals."""
        projection = ExpressionProjection(
            relation=person_relation,
            expressions={
                "name": PropertyLookup(
                    expression=Variable(name="p"),
                    property="name"
                ),
                "constant": IntegerLiteral(value=100),
            },
            variable_map={
                Variable(name="name"): "name",
                Variable(name="constant"): "constant",
            },
            variable_type_map={},
            column_names=["name", "constant"],
            identifier="test_projection",
        )
        
        result_df = projection.to_pandas(context=person_context)
        
        assert len(result_df) == 3
        assert list(result_df["name"]) == ["Alice", "Bob", "Carol"]
        assert all(result_df["constant"] == 100)


class TestWithClauseProcessing:
    """Test WITH clause processing in Star."""
    
    def test_with_single_property(self, person_context, person_relation):
        """Test WITH p.name AS person_name."""
        star = Star(context=person_context)
        
        # Create WITH clause
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias="person_name"
                )
            ]
        )
        
        # Process WITH clause
        result_relation = star._from_with_clause(with_clause, person_relation)
        
        # Check result
        assert isinstance(result_relation, ExpressionProjection)
        assert "person_name" in result_relation.column_names
        
        # Convert to DataFrame and check values
        result_df = result_relation.to_pandas(context=person_context)
        assert len(result_df) == 3
        assert list(result_df["person_name"]) == ["Alice", "Bob", "Carol"]
    
    def test_with_multiple_properties(self, person_context, person_relation):
        """Test WITH p.name AS name, p.age AS age."""
        star = Star(context=person_context)
        
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias="name"
                ),
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="age"
                    ),
                    alias="age"
                ),
            ]
        )
        
        result_relation = star._from_with_clause(with_clause, person_relation)
        
        # Check result
        assert len(result_relation.column_names) == 2
        assert "name" in result_relation.column_names
        assert "age" in result_relation.column_names
        
        result_df = result_relation.to_pandas(context=person_context)
        assert list(result_df["name"]) == ["Alice", "Bob", "Carol"]
        assert list(result_df["age"]) == [30, 40, 25]
    
    def test_with_variable_passthrough(self, person_context, person_relation):
        """Test WITH p AS person (variable passthrough)."""
        star = Star(context=person_context)
        
        with_clause = With(
            items=[
                ReturnItem(
                    expression=Variable(name="p"),
                    alias="person"
                )
            ]
        )
        
        result_relation = star._from_with_clause(with_clause, person_relation)
        
        # Check that variable is passed through
        assert "person" in result_relation.column_names
        
        result_df = result_relation.to_pandas(context=person_context)
        # Should have ID values
        assert list(result_df["person"]) == [1, 2, 3]
    
    def test_with_literal_expression(self, person_context, person_relation):
        """Test WITH 42 AS answer."""
        star = Star(context=person_context)
        
        with_clause = With(
            items=[
                ReturnItem(
                    expression=IntegerLiteral(value=42),
                    alias="answer"
                )
            ]
        )
        
        result_relation = star._from_with_clause(with_clause, person_relation)
        
        result_df = result_relation.to_pandas(context=person_context)
        assert len(result_df) == 3
        assert all(result_df["answer"] == 42)
    
    def test_with_rejects_missing_alias(self, person_context, person_relation):
        """Test that WITH without alias raises error in Phase 1."""
        star = Star(context=person_context)
        
        # Create WITH without alias
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias=None  # No alias
                )
            ]
        )
        
        with pytest.raises(ValueError, match="must have aliases"):
            star._from_with_clause(with_clause, person_relation)
    
    def test_with_rejects_where_clause(self, person_context, person_relation):
        """Test that WITH...WHERE raises error in Phase 1."""
        star = Star(context=person_context)
        
        from pycypher.ast_models import Comparison
        
        with_clause = With(
            items=[
                ReturnItem(
                    expression=Variable(name="p"),
                    alias="p"
                )
            ],
            where=Comparison(
                operator=">",
                left=PropertyLookup(
                    expression=Variable(name="p"),
                    property="age"
                ),
                right=IntegerLiteral(value=30)
            )
        )
        
        with pytest.raises(NotImplementedError, match="WHERE clause.*not supported"):
            star._from_with_clause(with_clause, person_relation)
    
    def test_with_rejects_distinct(self, person_context, person_relation):
        """Test that WITH DISTINCT raises error in Phase 1."""
        star = Star(context=person_context)
        
        with_clause = With(
            distinct=True,
            items=[
                ReturnItem(
                    expression=Variable(name="p"),
                    alias="p"
                )
            ]
        )
        
        with pytest.raises(NotImplementedError, match="DISTINCT.*not supported"):
            star._from_with_clause(with_clause, person_relation)
    
    def test_with_rejects_order_by(self, person_context, person_relation):
        """Test that WITH...ORDER BY raises error in Phase 1."""
        star = Star(context=person_context)
        
        from pycypher.ast_models import OrderByItem
        
        with_clause = With(
            items=[
                ReturnItem(
                    expression=Variable(name="p"),
                    alias="p"
                )
            ],
            order_by=[
                OrderByItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    )
                )
            ]
        )
        
        with pytest.raises(NotImplementedError, match="ORDER BY.*not supported"):
            star._from_with_clause(with_clause, person_relation)
    
    def test_with_new_variable_scope(self, person_context, person_relation):
        """Test that WITH creates new variable scope."""
        star = Star(context=person_context)
        
        # Original relation has variable 'p'
        assert Variable(name="p") in person_relation.variable_map
        
        # After WITH p.name AS name, only 'name' should be in scope
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias="name"
                )
            ]
        )
        
        result_relation = star._from_with_clause(with_clause, person_relation)
        
        # Check that 'p' is no longer in variable map
        assert Variable(name="p") not in result_relation.variable_map
        # Check that 'name' is in variable map
        assert Variable(name="name") in result_relation.variable_map
