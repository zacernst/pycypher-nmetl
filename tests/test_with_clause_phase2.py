"""Unit tests for WITH clause Phase 2: Simple aggregations.

Tests aggregation functions (COLLECT, COUNT, SUM, AVG, MIN, MAX) in WITH clauses
without GROUP BY (full-table aggregations only).
"""

import pytest
import pandas as pd
from pycypher.ast_models import (
    Variable,
    PropertyLookup,
    FunctionInvocation,
    CountStar,
    IntegerLiteral,
    FloatLiteral,
    StringLiteral,
)
from pycypher.expression_evaluator import ExpressionEvaluator
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    Aggregation,
    ColumnName,
    Projection,
)


@pytest.fixture
def person_context():
    """Create a test context with Person entities."""
    # Create Person data
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carol", "Dave"],
        "age": [30, 40, 25, 35],
        "city": ["NYC", "LA", "SF", "NYC"],
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


class TestAggregationEvaluator:
    """Test ExpressionEvaluator.evaluate_aggregation method."""
    
    def test_count_star(self, person_context, person_relation):
        """Test COUNT(*) aggregation."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        
        # Get the base DataFrame
        df = person_relation.to_pandas(context=person_context)
        
        # Create COUNT(*) expression
        count_star_expr = CountStar()
        
        # Evaluate
        result = evaluator.evaluate_aggregation(count_star_expr, df)
        
        # Assert
        assert result == 4  # Four people in the test data
    
    def test_count_expression(self, person_context, person_relation):
        """Test COUNT(expr) aggregation with variables."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        
        # Get base DataFrame
        df = person_relation.to_pandas(context=person_context)
        
        # Create COUNT(p) expression
        count_expr = FunctionInvocation(
            name="count",
            arguments={'expression': Variable(name="p")}
        )
        
        # Evaluate
        result = evaluator.evaluate_aggregation(count_expr, df)
        
        # Assert - should count all person IDs
        assert result == 4
    
    def test_collect_values(self, person_context, person_relation):
        """Test COLLECT() aggregation."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        
        # Get base DataFrame
        df = person_relation.to_pandas(context=person_context)
        
        # Create COLLECT(p) expression
        collect_expr = FunctionInvocation(
            name="collect",
            arguments={'expression': Variable(name="p")}
        )
        
        # Evaluate
        result = evaluator.evaluate_aggregation(collect_expr, df)
        
        # Assert - should collect all IDs
        assert isinstance(result, list)
        assert len(result) == 4
        assert set(result) == {1, 2, 3, 4}
    
    def test_sum_values(self, person_context):
        """Test SUM() aggregation."""
        # Create a simple relation with numeric values
        relation = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN],
            source_obj=pd.DataFrame({ID_COLUMN: [1, 2, 3, 4]}),
        )
        relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
        
        evaluator = ExpressionEvaluator(context=person_context, relation=relation)
        
        # Create DataFrame
        df = relation.to_pandas(context=person_context)
        
        # Create SUM() expression with literal
        sum_expr = FunctionInvocation(
            name="sum",
            arguments={'expression': IntegerLiteral(value=10)}
        )
        
        # Evaluate - will sum 10 four times = 40
        result = evaluator.evaluate_aggregation(sum_expr, df)
        
        # Assert
        assert result == 40.0
    
    def test_avg_values(self, person_context):
        """Test AVG() aggregation."""
        relation = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN],
            source_obj=pd.DataFrame({ID_COLUMN: [1, 2, 3]}),
        )
        relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
        
        evaluator = ExpressionEvaluator(context=person_context, relation=relation)
        
        df = relation.to_pandas(context=person_context)
        
        # Create AVG() expression
        avg_expr = FunctionInvocation(
            name="avg",
            arguments={'expression': IntegerLiteral(value=15)}
        )
        
        # Evaluate
        result = evaluator.evaluate_aggregation(avg_expr, df)
        
        # Assert - average of [15, 15, 15] is 15
        assert result == 15.0
    
    def test_min_values(self, person_context):
        """Test MIN() aggregation."""
        relation = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN],
            source_obj=pd.DataFrame({ID_COLUMN: [1, 2, 3, 4, 5]}),
        )
        relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
        
        evaluator = ExpressionEvaluator(context=person_context, relation=relation)
        
        df = relation.to_pandas(context=person_context)
        
        # Create MIN() expression
        min_expr = FunctionInvocation(
            name="min",
            arguments={'expression': IntegerLiteral(value=7)}
        )
        
        # Evaluate - will get min of [7, 7, 7, 7, 7] = 7
        result = evaluator.evaluate_aggregation(min_expr, df)
        
        # Assert
        assert result == 7
    
    def test_max_values(self, person_context):
        """Test MAX() aggregation."""
        relation = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN],
            source_obj=pd.DataFrame({ID_COLUMN: [1, 2, 3, 4, 5]}),
        )
        relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
        
        evaluator = ExpressionEvaluator(context=person_context, relation=relation)
        
        df = relation.to_pandas(context=person_context)
        
        # Create MAX() expression
        max_expr = FunctionInvocation(
            name="max",
            arguments={'expression': IntegerLiteral(value=99)}
        )
        
        # Evaluate
        result = evaluator.evaluate_aggregation(max_expr, df)
        
        # Assert
        assert result == 99
    
    def test_unsupported_aggregation(self, person_context, person_relation):
        """Test that unsupported aggregation functions raise errors."""
        evaluator = ExpressionEvaluator(context=person_context, relation=person_relation)
        
        df = person_relation.to_pandas(context=person_context)
        
        # Create unsupported function
        unsupported_expr = FunctionInvocation(
            name="median",
            arguments={'expression': IntegerLiteral(value=5)}
        )
        
        # Should raise ValueError
        with pytest.raises(ValueError, match="Unsupported aggregation function"):
            evaluator.evaluate_aggregation(unsupported_expr, df)


class TestAggregationRelation:
    """Test Aggregation relation model."""
    
    def test_aggregation_to_pandas(self, person_context, person_relation):
        """Test Aggregation.to_pandas() method."""
        # Create aggregations
        count_var = Variable(name="person_count")
        aggregations = {
            "person_count": CountStar()
        }
        
        # Create Aggregation relation
        agg_relation = Aggregation(
            relation=person_relation,
            aggregations=aggregations,
            variable_map={count_var: "person_count"},
            column_names=["person_count"]
        )
        
        # Execute
        result_df = agg_relation.to_pandas(context=person_context)
        
        # Assert
        assert len(result_df) == 1  # Single row result
        assert "person_count" in result_df.columns
        assert result_df["person_count"].iloc[0] == 4  # Four people in test data


class TestAggregationEdgeCases:
    """Test edge cases for aggregations."""
    
    def test_aggregation_on_empty_set(self):
        """Test aggregations on empty result set."""
        # Create empty Person data
        empty_df = pd.DataFrame({
            ID_COLUMN: [],
            "name": [],
            "age": [],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age"],
            source_obj_attribute_map={"name": "name", "age": "age"},
            attribute_map={"name": "name", "age": "age"},
            source_obj=empty_df,
        )
        
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table})
        )
        
        person_var = Variable(name="p")
        base_relation = Projection(
            relation=person_table,
            projected_column_names={f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}"},
            variable_map={person_var: f"Person__{ID_COLUMN}"},
            column_names=[f"Person__{ID_COLUMN}"],
            identifier="empty_projection",
        )
        
        # Create aggregation
        agg_relation = Aggregation(
            relation=base_relation,
            aggregations={"count": CountStar()},
            variable_map={Variable(name="count"): "count"},
            column_names=["count"]
        )
        
        # Execute
        result_df = agg_relation.to_pandas(context=context)
        
        # Assert - should return single row with count=0
        assert len(result_df) == 1
        assert result_df["count"].iloc[0] == 0
    
    def test_count_with_nulls(self, person_context):
        """Test COUNT excludes null values."""
        # Create DataFrame with nulls in test manually
        df_with_nulls = pd.DataFrame({
            f'Person__{ID_COLUMN}': [1, 2, None, 4, None]
        })
        
        person_var = Variable(name="p")
        relation = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN],
            source_obj=pd.DataFrame({ID_COLUMN: [1, 2, 3, 4, 5]}),
        )
        relation.variable_map = {person_var: f"Person__{ID_COLUMN}"}
        
        evaluator = ExpressionEvaluator(context=person_context, relation=relation)
        
        # COUNT should exclude nulls
        count_expr = FunctionInvocation(
            name="count",
            arguments={'expression': Variable(name="p")}
        )
        
        result = evaluator.evaluate_aggregation(count_expr, df_with_nulls)
        
        # Should count only non-null values
        assert result == 3
