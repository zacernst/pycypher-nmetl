"""Unit tests for RETURN clause Phase 1: Basic expression projection.

Tests simple RETURN functionality without aggregations.
"""

import pytest
import pandas as pd
from pycypher.ast_models import (
    Variable,
    PropertyLookup,
    ReturnItem,
    Return,
    IntegerLiteral,
    StringLiteral,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    Projection,
)
from pycypher.star import Star


@pytest.fixture
def person_context():
    """Create context with Person entities."""
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carol", "Dave"],
        "age": [30, 40, 25, 35],
        "city": ["NYC", "LA", "NYC", "SF"],
    })
    
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
    
    return Context(entity_mapping=EntityMapping(mapping={"Person": person_table}))


@pytest.fixture
def person_relation(person_context):
    """Create a simple relation with Person variable."""
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


class TestReturnClauseBasics:
    """Test basic RETURN clause functionality."""
    
    def test_return_single_property(self, person_context, person_relation):
        """Test RETURN p.name AS name."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 4
        assert list(result_df.columns) == ["name"]
        assert set(result_df["name"]) == {"Alice", "Bob", "Carol", "Dave"}
    
    def test_return_multiple_properties(self, person_context, person_relation):
        """Test RETURN p.name AS name, p.age AS age."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                ),
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="age"
                    ),
                    alias="age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 4
        assert set(result_df.columns) == {"name", "age"}
        assert "Alice" in result_df["name"].values
        assert 30 in result_df["age"].values
    
    def test_return_variable_reference(self, person_context, person_relation):
        """Test RETURN p AS person."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=person_var,
                    alias="person"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 4
        assert list(result_df.columns) == ["person"]
        # Variable reference returns IDs
        assert set(result_df["person"]) == {1, 2, 3, 4}
    
    def test_return_literal_expression(self, person_context, person_relation):
        """Test RETURN 'constant' AS value."""
        star = Star(context=person_context)
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=StringLiteral(value="constant"),
                    alias="value"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 4
        assert list(result_df.columns) == ["value"]
        # All rows should have the same constant value
        assert all(result_df["value"] == "constant")
    
    def test_return_mixed_expressions(self, person_context, person_relation):
        """Test RETURN with mix of property lookups and literals."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                ),
                ReturnItem(
                    expression=IntegerLiteral(value=42),
                    alias="number"
                ),
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="city"
                    ),
                    alias="city"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 4
        assert set(result_df.columns) == {"name", "number", "city"}
        assert all(result_df["number"] == 42)
        assert "Alice" in result_df["name"].values
        assert "NYC" in result_df["city"].values


class TestReturnClauseErrors:
    """Test error handling for RETURN clause."""
    
    def test_return_requires_alias(self, person_context, person_relation):
        """Test that RETURN items require aliases."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        # Missing alias
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias=None  # Missing alias
                )
            ]
        )
        
        with pytest.raises(ValueError, match="must have aliases"):
            star._from_return_clause(return_clause, person_relation)
    
    def test_return_requires_expression(self, person_context, person_relation):
        """Test that RETURN items require expressions."""
        star = Star(context=person_context)
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=None,  # Missing expression
                    alias="name"
                )
            ]
        )
        
        with pytest.raises(ValueError, match="must have an expression"):
            star._from_return_clause(return_clause, person_relation)
    
    def test_unsupported_distinct(self, person_context, person_relation):
        """Test that DISTINCT raises NotImplementedError."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            distinct=True,
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                )
            ]
        )
        
        with pytest.raises(NotImplementedError, match="DISTINCT"):
            star._from_return_clause(return_clause, person_relation)
    
    def test_unsupported_order_by(self, person_context, person_relation):
        """Test that ORDER BY raises NotImplementedError."""
        from pycypher.ast_models import OrderByItem
        
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                )
            ],
            order_by=[
                OrderByItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    ascending=True
                )
            ]
        )
        
        with pytest.raises(NotImplementedError, match="ORDER BY"):
            star._from_return_clause(return_clause, person_relation)
    
    def test_unsupported_skip_limit(self, person_context, person_relation):
        """Test that SKIP/LIMIT raises NotImplementedError."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                )
            ],
            skip=10,
            limit=5
        )
        
        with pytest.raises(NotImplementedError, match="SKIP/LIMIT"):
            star._from_return_clause(return_clause, person_relation)


class TestReturnClauseRouting:
    """Test that RETURN clause routes to correct implementation."""
    
    def test_routes_to_expression_projection(self, person_context, person_relation):
        """Test that non-aggregation RETURN uses ExpressionProjection."""
        from pycypher.relational_models import ExpressionProjection
        
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=person_var,
                        property="name"
                    ),
                    alias="name"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        
        # Should return ExpressionProjection
        assert isinstance(result_relation, ExpressionProjection)
        assert set(result_relation.column_names) == {"name"}
