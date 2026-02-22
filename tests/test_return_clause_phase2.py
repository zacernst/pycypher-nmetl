"""Unit tests for RETURN clause Phase 2: Aggregations.

Tests RETURN functionality with aggregation functions.
"""

import pytest
import pandas as pd
from pycypher.ast_models import (
    Variable,
    PropertyLookup,
    ReturnItem,
    Return,
    CountStar,
    FunctionInvocation,
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
        ID_COLUMN: [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "age": [30, 40, 25, 35, 28],
        "city": ["NYC", "LA", "NYC", "SF", "LA"],
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


class TestReturnAggregations:
    """Test RETURN clause with aggregation functions."""
    
    def test_return_count_star(self, person_context, person_relation):
        """Test RETURN count(*) AS total."""
        star = Star(context=person_context)
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=CountStar(),
                    alias="total"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["total"]
        assert result_df["total"].iloc[0] == 5
    
    def test_return_collect(self, person_context, person_relation):
        """Test RETURN collect(p) AS people."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="collect",
                        arguments={'expression': person_var}
                    ),
                    alias="people"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["people"]
        assert len(result_df["people"].iloc[0]) == 5
        assert set(result_df["people"].iloc[0]) == {1, 2, 3, 4, 5}
    
    def test_return_sum(self, person_context, person_relation):
        """Test RETURN sum(p.age) AS total_age."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="sum",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="total_age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["total_age"]
        assert result_df["total_age"].iloc[0] == 30 + 40 + 25 + 35 + 28  # 158
    
    def test_return_avg(self, person_context, person_relation):
        """Test RETURN avg(p.age) AS avg_age."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="avg",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="avg_age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["avg_age"]
        expected_avg = (30 + 40 + 25 + 35 + 28) / 5  # 31.6
        assert result_df["avg_age"].iloc[0] == pytest.approx(expected_avg)
    
    def test_return_min(self, person_context, person_relation):
        """Test RETURN min(p.age) AS min_age."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="min",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="min_age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["min_age"]
        assert result_df["min_age"].iloc[0] == 25
    
    def test_return_max(self, person_context, person_relation):
        """Test RETURN max(p.age) AS max_age."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="max",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="max_age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["max_age"]
        assert result_df["max_age"].iloc[0] == 40
    
    def test_return_multiple_aggregations(self, person_context, person_relation):
        """Test RETURN count(*) AS total, sum(p.age) AS total_age, avg(p.age) AS avg_age."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=CountStar(),
                    alias="total"
                ),
                ReturnItem(
                    expression=FunctionInvocation(
                        name="sum",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="total_age"
                ),
                ReturnItem(
                    expression=FunctionInvocation(
                        name="avg",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="avg_age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert set(result_df.columns) == {"total", "total_age", "avg_age"}
        assert result_df["total"].iloc[0] == 5
        assert result_df["total_age"].iloc[0] == 158
        assert result_df["avg_age"].iloc[0] == pytest.approx(31.6)
    
    def test_return_count_expression(self, person_context, person_relation):
        """Test RETURN count(p) AS person_count."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="count",
                        arguments={'expression': person_var}
                    ),
                    alias="person_count"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["person_count"]
        assert result_df["person_count"].iloc[0] == 5
    
    def test_return_collect_property(self, person_context, person_relation):
        """Test RETURN collect(p.name) AS names."""
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        return_clause = Return(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="collect",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="name"
                            )
                        }
                    ),
                    alias="names"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        result_df = result_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert list(result_df.columns) == ["names"]
        assert len(result_df["names"].iloc[0]) == 5
        assert set(result_df["names"].iloc[0]) == {"Alice", "Bob", "Carol", "Dave", "Eve"}


class TestReturnAggregationEdgeCases:
    """Test edge cases for aggregations in RETURN."""
    
    def test_return_aggregation_empty_set(self):
        """Test aggregations on empty result set."""
        # Create empty context
        empty_df = pd.DataFrame({
            ID_COLUMN: [],
            "age": [],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "age"],
            source_obj_attribute_map={"age": "age"},
            attribute_map={"age": "age"},
            source_obj=empty_df,
        )
        
        context = Context(entity_mapping=EntityMapping(mapping={"Person": person_table}))
        
        person_var = Variable(name="p")
        relation = Projection(
            relation=person_table,
            projected_column_names={f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}"},
            variable_map={person_var: f"Person__{ID_COLUMN}"},
            variable_type_map={person_var: "Person"},
            column_names=[f"Person__{ID_COLUMN}"],
            identifier="empty_relation",
        )
        
        star = Star(context=context)
        
        return_clause = Return(
            items=[
                ReturnItem(expression=CountStar(), alias="total"),
                ReturnItem(
                    expression=FunctionInvocation(
                        name="sum",
                        arguments={
                            'expression': PropertyLookup(
                                expression=person_var,
                                property="age"
                            )
                        }
                    ),
                    alias="total_age"
                )
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, relation)
        result_df = result_relation.to_pandas(context=context)
        
        # Should return single row with count=0
        assert len(result_df) == 1
        assert result_df["total"].iloc[0] == 0
        # Sum of empty set should be 0 or NaN (depending on implementation)
        assert result_df["total_age"].iloc[0] == 0 or pd.isna(result_df["total_age"].iloc[0])


class TestReturnAggregationRouting:
    """Test that RETURN routes aggregations correctly."""
    
    def test_routes_to_aggregation(self, person_context, person_relation):
        """Test that all-aggregation RETURN uses Aggregation."""
        from pycypher.relational_models import Aggregation
        
        star = Star(context=person_context)
        
        return_clause = Return(
            items=[
                ReturnItem(expression=CountStar(), alias="total")
            ]
        )
        
        result_relation = star._from_return_clause(return_clause, person_relation)
        
        # Should return Aggregation
        assert isinstance(result_relation, Aggregation)
        assert set(result_relation.column_names) == {"total"}
