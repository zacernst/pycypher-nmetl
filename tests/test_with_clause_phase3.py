"""Unit tests for WITH clause Phase 3: Grouped aggregations.

Tests GROUP BY functionality with aggregation functions in WITH clauses.
"""

import pytest
import pandas as pd
from pycypher.ast_models import (
    Variable,
    PropertyLookup,
    FunctionInvocation,
    CountStar,
    IntegerLiteral,
    StringLiteral,
)
from pycypher.expression_evaluator import ExpressionEvaluator
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    GroupedAggregation,
    Projection,
    ColumnName,
)


@pytest.fixture
def person_context():
    """Create context with Person entities grouped by city."""
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "age": [30, 40, 25, 35, 28],
        "city": ["NYC", "LA", "NYC", "NYC", "LA"],
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


class TestGroupedAggregationRelation:
    """Test GroupedAggregation relation model."""
    
    def test_single_grouping_column_with_count(self, person_context, person_relation):
        """Test grouping by single column with count aggregation."""
        # Create GroupedAggregation: GROUP BY city, count(*)
        person_var = Variable(name="p")
        
        grouped_agg = GroupedAggregation(
            relation=person_relation,
            grouping_expressions={
                "city": PropertyLookup(
                    expression=person_var,
                    property="city"
                )
            },
            aggregations={
                "count": CountStar()
            },
            variable_map={
                Variable(name="city"): "city",
                Variable(name="count"): "count"
            },
            column_names=["city", "count"]
        )
        
        # Execute
        result_df = grouped_agg.to_pandas(context=person_context)
        
        # Assert - should have one row per city
        assert len(result_df) == 2  # NYC and LA
        assert set(result_df.columns) == {"city", "count"}
        
        # Check counts
        nyc_row = result_df[result_df["city"] == "NYC"]
        la_row = result_df[result_df["city"] == "LA"]
        
        assert len(nyc_row) == 1
        assert nyc_row["count"].iloc[0] == 3  # Alice, Carol, Dave
        
        assert len(la_row) == 1
        assert la_row["count"].iloc[0] == 2  # Bob, Eve
    
    def test_multiple_grouping_columns(self, person_context):
        """Test grouping by multiple columns."""
        # Create data with multiple grouping dimensions
        person_df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3, 4, 5, 6],
            "city": ["NYC", "NYC", "LA", "LA", "SF", "NYC"],
            "age_group": ["young", "old", "young", "old", "young", "young"],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "city", "age_group"],
            source_obj_attribute_map={"city": "city", "age_group": "age_group"},
            attribute_map={"city": "city", "age_group": "age_group"},
            source_obj=person_df,
        )
        
        context = Context(entity_mapping=EntityMapping(mapping={"Person": person_table}))
        
        person_var = Variable(name="p")
        base_relation = Projection(
            relation=person_table,
            projected_column_names={f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}"},
            variable_map={person_var: f"Person__{ID_COLUMN}"},
            variable_type_map={person_var: "Person"},
            column_names=[f"Person__{ID_COLUMN}"],
            identifier="test_relation",
        )
        
        # GROUP BY city, age_group
        grouped_agg = GroupedAggregation(
            relation=base_relation,
            grouping_expressions={
                "city": PropertyLookup(expression=person_var, property="city"),
                "age_group": PropertyLookup(expression=person_var, property="age_group"),
            },
            aggregations={
                "count": CountStar()
            },
            variable_map={
                Variable(name="city"): "city",
                Variable(name="age_group"): "age_group",
                Variable(name="count"): "count"
            },
            column_names=["city", "age_group", "count"]
        )
        
        result_df = grouped_agg.to_pandas(context=context)
        
        # Should have one row per unique (city, age_group) combination
        assert len(result_df) >= 3  # At least NYC-young, NYC-old, LA-young, LA-old, SF-young
        assert set(result_df.columns) == {"city", "age_group", "count"}
        
        # Check specific groups
        nyc_young = result_df[(result_df["city"] == "NYC") & (result_df["age_group"] == "young")]
        assert len(nyc_young) == 1
        assert nyc_young["count"].iloc[0] == 2  # IDs 1, 6
    
    def test_grouped_aggregation_with_collect(self, person_context, person_relation):
        """Test GROUP BY with COLLECT aggregation."""
        person_var = Variable(name="p")
        
        grouped_agg = GroupedAggregation(
            relation=person_relation,
            grouping_expressions={
                "city": PropertyLookup(expression=person_var, property="city")
            },
            aggregations={
                "people": FunctionInvocation(
                    name="collect",
                    arguments={'expression': person_var}
                )
            },
            variable_map={
                Variable(name="city"): "city",
                Variable(name="people"): "people"
            },
            column_names=["city", "people"]
        )
        
        result_df = grouped_agg.to_pandas(context=person_context)
        
        assert len(result_df) == 2
        
        # Check NYC group
        nyc_row = result_df[result_df["city"] == "NYC"]
        nyc_people = nyc_row["people"].iloc[0]
        assert isinstance(nyc_people, list)
        assert len(nyc_people) == 3
        assert set(nyc_people) == {1, 3, 4}  # Alice, Carol, Dave
        
        # Check LA group
        la_row = result_df[result_df["city"] == "LA"]
        la_people = la_row["people"].iloc[0]
        assert isinstance(la_people, list)
        assert len(la_people) == 2
        assert set(la_people) == {2, 5}  # Bob, Eve
    
    def test_grouped_aggregation_multiple_aggs(self, person_context, person_relation):
        """Test GROUP BY with multiple aggregation functions."""
        person_var = Variable(name="p")
        
        grouped_agg = GroupedAggregation(
            relation=person_relation,
            grouping_expressions={
                "city": PropertyLookup(expression=person_var, property="city")
            },
            aggregations={
                "total": CountStar(),
                "person_count": FunctionInvocation(
                    name="count",
                    arguments={'expression': person_var}
                ),
                "people": FunctionInvocation(
                    name="collect",
                    arguments={'expression': person_var}
                )
            },
            variable_map={
                Variable(name="city"): "city",
                Variable(name="total"): "total",
                Variable(name="person_count"): "person_count",
                Variable(name="people"): "people"
            },
            column_names=["city", "total", "person_count", "people"]
        )
        
        result_df = grouped_agg.to_pandas(context=person_context)
        
        assert len(result_df) == 2
        assert set(result_df.columns) == {"city", "total", "person_count", "people"}
        
        # Verify NYC
        nyc_row = result_df[result_df["city"] == "NYC"].iloc[0]
        assert nyc_row["total"] == 3
        assert nyc_row["person_count"] == 3
        assert len(nyc_row["people"]) == 3


class TestGroupedAggregationEdgeCases:
    """Test edge cases for grouped aggregations."""
    
    def test_empty_result_set(self):
        """Test GROUP BY on empty data."""
        empty_df = pd.DataFrame({
            ID_COLUMN: [],
            "city": [],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "city"],
            source_obj_attribute_map={"city": "city"},
            attribute_map={"city": "city"},
            source_obj=empty_df,
        )
        
        context = Context(entity_mapping=EntityMapping(mapping={"Person": person_table}))
        
        person_var = Variable(name="p")
        base_relation = Projection(
            relation=person_table,
            projected_column_names={f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}"},
            variable_map={person_var: f"Person__{ID_COLUMN}"},
            variable_type_map={person_var: "Person"},
            column_names=[f"Person__{ID_COLUMN}"],
            identifier="empty_relation",
        )
        
        grouped_agg = GroupedAggregation(
            relation=base_relation,
            grouping_expressions={
                "city": PropertyLookup(expression=person_var, property="city")
            },
            aggregations={"count": CountStar()},
            variable_map={
                Variable(name="city"): "city",
                Variable(name="count"): "count"
            },
            column_names=["city", "count"]
        )
        
        result_df = grouped_agg.to_pandas(context=context)
        
        # Should return empty DataFrame with correct columns
        assert len(result_df) == 0
        assert set(result_df.columns) == {"city", "count"}
    
    def test_null_values_in_grouping_column(self, person_context):
        """Test GROUP BY with NULL values in grouping column."""
        # Create data with NULL city values
        person_df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3, 4],
            "city": ["NYC", None, "NYC", None],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "city"],
            source_obj_attribute_map={"city": "city"},
            attribute_map={"city": "city"},
            source_obj=person_df,
        )
        
        context = Context(entity_mapping=EntityMapping(mapping={"Person": person_table}))
        
        person_var = Variable(name="p")
        base_relation = Projection(
            relation=person_table,
            projected_column_names={f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}"},
            variable_map={person_var: f"Person__{ID_COLUMN}"},
            variable_type_map={person_var: "Person"},
            column_names=[f"Person__{ID_COLUMN}"],
            identifier="test_relation",
        )
        
        grouped_agg = GroupedAggregation(
            relation=base_relation,
            grouping_expressions={
                "city": PropertyLookup(expression=person_var, property="city")
            },
            aggregations={"count": CountStar()},
            variable_map={
                Variable(name="city"): "city",
                Variable(name="count"): "count"
            },
            column_names=["city", "count"]
        )
        
        result_df = grouped_agg.to_pandas(context=context)
        
        # Should have groups for NYC and NULL
        assert len(result_df) == 2
        
        # Check NULL group
        null_group = result_df[result_df["city"].isna()]
        assert len(null_group) == 1
        assert null_group["count"].iloc[0] == 2
        
        # Check NYC group
        nyc_group = result_df[result_df["city"] == "NYC"]
        assert len(nyc_group) == 1
        assert nyc_group["count"].iloc[0] == 2


class TestWithClauseGroupedAggregation:
    """Test WITH clause processing with grouped aggregations."""
    
    def test_with_clause_routes_to_grouped_agg(self, person_context, person_relation):
        """Test that _from_with_clause correctly routes mixed expressions to GroupedAggregation."""
        from pycypher.star import Star
        from pycypher.ast_models import With, ReturnItem
        
        star = Star(context=person_context)
        
        person_var = Variable(name="p")
        
        # Create WITH clause with mixed expressions
        # WITH p.city AS city, count(*) AS count
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(expression=person_var, property="city"),
                    alias="city"
                ),
                ReturnItem(
                    expression=CountStar(),
                    alias="count"
                )
            ]
        )
        
        result_relation = star._from_with_clause(with_clause, person_relation)
        
        # Should return GroupedAggregation
        assert isinstance(result_relation, GroupedAggregation)
        assert set(result_relation.column_names) == {"city", "count"}
        
        # Execute and verify
        result_df = result_relation.to_pandas(context=person_context)
        assert len(result_df) == 2
        assert "city" in result_df.columns
        assert "count" in result_df.columns
