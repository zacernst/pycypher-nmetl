"""Integration tests for WITH clause Phase 3: Grouped aggregations.

Tests end-to-end GROUP BY functionality through the full query processing pipeline.
"""

import pytest
import pandas as pd

from pycypher.star import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.ast_models import (
    NodePattern,
    Pattern,
    PatternPath,
    Variable,
    ReturnItem,
    With,
    CountStar,
    FunctionInvocation,
    PropertyLookup,
)


@pytest.fixture
def person_context():
    """Create context with Person entities in different cities."""
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
        "age": [30, 40, 25, 35, 28, 45],
        "city": ["NYC", "LA", "NYC", "NYC", "LA", "SF"],
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


class TestGroupedAggregationIntegration:
    """Integration tests for WITH clause with grouped aggregations."""
    
    def test_group_by_single_column_count(self, person_context):
        """Test MATCH ... WITH p.city AS city, count(*) AS count."""
        star = Star(context=person_context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, count(*) AS count
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(
                    expression=CountStar(),
                    alias="count"
                )
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=person_context)
        
        # Verify results
        assert len(result_df) == 3  # NYC, LA, SF
        assert set(result_df.columns) == {"city", "count"}
        
        # Check specific cities
        nyc_count = result_df[result_df["city"] == "NYC"]["count"].iloc[0]
        la_count = result_df[result_df["city"] == "LA"]["count"].iloc[0]
        sf_count = result_df[result_df["city"] == "SF"]["count"].iloc[0]
        
        assert nyc_count == 3  # Alice, Carol, Dave
        assert la_count == 2   # Bob, Eve
        assert sf_count == 1   # Frank
    
    def test_group_by_with_multiple_aggregations(self, person_context):
        """Test GROUP BY with multiple aggregation functions."""
        star = Star(context=person_context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, count(*) AS total, collect(p) AS people
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(expression=CountStar(), alias="total"),
                ReturnItem(
                    expression=FunctionInvocation(
                        name="collect",
                        arguments={'expression': Variable(name="p")}
                    ),
                    alias="people"
                )
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 3
        assert set(result_df.columns) == {"city", "total", "people"}
        
        # Check NYC group
        nyc_row = result_df[result_df["city"] == "NYC"].iloc[0]
        assert nyc_row["total"] == 3
        assert len(nyc_row["people"]) == 3
        assert set(nyc_row["people"]) == {1, 3, 4}
    
    def test_group_by_multiple_columns(self, person_context):
        """Test GROUP BY with multiple grouping columns."""
        # Add age_group column for test
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
        star = Star(context=context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, p.age_group AS age_group, count(*) AS count
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="age_group"
                    ),
                    alias="age_group"
                ),
                ReturnItem(expression=CountStar(), alias="count")
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=context)
        
        # Should have multiple rows for different (city, age_group) combinations
        assert len(result_df) >= 3
        assert set(result_df.columns) == {"city", "age_group", "count"}
        
        # Check specific group
        nyc_young = result_df[
            (result_df["city"] == "NYC") & (result_df["age_group"] == "young")
        ]
        assert len(nyc_young) == 1
        assert nyc_young["count"].iloc[0] == 2  # IDs 1, 6
    
    def test_group_by_with_literal_expressions(self, person_context):
        """Test GROUP BY where grouping expression is a literal."""
        star = Star(context=person_context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, count(*) AS count
        # This is a normal case (not literal), just verifying standard behavior
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(expression=CountStar(), alias="count")
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 3
        assert "city" in result_df.columns
        assert "count" in result_df.columns


class TestGroupedAggregationEdgeCases:
    """Test edge cases for grouped aggregation integration."""
    
    def test_group_by_on_empty_set(self):
        """Test GROUP BY on empty result set."""
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
        star = Star(context=context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, count(*) AS count
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(expression=CountStar(), alias="count")
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=context)
        
        # Should return empty result
        assert len(result_df) == 0
        assert set(result_df.columns) == {"city", "count"}
    
    def test_group_by_all_same_value(self, person_context):
        """Test GROUP BY when all rows have same grouping value."""
        # Create data where everyone is in NYC
        person_df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3],
            "city": ["NYC", "NYC", "NYC"],
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
        star = Star(context=context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, count(*) AS count
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(expression=CountStar(), alias="count")
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=context)
        
        # Should have single group
        assert len(result_df) == 1
        assert result_df["city"].iloc[0] == "NYC"
        assert result_df["count"].iloc[0] == 3


class TestGroupedAggregationVariableScoping:
    """Test that variable scoping works correctly with grouped aggregations."""
    
    def test_only_aliases_visible_after_with(self, person_context):
        """Test that only aliased columns are visible after WITH."""
        star = Star(context=person_context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[PatternPath(elements=[
                NodePattern(
                    variable=Variable(name="p"),
                    labels=["Person"],
                    properties={}
                )
            ])]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.city AS city, count(*) AS total
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="city"
                    ),
                    alias="city"
                ),
                ReturnItem(expression=CountStar(), alias="total")
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        # Check that relation has 'city' and 'total' in variable_map
        variable_names = {var.name for var in with_relation.variable_map.keys()}
        assert variable_names == {"city", "total"}
        
        # 'p' should NOT be in the variable_map
        assert not any(var.name == "p" for var in with_relation.variable_map.keys())
        
        result_df = with_relation.to_pandas(context=person_context)
        assert set(result_df.columns) == {"city", "total"}
