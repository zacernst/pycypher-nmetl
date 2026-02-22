"""Integration tests for WITH clause Phase 2: Aggregations.

Tests end-to-end aggregation functionality through the full query processing pipeline.
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
    """Create context with Person entities."""
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "age": [30, 40, 25, 35, 28],
        "city": ["NYC", "LA", "SF", "NYC", "LA"],
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


class TestWithAggregationIntegration:
    """Integration tests for WITH clause with aggregations."""
    
    def test_with_count_star_return(self, person_context):
        """Test MATCH ... WITH count(*) ... RETURN."""
        star = Star(context=person_context)
        
        # Step 1: MATCH (p:Person)
        match_pattern = Pattern(
            paths=[
                PatternPath(
                    elements=[
                        NodePattern(
                            variable=Variable(name="p"),
                            labels=["Person"],
                            properties={}
                        )
                    ]
                )
            ]
        )
        match_relation = star.to_relation(match_pattern)
        
        # Step 2: WITH count(*) AS total
        with_clause = With(
            items=[
                ReturnItem(
                    expression=CountStar(),
                    alias="total"
                )
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=person_context)
        
        # Verify result
        assert len(result_df) == 1
        assert "total" in result_df.columns
        assert result_df["total"].iloc[0] == 5
    
    def test_with_count_variable(self, person_context):
        """Test WITH count(var) AS alias."""
        star = Star(context=person_context)
        
        # MATCH (p:Person)
        match_pattern = Pattern(
            paths=[
                PatternPath(
                    elements=[
                        NodePattern(
                            variable=Variable(name="p"),
                            labels=["Person"],
                            properties={}
                        )
                    ]
                )
            ]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH count(p) AS person_count
        with_clause = With(
            items=[
                ReturnItem(
                    expression=FunctionInvocation(
                        name="count",
                        arguments={'expression': Variable(name="p")}
                    ),
                    alias="person_count"
                )
            ]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=person_context)
        
        assert len(result_df) == 1
        assert result_df["person_count"].iloc[0] == 5
    
    def test_with_collect(self, person_context):
        """Test WITH collect(var) AS alias."""
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
        
        # WITH collect(p) AS people
        with_clause = With(
            items=[
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
        
        assert len(result_df) == 1
        assert "people" in result_df.columns
        people_list = result_df["people"].iloc[0]
        assert isinstance(people_list, list)
        assert len(people_list) == 5
        assert set(people_list) == {1, 2, 3, 4, 5}
    
    def test_with_multiple_aggregations(self, person_context):
        """Test WITH clause with multiple aggregation functions."""
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
        
        # WITH count(*) AS total, count(p) AS person_count, collect(p) AS people
        with_clause = With(
            items=[
                ReturnItem(expression=CountStar(), alias="total"),
                ReturnItem(
                    expression=FunctionInvocation(
                        name="count",
                        arguments={'expression': Variable(name="p")}
                    ),
                    alias="person_count"
                ),
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
        
        assert len(result_df) == 1
        assert result_df["total"].iloc[0] == 5
        assert result_df["person_count"].iloc[0] == 5
        assert len(result_df["people"].iloc[0]) == 5


class TestAggregationErrors:
    """Test error handling for aggregations."""
    
    def test_mixed_aggregation_and_projection_now_supported(self, person_context):
        """Test that mixing aggregation and non-aggregation expressions is now supported in Phase 3."""
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
        
        # WITH p.name AS name, count(*) AS total (mixed aggregation/non-aggregation)
        # This should now work with Phase 3 grouped aggregation
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias="name"
                ),
                ReturnItem(expression=CountStar(), alias="total"),
            ]
        )
        
        # This should now work (Phase 3) instead of raising NotImplementedError
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        # Verify it created a GroupedAggregation
        from pycypher.relational_models import GroupedAggregation
        assert isinstance(with_relation, GroupedAggregation)
        
        # Verify it produces correct results
        result_df = with_relation.to_pandas(context=person_context)
        
        # Should group by name and count each group
        # With our fixture data, each person has a unique name, so count should be 1 for each
        assert len(result_df) == 5  # 5 unique people with distinct names
        assert set(result_df.columns) == {"name", "total"}
        
        # Each person should have count of 1 (since names are unique)
        for _, row in result_df.iterrows():
            assert row["total"] == 1


class TestAggregationEdgeCases:
    """Test edge cases for aggregation integration."""
    
    def test_aggregation_on_empty_match(self):
        """Test aggregation on empty result set."""
        # Create empty Person table
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
        
        # WITH count(*) AS total
        with_clause = With(
            items=[ReturnItem(expression=CountStar(), alias="total")]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=context)
        
        # Should return single row with count=0
        assert len(result_df) == 1
        assert result_df["total"].iloc[0] == 0
    
    def test_collect_on_empty_set(self):
        """Test COLLECT on empty result set."""
        empty_df = pd.DataFrame({
            ID_COLUMN: [],
            "name": [],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
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
        
        # WITH collect(p) AS people
        with_clause = With(
            items=[ReturnItem(
                expression=FunctionInvocation(
                    name="collect",
                    arguments={'expression': Variable(name="p")}
                ),
                alias="people"
            )]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        result_df = with_relation.to_pandas(context=context)
        
        # Should return single row with empty list
        assert len(result_df) == 1
        assert result_df["people"].iloc[0] == []


class TestAggregationVariableScoping:
    """Test that variable scoping works correctly with aggregations."""
    
    def test_only_aggregation_aliases_visible_after_with(self, person_context):
        """Test that only aliased aggregation results are visible after WITH."""
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
        
        # WITH count(*) AS total
        with_clause = With(
            items=[ReturnItem(expression=CountStar(), alias="total")]
        )
        
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        # Check that relation has 'total' in variable_map
        assert any(var.name == "total" for var in with_relation.variable_map.keys())
        
        result_df = with_relation.to_pandas(context=person_context)
        assert "total" in result_df.columns
