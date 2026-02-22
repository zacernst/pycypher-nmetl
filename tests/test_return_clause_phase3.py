"""Unit tests for RETURN clause Phase 3: Grouped Aggregations (GROUP BY).

Phase 3 tests RETURN clauses that mix aggregations with non-aggregation expressions,
requiring implicit GROUP BY behavior. These should route to GroupedAggregation.

Test Coverage:
- Single grouping key with aggregations
- Multiple grouping keys with aggregations
- Property lookups as grouping keys
- Variables as grouping keys
- Multiple aggregations with grouping
- Edge cases (empty groups, single-row groups)
"""

import pytest
import pandas as pd
from pycypher.star import Star
from pycypher.ast_models import (
    Query,
    Match,
    Return,
    ReturnItem,
    Pattern,
    PatternPath,
    NodePattern,
    RelationshipPattern,
    RelationshipDirection,
    Variable,
    PropertyLookup,
    FunctionInvocation,
    CountStar,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


@pytest.fixture
def context_with_grouped_data():
    """Context with data suitable for grouping tests."""
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "city": ["NYC", "NYC", "SF", "SF", "LA"],
        "age": [30, 25, 35, 40, 28],
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


class TestReturnGroupedAggregations:
    """Test RETURN with grouped aggregations (implicit GROUP BY)."""
    
    def test_return_single_grouping_key_with_count(self, context_with_grouped_data):
        """Test RETURN p.city, count(*) AS total."""
        star = Star(context=context_with_grouped_data)
        
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
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
                            alias="total"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        # Should have 3 groups: NYC (2), SF (2), LA (1)
        assert len(result_df) == 3
        assert set(result_df.columns) == {"city", "total"}
        
        # Verify counts
        city_counts = dict(zip(result_df["city"], result_df["total"]))
        assert city_counts["NYC"] == 2
        assert city_counts["SF"] == 2
        assert city_counts["LA"] == 1
    
    def test_return_single_grouping_key_with_collect(self, context_with_grouped_data):
        """Test RETURN p.city, collect(p.name) AS names."""
        star = Star(context=context_with_grouped_data)
        
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="city"
                            ),
                            alias="city"
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    'expression': PropertyLookup(
                                        expression=Variable(name="p"),
                                        property="name"
                                    )
                                }
                            ),
                            alias="names"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 3
        assert set(result_df.columns) == {"city", "names"}
        
        # Verify collected names by city
        city_names = {row["city"]: set(row["names"]) for _, row in result_df.iterrows()}
        assert city_names["NYC"] == {"Alice", "Bob"}
        assert city_names["SF"] == {"Carol", "Dave"}
        assert city_names["LA"] == {"Eve"}
    
    def test_return_multiple_grouping_keys(self, context_with_grouped_data):
        """Test RETURN p.city, p.age, count(*) AS total."""
        star = Star(context=context_with_grouped_data)
        
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
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
                                property="age"
                            ),
                            alias="age"
                        ),
                        ReturnItem(
                            expression=CountStar(),
                            alias="total"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        # Each person has unique (city, age) combination
        assert len(result_df) == 5
        assert set(result_df.columns) == {"city", "age", "total"}
        
        # All groups should have count of 1
        assert all(result_df["total"] == 1)
    
    def test_return_multiple_aggregations_with_grouping(self, context_with_grouped_data):
        """Test RETURN p.city, count(*) AS total, collect(p.name) AS names, avg(p.age) AS avg_age."""
        star = Star(context=context_with_grouped_data)
        
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
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
                            alias="total"
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    'expression': PropertyLookup(
                                        expression=Variable(name="p"),
                                        property="name"
                                    )
                                }
                            ),
                            alias="names"
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="avg",
                                arguments={
                                    'expression': PropertyLookup(
                                        expression=Variable(name="p"),
                                        property="age"
                                    )
                                }
                            ),
                            alias="avg_age"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 3
        assert set(result_df.columns) == {"city", "total", "names", "avg_age"}
        
        # Verify aggregations by city
        for _, row in result_df.iterrows():
            if row["city"] == "NYC":
                assert row["total"] == 2
                assert set(row["names"]) == {"Alice", "Bob"}
                assert row["avg_age"] == 27.5  # (30 + 25) / 2
            elif row["city"] == "SF":
                assert row["total"] == 2
                assert set(row["names"]) == {"Carol", "Dave"}
                assert row["avg_age"] == 37.5  # (35 + 40) / 2
            elif row["city"] == "LA":
                assert row["total"] == 1
                assert row["names"] == ["Eve"]
                assert row["avg_age"] == 28.0


class TestReturnGroupedAggregationRouting:
    """Test that mixed aggregation/non-aggregation items route to GroupedAggregation."""
    
    def test_routes_to_grouped_aggregation(self, context_with_grouped_data):
        """Verify that mixed RETURN items create GroupedAggregation relation."""
        from pycypher.relational_models import GroupedAggregation
        
        star = Star(context=context_with_grouped_data)
        
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
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
                            alias="total"
                        )
                    ]
                )
            ]
        )
        
        # Get the relation tree
        match_relation = star.to_relation(query.clauses[0].pattern)
        result_relation = star._from_return_clause(query.clauses[1], match_relation)
        
        # Should be GroupedAggregation
        assert isinstance(result_relation, GroupedAggregation)
        assert "city" in result_relation.grouping_expressions
        assert "total" in result_relation.aggregations


class TestReturnGroupedAggregationEdgeCases:
    """Test edge cases for grouped aggregations."""
    
    def test_single_row_per_group(self, context_with_grouped_data):
        """Test grouping where each group has exactly one row."""
        star = Star(context=context_with_grouped_data)
        
        # Group by name (unique per person)
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="name"
                            ),
                            alias="name"
                        ),
                        ReturnItem(
                            expression=CountStar(),
                            alias="total"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 5
        assert all(result_df["total"] == 1)
    
    def test_empty_match_with_grouping(self):
        """Test grouped aggregation on empty result set."""
        # Create empty context with empty Person table
        person_df = pd.DataFrame({
            ID_COLUMN: [],
            "name": [],
            "city": [],
        })
        
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "city"],
            source_obj_attribute_map={"name": "name", "city": "city"},
            attribute_map={"name": "name", "city": "city"},
            source_obj=person_df,
        )
        
        context = Context(entity_mapping=EntityMapping(mapping={"Person": person_table}))
        star = Star(context=context)
        
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
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
                ),
                Return(
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
                            alias="total"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        # Empty result
        assert len(result_df) == 0
        assert set(result_df.columns) == {"city", "total"}
