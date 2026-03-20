"""Integration tests for RETURN clause Phase 2: Aggregations with full queries.

Tests end-to-end query execution with MATCH...RETURN aggregations.
"""

import pandas as pd
import pytest
from pycypher.ast_models import (
    CountStar,
    FunctionInvocation,
    Match,
    NodePattern,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Variable,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

pytestmark = pytest.mark.integration


@pytest.fixture
def integration_context():
    """Create context with Person entities and KNOWS relationships."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 40, 25, 35, 28],
            "city": ["NYC", "LA", "NYC", "SF", "LA"],
        }
    )

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

    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 4],
            "since": [2020, 2021, 2019],
        }
    )

    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
            "since",
        ],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


class TestReturnAggregationIntegration:
    """Test execute_query with aggregations in RETURN."""

    def test_match_return_count_star(self, integration_context):
        """Test MATCH (p:Person) RETURN count(*) AS total."""
        star = Star(context=integration_context)

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
                                        properties={},
                                    )
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[ReturnItem(expression=CountStar(), alias="total")]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert list(result_df.columns) == ["total"]
        assert result_df["total"].iloc[0] == 5

    def test_match_return_collect(self, integration_context):
        """Test MATCH (p:Person) RETURN collect(p.name) AS names."""
        star = Star(context=integration_context)

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
                                        properties={},
                                    )
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=Variable(name="p"),
                                        property="name",
                                    )
                                },
                            ),
                            alias="names",
                        )
                    ]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert list(result_df.columns) == ["names"]
        assert len(result_df["names"].iloc[0]) == 5
        assert set(result_df["names"].iloc[0]) == {
            "Alice",
            "Bob",
            "Carol",
            "Dave",
            "Eve",
        }

    def test_match_return_sum_avg(self, integration_context):
        """Test MATCH (p:Person) RETURN sum(p.age) AS total_age, avg(p.age) AS avg_age."""
        star = Star(context=integration_context)

        person_var = Variable(name="p")

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=person_var,
                                        labels=["Person"],
                                        properties={},
                                    )
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="sum",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=person_var, property="age"
                                    )
                                },
                            ),
                            alias="total_age",
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="avg",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=person_var, property="age"
                                    )
                                },
                            ),
                            alias="avg_age",
                        ),
                    ]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert set(result_df.columns) == {"total_age", "avg_age"}
        assert result_df["total_age"].iloc[0] == 158  # 30+40+25+35+28
        assert result_df["avg_age"].iloc[0] == pytest.approx(31.6)

    def test_match_return_min_max(self, integration_context):
        """Test MATCH (p:Person) RETURN min(p.age) AS min_age, max(p.age) AS max_age."""
        star = Star(context=integration_context)

        person_var = Variable(name="p")

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=person_var,
                                        labels=["Person"],
                                        properties={},
                                    )
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="min",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=person_var, property="age"
                                    )
                                },
                            ),
                            alias="min_age",
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="max",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=person_var, property="age"
                                    )
                                },
                            ),
                            alias="max_age",
                        ),
                    ]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert set(result_df.columns) == {"min_age", "max_age"}
        assert result_df["min_age"].iloc[0] == 25
        assert result_df["max_age"].iloc[0] == 40

    def test_match_return_multiple_aggregations(self, integration_context):
        """Test MATCH (p:Person) RETURN count(*) AS total, collect(p) AS people, sum(p.age) AS total_age."""
        star = Star(context=integration_context)

        person_var = Variable(name="p")

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=person_var,
                                        labels=["Person"],
                                        properties={},
                                    )
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(expression=CountStar(), alias="total"),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={"expression": person_var},
                            ),
                            alias="people",
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="sum",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=person_var, property="age"
                                    )
                                },
                            ),
                            alias="total_age",
                        ),
                    ]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert set(result_df.columns) == {"total", "people", "total_age"}
        assert result_df["total"].iloc[0] == 5
        assert len(result_df["people"].iloc[0]) == 5
        assert result_df["total_age"].iloc[0] == 158


class TestReturnAggregationWithRelationships:
    """Test aggregations with relationship patterns."""

    def test_match_relationship_count(self, integration_context):
        """Test MATCH (a)-[r:KNOWS]->(b) RETURN count(*) AS total."""
        star = Star(context=integration_context)

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="a"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="r"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={},
                                    ),
                                    NodePattern(
                                        variable=Variable(name="b"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[ReturnItem(expression=CountStar(), alias="total")]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert result_df["total"].iloc[0] == 3  # 3 KNOWS relationships

    def test_match_relationship_collect_names(self, integration_context):
        """Test MATCH (a)-[r:KNOWS]->(b) RETURN collect(a.name) AS from_names, collect(b.name) AS to_names."""
        star = Star(context=integration_context)

        a_var = Variable(name="a")
        b_var = Variable(name="b")

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=a_var,
                                        labels=["Person"],
                                        properties={},
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="r"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={},
                                    ),
                                    NodePattern(
                                        variable=b_var,
                                        labels=["Person"],
                                        properties={},
                                    ),
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=a_var, property="name"
                                    )
                                },
                            ),
                            alias="from_names",
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=b_var, property="name"
                                    )
                                },
                            ),
                            alias="to_names",
                        ),
                    ]
                ),
            ]
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 1
        assert set(result_df.columns) == {"from_names", "to_names"}
        assert len(result_df["from_names"].iloc[0]) == 3
        assert len(result_df["to_names"].iloc[0]) == 3
        # Alice->Bob, Bob->Carol, Carol->Dave
        assert set(result_df["from_names"].iloc[0]) == {
            "Alice",
            "Bob",
            "Carol",
        }
        assert set(result_df["to_names"].iloc[0]) == {"Bob", "Carol", "Dave"}


class TestReturnAggregationStringParsing:
    """Test that aggregation queries can be parsed from strings."""

    def test_string_query_count_star(self, integration_context):
        """Test parsing 'MATCH (p:Person) RETURN count(*) AS total'."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN count(*) AS total"
        )

        assert len(result_df) == 1
        assert result_df["total"].iloc[0] == 5

    def test_string_query_collect(self, integration_context):
        """Test parsing 'MATCH (p:Person) RETURN collect(p.name) AS names'."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN collect(p.name) AS names"
        )

        assert len(result_df) == 1
        assert len(result_df["names"].iloc[0]) == 5

    def test_string_query_multiple_aggregations(self, integration_context):
        """Test parsing multiple aggregations."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN count(*) AS total, sum(p.age) AS total_age"
        )

        assert len(result_df) == 1
        assert result_df["total"].iloc[0] == 5
        assert result_df["total_age"].iloc[0] == 158
