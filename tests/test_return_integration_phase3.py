"""Integration tests for RETURN clause Phase 3: Grouped Aggregations.

Phase 3 integration tests verify end-to-end query execution with grouped
aggregations, including string query parsing and relationship pattern grouping.

Test Coverage:
- Full query execution with grouping
- String query parsing with GROUP BY semantics
- Relationship patterns with grouping
- Complex multi-level grouping
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
    """Context with rich data for integration testing."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "city": ["NYC", "NYC", "SF", "SF", "LA"],
            "age": [30, 25, 35, 40, 28],
            "salary": [100000, 80000, 120000, 140000, 90000],
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "city", "salary"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
            "salary": "salary",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
            "salary": "salary",
        },
        source_obj=person_df,
    )

    # Add some relationships for relationship-based grouping tests
    # Alice (1) knows Bob (2), Carol (3)
    # Bob (2) knows Carol (3)
    # Carol (3) knows Dave (4)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12, 13],
            RELATIONSHIP_SOURCE_COLUMN: [1, 1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 3, 4],
        },
    )

    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


class TestReturnGroupedAggregationIntegration:
    """Integration tests for full query execution with grouped aggregations."""

    def test_match_return_group_by_city_count(self, integration_context):
        """Test MATCH (p:Person) RETURN p.city, count(*) AS total."""
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
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="city",
                            ),
                            alias="city",
                        ),
                        ReturnItem(expression=CountStar(), alias="total"),
                    ],
                ),
            ],
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 3
        city_counts = dict(zip(result_df["city"], result_df["total"]))
        assert city_counts["NYC"] == 2
        assert city_counts["SF"] == 2
        assert city_counts["LA"] == 1

    def test_match_return_group_by_city_with_aggregations(
        self,
        integration_context,
    ):
        """Test MATCH (p:Person) RETURN p.city, count(*), avg(p.salary), collect(p.name)."""
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
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="city",
                            ),
                            alias="city",
                        ),
                        ReturnItem(expression=CountStar(), alias="total"),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="avg",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=Variable(name="p"),
                                        property="salary",
                                    ),
                                },
                            ),
                            alias="avg_salary",
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=Variable(name="p"),
                                        property="name",
                                    ),
                                },
                            ),
                            alias="names",
                        ),
                    ],
                ),
            ],
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 3
        assert set(result_df.columns) == {
            "city",
            "total",
            "avg_salary",
            "names",
        }

        for _, row in result_df.iterrows():
            if row["city"] == "NYC":
                assert row["total"] == 2
                assert row["avg_salary"] == 90000.0  # (100000 + 80000) / 2
                assert set(row["names"]) == {"Alice", "Bob"}
            elif row["city"] == "SF":
                assert row["total"] == 2
                assert row["avg_salary"] == 130000.0  # (120000 + 140000) / 2
                assert set(row["names"]) == {"Carol", "Dave"}
            elif row["city"] == "LA":
                assert row["total"] == 1
                assert row["avg_salary"] == 90000.0
                assert row["names"] == ["Eve"]

    def test_match_return_multiple_grouping_keys(self, integration_context):
        """Test MATCH (p:Person) RETURN p.city, p.age, count(*) AS total."""
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
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="city",
                            ),
                            alias="city",
                        ),
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="age",
                            ),
                            alias="age",
                        ),
                        ReturnItem(expression=CountStar(), alias="total"),
                    ],
                ),
            ],
        )

        result_df = star.execute_query(query)

        # Each person has unique (city, age)
        assert len(result_df) == 5
        assert all(result_df["total"] == 1)


class TestReturnGroupedAggregationWithRelationships:
    """Test grouped aggregations with relationship patterns."""

    def test_match_relationship_group_by_source_city(
        self,
        integration_context,
    ):
        """Test MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.city, count(*) AS connections."""
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
                                ],
                            ),
                        ],
                    ),
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=a_var,
                                property="city",
                            ),
                            alias="source_city",
                        ),
                        ReturnItem(
                            expression=CountStar(),
                            alias="connections",
                        ),
                    ],
                ),
            ],
        )

        result_df = star.execute_query(query)

        # Alice (NYC) -> 2 connections (Bob, Carol)
        # Bob (NYC) -> 1 connection (Carol)
        # Carol (SF) -> 1 connection (Dave)
        # So: NYC=3, SF=1
        assert len(result_df) == 2
        city_connections = dict(
            zip(result_df["source_city"], result_df["connections"]),
        )
        assert city_connections["NYC"] == 3
        assert city_connections["SF"] == 1

    def test_match_relationship_group_by_cities_collect_names(
        self,
        integration_context,
    ):
        """Test MATCH (a)-[:KNOWS]->(b) RETURN a.city, collect(b.name) AS known_people."""
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
                                ],
                            ),
                        ],
                    ),
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=a_var,
                                property="city",
                            ),
                            alias="source_city",
                        ),
                        ReturnItem(
                            expression=FunctionInvocation(
                                name="collect",
                                arguments={
                                    "expression": PropertyLookup(
                                        expression=b_var,
                                        property="name",
                                    ),
                                },
                            ),
                            alias="known_people",
                        ),
                    ],
                ),
            ],
        )

        result_df = star.execute_query(query)

        assert len(result_df) == 2

        for _, row in result_df.iterrows():
            if row["source_city"] == "NYC":
                # Alice knows Bob, Carol; Bob knows Carol
                assert set(row["known_people"]) == {"Bob", "Carol"}
            elif row["source_city"] == "SF":
                # Carol knows Dave
                assert set(row["known_people"]) == {"Dave"}


class TestReturnGroupedAggregationStringParsing:
    """Test that grouped aggregation queries can be parsed from strings."""

    def test_string_query_group_by_single_key(self, integration_context):
        """Test parsing 'MATCH (p:Person) RETURN p.city AS city, count(*) AS total'."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN p.city AS city, count(*) AS total",
        )

        assert len(result_df) == 3
        city_counts = dict(zip(result_df["city"], result_df["total"]))
        assert city_counts["NYC"] == 2
        assert city_counts["SF"] == 2
        assert city_counts["LA"] == 1

    def test_string_query_group_by_with_collect(self, integration_context):
        """Test parsing 'MATCH (p:Person) RETURN p.city AS city, collect(p.name) AS names'."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN p.city AS city, collect(p.name) AS names",
        )

        assert len(result_df) == 3
        assert set(result_df.columns) == {"city", "names"}

        city_names = {
            row["city"]: set(row["names"]) for _, row in result_df.iterrows()
        }
        assert city_names["NYC"] == {"Alice", "Bob"}
        assert city_names["SF"] == {"Carol", "Dave"}
        assert city_names["LA"] == {"Eve"}

    def test_string_query_multiple_grouping_keys_and_aggregations(
        self,
        integration_context,
    ):
        """Test parsing complex grouped aggregation query."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN p.city AS city, count(*) AS total, avg(p.salary) AS avg_salary",
        )

        assert len(result_df) == 3
        assert set(result_df.columns) == {"city", "total", "avg_salary"}

        for _, row in result_df.iterrows():
            if row["city"] == "NYC":
                assert row["total"] == 2
                assert row["avg_salary"] == 90000.0
            elif row["city"] == "SF":
                assert row["total"] == 2
                assert row["avg_salary"] == 130000.0
            elif row["city"] == "LA":
                assert row["total"] == 1
                assert row["avg_salary"] == 90000.0

    def test_string_query_group_by_multiple_keys(self, integration_context):
        """Test parsing 'MATCH (p:Person) RETURN p.city AS city, p.age AS age, count(*) AS total'."""
        star = Star(context=integration_context)

        result_df = star.execute_query(
            "MATCH (p:Person) RETURN p.city AS city, p.age AS age, count(*) AS total",
        )

        assert len(result_df) == 5
        assert set(result_df.columns) == {"city", "age", "total"}
        assert all(result_df["total"] == 1)
