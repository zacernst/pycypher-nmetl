"""Integration tests for RETURN clause Phase 1: Full query execution.

Tests end-to-end query execution with MATCH...RETURN.
"""

import pytest
import pandas as pd
from pycypher.ast_models import (
    Query,
    Match,
    Return,
    NodePattern,
    Pattern,
    PatternPath,
    Variable,
    PropertyLookup,
    ReturnItem,
    RelationshipPattern,
    RelationshipDirection,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    RelationshipMapping,
    EntityTable,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def integration_context():
    """Create context with Person entities and KNOWS relationships."""
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
    
    knows_df = pd.DataFrame({
        ID_COLUMN: [10, 11, 12],
        RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
        RELATIONSHIP_TARGET_COLUMN: [2, 3, 4],
        "since": [2020, 2021, 2019],
    })
    
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN, "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
    )
    
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table})
    )


class TestExecuteQueryBasic:
    """Test execute_query with simple MATCH...RETURN queries."""
    
    def test_simple_match_return_property(self, integration_context):
        """Test MATCH (p:Person) RETURN p.name AS name."""
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
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 4
        assert list(result_df.columns) == ["name"]
        assert set(result_df["name"]) == {"Alice", "Bob", "Carol", "Dave"}
    
    def test_match_return_multiple_properties(self, integration_context):
        """Test MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.city AS city."""
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
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="age"
                            ),
                            alias="age"
                        ),
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"),
                                property="city"
                            ),
                            alias="city"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 4
        assert set(result_df.columns) == {"name", "age", "city"}
        
        # Check specific row
        alice_row = result_df[result_df["name"] == "Alice"].iloc[0]
        assert alice_row["age"] == 30
        assert alice_row["city"] == "NYC"
    
    def test_match_return_variable(self, integration_context):
        """Test MATCH (p:Person) RETURN p AS person_id."""
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
                            expression=Variable(name="p"),
                            alias="person_id"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 4
        assert list(result_df.columns) == ["person_id"]
        assert set(result_df["person_id"]) == {1, 2, 3, 4}


class TestExecuteQueryWithRelationships:
    """Test execute_query with relationship patterns."""
    
    def test_match_relationship_return_nodes(self, integration_context):
        """Test MATCH (a)-[r:KNOWS]->(b) RETURN a AS from_id, b AS to_id."""
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
                                        properties={}
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="r"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={}
                                    ),
                                    NodePattern(
                                        variable=Variable(name="b"),
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
                            expression=Variable(name="a"),
                            alias="from_id"
                        ),
                        ReturnItem(
                            expression=Variable(name="b"),
                            alias="to_id"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        # Should have 3 rows (3 KNOWS relationships)
        assert len(result_df) == 3
        assert set(result_df.columns) == {"from_id", "to_id"}
        
        # Check specific relationships
        assert (1, 2) in zip(result_df["from_id"], result_df["to_id"])
        assert (2, 3) in zip(result_df["from_id"], result_df["to_id"])
        assert (3, 4) in zip(result_df["from_id"], result_df["to_id"])
    
    def test_match_relationship_return_properties(self, integration_context):
        """Test MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS from_name, b.name AS to_name."""
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
                                        properties={}
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="r"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={}
                                    ),
                                    NodePattern(
                                        variable=Variable(name="b"),
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
                                expression=Variable(name="a"),
                                property="name"
                            ),
                            alias="from_name"
                        ),
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="b"),
                                property="name"
                            ),
                            alias="to_name"
                        )
                    ]
                )
            ]
        )
        
        result_df = star.execute_query(query)
        
        assert len(result_df) == 3
        assert set(result_df.columns) == {"from_name", "to_name"}
        
        # Check specific relationships
        assert ("Alice", "Bob") in zip(result_df["from_name"], result_df["to_name"])
        assert ("Bob", "Carol") in zip(result_df["from_name"], result_df["to_name"])
        assert ("Carol", "Dave") in zip(result_df["from_name"], result_df["to_name"])


class TestExecuteQueryErrors:
    """Test error handling in execute_query."""
    
    def test_empty_query_fails(self, integration_context):
        """Test that empty query raises error."""
        star = Star(context=integration_context)
        
        query = Query(clauses=[])
        
        with pytest.raises(ValueError, match="at least one clause"):
            star.execute_query(query)
    
    def test_return_without_match_fails(self, integration_context):
        """Test that RETURN without MATCH raises error."""
        star = Star(context=integration_context)
        
        query = Query(
            clauses=[
                Return(
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
            ]
        )
        
        with pytest.raises(ValueError, match="requires preceding MATCH"):
            star.execute_query(query)
    
    def test_string_query_parsing(self, integration_context):
        """Test that execute_query can parse string queries."""
        star = Star(context=integration_context)
        
        # This should parse the query string
        result_df = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        
        assert len(result_df) == 4
        assert "name" in result_df.columns
