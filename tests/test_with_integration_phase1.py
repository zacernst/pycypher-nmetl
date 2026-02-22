"""Integration tests for WITH clause with MATCH and RETURN (Phase 1).

Tests the complete pipeline from parsing through execution.
"""

import pandas as pd
import pytest

from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import ASTConverter, Query, Match, With, Return
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture
def integration_context():
    """Create a test context with Person and City entities."""
    # Person data
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carol", "Dave"],
        "age": [30, 40, 25, 35],
        "city": ["NYC", "LA", "SF", "NYC"],
        "salary": [100000, 120000, 90000, 110000],
    })
    
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
    
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table})
    )
    
    return context


class TestWithIntegration:
    """Integration tests for WITH clause."""
    
    def test_match_with_return_single_property(self, integration_context):
        """Test MATCH (p:Person) WITH p.name AS person_name RETURN person_name."""
        # This would normally be parsed, but we'll construct manually for now
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
        )
        
        star = Star(context=integration_context)
        
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
        
        # Step 2: WITH p.name AS person_name
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias="person_name"
                )
            ]
        )
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        # Step 3: Get final DataFrame
        result_df = with_relation.to_pandas(context=integration_context)
        
        # Verify results
        assert len(result_df) == 4
        assert list(result_df.columns) == ["person_name"]
        assert set(result_df["person_name"]) == {"Alice", "Bob", "Carol", "Dave"}
    
    def test_match_with_return_multiple_properties(self, integration_context):
        """Test MATCH (p:Person) WITH p.name AS name, p.age AS age RETURN name, age."""
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
        )
        
        star = Star(context=integration_context)
        
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
        
        # WITH p.name AS name, p.age AS age
        with_clause = With(
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
            ]
        )
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        result_df = with_relation.to_pandas(context=integration_context)
        
        # Verify results
        assert len(result_df) == 4
        assert set(result_df.columns) == {"name", "age"}
        assert set(result_df["name"]) == {"Alice", "Bob", "Carol", "Dave"}
        assert set(result_df["age"]) == {30, 40, 25, 35}
    
    def test_match_with_mixed_expressions(self, integration_context):
        """Test WITH combining properties, variables, and literals."""
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
            StringLiteral,
        )
        
        star = Star(context=integration_context)
        
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
        
        # WITH p.name AS name, p AS person_id, 'constant' AS const
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="name"
                    ),
                    alias="name"
                ),
                ReturnItem(
                    expression=Variable(name="p"),
                    alias="person_id"
                ),
                ReturnItem(
                    expression=StringLiteral(value="constant"),
                    alias="const"
                ),
            ]
        )
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        result_df = with_relation.to_pandas(context=integration_context)
        
        # Verify results
        assert len(result_df) == 4
        assert set(result_df.columns) == {"name", "person_id", "const"}
        assert list(result_df["person_id"]) == [1, 2, 3, 4]
        assert all(result_df["const"] == "constant")
    
    def test_match_with_filters_applied_before_with(self, integration_context):
        """Test MATCH (p:Person {age: 30}) WITH p.name AS name."""
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
        )
        
        star = Star(context=integration_context)
        
        # MATCH (p:Person {age: 30})
        match_pattern = Pattern(
            paths=[
                PatternPath(
                    elements=[
                        NodePattern(
                            variable=Variable(name="p"),
                            labels=["Person"],
                            properties={"age": 30}
                        )
                    ]
                )
            ]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.name AS name
        with_clause = With(
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
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        result_df = with_relation.to_pandas(context=integration_context)
        
        # Should only have Alice (age 30)
        assert len(result_df) == 1
        assert list(result_df["name"]) == ["Alice"]
    
    def test_variable_scope_isolation(self, integration_context):
        """Test that WITH creates new variable scope."""
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
        )
        
        star = Star(context=integration_context)
        
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
        
        # Original relation should have 'p' variable
        assert Variable(name="p") in match_relation.variable_map
        
        # WITH p.name AS name
        with_clause = With(
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
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        # After WITH, only 'name' should be in scope
        assert Variable(name="p") not in with_relation.variable_map
        assert Variable(name="name") in with_relation.variable_map
        
        # Result should still be correct
        result_df = with_relation.to_pandas(context=integration_context)
        assert len(result_df) == 4
        assert "name" in result_df.columns


class TestWithEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_with_on_empty_result(self, integration_context):
        """Test WITH clause on empty result set."""
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
        )
        
        star = Star(context=integration_context)
        
        # MATCH (p:Person {age: 999}) - no such person
        match_pattern = Pattern(
            paths=[
                PatternPath(
                    elements=[
                        NodePattern(
                            variable=Variable(name="p"),
                            labels=["Person"],
                            properties={"age": 999}
                        )
                    ]
                )
            ]
        )
        match_relation = star.to_relation(match_pattern)
        
        # WITH p.name AS name
        with_clause = With(
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
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        result_df = with_relation.to_pandas(context=integration_context)
        
        # Should be empty
        assert len(result_df) == 0
        assert "name" in result_df.columns
    
    def test_with_all_properties(self, integration_context):
        """Test WITH projecting all properties."""
        from pycypher.ast_models import (
            NodePattern,
            Pattern,
            PatternPath,
            Variable,
            PropertyLookup,
            ReturnItem,
        )
        
        star = Star(context=integration_context)
        
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
        
        # WITH p.name AS name, p.age AS age, p.city AS city, p.salary AS salary
        with_clause = With(
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
                ),
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"),
                        property="salary"
                    ),
                    alias="salary"
                ),
            ]
        )
        with_relation = star._from_with_clause(with_clause, match_relation)
        
        result_df = with_relation.to_pandas(context=integration_context)
        
        # Verify all properties present
        assert len(result_df) == 4
        assert set(result_df.columns) == {"name", "age", "city", "salary"}
        assert set(result_df["name"]) == {"Alice", "Bob", "Carol", "Dave"}
