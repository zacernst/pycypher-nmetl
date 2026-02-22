"""
Unit tests for comma-separated pattern translation.

Tests whether the current implementation can handle comma-separated patterns
in MATCH clauses, specifically patterns like:
    (p:Person), (p)-[k:KNOWS]->(q:Person)

This combines a standalone NodePattern with a PatternPath, which requires
joining (NodePattern, PatternPath) - a capability not currently implemented
in Star._binary_join().
"""

import pandas as pd
import pytest
from pycypher.ast_models import ASTConverter, Variable
from pycypher.grammar_parser import GrammarParser
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture
def test_context():
    """Create a test context with Person nodes and KNOWS relationships."""
    # Person entity data
    entity_df_person = pd.DataFrame(
        data={
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 40, 50],
        }
    )

    # KNOWS relationship data
    relationship_df_knows = pd.DataFrame(
        data={
            ID_COLUMN: [10, 11, 12],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 1],
            "since": [2020, 2019, 2021],
        }
    )

    # Import the table types
    from pycypher.relational_models import EntityTable, RelationshipTable

    # Create entity table
    entity_table_person = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=entity_df_person,
    )

    # Create relationship table
    relationship_table_knows = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
            "since",
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        source_obj=relationship_df_knows,
    )

    # Create context
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": entity_table_person}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": relationship_table_knows}
        ),
    )

    return context


class TestCommaSeparatedPatterns:
    """Test comma-separated pattern translation capabilities."""

    def test_comma_separated_node_and_path(self, test_context):
        """Test comma-separated NodePattern and PatternPath.

        This test verifies that the query:
            MATCH (p:Person), (p)-[k:KNOWS]->(q:Person)

        Can be correctly translated to relational algebra. The pattern combines:
            - PatternPath([NodePattern(p:Person)])
            - PatternPath([NodePattern(p, labels=[]), RelationshipPattern(k:KNOWS), NodePattern(q:Person)])

        The implementation should:
        1. Register p's type (Person) from the first pattern
        2. Use the registered type when processing the second pattern's NodePattern(p) with empty labels
        3. Join the two paths on the common variable p
        """
        parser = GrammarParser()
        converter = ASTConverter()
        star = Star(context=test_context)

        # This query has comma-separated patterns
        cypher_query = "MATCH (p:Person), (p)-[k:KNOWS]->(q:Person) RETURN p, k, q"

        # Parse the query
        tree = parser.parse(cypher_query)
        ast_dict = parser.transformer.transform(tree)
        query_ast = converter.convert(ast_dict)

        # Extract the pattern from the MATCH clause
        pattern = query_ast.clauses[0].pattern

        # Translate pattern to relational algebra
        result = star.to_relation(pattern)

        # Verify the result has all three variables
        assert Variable(name="p") in result.variable_map, "Variable 'p' should be in result"
        assert Variable(name="k") in result.variable_map, "Variable 'k' should be in result"
        assert Variable(name="q") in result.variable_map, "Variable 'q' should be in result"

        # Verify the result produces valid data
        df = result.to_pandas(context=test_context)
        assert len(df) > 0, "Should have at least one result row"
        # Note: pandas merge may create duplicate column names with suffixes
        # We should have 3 unique variables, but may have more columns due to join mechanics
        assert len(df.columns) >= 3, f"Should have at least 3 columns, got {len(df.columns)}"

    def test_comma_separated_two_nodes(self, test_context):
        """Test comma-separated NodePattern instances.

        Query: MATCH (p:Person), (q:Person)

        This creates two NodePatterns with different variables, resulting in
        a cross product of all Person nodes (each p paired with each q).
        """
        parser = GrammarParser()
        converter = ASTConverter()
        star = Star(context=test_context)

        cypher_query = "MATCH (p:Person), (q:Person) RETURN p, q"

        tree = parser.parse(cypher_query)
        ast_dict = parser.transformer.transform(tree)
        query_ast = converter.convert(ast_dict)

        pattern = query_ast.clauses[0].pattern

        # Should successfully translate to a cross product
        result = star.to_relation(pattern)

        # Verify both variables are present
        assert Variable(name="p") in result.variable_map
        assert Variable(name="q") in result.variable_map

        # Verify cross product: should have 3 * 3 = 9 rows
        df = result.to_pandas(context=test_context)
        assert len(df) == 9, f"Cross product should have 9 rows, got {len(df)}"
        # Note: Number of columns depends on join implementation details
        assert len(df.columns) >= 2, f"Should have at least 2 columns, got {len(df.columns)}"

    def test_comma_separated_paths(self, test_context):
        """Test comma-separated PatternPath instances.

        Query: MATCH (p)-[k:KNOWS]->(q), (q)-[m:KNOWS]->(r)

        This creates two PatternPath objects that need to be joined on 'q'.
        Tests the ability to join two multi-element paths on a common variable.
        """
        parser = GrammarParser()
        converter = ASTConverter()
        star = Star(context=test_context)

        cypher_query = (
            "MATCH (p:Person)-[k:KNOWS]->(q:Person), "
            "(q)-[m:KNOWS]->(r:Person) RETURN p, k, q, m, r"
        )

        tree = parser.parse(cypher_query)
        ast_dict = parser.transformer.transform(tree)
        query_ast = converter.convert(ast_dict)

        pattern = query_ast.clauses[0].pattern

        # Should successfully translate and join on q
        result = star.to_relation(pattern)

        # Verify all variables are present
        for var_name in ["p", "k", "q", "m", "r"]:
            var = Variable(name=var_name)
            assert var in result.variable_map, f"Variable '{var_name}' should be in result"

        # Verify the result produces valid data
        df = result.to_pandas(context=test_context)
        assert len(df) > 0, "Should have at least one result row"
        # Note: Number of columns depends on join implementation details  
        assert len(df.columns) >= 3, f"Should have at least 3 columns, got {len(df.columns)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
