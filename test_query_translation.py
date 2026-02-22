"""Test script to check if the query can be translated."""
from pycypher.grammar_parser import GrammarParser
from pycypher.star import Star
from pycypher.relational_models import Context, EntityMapping, RelationshipMapping
import pandas as pd

# Parse the query
query = "MATCH (p:Person), (p)-[k:KNOWS]->(q:Person) RETURN p, k, q"
parser = GrammarParser()

try:
    print(f"Parsing query: {query}")
    parsed = parser.parse(query)
    print("✓ Query parsed successfully")
    print(f"Parsed AST type: {type(parsed)}")
    print(f"AST: {parsed}")
    
    # Try to convert to relational algebra
    # We need a minimal context with Person and KNOWS
    from pycypher.relational_models import EntityTable, RelationshipTable, ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN
    
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })
    
    knows_df = pd.DataFrame({
        ID_COLUMN: [10, 11],
        RELATIONSHIP_SOURCE_COLUMN: [1, 2],
        RELATIONSHIP_TARGET_COLUMN: [2, 3]
    })
    
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=person_df
    )
    
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN
        },
        source_obj=knows_df
    )
    
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table})
    )
    
    star = Star(context=context)
    
    # Get the MATCH clause
    match_clause = parsed.clauses[0]
    pattern = match_clause.pattern
    
    print(f"\nPattern has {len(pattern.paths)} paths:")
    for i, path in enumerate(pattern.paths):
        print(f"  Path {i}: {path}")
    
    print("\nAttempting to translate to relational algebra...")
    relation = star.to_relation(pattern)
    print("✓ Translation successful!")
    print(f"Result type: {type(relation)}")
    
except Exception as e:
    print(f"\n✗ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
