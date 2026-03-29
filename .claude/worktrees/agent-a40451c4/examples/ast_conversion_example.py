"""Example script to demonstrate AST conversion usage."""

import json

from pycypher.ast_models import convert_ast
from pycypher.grammar_parser import GrammarParser

# Create parser
parser = GrammarParser()

# Simple query
query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
print(f"Query: {query}\n")

# Parse to raw AST (dict-based)
raw_ast = parser.parse_to_ast(query)
print("Raw AST structure:")
print(json.dumps(raw_ast, indent=2, default=str)[:500])
print("...\n")

# Convert to typed AST (Pydantic models)
typed_ast = convert_ast(raw_ast)
print(f"Typed AST root type: {type(typed_ast).__name__}")

# Pretty print
if typed_ast:
    print("\nPretty printed AST:")
    print(typed_ast.pretty())

# Traverse nodes
if typed_ast:
    print("\n" + "=" * 60)
    print("All node types in AST:")
    node_types = {}
    for node in typed_ast.traverse():
        node_type = type(node).__name__
        node_types[node_type] = node_types.get(node_type, 0) + 1

    for node_type, count in sorted(node_types.items()):
        print(f"  {node_type}: {count}")
