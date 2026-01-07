# AST Variable Representation

## Overview

As of January 2026, the PyCypher AST models have been refactored to use a consistent representation for variables throughout the AST structure. All variable references are now represented as `Variable` instances rather than plain strings.

## Motivation

Previously, the AST used an inconsistent approach:
- **Binding contexts** (patterns, comprehensions): Plain `str` fields
- **Expression contexts**: `Variable` class instances

This inconsistency led to:
- Complex validation logic needing to handle both cases
- Potential confusion about when to use strings vs. Variable instances
- More difficult AST traversal and transformation

## Current Implementation

### Variable Class

```python
class Variable(Expression):
    """Variable reference in Cypher queries.
    
    Represents a variable name used in expressions, patterns, and bindings.
    
    Attributes:
        name: The variable name (e.g., 'n', 'person', 'rel')
    """
    name: str
```

### Where Variables Are Used

All of the following AST nodes now use `Variable` instances for their `variable` fields:

#### Pattern Nodes
- `NodePattern` - Node variables in patterns
- `RelationshipPattern` - Relationship variables
- `PatternPath` - Path binding variables

#### Comprehensions
- `ListComprehension` - Iteration variable
- `PatternComprehension` - Path binding variable
- `MapProjection` - Source object variable

#### Quantifiers and Reductions
- `Quantifier` (ALL, ANY, NONE, SINGLE) - Iteration variable
- `Reduce` - Both accumulator and iteration variables

#### Clause Items
- `SetItem` - Variable being modified
- `RemoveItem` - Variable being modified
- `YieldItem` - Yielded variable (in CALL clauses)

#### Legacy Support
- `PropertyLookup` - Deprecated variable field (use expression instead)

## Usage Examples

### Creating AST Nodes Programmatically

```python
from pycypher.ast_models import NodePattern, Variable, RelationshipPattern

# Create a node pattern
node = NodePattern(
    variable=Variable(name="person"),
    labels=["Person"],
    properties={"name": "Alice"}
)

# Access the variable name
print(node.variable.name)  # "person"

# Create a relationship pattern
rel = RelationshipPattern(
    variable=Variable(name="knows"),
    types=["KNOWS"],
    direction="right"
)
```

### Parsing Queries

The `ASTConverter` automatically wraps string variable names in `Variable` instances during conversion:

```python
from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import ASTConverter

parser = GrammarParser()
converter = ASTConverter()

# Parse query
raw_ast = parser.parse_to_ast("MATCH (n:Person) RETURN n")
typed_ast = converter.convert(raw_ast)

# Variables are automatically wrapped
# The 'n' in the pattern becomes Variable(name="n")
```

### Working with Variables in Validation

```python
from pycypher.ast_models import NodePattern, Variable

def collect_node_variables(pattern):
    """Collect all node variable names from a pattern."""
    variables = []
    for path in pattern.paths:
        for elem in path.elements:
            if isinstance(elem, NodePattern) and elem.variable:
                # Access the variable name
                variables.append(elem.variable.name)
    return variables
```

## Migration Guide

If you have existing code that creates AST nodes directly:

### Before (Old Way - No Longer Works)
```python
# This will raise a ValidationError
node = NodePattern(
    variable="n",  # ❌ Plain string
    labels=["Person"]
)
```

### After (New Way - Required)
```python
# Correct usage
node = NodePattern(
    variable=Variable(name="n"),  # ✅ Variable instance
    labels=["Person"]
)
```

### Accessing Variable Names

```python
# Old way (when it was a string)
# var_name = node.variable  # ❌ No longer works

# New way
if node.variable:
    var_name = node.variable.name  # ✅ Access the name attribute
```

## Benefits

1. **Consistency**: One representation for all variables
2. **Type Safety**: Strong typing throughout the AST
3. **Simpler Code**: Validation and traversal logic is cleaner
4. **Better Semantics**: Variables as objects vs. strings is more explicit
5. **Easier Maintenance**: Changes to variable handling happen in one place

## See Also

- [AST Models API Reference](../api/pycypher.rst#module-pycypher.ast_models)
- [Grammar Parser Guide](GRAMMAR_PARSER_GUIDE.md)
- [Examples](../examples/ast_conversion_example.py)
