# Grammar Parser User Guide

A comprehensive guide to using the openCypher grammar parser for parsing, validating, and analyzing Cypher queries.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Basic Usage](#basic-usage)
- [Working with the AST](#working-with-the-ast)
- [Practical Examples](#practical-examples)
- [API Reference](#api-reference)
- [AST Structure Reference](#ast-structure-reference)
- [Advanced Use Cases](#advanced-use-cases)
- [Error Handling](#error-handling)

## Overview

The grammar parser provides a complete implementation of the openCypher query language specification. It can:

- **Parse** Cypher queries into Abstract Syntax Trees (AST)
- **Validate** query syntax
- **Analyze** query structure and patterns
- **Transform** queries programmatically
- **Extract** metadata from queries

### Key Features

✅ Full openCypher specification support  
✅ Comprehensive AST generation  
✅ Detailed error reporting  
✅ Support for advanced features (EXISTS, comprehensions, quantifiers)  
✅ Production-ready with 118 passing tests  

## Installation

The grammar parser is part of the `pycypher` package:

```python
from pycypher.grammar_parser import GrammarParser
```

Dependencies:
- `lark>=1.3.1` - Parsing library with Earley algorithm support

## Quick Start

```python
from pycypher.grammar_parser import GrammarParser

# Create a parser instance
parser = GrammarParser()

# Parse a simple query
query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
tree = parser.parse(query)

# Parse to AST
ast = parser.parse_to_ast(query)
print(ast)

# Validate a query
is_valid = parser.validate(query)
print(f"Query is valid: {is_valid}")
```

## Basic Usage

### Creating a Parser

```python
from pycypher.grammar_parser import GrammarParser

# Create parser instance (reusable)
parser = GrammarParser()
```

### Parsing Queries

The parser provides three main methods:

#### 1. Parse to Tree (Low-level)

```python
# Returns a Lark Tree object
tree = parser.parse("MATCH (n) RETURN n")
print(tree.pretty())  # Pretty-print the parse tree
```

#### 2. Parse to AST (Recommended)

```python
# Returns a structured Python dict/list AST
ast = parser.parse_to_ast("MATCH (n:Person) RETURN n.name")
```

Example AST output:
```python
{
    'type': 'Query',
    'clauses': [
        {
            'type': 'Match',
            'optional': False,
            'pattern': [...],
            'where': None
        },
        {
            'type': 'Return',
            'distinct': False,
            'items': [...]
        }
    ]
}
```

#### 3. Validate Query

```python
# Returns True/False
is_valid = parser.validate("MATCH (n) RETURN n")

# Invalid query returns False
is_valid = parser.validate("MATCH (n RETURN n")  # False
```

## Working with the AST

### Understanding AST Structure

The AST is a nested dictionary structure representing the query's semantic content:

```python
from pycypher.grammar_parser import GrammarParser
import json

parser = GrammarParser()
query = "MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age"
ast = parser.parse_to_ast(query)

# Pretty print the AST
print(json.dumps(ast, indent=2))
```

### Extracting Query Information

#### Example 1: Find all node labels

```python
def extract_node_labels(ast):
    """Extract all node labels from a query."""
    labels = set()
    
    def traverse(node):
        if isinstance(node, dict):
            # Check if this is a node pattern with labels
            if node.get('type') == 'NodePattern':
                node_labels = node.get('labels', [])
                if isinstance(node_labels, list):
                    labels.update(node_labels)
            
            # Recursively traverse all dict values
            for value in node.values():
                traverse(value)
        elif isinstance(node, list):
            for item in node:
                traverse(item)
    
    traverse(ast)
    return labels

# Usage
query = """
MATCH (p:Person)-[:KNOWS]->(f:Person|Employee)
WHERE p.age > 30
RETURN p, f
"""
ast = parser.parse_to_ast(query)
labels = extract_node_labels(ast)
print(f"Node labels: {labels}")
# Output: Node labels: {'Person', 'Employee'}
```

#### Example 2: Find all relationship types

```python
def extract_relationship_types(ast):
    """Extract all relationship types from a query."""
    rel_types = set()
    
    def traverse(node):
        if isinstance(node, dict):
            if node.get('type') == 'RelationshipPattern':
                types = node.get('types', [])
                if isinstance(types, list):
                    rel_types.update(types)
            
            for value in node.values():
                traverse(value)
        elif isinstance(node, list):
            for item in node:
                traverse(item)
    
    traverse(ast)
    return rel_types

# Usage
query = "MATCH (a)-[r:KNOWS|LIKES]->(b) RETURN a, b"
ast = parser.parse_to_ast(query)
rel_types = extract_relationship_types(ast)
print(f"Relationship types: {rel_types}")
# Output: Relationship types: {'KNOWS', 'LIKES'}
```

#### Example 3: Identify query type

```python
def identify_query_type(ast):
    """Identify the primary type of query (read, write, mixed)."""
    if not isinstance(ast, dict):
        return "unknown"
    
    clauses = ast.get('clauses', [])
    
    read_clauses = {'Match', 'OptionalMatch', 'Return', 'With', 'Unwind'}
    write_clauses = {'Create', 'Merge', 'Delete', 'Set', 'Remove'}
    
    has_read = any(c.get('type') in read_clauses for c in clauses)
    has_write = any(c.get('type') in write_clauses for c in clauses)
    
    if has_read and has_write:
        return "mixed"
    elif has_write:
        return "write"
    elif has_read:
        return "read"
    return "unknown"

# Usage
read_query = "MATCH (n) RETURN n"
write_query = "CREATE (n:Person {name: 'Alice'})"
mixed_query = "MATCH (n:Person) SET n.visited = true RETURN n"

for query in [read_query, write_query, mixed_query]:
    ast = parser.parse_to_ast(query)
    qtype = identify_query_type(ast)
    print(f"Query type: {qtype}")
```

#### Example 4: Extract property access patterns

```python
def extract_property_accesses(ast):
    """Find all property accesses (e.g., n.name, person.age)."""
    properties = []
    
    def traverse(node):
        if isinstance(node, dict):
            if node.get('type') == 'PropertyLookup':
                # Note: variable field contains a Variable instance with 'name' property
                var = node.get('variable')
                prop = node.get('property')
                var_name = var.get('name') if isinstance(var, dict) else str(var)
                properties.append(f"{var_name}.{prop}")
            
            for value in node.values():
                traverse(value)
        elif isinstance(node, list):
            for item in node:
                traverse(item)
    
    traverse(ast)
    return properties

# Usage
query = """
MATCH (p:Person)
WHERE p.age > 30 AND p.name STARTS WITH 'A'
RETURN p.name, p.age, p.email
"""
ast = parser.parse_to_ast(query)
props = extract_property_accesses(ast)
print(f"Properties accessed: {props}")
# Output: Properties accessed: ['p.age', 'p.name', 'p.name', 'p.age', 'p.email']
```

## Practical Examples

### Example 1: Query Validator

Build a query validator that checks for common issues:

```python
class QueryValidator:
    def __init__(self):
        self.parser = GrammarParser()
    
    def validate_query(self, query):
        """Validate query and return detailed feedback."""
        results = {
            'valid': False,
            'errors': [],
            'warnings': []
        }
        
        # Basic syntax validation
        if not self.parser.validate(query):
            results['errors'].append("Syntax error in query")
            return results
        
        # Parse to AST for semantic checks
        ast = self.parser.parse_to_ast(query)
        results['valid'] = True
        
        # Check for SELECT * (anti-pattern in some contexts)
        if self._has_return_all(ast):
            results['warnings'].append(
                "Using RETURN * - consider specifying columns explicitly"
            )
        
        # Check for missing indexes (variables used but not bound)
        unbound = self._find_unbound_variables(ast)
        if unbound:
            results['warnings'].append(
                f"Potentially unbound variables: {', '.join(unbound)}"
            )
        
        return results
    
    def _has_return_all(self, ast):
        """Check if query uses RETURN *."""
        clauses = ast.get('clauses', [])
        for clause in clauses:
            if clause.get('type') == 'Return':
                items = clause.get('items', [])
                if any(item.get('type') == 'ReturnAll' for item in items):
                    return True
        return False
    
    def _find_unbound_variables(self, ast):
        """Find variables that might not be bound."""
        # Simplified implementation
        bound_vars = set()
        used_vars = set()
        
        # This would need more sophisticated logic in production
        # For now, return empty set
        return set()

# Usage
validator = QueryValidator()

query1 = "MATCH (n) RETURN *"
result1 = validator.validate_query(query1)
print(f"Valid: {result1['valid']}")
print(f"Warnings: {result1['warnings']}")

query2 = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age"
result2 = validator.validate_query(query2)
print(f"Valid: {result2['valid']}")
```

### Example 2: Query Analyzer

Analyze query complexity and performance characteristics:

```python
class QueryAnalyzer:
    def __init__(self):
        self.parser = GrammarParser()
    
    def analyze(self, query):
        """Analyze query and return metrics."""
        ast = self.parser.parse_to_ast(query)
        
        return {
            'num_clauses': self._count_clauses(ast),
            'has_optional_match': self._has_optional_match(ast),
            'max_path_length': self._max_path_length(ast),
            'num_filters': self._count_filters(ast),
            'aggregations': self._find_aggregations(ast),
            'complexity_score': self._complexity_score(ast)
        }
    
    def _count_clauses(self, ast):
        """Count number of clauses."""
        return len(ast.get('clauses', []))
    
    def _has_optional_match(self, ast):
        """Check if query has OPTIONAL MATCH."""
        clauses = ast.get('clauses', [])
        return any(
            c.get('type') == 'Match' and c.get('optional')
            for c in clauses
        )
    
    def _max_path_length(self, ast):
        """Find maximum path length in variable-length patterns."""
        max_length = 0
        
        def traverse(node):
            nonlocal max_length
            if isinstance(node, dict):
                if node.get('type') == 'PathLength':
                    max_val = node.get('max')
                    if max_val and isinstance(max_val, int):
                        max_length = max(max_length, max_val)
                
                for value in node.values():
                    traverse(value)
            elif isinstance(node, list):
                for item in node:
                    traverse(item)
        
        traverse(ast)
        return max_length if max_length > 0 else None
    
    def _count_filters(self, ast):
        """Count WHERE clauses."""
        count = 0
        
        def traverse(node):
            nonlocal count
            if isinstance(node, dict):
                if 'where' in node and node['where'] is not None:
                    count += 1
                for value in node.values():
                    traverse(value)
            elif isinstance(node, list):
                for item in node:
                    traverse(item)
        
        traverse(ast)
        return count
    
    def _find_aggregations(self, ast):
        """Find aggregation functions used."""
        aggs = set()
        agg_functions = {'count', 'sum', 'avg', 'min', 'max', 'collect'}
        
        def traverse(node):
            if isinstance(node, dict):
                if node.get('type') == 'FunctionInvocation':
                    name = node.get('name', '')
                    if isinstance(name, dict):
                        name = name.get('name', '')
                    if str(name).lower() in agg_functions:
                        aggs.add(str(name).lower())
                
                for value in node.values():
                    traverse(value)
            elif isinstance(node, list):
                for item in node:
                    traverse(item)
        
        traverse(ast)
        return list(aggs)
    
    def _complexity_score(self, ast):
        """Calculate a simple complexity score."""
        score = 0
        score += self._count_clauses(ast) * 10
        score += self._count_filters(ast) * 5
        score += len(self._find_aggregations(ast)) * 15
        
        max_path = self._max_path_length(ast)
        if max_path:
            score += max_path * 20
        
        if self._has_optional_match(ast):
            score += 25
        
        return score

# Usage
analyzer = QueryAnalyzer()

simple_query = "MATCH (n:Person) RETURN n.name"
complex_query = """
MATCH path = (start:Person)-[rels:KNOWS*1..5]->(end:Person)
WHERE start.age > 30
  AND ALL(r IN rels WHERE r.trust > 0.5)
WITH end, length(path) AS pathLength, 
     count(*) AS connections
WHERE connections > 3
RETURN end.name, pathLength, connections
ORDER BY connections DESC
LIMIT 10
"""

for query in [simple_query, complex_query]:
    metrics = analyzer.analyze(query)
    print(f"\nQuery: {query[:50]}...")
    print(f"Complexity Score: {metrics['complexity_score']}")
    print(f"Clauses: {metrics['num_clauses']}")
    print(f"Filters: {metrics['num_filters']}")
    print(f"Aggregations: {metrics['aggregations']}")
```

### Example 3: Query Rewriter

Transform queries programmatically:

```python
class QueryRewriter:
    def __init__(self):
        self.parser = GrammarParser()
    
    def add_limit(self, query, limit=100):
        """Add LIMIT clause to query if not present."""
        ast = self.parser.parse_to_ast(query)
        
        # Check if query already has LIMIT
        clauses = ast.get('clauses', [])
        for clause in clauses:
            if clause.get('type') == 'Return':
                if clause.get('limit') is not None:
                    return query  # Already has LIMIT
        
        # Add LIMIT
        if query.rstrip().endswith(';'):
            query = query.rstrip()[:-1]
        return f"{query} LIMIT {limit}"
    
    def add_index_hint(self, query, variable, label, property):
        """Add an index hint comment to the query."""
        hint = f"// INDEX HINT: Use {label}.{property} index for {variable}\n"
        return hint + query

# Usage
rewriter = QueryRewriter()

original = "MATCH (n:Person) WHERE n.age > 30 RETURN n"
with_limit = rewriter.add_limit(original, 50)
print(with_limit)
# Output: MATCH (n:Person) WHERE n.age > 30 RETURN n LIMIT 50

with_hint = rewriter.add_index_hint(original, 'n', 'Person', 'age')
print(with_hint)
```

### Example 4: Query Documentation Generator

Generate documentation from queries:

```python
class QueryDocGenerator:
    def __init__(self):
        self.parser = GrammarParser()
    
    def generate_doc(self, query, name="Unnamed Query"):
        """Generate documentation for a query."""
        ast = self.parser.parse_to_ast(query)
        
        doc = f"## {name}\n\n"
        doc += f"```cypher\n{query}\n```\n\n"
        
        # Extract components
        labels = self._extract_labels(ast)
        rel_types = self._extract_rel_types(ast)
        properties = self._extract_properties(ast)
        
        doc += "### Query Components\n\n"
        if labels:
            doc += f"**Node Labels:** {', '.join(sorted(labels))}\n\n"
        if rel_types:
            doc += f"**Relationship Types:** {', '.join(sorted(rel_types))}\n\n"
        if properties:
            doc += f"**Properties Accessed:** {', '.join(sorted(set(properties)))}\n\n"
        
        # Identify query purpose
        qtype = self._identify_purpose(ast)
        doc += f"**Purpose:** {qtype}\n\n"
        
        return doc
    
    def _extract_labels(self, ast):
        labels = set()
        def traverse(node):
            if isinstance(node, dict):
                if node.get('type') == 'NodePattern':
                    node_labels = node.get('labels', [])
                    if isinstance(node_labels, list):
                        labels.update(node_labels)
                for value in node.values():
                    traverse(value)
            elif isinstance(node, list):
                for item in node:
                    traverse(item)
        traverse(ast)
        return labels
    
    def _extract_rel_types(self, ast):
        rel_types = set()
        def traverse(node):
            if isinstance(node, dict):
                if node.get('type') == 'RelationshipPattern':
                    types = node.get('types', [])
                    if isinstance(types, list):
                        rel_types.update(types)
                for value in node.values():
                    traverse(value)
            elif isinstance(node, list):
                for item in node:
                    traverse(item)
        traverse(ast)
        return rel_types
    
    def _extract_properties(self, ast):
        props = []
        def traverse(node):
            if isinstance(node, dict):
                if node.get('type') == 'PropertyLookup':
                    var = node.get('variable')
                    prop = node.get('property')
                    props.append(f"{var}.{prop}")
                for value in node.values():
                    traverse(value)
            elif isinstance(node, list):
                for item in node:
                    traverse(item)
        traverse(ast)
        return props
    
    def _identify_purpose(self, ast):
        clauses = ast.get('clauses', [])
        clause_types = [c.get('type') for c in clauses]
        
        if 'Create' in clause_types:
            return "Data Creation"
        elif 'Merge' in clause_types:
            return "Data Upsert"
        elif 'Delete' in clause_types:
            return "Data Deletion"
        elif 'Set' in clause_types or 'Remove' in clause_types:
            return "Data Update"
        else:
            return "Data Retrieval"

# Usage
doc_gen = QueryDocGenerator()

query = """
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE p.age > 30 AND c.industry = 'Technology'
RETURN p.name, p.age, c.name AS company
ORDER BY p.age DESC
LIMIT 10
"""

doc = doc_gen.generate_doc(query, "Find Senior Tech Workers")
print(doc)
```

## API Reference

### GrammarParser Class

```python
class GrammarParser:
    """OpenCypher grammar parser."""
    
    def __init__(self):
        """Initialize the parser."""
        
    def parse(self, query: str) -> Tree:
        """
        Parse a Cypher query into a Lark Tree.
        
        Args:
            query: Cypher query string
            
        Returns:
            Lark Tree object
            
        Raises:
            LarkError: If query has syntax errors
        """
        
    def parse_to_ast(self, query: str) -> dict:
        """
        Parse a Cypher query into an Abstract Syntax Tree.
        
        Args:
            query: Cypher query string
            
        Returns:
            Dictionary representing the AST
            
        Raises:
            LarkError: If query has syntax errors
        """
        
    def validate(self, query: str) -> bool:
        """
        Validate a Cypher query syntax.
        
        Args:
            query: Cypher query string
            
        Returns:
            True if valid, False otherwise
        """
```

## AST Structure Reference

### Query Structure

```python
{
    'type': 'Query',
    'clauses': [...]  # List of clause objects
}
```

### Common Clause Types

#### Match Clause
```python
{
    'type': 'Match',
    'optional': bool,
    'pattern': {...},
    'where': {...} or None
}
```

#### Return Clause
```python
{
    'type': 'Return',
    'distinct': bool,
    'items': [...],
    'order_by': [...] or None,
    'skip': int or None,
    'limit': int or None
}
```

#### Create Clause
```python
{
    'type': 'Create',
    'pattern': {...}
}
```

#### Where Clause
```python
{
    'type': 'Comparison',
    'operator': '>' | '<' | '=' | etc.,
    'left': {...},
    'right': {...}
}
```

### Pattern Structures

#### Node Pattern
```python
{
    'type': 'NodePattern',
    'variable': {'type': 'Variable', 'name': str} or None,  # Variable instance
    'labels': [str, ...],
    'properties': {...} or None
}
```

Note: The `variable` field contains a `Variable` AST node (dict with 'type' and 'name'), not a plain string.

#### Relationship Pattern
```python
{
    'type': 'RelationshipPattern',
    'variable': {'type': 'Variable', 'name': str} or None,  # Variable instance
    'types': [str, ...],
    'properties': {...} or None,
    'direction': 'left' | 'right' | 'both' | 'any',
    'length': {...} or None
}
```

Note: The `variable` field contains a `Variable` AST node (dict with 'type' and 'name'), not a plain string.

## Advanced Use Cases

### Query Security Scanner

```python
class SecurityScanner:
    """Scan queries for security issues."""
    
    def __init__(self):
        self.parser = GrammarParser()
    
    def scan(self, query):
        """Scan for security issues."""
        issues = []
        
        # Check for string concatenation in WHERE (SQL injection style)
        if '+' in query and 'WHERE' in query.upper():
            issues.append("Potential injection via string concatenation")
        
        # Check for unbounded variable-length paths
        ast = self.parser.parse_to_ast(query)
        if self._has_unbounded_path(ast):
            issues.append("Unbounded variable-length path (may cause performance issues)")
        
        # Check for missing LIMIT on potentially large results
        if 'MATCH' in query.upper() and 'LIMIT' not in query.upper():
            if not self._has_aggregation(ast):
                issues.append("Missing LIMIT clause (may return large result set)")
        
        return issues
    
    def _has_unbounded_path(self, ast):
        """Check for [*] patterns."""
        # Implementation omitted for brevity
        return False
    
    def _has_aggregation(self, ast):
        """Check if query uses aggregation."""
        # Implementation omitted for brevity
        return False
```

### Query Optimizer Hints Generator

```python
class OptimizerHints:
    """Generate optimizer hints for queries."""
    
    def __init__(self):
        self.parser = GrammarParser()
    
    def suggest_indexes(self, query):
        """Suggest indexes based on query patterns."""
        ast = self.parser.parse_to_ast(query)
        suggestions = []
        
        # Find property accesses in WHERE clauses
        # Suggest indexes for those properties
        # Implementation would analyze AST structure
        
        return suggestions
```

## Error Handling

### Handling Parse Errors

```python
from pycypher.grammar_parser import GrammarParser
from lark.exceptions import LarkError

parser = GrammarParser()

try:
    ast = parser.parse_to_ast("MATCH (n RETURN n")  # Syntax error
except LarkError as e:
    print(f"Parse error: {e}")
    # Handle error appropriately
```

### Safe Validation

```python
def safe_parse(query):
    """Safely parse a query with error handling."""
    parser = GrammarParser()
    
    try:
        if not parser.validate(query):
            return None, "Invalid syntax"
        
        ast = parser.parse_to_ast(query)
        return ast, None
    except Exception as e:
        return None, str(e)

# Usage
ast, error = safe_parse("MATCH (n) RETURN n")
if error:
    print(f"Error: {error}")
else:
    print("Successfully parsed!")
```

## Best Practices

### 1. Reuse Parser Instances

```python
# Good - reuse parser
parser = GrammarParser()
for query in queries:
    ast = parser.parse_to_ast(query)

# Bad - creating new parser each time
for query in queries:
    parser = GrammarParser()  # Inefficient
    ast = parser.parse_to_ast(query)
```

### 2. Validate Before Parsing

```python
# Good - validate first
if parser.validate(query):
    ast = parser.parse_to_ast(query)
    # Process AST
else:
    # Handle invalid query

# Bad - catching exceptions
try:
    ast = parser.parse_to_ast(query)  # May be expensive if invalid
except:
    pass
```

### 3. Use Type Checking

```python
def process_node(node):
    """Process AST node with type checking."""
    if not isinstance(node, dict):
        return
    
    node_type = node.get('type')
    if node_type == 'Match':
        # Process Match clause
        pass
    elif node_type == 'Return':
        # Process Return clause
        pass
```

## Conclusion

The grammar parser provides a powerful foundation for:
- Query analysis and validation
- Security scanning
- Performance optimization
- Query transformation
- Documentation generation
- Custom tooling

For more examples, see:
- [Test Suite](../tests/test_grammar_parser.py) - 118 comprehensive tests
- [Advanced Examples](../examples/advanced_grammar_examples.py) - Complex query patterns
- [Test Coverage Summary](../TEST_COVERAGE_SUMMARY.md) - Feature coverage details

## Support

For issues or questions:
1. Check the [test suite](../tests/test_grammar_parser.py) for examples
2. Review [AST structure documentation](#ast-structure-reference)
3. Examine the [grammar specification](../packages/pycypher/src/pycypher/grammar_parser.py)
