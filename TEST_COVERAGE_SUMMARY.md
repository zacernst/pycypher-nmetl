# Grammar Parser Test Coverage Summary

## Overview
The `grammar_parser.py` module has **118 comprehensive unit tests** covering queries from simple to very complex.

**Test Success Rate:** 100% (118/118 passing)

## Test Organization

### 1. Basic Parsing (5 tests)
- Simple MATCH...RETURN queries
- Node labels and properties
- Relationships
- WHERE clauses

### 2. CREATE Statements (3 tests)
- Simple node creation
- Nodes with properties
- Relationships

### 3. RETURN Statements (6 tests)
- RETURN *
- Aliases (AS)
- DISTINCT
- ORDER BY (ASC/DESC)
- LIMIT and SKIP

### 4. Data Update Statements (5 tests)
- DELETE
- DETACH DELETE
- SET properties
- REMOVE properties
- MERGE

### 5. Complex Queries (4 tests)
- Multiple MATCH clauses
- OPTIONAL MATCH
- WITH clause
- UNWIND

### 6. Literals (9 tests)
- Integers, floats, strings (single/double quotes)
- Booleans (TRUE/FALSE)
- NULL
- Lists and maps

### 7. Expressions (7 tests)
- Arithmetic operations (+, -, *, /, %)
- Comparisons (=, <>, <, >, <=, >=)
- Boolean logic (AND, OR, NOT)

### 8. Functions (3 tests)
- COUNT(*)
- Named functions with arguments
- Aggregation functions

### 9. Validation (2 tests)
- Valid query validation
- Invalid query rejection

### 10. Comments (2 tests)
- Single-line comments (//)
- Multi-line comments (/* */)

### 11. Advanced Patterns (10 tests)
- Variable-length relationships ([*1..5], [*])
- Bidirectional relationships
- Multiple labels on nodes
- Label expressions (OR, NOT, wildcards)
- Relationship type OR (|:)
- Shortest paths (SHORTESTPATH, ALLSHORTESTPATHS)

### 12. EXISTS Subqueries (2 tests)
- Pattern-based EXISTS
- MATCH...WHERE EXISTS

### 13. List & Pattern Comprehensions (4 tests)
- Simple list comprehensions
- Comprehensions with WHERE clause
- Pattern comprehensions
- Nested comprehensions

### 14. Map Projections (4 tests)
- Simple property projection {.name, .age}
- Computed properties {birthYear: expr}
- All properties {.*}
- Mixed syntax

### 15. Quantifiers (4 tests)
- ALL(x IN list WHERE pred)
- ANY(x IN list WHERE pred)
- NONE(x IN list WHERE pred)
- SINGLE(x IN list WHERE pred)

### 16. REDUCE Expressions (2 tests)
- Simple reduction
- Complex reduction with relationships

### 17. CASE Expressions (2 tests)
- Simple CASE (with value)
- Searched CASE (with conditions)

### 18. String Predicates (7 tests)
- STARTS WITH
- ENDS WITH
- CONTAINS
- Regex match (=~)
- IS NULL
- IS NOT NULL
- IS NULL in complex expressions

### 19. Advanced Literals (5 tests)
- Hexadecimal (0x...)
- Octal (0o...)
- Scientific notation (1.5e10)
- INF (infinity)
- NaN (not a number)

### 20. Array Operations (4 tests)
- Array indexing [0]
- Array slicing [1..3]
- Open-ended slicing [2..]
- String indexing

### 21. CALL Statements (4 tests)
- Simple procedure calls
- CALL with YIELD
- CALL with YIELD and WHERE
- Standalone CALL

### 22. Advanced Functions (3 tests)
- Namespaced functions (apoc.*)
- Power operator (^)
- Nested function calls

### 23. Very Complex Queries (9 tests)
Complex real-world scenarios combining multiple features:
- **Complex aggregation**: Multi-level grouping with filtering
- **Pattern matching with EXISTS**: Combining pattern matching with subqueries
- **UNION queries**: Combining multiple query results
- **Complex CREATE**: Multiple nodes and relationships in one statement
- **MERGE with ON CREATE/MATCH**: Conditional property setting
- **Graph traversal**: Variable-length paths with ALL/NONE quantifiers, EXISTS
- **Data transformation**: List comprehensions, map projections, CASE
- **Recommendation query**: Collaborative filtering pattern
- **Multi-hop patterns**: Quantifiers with comprehensions

### 24. Edge Cases (9 tests)
- Empty lists and maps
- Nested data structures
- Property chains (a.b.c.d)
- Multiple WHERE predicates
- Multiple NULL checks
- Escaped strings
- Unicode characters

### 25. Performance Tests (3 tests)
- Many RETURN items (15+ columns)
- Many WHERE conditions (10+ OR clauses)
- Deeply nested expressions

## Query Complexity Spectrum

### Simple Queries
```cypher
RETURN 42
MATCH (n) RETURN n
```

### Medium Complexity
```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n.name
MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b
```

### Complex Queries
```cypher
MATCH (person:Person)
WHERE EXISTS { (person)-[:KNOWS]->(:Person {country: 'USA'}) }
RETURN person.name, person.age
```

### Very Complex Queries
```cypher
MATCH path = (start:Person {name: 'Alice'})-[rels:KNOWS*1..5]->(end:Person)
WHERE ALL(r IN rels WHERE r.trust > 0.5)
  AND NONE(n IN nodes(path)[1..-1] WHERE n.blocked = true)
  AND EXISTS { (end)-[:LIVES_IN]->(:City {name: 'Boston'}) }
WITH end, 
     [r IN rels | r.trust] AS trustScores,
     length(path) AS pathLength,
     REDUCE(totalTrust = 1.0, r IN rels | totalTrust * r.trust) AS trustProduct
WHERE trustProduct > 0.1
RETURN end.name AS person,
       pathLength,
       trustProduct,
       end{.age, .occupation, .email} AS details
ORDER BY trustProduct DESC, pathLength ASC
LIMIT 20
```

## Coverage by Feature Category

| Category | Features Tested | Test Count |
|----------|----------------|------------|
| **Basic Syntax** | MATCH, RETURN, WHERE | 5 |
| **Data Manipulation** | CREATE, MERGE, SET, REMOVE, DELETE | 8 |
| **Literals** | All literal types including advanced | 14 |
| **Expressions** | Arithmetic, comparison, boolean, string | 14 |
| **Patterns** | Variable-length, labels, relationships | 10 |
| **Advanced Features** | Comprehensions, EXISTS, REDUCE, quantifiers | 17 |
| **Aggregation** | Functions, COUNT, grouping | 6 |
| **Complex Queries** | Multi-clause, subqueries, UNION | 13 |
| **Edge Cases** | Nested structures, unicode, escaping | 9 |
| **Validation** | Error handling | 2 |

## Features Covered

✅ **Query Clauses**: MATCH, OPTIONAL MATCH, CREATE, MERGE, DELETE, DETACH DELETE, SET, REMOVE, RETURN, WITH, UNWIND, UNION, CALL

✅ **Pattern Matching**: Node patterns, relationship patterns, variable-length paths, shortest paths, label expressions

✅ **Expressions**: All arithmetic operators, comparisons, boolean logic, string predicates, NULL checks

✅ **Advanced Features**: List comprehensions, pattern comprehensions, map projections, EXISTS subqueries, REDUCE, quantifiers (ALL/ANY/NONE/SINGLE)

✅ **Literals**: Integers, floats, strings, booleans, NULL, lists, maps, hex, octal, scientific notation, INF, NaN

✅ **Functions**: Named functions, namespaced functions, COUNT(*), aggregations

✅ **Modifiers**: DISTINCT, ORDER BY, LIMIT, SKIP, ON CREATE, ON MATCH

✅ **Comments**: Single-line and multi-line

## Known Limitations (Not Yet Supported)

⚠️ **COUNT Subqueries**: `COUNT { pattern }` syntax (commented out in tests)

⚠️ **Full IS label syntax**: `IS :Label` (partial support)

## Testing Philosophy

The test suite follows a **progressive complexity** approach:

1. **Simple tests** validate basic parsing correctness
2. **Medium tests** combine 2-3 features
3. **Complex tests** simulate real-world query patterns
4. **Very complex tests** stress-test the parser with advanced feature combinations
5. **Edge case tests** ensure robustness with unusual inputs

## Running Tests

```bash
# Run all tests
uv run pytest tests/test_grammar_parser.py -v

# Run specific test class
uv run pytest tests/test_grammar_parser.py::TestComplexQueriesAdvanced -v

# Run with coverage
uv run pytest tests/test_grammar_parser.py --cov=pycypher.grammar_parser
```

## Summary

The grammar parser has **comprehensive test coverage** ranging from the simplest queries like `RETURN 42` to extremely complex graph traversal queries with multiple advanced features. The 118 tests provide confidence that the parser correctly handles the full openCypher specification as implemented.

**Test Success Rate: 100%** ✅
