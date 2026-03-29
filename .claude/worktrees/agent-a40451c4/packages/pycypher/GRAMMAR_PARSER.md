# Grammar Parser for openCypher

This module provides a comprehensive parser for the openCypher query language based on the official BNF grammar specification found in `grammar.bnf`.

## Features

- **Complete Grammar Coverage**: Implements the full openCypher grammar specification
- **Parse Tree Generation**: Creates detailed parse trees using the Lark parsing library
- **AST Transformation**: Converts parse trees into simplified Abstract Syntax Trees (ASTs)
- **Validation**: Validate Cypher queries without executing them
- **Command-line Interface**: Parse queries from files or stdin
- **Error Reporting**: Detailed syntax error messages

## Installation

This parser requires the `lark` library:

```bash
pip install lark
```

Or using uv in the project:

```bash
uv pip install lark
```

## Usage

### Basic Usage

```python
from pycypher.grammar_parser import GrammarParser

# Create a parser instance
parser = GrammarParser()

# Parse a simple query
query = "MATCH (n:Person {name: 'Alice'}) RETURN n"
tree = parser.parse(query)
print(tree.pretty())

# Parse to AST
ast = parser.parse_to_ast(query)
print(ast)
```

### Validation

```python
from pycypher.grammar_parser import GrammarParser

parser = GrammarParser()

# Validate a query
valid_query = "MATCH (n) RETURN n"
if parser.validate(valid_query):
    print("Query is valid!")

# Invalid query
invalid_query = "MATCH (n RETURN n"  # Missing closing parenthesis
if not parser.validate(invalid_query):
    print("Query has syntax errors!")
```

### Parsing from Files

```python
from pycypher.grammar_parser import GrammarParser

parser = GrammarParser()

# Parse from a file
tree = parser.parse_file("query.cypher")
ast = parser.parse_file_to_ast("query.cypher")
```

### Command-line Usage

The parser can be used from the command line:

```bash
# Parse a query directly
python -m pycypher.grammar_parser "MATCH (n) RETURN n"

# Parse from a file
python -m pycypher.grammar_parser -f query.cypher

# Get AST output
python -m pycypher.grammar_parser -a "MATCH (n) RETURN n"

# Get JSON output
python -m pycypher.grammar_parser -j -a "MATCH (n) RETURN n"

# Validate a query
python -m pycypher.grammar_parser -v "MATCH (n) RETURN n"

# Read from stdin
echo "MATCH (n) RETURN n" | python -m pycypher.grammar_parser

# Enable debug mode for verbose errors
python -m pycypher.grammar_parser -d "MATCH (n RETURN n"
```

## Supported openCypher Features

The parser supports the complete openCypher grammar including:

### Query Clauses
- `MATCH` - Pattern matching
- `OPTIONAL MATCH` - Optional pattern matching
- `RETURN` - Return results
- `WITH` - Pass results between query parts
- `UNWIND` - Expand lists to rows
- `WHERE` - Filter results
- `ORDER BY` - Sort results
- `SKIP` and `LIMIT` - Pagination

### Data Modification
- `CREATE` - Create nodes and relationships
- `MERGE` - Match or create patterns
- `SET` - Set properties and labels
- `REMOVE` - Remove properties and labels
- `DELETE` - Delete nodes and relationships
- `DETACH DELETE` - Delete nodes and their relationships

### Pattern Elements
- Node patterns: `(n)`, `(n:Label)`, `(n {prop: 'value'})`
- Relationship patterns: `-->`, `<--`, `-[r:TYPE]->`, `-[*1..5]->`
- Variable-length paths: `-[*]->`, `-[*1..5]->`
- Path quantifiers: `+`, `*`, `{n}`, `{min,max}`

### Expressions
- Arithmetic operators: `+`, `-`, `*`, `/`, `%`, `^`
- Comparison operators: `=`, `<>`, `<`, `>`, `<=`, `>=`
- Boolean operators: `AND`, `OR`, `XOR`, `NOT`
- String operators: `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~` (regex)
- List operators: `IN`, `[]` (indexing), `[..]` (slicing)
- Property access: `.property`, `['property']`

### Literals
- Numbers: `42`, `3.14`, `1e10`, `0xFF`, `0o77`
- Strings: `'string'`, `"string"`
- Booleans: `TRUE`, `FALSE`
- Null: `NULL`
- Lists: `[1, 2, 3]`
- Maps: `{key: 'value', age: 30}`

### Functions and Expressions
- Aggregation: `COUNT(*)`, `sum()`, `avg()`, `min()`, `max()`
- Scalar functions: Function invocations with parameters
- List comprehensions: `[x IN list WHERE condition | expression]`
- Pattern comprehensions: `[(n)-[r]->(m) WHERE condition | expression]`
- Case expressions: `CASE WHEN ... THEN ... ELSE ... END`
- Exists expressions: `EXISTS { MATCH ... }`
- Map projections: `variable{.property, field: value}`

### Advanced Features
- Subqueries: `EXISTS { ... }`
- Procedure calls: `CALL procedure(args) YIELD results`
- Set operations: `UNION`, `UNION ALL`
- Quantifier expressions: `ALL()`, `ANY()`, `SINGLE()`, `NONE()`
- Reduce expressions: `REDUCE()`
- Shortest path: `shortestPath()`, `allShortestPaths()`

## Grammar Specification

The parser is based on the official openCypher BNF grammar specification located in `grammar.bnf`. The Lark grammar has been carefully translated from the BNF format while preserving all semantic meaning.

## Architecture

The parser consists of three main components:

1. **CYPHER_GRAMMAR**: The Lark EBNF grammar definition
2. **CypherASTTransformer**: Transforms parse trees into simplified ASTs
3. **GrammarParser**: High-level API for parsing and validation

### Parser Selection

The parser uses LALR (Look-Ahead LR) parsing for efficiency. For more complex or ambiguous grammars, you can switch to Earley parsing by modifying the parser parameter:

```python
parser = Lark(CYPHER_GRAMMAR, parser='earley', ...)
```

## Testing

Run the test suite:

```bash
pytest tests/test_grammar_parser.py -v
```

## Differences from Existing Parser

This grammar parser (`grammar_parser.py`) differs from the existing `cypher_parser.py`:

- **Based on official BNF**: Directly implements the official openCypher grammar
- **More complete**: Covers more language features
- **Uses Lark**: Modern parsing library vs PLY
- **Declarative grammar**: Easier to maintain and extend
- **Better error messages**: Lark provides detailed error reporting

## Performance Considerations

- **LALR parsing**: Fast for most queries
- **Caching**: The parser can be reused across multiple queries
- **Memory**: Parse trees can be large for complex queries; use AST transformation for compact representation

## Troubleshooting

### Common Issues

1. **Import Error**: Make sure `lark` is installed
   ```bash
   pip install lark
   ```

2. **Syntax Errors**: Use debug mode to see detailed error messages
   ```python
   parser = GrammarParser(debug=True)
   ```

3. **Ambiguous Grammar**: If you encounter parsing conflicts, check the query syntax

## Contributing

To extend the grammar:

1. Update the `CYPHER_GRAMMAR` constant in `grammar_parser.py`
2. Add corresponding transformations in `CypherASTTransformer`
3. Add tests in `test_grammar_parser.py`
4. Update this README with the new features

## References

- [openCypher Project](http://www.opencypher.org/)
- [Lark Parsing Library](https://lark-parser.readthedocs.io/)
- [Cypher Query Language](https://neo4j.com/developer/cypher/)
- [BNF Grammar Specification](grammar.bnf)

## License

This parser implementation is part of the pycypher project and follows the same license.
