# PyCypher

A comprehensive Cypher query parser and relational algebra engine with SAT-based constraint solving.

## Overview

PyCypher is a Python framework for parsing, analyzing, and executing Cypher graph queries. It provides:

- Full Cypher query parser with complete AST representation
- Translation to relational algebra for query optimization
- SAT-based constraint solver for complex graph pattern queries
- Multiple storage backend abstractions (FoundationDB, RocksDB, in-memory)
- Type-safe query processing with full Python type hints

## Key Features

### 🔍 Cypher Query Parser
- Complete Cypher grammar using Lark parser
- Full AST (Abstract Syntax Tree) representation with Pydantic models
- Support for MATCH, WHERE, WITH, RETURN, CREATE, SET, DELETE
- Graph pattern matching including relationship chains
- Aggregation functions (COLLECT, COUNT, SIZE, etc.)
- Property access and expression evaluation

### 🧮 Relational Algebra Engine
- Translate Cypher queries to relational algebra operators
- Type-based column namespacing to prevent collisions
- ID-only column preservation strategy for efficiency
- Support for EntityTable, RelationshipTable, Join, FilterRows, Projection
- Pandas DataFrame integration for data processing

### 🔬 SAT Solver Integration
- Convert Cypher queries to CNF (Conjunctive Normal Form)
- Integration with python-sat and pycosat
- DIMACS format export for external solvers
- Constraint types: Conjunction, Disjunction, Negation, Implication, Cardinality
- Find all solutions or verify query satisfiability

### 💾 Storage Backend Abstraction
- **FoundationDB**: Distributed ACID transactions, fault-tolerant
- **RocksDB**: High-performance local key-value store
- **SimpleFactCollection**: In-memory for testing
- Abstract `FactCollection` interface for custom backends
- Fact-based data model (nodes, attributes, relationships)

## Architecture

The project is organized as a monorepo workspace:

- **`packages/pycypher/`** - Cypher parser, AST models, relational algebra, SAT solver
- **`packages/shared/`** - Common utilities, logging, telemetry

## Installation

```bash
# Clone the repository
git clone https://github.com/zacernst/pycypher.git
cd pycypher

# Install with uv (recommended)
uv sync

# Run Python scripts
uv run python script.py
```

## Quick Start

### Parse a Cypher Query

```python
from pycypher.cypher_parser import parse

# Parse query into AST
ast = parse("MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 25 RETURN p.name, f.name")

# AST is a Pydantic model with full type safety
print(ast.model_dump_json(indent=2))
```

### Execute Query on Fact Collection

```python
from pycypher.fact_collection.simple import SimpleFactCollection
from pycypher.fact import FactNodeHasLabel, FactNodeHasAttributeWithValue

# Create fact collection
fc = SimpleFactCollection()

# Add facts
fc.add_fact(FactNodeHasLabel(node_id="person1", label="Person"))
fc.add_fact(FactNodeHasAttributeWithValue(
    node_id="person1", 
    attribute="name", 
    value="Alice"
))
fc.add_fact(FactNodeHasAttributeWithValue(
    node_id="person1", 
    attribute="age", 
    value=30
))

# Query the collection
results = fc.query("MATCH (p:Person) WHERE p.age > 25 RETURN p.name")
for result in results:
    print(result['p.name'])  # "Alice"
```

### Translate to Relational Algebra

```python
from pycypher.star import from_match_clause
from pycypher.cypher_parser import parse

# Parse query
ast = parse("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p, f")

# Convert to relational algebra
match_clause = ast.clauses[0]
rel_algebra = from_match_clause(match_clause)

# Get pandas DataFrame
df = rel_algebra.to_pandas()
print(df.columns)  # ['Person____ID__']  (ID-only columns)
```

### SAT Solver for Complex Queries

```python
from pycypher.fact_collection.solver import CypherQuerySolver
from pycypher.fact_collection.simple import SimpleFactCollection

# Initialize solver with fact collection
fc = SimpleFactCollection()
solver = CypherQuerySolver(fc)

# Parse complex query
query = """
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE p.age > 30 AND c.revenue > 1000000
RETURN p, c
"""

# Get SAT clauses
clauses = solver.get_sat_clauses(query)

# Export to DIMACS format for external solvers
dimacs = solver.to_dimacs(clauses)
print(dimacs)
```

## Core Concepts

### Fact-Based Data Model

Data is stored as immutable facts:

```python
# Node facts
FactNodeHasLabel(node_id="person1", label="Person")
FactNodeHasAttributeWithValue(node_id="person1", attribute="name", value="Alice")

# Relationship facts
FactNodeRelatedToNode(
    source_node_id="person1", 
    target_node_id="person2", 
    relationship_id="rel1"
)
FactRelationshipHasLabel(relationship_id="rel1", label="KNOWS")
```

### Type-Based Column Namespacing

To prevent column name collisions in joins, PyCypher uses type-based prefixing:

```python
# Person.name becomes Person__name
# Person.__ID__ becomes Person____ID__
# KNOWS.__SOURCE__ becomes KNOWS____SOURCE__
```

This ensures deterministic, collision-free column names across complex joins.

### ID-Only Column Preservation

Relational algebra operators (Join, FilterRows) only preserve ID columns, not attributes. Attributes are fetched on-demand via Projection:

```python
# Join returns only ID columns: ['Person____ID__', 'Company____ID__']
# Projection fetches attributes: ['Person__name', 'Company__revenue']
```

This separates ID tracking from attribute access, improving efficiency.

## Development

### Environment Setup

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync all dependencies
uv sync

# Run tests
uv run pytest

# Run type checking
uv run ty check

# Format code
make format
```

### Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
make coverage

# Run specific test file
uv run pytest tests/test_ast_models.py

# Run in parallel
uv run pytest -n 4
```

### Type Checking

All code must have type annotations. Use `ty` (NOT mypy):

```bash
uv run ty check
```

### Code Formatting

```bash
make format  # Runs isort + ruff format
```

## Documentation

Full documentation is available in the `docs/` directory:

- [Getting Started](docs/getting_started.rst)
- [Architecture](docs/developer_guide/architecture.rst)
- [AST Nodes Reference](docs/user_guide/ast_nodes.rst)
- [Query Processing](docs/user_guide/query_processing.rst)
- [Storage Backends](docs/user_guide/backends.rst)

Build documentation locally:

```bash
make docs  # Outputs to docs/_build/html
```

## CLI Tools

### Grammar Parser CLI

```bash
# Parse Cypher query from command line
uv run python -m pycypher.grammar_parser_cli "MATCH (n) RETURN n"

# Validate query syntax
uv run python -m pycypher.grammar_parser_cli --validate "MATCH (n) WHERE n.age > 25 RETURN n"

# Output AST as JSON
uv run python -m pycypher.grammar_parser_cli --json "MATCH (n) RETURN n" > ast.json
```

## Examples

See the `examples/` directory for more detailed examples:

- `ast_conversion_example.py` - Converting between AST formats
- `projection_conversion_example.py` - Working with query projections
- `solver_usage.py` - SAT solver integration
- `advanced_grammar_examples.py` - Complex Cypher patterns

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](docs/developer_guide/contributing.rst) for guidelines.

## License

MIT License - see [LICENSE.txt](LICENSE.txt)

## Authors

- **Zachary Ernst** - [zac.ernst@gmail.com](mailto:zac.ernst@gmail.com)

## Links

- [GitHub Repository](https://github.com/zacernst/pycypher)
- [Documentation](https://zacernst.github.io/pycypher/)
- [Issue Tracker](https://github.com/zacernst/pycypher/issues)
