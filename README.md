# PyCypher

A comprehensive Cypher query parser and relational algebra engine with SAT-based constraint solving.

## Overview

PyCypher is a Python framework for parsing, analyzing, and executing Cypher graph queries. It provides:

- Full Cypher query parser with complete AST representation
- Translation to relational algebra for query optimization
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
- Graph-native indexes (adjacency, property, label) for O(degree) pattern matching
- LeapfrogTriejoin for worst-case optimal multi-way joins
- Adaptive cardinality feedback loops for self-improving query plans
- Pluggable backend engines (Pandas, DuckDB, Polars)

### ML-Powered Query Optimization

PyCypher includes a lightweight online learning system that improves query plans over time
without heavyweight ML dependencies. The `query_learning` module provides:

- **Query Fingerprinting** — Structural similarity detection that groups queries by clause
  structure, entity types, and predicate shapes (ignoring literal values). Queries like
  `WHERE p.age > 30` and `WHERE p.age > 50` share the same fingerprint and reuse cached plans.

- **Predicate Selectivity Learning** — Tracks actual vs. estimated selectivity per
  `(entity_type, property, operator)` triple using exponential moving averages (EMA).
  After sufficient observations, learned selectivity overrides heuristic defaults for
  more accurate cardinality estimates.

- **Join Strategy Learning** — Records join execution performance (elapsed time, output
  accuracy) per size bucket and strategy. Over time, the planner automatically selects
  the historically fastest strategy for each input size combination.

- **Adaptive Plan Cache** — LRU cache with TTL that stores and reuses analysis results
  keyed by query fingerprint. Automatically invalidated on data mutations (CREATE/SET/DELETE).

```python
from pycypher.query_learning import QueryLearningStore

store = QueryLearningStore()

# Record observed selectivity after query execution
store.record_selectivity("Person", "age", ">", estimated=0.33, actual=0.12)

# Retrieve learned selectivity for future planning
learned = store.get_learned_selectivity("Person", "age", ">")

# Record join performance for adaptive strategy selection
store.record_join_performance(
    strategy="hash", left_rows=10000, right_rows=500,
    actual_output_rows=450, elapsed_ms=12.3,
)

# Get diagnostics snapshot
print(store.diagnostics())
# {'plan_cache': {'entries': 0, 'hits': 0, 'misses': 0, 'hit_rate': 0.0, ...}, ...}
```

All learning components are thread-safe with fine-grained locking and use bounded rolling
windows (64 observations max) for predictable memory usage.

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

## Quick Start — Working Query in 60 Seconds

```python
import pandas as pd
from pycypher import Star
from pycypher.ingestion import ContextBuilder

people = pd.DataFrame({
    "__ID__": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "age": [30, 25, 35],
})

context = ContextBuilder.from_dict({"Person": people})
star = Star(context=context)

result = star.execute_query(
    "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name, p.age AS age"
)
print(result)
#    name  age
# 0  Alice   30
# 1  Carol   35
```

See **[Zero to Hello World](docs/hello_world.rst)** for a 5-level progressive tutorial, or run the example directly:

```bash
uv run python examples/hello_world.py
```

### Parse a Cypher Query

```python
from pycypher.cypher_parser import parse

# Parse query into AST
ast = parse("MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 25 RETURN p.name, f.name")

# AST is a Pydantic model with full type safety
print(ast.model_dump_json(indent=2))
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

## Core Concepts

### Type-Based Column Namespacing
`

This ensures deterministic, collision-free column names across complex joins.

### ID-Only Column Preservation

Relational algebra operators (Join, FilterRows) only preserve ID columns, not attributes. Attributes are fetched on-demand via Projection:

```python
# Join returns only ID columns: ['Person____ID__', 'Company____ID__']
# Projection fetches attributes: ['Person__name', 'Company__revenue']
```

This separates ID tracking from attribute access, improving efficiency.

## Development

```bash
# One-command setup (core deps, no Spark/Dask/Polars)
make setup

# Or full setup (all deps including Spark/Dask/Polars/Neo4j)
make setup-full

# Run tests
make test-fast

# Pre-commit quality check
make check
```

See **[DEVELOPMENT.md](DEVELOPMENT.md)** for the complete development guide covering:
dependency groups, optional extras, testing workflows, debugging, Docker setup,
benchmarks, and environment variables.

## Documentation

Full documentation is available in the `docs/` directory:

- [Getting Started](docs/getting_started.rst)
- [Architecture](docs/developer_guide/architecture.rst)
- [AST Nodes Reference](docs/user_guide/ast_nodes.rst)
- [Query Processing](docs/user_guide/query_processing.rst)

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
