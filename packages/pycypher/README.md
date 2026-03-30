# PyCypher

A Python Cypher query parser and execution engine built on relational algebra.

## What is PyCypher?

PyCypher parses [Cypher](https://neo4j.com/docs/cypher-manual/) graph queries and executes them against in-memory DataFrames. It provides a complete pipeline from query string to result set, with full support for pattern matching, aggregation, mutations, and 131+ scalar functions.

## Installation

```bash
pip install pycypher
```

Optional extras:

```bash
pip install pycypher[neo4j]          # Neo4j connectivity
pip install pycypher[polars]         # Polars backend
pip install pycypher[large-dataset]  # Dask for large datasets
pip install pycypher[all]            # Everything
```

## Quick Start

```python
from pycypher.star import Star

# Build a graph context
star = Star.from_context_builder(
    nodes=[
        {"labels": ["Person"], "properties": {"name": "Alice", "age": 30}},
        {"labels": ["Person"], "properties": {"name": "Bob", "age": 25}},
    ],
    relationships=[
        {"type": "KNOWS", "from": ("Person", "Alice", "name"),
         "to": ("Person", "Bob", "name")},
    ],
)

# Execute a Cypher query
result = star.execute_query(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
)
print(result)
```

## Key Features

- **Full Cypher support** -- MATCH, WHERE, WITH, RETURN, CREATE, MERGE, SET, DELETE, FOREACH, UNION
- **Pattern matching** -- variable-length paths, shortestPath, OPTIONAL MATCH
- **Aggregation** -- COUNT, SUM, AVG, COLLECT, plus ORDER BY / LIMIT / SKIP
- **131+ scalar functions** -- string, math, temporal, list, and type conversion
- **Query optimization** -- cardinality estimation, join reordering, filter pushdown
- **CLI tools** -- `nmetl` command for pipelines, REPL, health monitoring, and metrics

## CLI

```bash
# Interactive REPL
nmetl repl

# Run a YAML-defined ETL pipeline
nmetl run pipeline.yaml

# View query metrics
nmetl metrics --diagnostic
```

## Documentation

Full documentation: [https://zacernst.github.io/pycypher/](https://zacernst.github.io/pycypher/)

- [Getting Started](https://zacernst.github.io/pycypher/getting_started.html)
- [Tutorials](https://zacernst.github.io/pycypher/tutorials/index.html)
- [API Reference](https://zacernst.github.io/pycypher/api/index.html)

## License

MIT
