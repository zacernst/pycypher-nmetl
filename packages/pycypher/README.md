# PyCypher

A Python Cypher query parser and execution engine built on relational algebra.

## What is PyCypher?

PyCypher parses [Cypher](https://neo4j.com/docs/cypher-manual/) graph queries and executes them against in-memory DataFrames. It provides a complete pipeline from query string to result set, with full support for pattern matching, aggregation, mutations, and 131+ scalar functions.

## Installation

PyCypher requires **Python 3.14+** and uses `uv` for dependency management.
It is not yet published on PyPI — install from source:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/zacernst/pycypher.git
cd pycypher
uv sync

# Verify
uv run python -c "from pycypher import Star; print('OK')"
```

## Quick Start

```python
import pandas as pd
from pycypher import ContextBuilder, Star

# 1. Create data as DataFrames (use __ID__ for node identity)
people = pd.DataFrame({
    "__ID__": [1, 2],
    "name": ["Alice", "Bob"],
    "age": [30, 25],
})

knows = pd.DataFrame({
    "__ID__": [1],
    "__SOURCE__": [1],  # Alice
    "__TARGET__": [2],  # Bob
})

# 2. Build a graph context
context = (
    ContextBuilder()
    .add_entity("Person", people)
    .add_relationship("KNOWS", knows,
                      source_col="__SOURCE__", target_col="__TARGET__")
    .build()
)

# 3. Execute Cypher queries
star = Star(context=context)
result = star.execute_query(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
)
print(result)
```

For the simplest case with entities only:

```python
context = ContextBuilder.from_dict({"Person": people, "Product": products})
star = Star(context=context)
```

## Error Handling

PyCypher provides specific exception classes with helpful diagnostics:

```python
from pycypher import Star, VariableNotFoundError, UnsupportedFunctionError

try:
    result = star.execute_query("MATCH (n:Person) RETURN m.name")
except VariableNotFoundError as e:
    print(e.variable_name)        # "m"
    print(e.available_variables)   # ["n"]
except UnsupportedFunctionError as e:
    print(e.supported_functions)   # list of valid function names
```

See the module docstring (`help(pycypher)`) for the full exception hierarchy.

## Configuration

Configure via environment variables or presets:

```python
from pycypher import apply_preset

apply_preset("production")   # 30s timeout, 100K cross-join ceiling, rate limiting
apply_preset("development")  # No timeouts, generous limits (default)
```

Key environment variables:

| Variable | Default | Description |
|---|---|---|
| `PYCYPHER_QUERY_TIMEOUT_S` | None | Query wall-clock budget (seconds) |
| `PYCYPHER_MAX_CROSS_JOIN_ROWS` | 10,000,000 | Cartesian product ceiling |
| `PYCYPHER_RESULT_CACHE_MAX_MB` | 100 | Query result cache size (MB) |
| `PYCYPHER_AUDIT_LOG` | off | Enable structured query audit logging |

## Key Features

- **Full Cypher support** — MATCH, WHERE, WITH, RETURN, CREATE, MERGE, SET, DELETE, FOREACH, UNION
- **Pattern matching** — variable-length paths, shortestPath, OPTIONAL MATCH
- **Aggregation** — COUNT, SUM, AVG, COLLECT, plus ORDER BY / LIMIT / SKIP
- **131+ scalar functions** — string, math, temporal, list, and type conversion
- **Query optimization** — cardinality estimation, join reordering, filter pushdown
- **Pre-execution validation** — `validate_query()` catches errors before execution
- **CLI tools** — `nmetl` command for pipelines, REPL, health monitoring, and metrics

## CLI

```bash
# Interactive REPL
nmetl repl

# Run a YAML-defined ETL pipeline
nmetl run pipeline.yaml

# View query metrics
nmetl metrics --diagnostic
```

## Examples

See the [`examples/`](examples/) directory for runnable scripts:

- `hello_world.py` — 5-level progression from basic MATCH to aggregation
- `quickstart.py` — 30-line intro with error handling
- `production_patterns.py` — timeouts, caching, rate limiting, audit logging
- `backend_selection.py` — choosing between pandas, DuckDB, Polars
- `multi_query_composition.py` — multi-query ETL pipelines

## API Stability

PyCypher is in **Alpha** (`0.0.x`). The following symbols are considered **Stable** (breaking changes announced in CHANGELOG):

- `Star`, `Star.execute_query()`, `Star.available_functions()`
- `ContextBuilder`, `Context`
- All exception classes
- `validate_query()`, `SemanticValidator`

**Provisional** (API may change in `0.1.0`): `Pipeline`, `Stage`, `ResultCache`, `get_cache_stats()`

## Documentation

Full documentation: [https://zacernst.github.io/pycypher/](https://zacernst.github.io/pycypher/)

## License

MIT
