# Social Network Example

A complete end-to-end example demonstrating PyCypher's Cypher query engine
against a small social network graph.  No graph database required — just
pandas DataFrames and CSV files.

## Data Model

```
 ┌──────────────────────┐          ┌───────────────────────┐
 │       Person         │          │       Company         │
 ├──────────────────────┤          ├───────────────────────┤
 │ __ID__   (pk)        │          │ __ID__   (pk)         │
 │ name     string      │          │ name     string       │
 │ age      int         │          │ industry string       │
 │ city     string      │          │ city     string       │
 │ email    string      │          │ founded_year int      │
 │ join_date date       │          └───────────────────────┘
 └──────────┬───────────┘                    ▲
            │                                │
            │ KNOWS                          │ WORKS_AT
            │ (since_year)                   │ (role, start_date)
            ▼                                │
 ┌──────────────────────┐                    │
 │       Person         │────────────────────┘
 └──────────────────────┘

 12 People  ──KNOWS──>  18 relationships (directed)
 12 People  ──WORKS_AT──>  5 Companies (12 employment edges)
```

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) package manager
- PyCypher installed (from the monorepo root: `uv sync`)

## Quick Start

```bash
uv run python examples/social_network/run_demo.py
```

## What the Demo Covers

### 1. Data Loading

Load CSV files into a PyCypher context using `ContextBuilder`:

```python
from pycypher.ingestion import ContextBuilder

ctx = (
    ContextBuilder()
    .add_entity_csv("Person", "data/people.csv")
    .add_entity_csv("Company", "data/companies.csv")
    .add_relationship_csv("KNOWS", "data/knows.csv")
    .add_relationship_csv("WORKS_AT", "data/works_at.csv")
    .build()
)
```

### 2. Basic Queries

```cypher
MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY age DESC
```

### 3. Filtering with WHERE

```cypher
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name AS name, p.city AS city
```

### 4. Relationship Traversal

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name AS person, b.name AS knows
```

### 5. Multi-Hop Patterns

```cypher
MATCH (a:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Person)
WHERE a.name <> b.name
RETURN a.name AS person, b.name AS colleague, c.name AS company
```

### 6. Aggregation

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name AS company, count(p) AS headcount, avg(p.age) AS avg_age
ORDER BY headcount DESC
```

### 7. WITH Clause (Pipelining)

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH c.name AS company, count(p) AS headcount
WHERE headcount > 2
RETURN company, headcount ORDER BY headcount DESC
```

### 8. SET Operations

```cypher
MATCH (p:Person)
WHERE p.name = 'Alice Chen'
SET p.city = 'Los Angeles'
RETURN p.name AS name, p.city AS city
```

### 9. Scalar Functions

```cypher
MATCH (p:Person)
RETURN toUpper(p.name) AS upper_name, size(p.email) AS email_length
```

### 10. OPTIONAL MATCH

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name AS person, c.name AS company
```

People without a company affiliation return `null` for the company column
(left-join semantics).

### 11. UNWIND

```cypher
UNWIND ['San Francisco', 'New York', 'Seattle'] AS city
MATCH (p:Person)
WHERE p.city = city
RETURN city, collect(p.name) AS residents
```

### 12. Query Validation

```python
from pycypher import validate_query

errors = validate_query("MATCH (n:Person) RETURN m.name")
for e in errors:
    print(e)  # "Variable 'm' is not defined"
```

### 13. Error Handling

PyCypher provides helpful error messages with "did you mean?" suggestions:

```python
try:
    star.execute_query("MATCH (p:Persn) RETURN p.name")
except Exception as e:
    print(e)  # suggests "Person"
```

## Project Structure

```
examples/social_network/
├── README.md           ← you are here
├── run_demo.py         ← main demo script
└── data/
    ├── people.csv      ← 12 people with name, age, city, email, join_date
    ├── companies.csv   ← 5 companies with name, industry, city, founded_year
    ├── knows.csv       ← 18 KNOWS relationships with since_year
    └── works_at.csv    ← 12 WORKS_AT relationships with role, start_date
```

## Troubleshooting

**`ModuleNotFoundError: No module named 'pycypher'`**
Run `uv sync` from the monorepo root to install workspace packages.

**`FileNotFoundError` on CSV files**
Run the demo from the monorepo root directory, not from inside `examples/`.

**Empty results for a query**
Check that entity/relationship type names match exactly (case-sensitive).
`Person` and `person` are different types.

**`QueryTimeoutError`**
Large cross-joins or unbounded variable-length paths can be slow.
Add `timeout_seconds=10.0` to `execute_query()` or set
`PYCYPHER_QUERY_TIMEOUT_S=10` in your environment.

## Next Steps

- [Full API Reference](../../docs/api/pycypher.rst)
- [Query Processing Guide](../../docs/user_guide/query_processing.rst)
- [Performance Tuning](../../docs/user_guide/performance_tuning.rst)
- [More Tutorials](../../docs/tutorials/index.rst)
