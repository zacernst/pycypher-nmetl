# Data Scientist Showcase: PyCypher-nmetl in Action

A series of progressive demonstration scripts that showcase how **PyCypher-nmetl**
transforms the way data scientists work with relational and graph-structured data.
Each script is self-contained, runnable in minutes, and builds on real-world
patterns you'll encounter in production.

## Who This Is For

- **Data scientists** exploring graph-based analytics on tabular data
- **Data engineers** evaluating PyCypher for ETL pipelines
- **Technical leads** assessing PyCypher's capabilities for their teams

**Prerequisites**: Python 3.11+, familiarity with pandas and SQL concepts.
No graph database experience required — that's the whole point.

## Quick Start

```bash
# From the monorepo root
cd /path/to/pycypher-nmetl

# Run any script directly
uv run python demos/data_scientist_showcase/01_quick_start.py

# Or run them all in sequence
for script in demos/data_scientist_showcase/0*.py; do
  echo "=== Running $(basename $script) ==="
  uv run python "$script"
  echo
done
```

## The Scripts

| # | Script | Focus | Time |
|---|--------|-------|------|
| 1 | `01_quick_start.py` | First query in 30 seconds — DataFrames to Cypher | ~2 min |
| 2 | `02_backend_performance.py` | Automatic backend selection as data scales | ~3 min |
| 3 | `03_real_world_messiness.py` | Handling messy government/public datasets | ~3 min |
| 4 | `04_multi_dataset_integration.py` | Cross-source data fusion with graph queries | ~4 min |
| 5 | `05_production_patterns.py` | Enterprise patterns: caching, timeouts, validation | ~3 min |
| 6 | `06_advanced_analytics.py` | Sophisticated graph analytics and aggregations | ~4 min |

### Script 1: Quick Start

Your first graph query. Load a pandas DataFrame, write a Cypher query, get results.
Demonstrates `ContextBuilder`, `Star`, basic MATCH/RETURN/WHERE patterns, and
error handling — everything you need to evaluate whether PyCypher fits your workflow.

### Script 2: Backend Performance

What happens when your data grows from 1K to 100K+ rows? PyCypher automatically
selects the optimal backend (pandas, DuckDB, Polars) based on data characteristics.
See the performance differences and understand when each backend shines.

### Script 3: Real-World Messiness

Real data is messy. This script works with government contracting data patterns —
inconsistent formats, missing values, mixed types — and shows how PyCypher's
graph model handles complexity that would require complex SQL joins.

### Script 4: Multi-Dataset Integration

Fuse data from multiple sources (HR records, project assignments, department
budgets) into a unified graph. Demonstrates cross-entity queries that would
require multiple SQL JOINs but read naturally in Cypher.

### Script 5: Production Patterns

Move from exploration to production. Covers query timeouts, result caching,
semantic validation, configuration presets, and the Pipeline API for multi-stage
ETL workflows — the patterns that make PyCypher production-ready.

### Script 6: Advanced Analytics

Sophisticated graph queries: multi-hop traversals, aggregation pipelines,
pattern matching across relationship chains, and analytical queries that
showcase Cypher's expressiveness for complex data questions.

## What You'll Learn

After running all six scripts, you'll understand:

1. **Core API** — `ContextBuilder` → `Star` → `execute_query()` workflow
2. **Data modeling** — How to map relational data to graph entities and relationships
3. **Query patterns** — MATCH, WHERE, RETURN, ORDER BY, aggregation, multi-hop
4. **Backend selection** — When to use pandas vs. DuckDB vs. Polars
5. **Production readiness** — Caching, timeouts, validation, pipeline composition
6. **Real-world application** — Handling messy data, cross-source integration

## Project Structure

```
demos/data_scientist_showcase/
├── README.md                           ← This file
├── _common.py                          ← Shared utilities and helpers
├── data/
│   └── generators.py                   ← Data generation utilities
├── 01_quick_start.py                   ← Script 1: First query
├── 02_backend_performance.py           ← Script 2: Backend selection
├── 03_real_world_messiness.py          ← Script 3: Messy data
├── 04_multi_dataset_integration.py     ← Script 4: Data fusion
├── 05_production_patterns.py           ← Script 5: Enterprise patterns
└── 06_advanced_analytics.py            ← Script 6: Graph analytics
```

## Troubleshooting

**Import errors:**
```bash
# Ensure you're running from the monorepo root with uv
uv run python demos/data_scientist_showcase/01_quick_start.py
```

**DuckDB/Polars not available:**
Scripts gracefully degrade to pandas when optional backends aren't installed.
To install optional backends:
```bash
uv pip install duckdb polars
```

**Logging noise:**
Scripts suppress internal logging for clean output. Set `PYCYPHER_LOG_LEVEL=DEBUG`
to see internal execution details.
