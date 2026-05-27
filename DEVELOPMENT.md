# Development Guide

Complete setup and development reference for PyCypher contributors.

## Prerequisites

- **Python 3.14+** (free-threaded build `3.14t` recommended)
- **[uv](https://docs.astral.sh/uv/)** for dependency management
- **Git**
- **Docker** (optional, for Spark/Neo4j integration tests)

## Quick Start

```bash
# Clone and enter the repository
git clone https://github.com/zacernst/pycypher.git
cd pycypher-nmetl

# One-command setup (core deps only — no Spark/Dask/Polars/Neo4j)
make setup

# Verify everything works
make test-fast
```

For the full dependency set (includes Spark, Dask, Polars, Neo4j):

```bash
make setup-full
```

## Dependency Groups

The project uses two dependency groups defined in the root `pyproject.toml`:

### `dev-core` — Lightweight Development

Install with: `uv sync --group dev-core`

Includes everything needed for core pycypher work:

| Category | Packages |
|----------|----------|
| Testing | pytest, pytest-cov, pytest-xdist, pytest-mock, pytest-timeout, pytest-benchmark, pytest-watch, hypothesis |
| Code quality | ruff, ty, pip-audit, bandit, pre-commit |
| Documentation | sphinx, myst-parser, sphinx-rtd-theme |
| Runtime | psutil |

### `dev` — Full Development

Install with: `uv sync --group dev`

Includes everything in `dev-core` plus:

| Category | Packages |
|----------|----------|
| Spark | pyspark[sql], delta-spark |
| Distributed | dask[dataframe], distributed |
| Alternative backends | polars, deltalake |
| Graph database | neo4j |

## Optional Extras (pycypher package)

The `pycypher` package itself has optional dependency extras for production use:

| Extra | What it adds | When you need it |
|-------|-------------|-----------------|
| `neo4j` | Neo4j Python driver | Connecting to a Neo4j database |
| `large-dataset` | Dask, distributed | Processing datasets that don't fit in memory |
| `storage` | deltalake | Delta Lake table storage |
| `cloud` | s3fs, gcsfs | Reading/writing data on S3 or GCS |
| `polars` | Polars | Using Polars as an alternative backend |
| `all` | All of the above | Full feature set |

Install extras via uv (from the monorepo root):

```bash
uv sync --group dev          # Core dev dependencies
uv sync --group dev-full     # All extras including Neo4j, Polars, Dask
```

## Project Structure

```
pycypher-nmetl/
├── packages/
│   ├── pycypher/       # Cypher parser, AST, relational algebra, query engine
│   ├── shared/         # Common utilities, logging, telemetry
│   └── fastopendata/   # Open data pipeline (Census, TIGER, OSM)
├── tests/              # All tests (flat structure, ~300 files)
│   ├── conftest.py     # Shared fixtures, auto-markers, test isolation
│   ├── fixtures/data/  # Static test data (Parquet, CSV)
│   ├── benchmarks/     # Performance benchmarks (auto: @performance)
│   ├── large_dataset/  # Large-dataset tests (auto: @integration+@slow)
│   ├── load_testing/   # Load and stress tests (auto: @performance+@slow)
│   └── property_based/ # Property-based tests (auto: @unit)
├── docs/               # Sphinx documentation
├── examples/           # Runnable example scripts
├── scripts/            # Development scripts
├── pyproject.toml      # Workspace configuration
└── Makefile            # Development targets (run `make help`)
```

Dependency order: `shared` → `pycypher` → `fastopendata`.

## Running Tests

### Everyday Commands

```bash
make test          # Full suite, parallel (8 threads)
make test-fast     # Stop on first failure, parallel
make test-quick    # Minimal output, no coverage
make test-serial   # Single-threaded (for debugging)
```

### Targeting Specific Tests

```bash
# Single file
make test-file FILE=tests/test_ast_models.py

# Search test names
make test-find QUERY=binding

# Keyword expression
make test-k EXPR="binding AND frame"

# By marker
make test-mark MARK=security

# Re-run only previously failed tests
make test-failed
```

### Test Markers

Tests use these pytest markers (configured in `pyproject.toml`):

| Marker | Purpose | Default timeout |
|--------|---------|----------------|
| `unit` | Fast unit tests | 30s |
| `integration` | Integration tests | 120s |
| `slow` | Slow tests | 300s |
| `spark` | Requires PySpark | 120s |
| `neo4j` | Requires live Neo4j | 120s |
| `performance` | Performance benchmarks | 30s |
| `security` | Security-focused tests | 30s |

Exclude markers: `uv run pytest -m "not slow and not spark"`

### Coverage

```bash
make coverage              # HTML report in coverage_report/
make coverage-check        # Enforce coverage floor (default 50%)
```

### TDD Workflow

```bash
make watch                                # Re-run tests on any file change
make watch WATCH_FILE=tests/test_foo.py   # Watch a specific test file
```

## Code Quality

### Pre-commit Check (Before Pushing)

```bash
make check    # Runs: lock-check → format → lint → typecheck → test-fast
```

### Individual Tools

```bash
make format        # ruff import sorting + format
make lint          # ruff import check + format check + lint
make lint-changed  # Lint only files changed vs main
make typecheck     # ty type checker (NOT mypy)
```

### Ruff Configuration

Ruff is configured with `select = ["ALL"]` (strict linting). Key relaxations:

- **Tests**: No docstrings, type annotations, or magic-value warnings required
- **Scripts/examples**: No docstrings or annotations required
- Line length: 79 characters

### Security Scanning

```bash
make audit     # pip-audit: dependency vulnerability scan
```

Bandit SAST is configured in `pyproject.toml` with project-specific suppressions.

## Debugging

### Debugging a Failing Test

1. **Run the specific test in isolation** to confirm the failure:
   ```bash
   uv run pytest tests/test_foo.py::TestClass::test_method -v
   ```

2. **Add `-s` to see print output** (pytest captures stdout by default):
   ```bash
   uv run pytest tests/test_foo.py::test_method -v -s
   ```

3. **Drop into pdb on failure**:
   ```bash
   uv run pytest tests/test_foo.py::test_method --pdb
   ```

4. **Use `--tb=long` for full tracebacks**:
   ```bash
   uv run pytest tests/test_foo.py --tb=long
   ```

### Debugging Query Execution

Enable query execution logging to trace the pipeline:

```python
import logging
logging.getLogger("shared.logger").setLevel(logging.DEBUG)

star = Star(context=context)
result = star.execute_query("MATCH (p:Person) RETURN p.name")
```

Use the query profiler for timing information:

```python
from pycypher.query_profiler import QueryProfiler

profiler = QueryProfiler()
result = star.execute_query(
    "MATCH (p:Person) RETURN p.name",
    profiler=profiler,
)
print(profiler.report())
```

### Debugging Parse Errors

Use the grammar parser CLI to inspect the AST:

```bash
# Parse and print AST
uv run python -m pycypher.grammar_parser_cli "MATCH (n) RETURN n"

# Validate syntax only
uv run python -m pycypher.grammar_parser_cli --validate "MATCH (n) WHERE n.age > 25 RETURN n"

# Output AST as JSON
uv run python -m pycypher.grammar_parser_cli --json "MATCH (n) RETURN n"
```

### Common Fixture Setup

The `tests/conftest.py` provides reusable fixtures. Use these instead of creating one-off test data:

| Fixture | What it provides |
|---------|-----------------|
| `people_df` | 4-row Person DataFrame (Alice, Bob, Carol, Dave) with name, age, dept, salary |
| `knows_df` | 3-row KNOWS relationship DataFrame with since attribute |
| `person_star` | Star with Person entities only |
| `social_star` | Star with Person + KNOWS relationships |
| `empty_star` | Star with empty context |
| `scalar_registry` | Shared ScalarFunctionRegistry singleton |
| `spark_session` | Session-scoped SparkSession (skips if PySpark unavailable) |
| `neo4j_driver` | Session-scoped Neo4j driver (skips if Neo4j unavailable) |
| `neo4j_session` | Function-scoped Neo4j session (wipes graph before each test) |

Factory helpers for custom test data:

```python
from tests.conftest import make_star, make_context, make_entity_table

# One-liner Star creation
star = make_star({"Person": {"__ID__": [1, 2], "name": ["A", "B"]}})
result = star.execute_query("MATCH (p:Person) RETURN p.name")
```

## Docker Development

### Setup

```bash
cp .env.example .env   # Edit with real credentials
```

### Containers

```bash
make dev-up            # Start dev container + Spark + Neo4j
make dev-up-minimal    # Dev container only (no Spark/Neo4j)
make dev-down          # Stop all containers
make dev-shell         # Open shell in dev container
make dev-test          # Run tests inside container
```

### Infrastructure Only

```bash
make spark-up          # Start Spark cluster
make neo4j-up          # Start Neo4j
make infra-up          # Start both Spark + Neo4j
make infra-down        # Stop both
```

### Integration Tests

```bash
make test-spark           # Spark tests (requires dev container or local Spark)
make test-neo4j           # Neo4j tests (requires Neo4j)
make test-integration     # All integration tests
make test-large-dataset   # Large-dataset tests (timeout=120s)
make test-backends        # Backend equivalence tests (timeout=60s)
```

## Benchmarks

```bash
make bench             # Run performance benchmarks
make bench-save        # Save benchmark baseline
make bench-compare     # Compare against saved baseline
make bench-memory      # Memory profiling benchmark
```

## Documentation

```bash
make docs              # Build Sphinx docs → docs/_build/html
```

Browse the built docs by opening `docs/_build/html/index.html`.

## Environment Variables

### Query Engine Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PYCYPHER_QUERY_TIMEOUT_S` | None | Default query timeout (seconds) |
| `PYCYPHER_MAX_CROSS_JOIN_ROWS` | 10,000,000 | Hard ceiling on cross-join result rows |
| `PYCYPHER_RESULT_CACHE_MAX_MB` | 100 | Maximum result cache size (MB) |
| `PYCYPHER_RESULT_CACHE_TTL_S` | 0 | Cache TTL in seconds (0 = no expiry) |
| `PYCYPHER_MAX_UNBOUNDED_PATH_HOPS` | 20 | BFS hop limit for `[*]` paths |
| `PYCYPHER_BACKEND` | auto | Backend engine: auto, pandas, duckdb, polars |

### Docker/Infrastructure

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER_URL` | spark://spark-master:7077 | Spark cluster URL |
| `SPARK_RPC_SECRET` | (none) | Spark authentication secret |
| `NEO4J_URI` | bolt://localhost:7687 | Neo4j connection URI |
| `NEO4J_USER` | neo4j | Neo4j username |
| `NEO4J_PASSWORD` | pycypher | Neo4j password |

## Editor Integration (LSP)

PyCypher includes a built-in Cypher Language Server:

```bash
python -m pycypher.cypher_lsp
```

Features: diagnostics, completion, hover, signature help, go-to-definition, formatting.

See `docs/developer_guide/contributing.rst` for VS Code, Neovim, and Emacs configuration.

## CI Pipeline

The GitHub Actions CI (`.github/workflows/ci.yml`) runs:

1. **Tests** — Full pytest suite with 30s timeout on Python 3.14t
2. **Dependency Compatibility** — Verifies all backend imports
3. **Documentation** — Sphinx build to catch broken references

All checks must pass before merging to `main`.
