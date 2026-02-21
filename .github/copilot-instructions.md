# PyCypher-NMETL Development Guide

## Role of the LLM agent

Throughout all the work, **it is crucial to think and behave like an INTJ Meyers-Briggs personality**. Always
consider the larger implications of any changes; always consider the purpose of the project and how it
interacts with the purpose of the specific changes that are being proposed.

## Project Architecture

This is a **monorepo workspace** containing four interdependent packages:
- **`packages/pycypher/`** - Cypher query parser, AST models, fact collections, and SAT solver integration
- **`packages/nmetl/`** - Core ETL framework with sessions, triggers, and queue processors  
- **`packages/shared/`** - Common utilities, logging, telemetry
- **`packages/fastopendata/`** - Open data source integrations (Census, OpenStreetMap)

**Dependency order**: `shared` → `pycypher` → `nmetl` → `fastopendata`

## Development Workflow

I prefer to do development and testing in a Docker container. The Docker daemon is running through Rancher Desktop.

### Environment Management
**CRITICAL**: Use `uv` for ALL Python operations - it manages the workspace virtual environment.

```bash
# Install dependencies
uv sync

# Run Python scripts
uv run python script.py

# Install new package to workspace
uv pip install <package>

# Add dependency to a package
# Edit packages/<package>/pyproject.toml, then:
uv sync
```

The Makefile handles build ordering and dependencies automatically. Use it instead of manual `pip install -e` commands.

### Testing
```bash
# Run all tests
uv run pytest

# Run with coverage
make coverage              # Generates HTML report in coverage_report/

# Run specific test file
uv run pytest tests/test_ast_models.py

# Run tests in parallel
uv run pytest -n 4
```

**Important**: Tests marked with `@pytest.mark.fact_collection` require a running FoundationDB instance.

### Type Checking
Use `ty` (NOT mypy) for type checking:

```bash
uv run ty check
```

**All functions and methods MUST have type annotations**. The project uses Python 3.14.0a6+freethreaded.

### Code Formatting
```bash
make format                # Runs isort + ruff format
```

Ruff config is in `pyproject.toml` (root) with `select = ["ALL"]` - highly strict linting enabled.

## Key Patterns and Conventions

### 1. Workspace Package References
Packages reference each other via workspace sources in root `pyproject.toml`:

```toml
[tool.uv.sources]
pycypher = { workspace = true }
nmetl = { workspace = true }
fastopendata = { workspace = true }
shared = { workspace = true }
```

When adding cross-package imports, ensure the dependency is declared in the consuming package's `pyproject.toml`.

### 2. Fact Model Pattern
Facts are immutable, atomic data points. Never modify facts - only add new ones:

```python
from pycypher.fact import (
    FactNodeHasLabel,
    FactNodeHasAttributeWithValue,
    FactRelationshipHasSourceNode,
)

# Create facts, never mutate
FactNodeHasLabel(node_id="person1", label="Person")
FactNodeHasAttributeWithValue(node_id="person1", attribute="age", value=30)
```

### 3. Trigger Definition Pattern
Triggers MUST use proper type hints for return values:

```python
from nmetl.trigger import VariableAttribute, NodeRelationship

@session.trigger("MATCH (c:City) RETURN c.population AS pop")
def classify_city(pop) -> VariableAttribute["c", "size_class"]:
    """Type hints specify variable and attribute name."""
    return "large" if pop > 1000000 else "medium"

@session.trigger("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.id, c.id")  
def relationship_trigger(p_id, c_id) -> NodeRelationship["p", "MANAGES", "c"]:
    """Creates a relationship between matched nodes."""
    return True
```

The generic parameters in type hints (`["c", "size_class"]`, `["p", "MANAGES", "c"]`) are CRITICAL - they map results to graph structure.

### 4. Queue Processor Extension
When adding new processors, inherit from `QueueProcessor` and implement `_process_item`:

```python
from nmetl.queue_processor import QueueProcessor

class CustomProcessor(QueueProcessor):
    def _process_item(self, item, worker_context):
        """Process single queue item. Must handle exceptions."""
        # Your logic here
        pass
```

Processors run in multiple threads - use `worker_context` for thread-local state.

### 5. Storage Backend Implementation
Implement `FactCollection` abstract interface for new backends:

```python
from pycypher.fact_collection import FactCollection

class MyBackend(FactCollection):
    def add_fact(self, fact): ...
    def get_facts(self, **constraints): ...
    def nodes(self): ...
    # See pycypher/fact_collection/foundationdb.py for reference
```

## Critical Files and Entry Points

- **`packages/nmetl/src/nmetl/session.py`** - Main ETL session orchestration
- **`packages/nmetl/src/nmetl/trigger.py`** - Trigger base classes and execution logic
- **`packages/pycypher/src/pycypher/cypher_parser.py`** - Cypher query parser (uses PLY - excluded from type checking)
- **`packages/pycypher/src/pycypher/ast_models.py`** - Pydantic-based AST node definitions
- **`packages/pycypher/src/pycypher/fact_collection/solver.py`** - SAT solver integration for query optimization
- **`Makefile`** - Build orchestration and data pipeline commands

## Data Pipeline Commands

The project includes extensive data processing capabilities via Makefile targets:

```bash
make fod_ingest           # Run FastOpenData ingest
make fdbclear            # Clear FoundationDB data
```

Census and OpenStreetMap data processing requires downloading large datasets to `packages/fastopendata/raw_data/`.

## Documentation

Documentation uses Sphinx with Google-style docstrings:

```bash
# Build docs
make docs                # Outputs to docs/build/html

# Regenerate after changes
uv run sphinx-build -b html docs docs/build/html
```

**Always update docstrings** when modifying public APIs. Sphinx autodoc extracts from source.

## Common Gotchas

1. **Python version**: Requires exactly `3.14.0a6+freethreaded` - other versions will fail
2. **FoundationDB**: Many features require FDB running locally. Use Docker: `docker-compose up fdb_build`
3. **PLY parser**: `cypher_parser.py` uses PLY's magic imports - excluded from `ty` checking  
4. **Trigger type hints**: Missing or incorrect generic parameters cause runtime errors
5. **Workspace sync**: After editing `pyproject.toml` files, always run `uv sync`

## Resources

- Main README: [/README.md](../README.md)
- Docs: [/docs/](../docs/)
- Example queries: [/examples/](../examples/)
- Test coverage summary: [/TEST_COVERAGE_SUMMARY.md](../TEST_COVERAGE_SUMMARY.md)

# Spyglass Data Tools Development Guide

## Project Architecture

This is a **Python monorepo** for New Relic data processing tools, organized as independent packages:

- **`packages/sg_shared/`** - Core utilities including NRQL parser/AST (ANTLR4-based), logging, typing
- **`packages/sg_synth/`** - Synthetic data generation and transmission to New Relic
- **`packages/sg_collector/`** - Data aggregation, processing, and serialization from New Relic accounts
- **`packages/sg_api_builder/`** - Templates and CLI for building FastAPI applications
- **`packages/sg_catalog/`** - Data documentation and sharing tools
- **`packages/sg_data_reader/`** - Multi-format data reading/writing utilities

**Dependency hierarchy**: Most packages depend on `sg_shared`; the umbrella `spyglass-data-tools` package includes all as dependencies.

## Critical Development Workflow

### Environment Management
**CRITICAL**: Use `uv` for ALL Python operations - it manages virtual environments and dependencies.

```bash
# Install uv (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh
# or: brew install uv

# Set up Artifactory authentication (required for installation)
export UV_INDEX_PYPI_NEWRELIC_PASSWORD=<YOUR_ARTIFACTORY_TOKEN>
# Get token from: https://artifacts.datanerd.us - click "Set me up"

# Sync all dependencies
uv sync

# Run any Python command in the right venv
uv run python script.py
uv run pytest tests/
```

### Build and Test Workflow
```bash
# Complete build pipeline (from Makefile)
make all              # format → veryclean → install → tests → docs

# Individual steps
make format           # Run isort + ruff format
make install          # Install all packages in dev mode
make tests            # Run pytest across all packages
make docs             # Build Sphinx documentation
make publish          # Publish to Artifactory (requires UV_PUBLISH_* env vars)

# Build individual packages (creates dist/ in each package dir)
make build            # Builds all packages to dist/
cd packages/<package> && uv build  # Build single package
```

**Testing convention**: Tests are in both `/tests/` (integration) and `packages/*/tests/` (unit).

### Working with NRQL Parser (sg_shared)

The NRQL parser is the **core innovation** of this project - it's an ANTLR4-based parser that converts New Relic Query Language to AST.

**Key files**:
- `packages/sg_shared/src/sg_shared/Nrql.g4` - ANTLR grammar definition
- `packages/sg_shared/src/sg_shared/NrqlListener.py` - AST builder and listener (3800+ lines)
- `packages/sg_shared/src/sg_shared/NrqlParser.py` - ANTLR-generated parser
- `packages/sg_shared/src/sg_shared/NrqlLexer.py` - ANTLR-generated lexer
