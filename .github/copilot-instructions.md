# PyCypher Development Guide

## Role of the LLM Agent

Throughout all the work, **it is crucial to think and behave like an INTJ Meyers-Briggs personality**. Always
consider the larger implications of any changes; always consider the purpose of the project and how it
interacts with the purpose of the specific changes that are being proposed.

## Execution of commands in the terminal

Whenever possible, do **not** open new terminal instances. Instead, execute all commands in the same terminal instance. This allows you to maintain context and continuity, and also allows you to see the full history of commands and their outputs.

## Project Architecture

This is a **monorepo workspace** containing two interdependent packages:
- **`packages/pycypher/`** - Cypher query parser, AST models, and relational algebra engine
- **`packages/shared/`** - Common utilities, logging, telemetry

**Dependency order**: `shared` → `pycypher`

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
shared = { workspace = true }
```

When adding cross-package imports, ensure the dependency is declared in the consuming package's `pyproject.toml`.

### 2. Type-Based Column Naming
All relational operators use type prefixes to prevent naming collisions:

```python
# EntityTable prefixes all columns with entity type
# Person.__ID__ becomes Person____ID__
# Person.name becomes Person__name

# RelationshipTable prefixes with relationship type
# KNOWS.__SOURCE__ becomes KNOWS____SOURCE__
# KNOWS.__TARGET__ becomes KNOWS____TARGET__
```

This ensures deterministic, collision-free column names across complex joins.

### 3. ID-Only Column Preservation
`FilterRows` and `Join` operators only preserve ID columns, not attributes:

```python
# FilterRows returns semi-join result with only ID columns
# Join merges on ID columns and filters result to only IDs
# Projection fetches attributes on-demand when needed
```

This separates ID tracking from attribute access, improving query efficiency.

## Critical Files and Entry Points

- **`packages/pycypher/src/pycypher/grammar_parser.py`** - Lark-based Cypher grammar parser
- **`packages/pycypher/src/pycypher/ast_models.py`** - Pydantic-based AST node definitions
- **`packages/pycypher/src/pycypher/relational_models.py`** - Relational algebra operators (EntityTable, Join, FilterRows, Projection)
- **`packages/pycypher/src/pycypher/star.py`** - Translates AST patterns to relational algebra
- **`Makefile`** - Build orchestration and test commands

## Documentation

Documentation uses Sphinx with Google-style docstrings:

```bash
# Build docs
make docs                # Outputs to docs/_build/html

# Regenerate after changes
uv run sphinx-build -b html docs docs/_build/html
```
**Always update docstrings** when modifying public APIs. Sphinx autodoc extracts from source.

## Common Gotchas

1. **Python version**: Requires exactly `3.14.0a6+freethreaded` - other versions will fail
2. **Grammar parser**: Uses Lark parser with custom visitor pattern for AST construction
3. **Optional variables**: `NodePattern.variable` and `RelationshipPattern.variable` are Optional to support anonymous nodes/relationships
4. **Workspace sync**: After editing `pyproject.toml` files, always run `uv sync`
5. **Column naming**: Always use type-prefixed column names in relational operators (e.g., `Person____ID__` not `__ID__`)

## Resources

- Main README: [/README.md](../README.md)
- Docs: [/docs/](../docs/)
- Example queries: [/examples/](../examples/)
- Test coverage summary: [/TEST_COVERAGE_SUMMARY.md](../TEST_COVERAGE_SUMMARY.md)

## Testing Philosophy
Grammar parser**: Uses Lark parser with custom visitor pattern for AST construction
3. **Optional variables**: `NodePattern.variable` and `RelationshipPattern.variable` are Optional to support anonymous nodes/relationships
4. **Workspace sync**: After editing `pyproject.toml` files, always run `uv sync`
5 SAT solver tests verify CNF conversion and constraint satisfaction

## Recent Architectural Changes

### Column Naming Strategy (Latest)
- Implemented type-based prefixing in all relational operators
- Changed `FilterRows.to_pandas()` to use semi-join and return only ID columns
- Changed `Join.to_pandas()` to filter results to only ID columns
- Updated `star.py` to expect prefixed columns throughout

### AST Validation Fix
- Made `NodePattern.variable` and `RelationshipPattern.variable` Optional[Variable] = None
- Allows anonymous nodes/relationships in Cypher queries
- All 39 AST validation tests passing


### Grammar Parser CLI

```bash
# Parse and validate Cypher query
uv run python -m pycypher.grammar_parser_cli "MATCH (n:Person) RETURN n.name"

# Output AST as JSON
uv run python -m pycypher.grammar_parser_cli --json "MATCH (n) RETURN n" > ast.json
```
