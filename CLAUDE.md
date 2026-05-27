# CLAUDE.md

Orientation for future Claude sessions in this repository. Read this first.

## What this is

A Python monorepo (`pycypher-workspace`) implementing a Cypher query engine
plus surrounding tooling. Managed by `uv` workspaces.

Build/dev: `uv sync --all-extras`. Tests: `uv run pytest <pkg>/tests/`.

## Package layout (`packages/`)

| Package        | Source dir                          | Purpose |
|----------------|-------------------------------------|---------|
| `pycypher`     | `src/pycypher/`                     | Core Cypher parser + query execution engine |
| `pycypher-tui` | `src/pycypher_tui/`                 | Textual-based terminal UI for ETL data model visualization and pipeline running |
| `shared`       | `src/shared/`                       | Cross-package utilities |
| `fastopendata` | `src/fastopendata/`                 | FastAPI service for open-data lookups |

There is no separate `nmetl` package. `nmetl` is a CLI entry point declared in
`packages/pycypher/pyproject.toml` pointing at `pycypher.nmetl_cli:main`.

## CLI entry points

- `nmetl` — declared in `pycypher` package; entrypoint `pycypher.nmetl_cli:main`
- `pycypher-tui` — declared in `pycypher-tui`; entrypoint `pycypher_tui.cli:cli_main`

## Key abstractions

- `Star` (`packages/pycypher/src/pycypher/star.py:119`) — main user-facing entry
  point for query execution. Thin facade that delegates to:
  - `PatternMatcher` — MATCH translation
  - `PathExpander` — variable-length path BFS
  - `MutationEngine` — CREATE/SET/DELETE
  - `ClauseExecutor` — clause dispatch / execution loop
  - `QueryAnalyzer` — pre-execution planning
  - `QueryExplainer` — EXPLAIN text plans
- `ContextBuilder` (`packages/pycypher/src/pycypher/ingestion/context_builder.py`) —
  fluent API for building a `Context` of `EntityTable` / `RelationshipTable`
  objects from pandas DataFrames before passing to `Star`.
- `PipelineConfig` (`packages/pycypher/src/pycypher/ingestion/config.py`) —
  YAML-driven ETL pipeline configuration.

Typical user flow:

```python
from pycypher import ContextBuilder, Star

ctx = ContextBuilder().add_entity("Person", people_df).build()
star = Star(ctx)
result = star.execute_query("MATCH (p:Person) WHERE p.age > 28 RETURN p.name")
```

## Backends

`packages/pycypher/src/pycypher/backends/` contains `pandas_backend.py`,
`polars_backend.py`, `duckdb_backend.py`. **Gotcha (see T9 investigation,
2026-05-27):** the planning path in `star.py::_analyze_and_plan` does not
dispatch to these backends during normal `execute_query` — execution flows
through `BindingFrame` directly against the registered DataFrames regardless of
`context.backend_name`. The TUI's backend-engine setting therefore has no
runtime effect today. Treat `backend_name` as metadata until the dispatch is
wired (or deleted).

## TUI notes

- `packages/pycypher-tui/src/pycypher_tui/app.py` is the Textual `App` subclass.
- Screens live under `screens/`. Note: `screens/__init__.py` historically
  lagged behind `app.py`'s imports — verify exports align before adding new
  screens.
- Pipeline Run / Overview / Testing screens drive the ETL pipeline-execution
  feature added on `feat/tui-run-pipeline`.
- Tests use `pytest-asyncio`; if TUI tests fail to collect, run
  `uv sync --all-extras` to ensure `pytest-asyncio` is installed.

## FastOpenData notes

- `packages/fastopendata/src/fastopendata/api.py:363-365` mounts a static
  `/site` directory only if `_SITE_DIR.exists()`. The `site/` directory is not
  committed (no assets); the mount is therefore a no-op in practice and
  requesting `/site` returns 404. Either generate assets via the `wikidata_to_csv.py`
  pipeline / Makefile target before deploy, or remove the mount.
- Build performance: keep an eye on hatchling include patterns — historical
  bug shipped 33GB packages including raw data; fixed by tightening
  `[tool.hatchling.build.targets.wheel]` includes.

## Repo-level gotchas

- **`.gitignore` historical landmines** (fixed 2026-05-27):
  - bare `app.py` line silently gitignored every `app.py` in the tree,
    including `packages/pycypher-tui/src/pycypher_tui/app.py`. Removed.
  - bare `*.html` / `*.js` were also too broad. Scope any future patterns
    relative to repo root (`/foo.py`) or to a specific subdirectory.
  - Verify any new pattern with: `git check-ignore -v <path>`.
- **Workspace Python version**: root `pyproject.toml` `requires-python`
  is the source of truth. Sub-package classifiers must match — drift causes
  `tests/test_python_compatibility.py::test_consistent_metadata_across_entire_workspace`
  to fail.
- **Memory pollution (`~/.claude/projects/.../memory/`)**: periodically prune
  cycle-counter spam files (`improvement_loop_*`,
  `methodology_framework_autonomous_operation_*x_*`, etc.). MEMORY.md should
  stay under 24.4KB; one line per substantive entry, no superlatives.

## Conventions

- Factual reporting in docs and commits — no "transcendent", "ultimate",
  "historic", "unprecedented", or other superlatives. Quantify (lines, tests,
  bytes) when possible.
- New features land on a feature branch (e.g. `feat/tui-run-pipeline`); do not
  push directly to `main`.
- Tests must stay green: regressions are not acceptable as part of feature
  work.

## Known active work (as of 2026-05-27)

- `feat/tui-run-pipeline`: Run-Pipeline TUI feature integration.
- Workspace metadata Python-version consistency fix.
- `screens/__init__.py` exports reconciliation with `app.py` imports.

## Top-level files to keep

`README.md`, `CHANGELOG.md`, `REQUIREMENTS.md`, `DEVELOPMENT.md`,
`CONTRIBUTING.md`, `UNIFIED_MULTI_AGENT_FRAMEWORK.md`, this file. Other
ad-hoc plan/critique `.md` files at repo root are normally cleanup
candidates — don't recreate them; put working notes in PRs or task descriptions.
