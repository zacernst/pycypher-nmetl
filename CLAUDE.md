# CLAUDE.md

Orientation for future Claude sessions in this repository. Read this first.

## What this is

A Python monorepo (`pycypher-workspace`) implementing a Cypher query engine
plus surrounding tooling. Managed by `uv` workspaces.

Build/dev: `uv sync --all-extras`. Tests: the core `pycypher` suite lives at
root `tests/` (`uv run pytest tests/`); `fastopendata` and `pycypher-tui`
each have their own `packages/<pkg>/tests/` (`uv run pytest packages/<pkg>/tests/`).

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
`polars_backend.py`, `duckdb_backend.py`; the `BackendEngine` protocol,
factory/registry, and `select_backend` live in
`packages/pycypher/src/pycypher/backend_engine.py`.

**Dispatch is live (verified 2026-07-14).** `BindingFrame` delegates
`filter`/`join`/`left_join`/`cross_join`/`rename` to `context.backend`
(`binding_frame.py`), and `QueryAnalyzer` can swap the backend mid-query on
optimizer hints (`query_analyzer.py`). `Context.__init__` always resolves a
concrete engine (min. `PandasBackend`), so a `Context(backend="duckdb")`
genuinely routes joins/filters through DuckDB. The earlier "backends are never
called" gotcha is obsolete — do not reintroduce it.

**Config threading (added 2026-07-14):** `PipelineConfig.backend_engine`
(`auto`/`pandas`/`duckdb`/`polars`, `config.py:626`) is forwarded into the run
via `builder.build(backend=...)` at `cli/pipeline.py:566`. `run_impl` also
calls `context.backend.close()` at run end to release the DuckDB in-memory
connection rather than relying on `__del__`.

**Spark: implemented (Phases 1–8, 2026-07-14).** `SparkBackend`
(`backends/spark_backend.py`) implements all 16 `BackendEngine` methods,
is registered in `_BACKEND_FACTORIES`, and is a valid `backend_engine: spark`
config value. It passes `check_backend_health`, the equivalence matrix
(`test_backend_equivalence_comprehensive.py` — enrolled via `_available_backends()`),
the e2e acceptance matrix (`test_backend_e2e_acceptance.py`), and an
end-to-end `nmetl run`. See `docs/spark_backend_design.md`. Key properties:
  - **Return contract:** every op returns pandas (like DuckDB); Spark powers
    the set ops (`scan_entity`/`join`/`distinct`/`aggregate`/`sort`) and
    materialises with `.toPandas()`. `filter`/`rename`/`concat`/`assign_column`/
    `limit`/`skip` delegate to pandas.
  - **Session:** `getOrCreate`; `close()` only stops a session the backend
    created (`_owned`), so the run-path `close()` never tears down a shared
    session (tests/embedded).
  - **Explicit-only:** excluded from `_FALLBACK_CHAIN` — `auto` never selects
    Spark (JVM startup cost).
  - **Not yet distributed (Phase 9, deferred):** `BindingFrame.get_property`
    and `AggregationEvaluator` still run on pandas, and bindings round-trip
    through pandas between ops, so Spark is correct but collects to the driver.
  - Tests carry `@pytest.mark.spark` and skip when pyspark is absent, so the
    default `make test` is unaffected. Requires a JVM (verified: OpenJDK 21).

Readiness: **pandas** (live default) and **duckdb** are production-ready —
complete, tested by the equivalence + e2e acceptance suites, and validated
end-to-end through `nmetl run`. **polars** is functional and passes the
equivalence suite but is absent from the e2e acceptance matrix
(`test_backend_e2e_acceptance.py`) and round-trips pandas↔polars on every op;
treat it as experimental until it's in the e2e matrix. Two paths still run on
pandas regardless of backend — `BindingFrame.get_property` and
`AggregationEvaluator` (documented at `backend_engine.py:61-64`); this bounds
speedup, not correctness.

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
- **Loop-skill hardened (2026-05-27)**: the embiggen skill at `~/.claude/skills/embiggen.md` was rewritten 2026-05-27 to prevent the cycle-counter spam pattern (234 spam memory files removed across two cleanup sessions; root cause was open-ended "never idle" + memory-write-on-amendment directives in `UNIFIED_MULTI_AGENT_FRAMEWORK.md`). Future loop runs respect the four constraints in that skill.

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
