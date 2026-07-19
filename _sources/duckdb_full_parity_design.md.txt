# DuckDB full-parity design

Status: planning only, not yet implemented, as of 2026-07-17.

## Goal

Run a full Cypher ETL pipeline — including `SET`/`CREATE`/`DELETE`/`MERGE`
mutations and aggregations, not just read-only `MATCH ... RETURN` — entirely
through DuckDB, so that `nmetl run` with `backend_engine: duckdb` can process
datasets larger than available RAM end to end, with no step that forces full
materialization into pandas. This supersedes the deleted
`duckdb_out_of_core_design.md`, which covered only the read-only subset and
explicitly deferred mutations.

## Current state (verified 2026-07-17)

There are two independent execution paths when `backend_engine: duckdb`:

1. **Eager path (default, always used for mutations).** `ClauseExecutor`
   drives `BindingFrame` through pandas at every step regardless of backend.
   `DuckDBBackend.filter()` (`backends/duckdb_backend.py:402-408`) delegates
   to pandas because the WHERE mask is precomputed in pandas
   (`BindingExpressionEvaluator`/`clause_executor.py:103-152`).
   `BindingFrame.get_property` and `AggregationEvaluator` are pandas-only
   unconditionally (`backend_engine.py:61-64`). Frames are registered as
   DuckDB *views* over pandas dataframes per-operation
   (`self._conn.register(view_name, _to_df(frame))`,
   `duckdb_backend.py:364,567`), not persistent tables.

2. **Relation path (opt-in, read-only).** `relation_engine.py` compiles an
   eligible subset of `MATCH ... RETURN` queries to lazy DuckDB relations.
   It is off by default: `relation_engine_enabled()` only checks a Python
   attribute (`context._relation_engine_enabled`) or the
   `PYCYPHER_DUCKDB_RELATION_ENGINE` env var — `PipelineConfig` has no field
   for it, so `backend_engine: duckdb` in `pipeline.yml` alone does not
   enable it. Its entity/relationship scans go through
   `DataSource.read_relation(con)` (`relation_engine.py:111-112`), which is a
   **lazy scan of the original source file** (CSV/Parquet), not a table DuckDB
   owns — there is nothing to `UPDATE`. `is_relation_eligible()` hard-requires
   the query's final clause to be `Return` (`relation_engine.py:680-681`), so
   any query containing `SET`/`CREATE`/`DELETE`/`MERGE` is unconditionally
   ineligible and falls back to path 1.

3. **Mutations are pandas-only by construction.** `MutationEngine` has no
   DuckDB-aware code path at all (`mutation_engine.py`, grep confirms zero
   `duckdb`/`backend.` SQL usage in the mutation methods). It reads the base
   table to pandas (`_source_to_pandas`), computes new/changed rows, and
   accumulates them in an in-memory dict, `context._shadow[entity_type] =
   backend.concat([base_df, new_df])` (`mutation_engine.py:389`). Every
   subsequent query in the pipeline reads through this shadow overlay.

4. **The DuckDB connection is always `:memory:`**
   (`duckdb.connect(":memory:", ...)`, `duckdb_backend.py:131,283`). There is
   no file-backed database option today, so even DuckDB-native tables would
   not be genuinely out-of-core for *storage* — only spill-to-disk temp
   directories exist for intermediate query execution.

## Core architectural decision

The relation engine's lazy file scan and the mutation engine's pandas shadow
dict are two different workarounds for the same missing piece: **a mutable,
persistent, out-of-core table that both reads and writes can target.**
Closing that gap is the linchpin for this whole effort — without it, "SET on
DuckDB" has no table to issue `UPDATE` against, and "read after mutate"
correctness would otherwise require re-deriving today's pandas shadow-merge
logic in SQL, which is strictly more complex than just making the table real.

Proposed direction: back the DuckDB connection with a file-backed database
(configurable path, defaulting to a temp file cleaned up at run end unless
the user opts to keep it) and materialize each entity/relationship source
into a genuine DuckDB `TABLE` once at ingestion (`CREATE TABLE AS SELECT`
from `read_relation()`, which is already a lazy, out-of-core scan — so the
materialization step itself doesn't need to hold data in RAM). All reads and
writes for the rest of the pipeline then target that table directly:
mutations become ordinary `UPDATE`/`INSERT`/`DELETE`, later queries simply
`SELECT` current state, and the pandas shadow-dict and per-query lazy
file-rescan both disappear as separate mechanisms.

This is a bigger change than the deleted doc scoped, but it removes
complexity rather than adding it — one storage model instead of two.

## Phased plan

**Phase 0 — Config wiring (small, independent, do first).**
Add a `relation_engine: bool` (or fold into `backend_engine: duckdb`)
`PipelineConfig` field, threaded the same way `backend_engine` already is
(`config.py:626` → `cli/pipeline.py:566`). Without this, none of the
downstream phases are reachable from a YAML pipeline at all today.

**Phase 1 — Persistent, mutable entity/relationship tables.**
Add a file-backed DuckDB database option to `DuckDBBackend` (currently
hardcoded to `:memory:`). At ingestion, materialize each source into a real
`TABLE` (via `read_relation()` + `CREATE TABLE AS SELECT`, so ingestion stays
streaming) instead of re-scanning the file lazily per query. This is the
prerequisite for Phase 2 and also removes the current re-scan-per-query cost
in the relation engine.

**Phase 2 — Mutations as SQL DML.**
Rewrite `MutationEngine`'s DuckDB path to translate Cypher mutations into
native statements against the Phase 1 tables instead of pandas
concat/shadow-dict:
- `SET` → `UPDATE <table> SET <col> = <expr> FROM (<subquery>) WHERE
  <table>.id = <subquery>.id`, reusing `relation_sql.compile_expression()`
  for the RHS (already handles literals, arithmetic, comparisons, and — via
  `compile_aggregate`/`_AGG_FUNCS`, currently `count`/`sum`/`avg`/`min`/`max`
  — pre-aggregated values from a prior `WITH`).
- `CREATE` → `INSERT INTO <table> (...) SELECT ...`, with ID generation via
  a DuckDB `SEQUENCE` instead of `_next_ids`' pandas max-scan
  (`mutation_engine.py:229-320`).
- `DELETE` → `DELETE FROM <table> WHERE id IN (...)`.
- `MERGE`/`FOREACH` are more involved (conditional insert-or-update,
  nested clause execution) — scope in detail once SET/CREATE/DELETE land, do
  not attempt all four at once.
- Once tables are genuinely mutable, `is_relation_eligible()`'s
  RETURN-terminal requirement (`relation_engine.py:680-681`) needs a
  parallel "mutation-terminal" plan shape, not a relaxation of the existing
  one — reads and writes have different eligibility rules.

**SET/CREATE/DELETE slices: done.** Deliberately narrow, single-table
subsets of `SET`, `CREATE`, and `DELETE` compile to native DuckDB DML
against a registered streaming source's real table, dispatched through a
combined classifier/executor (`is_relation_mutation_eligible` /
`execute_relation_mutation`, `relation_engine.py`) from `ExecuteStage.execute()`
(`pipeline.py`, sibling `elif` to the read-eligibility branch) and from
`cli/pipeline.py`'s `_try_streaming_run`. Any query outside the shapes below,
or on a label without a registered streaming source, falls back unchanged to
the pandas `MutationEngine` path.
- `SET` — single-node, non-optional `MATCH` with an optional `WHERE`,
  immediately followed by a `SET` of plain `var.prop = expr` items only (no
  `SET n:Label`, `SET n = {..}`, `SET n += {..}`) → `UPDATE <table> SET ...
  WHERE ...` (`_analyze_set_query`/`is_relation_set_eligible`/`execute_relation_set`).
- `DELETE` — same `MATCH` shape, immediately followed by a non-`DETACH`
  `DELETE` of exactly the bound variable → `DELETE FROM <table> WHERE ...`,
  compiling the `MATCH`'s `WHERE` + inline predicates straight into the
  `DELETE`'s `WHERE` rather than an `id IN (...)` subquery (the eligible
  shape only ever has one bound variable, and this avoids depending on every
  streaming source having a configured ID column)
  (`_analyze_delete_query`/`is_relation_delete_eligible`/`execute_relation_delete`).
  `DETACH DELETE` and deleting anything other than the bound node are out of
  scope. Both `SET` and `DELETE` share their MATCH-shape validation via
  `_analyze_single_node_match(query, context, clause_type)`.
- `CREATE` — a standalone single new node, no preceding `MATCH`, no
  relationship (row-per-matched-row `CREATE` and relationship `CREATE`,
  both supported by the pandas `process_create`, are out of scope) →
  `INSERT INTO <table> (...) SELECT ...`. Every property key must already
  resolve to an existing column (no `ALTER TABLE` in this slice — an unknown
  property key falls back to pandas). When the label has a registered
  `id_col`, the ID is generated via a lazily-created, idempotent DuckDB
  `SEQUENCE` seeded above the current max — `_next_ids`' pandas max-scan
  (`mutation_engine.py:229-320`) is replaced only for this path; the pandas
  path itself is untouched. `id_col` must be an integer type — non-integer
  ID columns make `CREATE` ineligible for this slice
  (`_analyze_create_query`/`is_relation_create_eligible`/`execute_relation_create`).
  `register_streaming_source` now stores `id_col` alongside the materialized
  relation and attribute map so `CREATE` can find it.
- `MERGE`/`FOREACH` remain out of scope — more involved (conditional
  insert-or-update, nested clause execution); scope in detail separately.
- Once tables are genuinely mutable, `is_relation_eligible()`'s
  RETURN-terminal requirement (`relation_engine.py:680-681`) needs a
  parallel "mutation-terminal" plan shape, not a relaxation of the existing
  one — reads and writes have different eligibility rules.

Tests: `tests/test_relation_engine_set_unit.py`,
`tests/test_relation_engine_delete_unit.py`,
`tests/test_relation_engine_create_unit.py`,
`tests/test_nmetl_run_streaming.py::TestMutationInterleavedWithRead`.

**Phase 3 — Expand read-query eligibility.**
Currently ineligible and pandas-bound regardless of mutations: undirected
paths, variable-length paths (`*1..3` — candidate: DuckDB recursive CTE),
`OPTIONAL MATCH` combined with aggregation, `UNWIND` in pattern scope, and
bare node-variable `RETURN` (`RETURN a, b` with no property access — needs a
struct/row passthrough rather than scalar-expression compilation). Each is
architecturally independent; sequence by pipeline usage frequency once
Phase 2 is stable, not by doc-estimated effort.

**Phase 3 slice 1 — second required `MATCH` after `WITH`: done.** A `MATCH`
embedded immediately after a `WITH` (multi-pattern cross join) now compiles
to a native DuckDB `.join(other, "true")` (a cartesian product; `.cross()`
was tried first but drops component-alias metadata once a `.filter()`/
`.project()` is chained after it) instead of falling back to pandas. MVP
boundary: exactly one embedded `MATCH`, non-optional, always immediately
preceded by a `WITH` — no `OPTIONAL MATCH` variant, no arbitrary N-way
chaining (`_analyze_second_match`, `_valid_stages`, `relation_engine.py`).
Real pipelines chain more `MATCH`/`WITH` pairs
(`examples/retail_analytics/queries/business_report.cypher`); that's a
structurally identical future extension, not attempted here. Tests:
`tests/test_relation_with.py`.

**Phase 4 — Retire the eager pandas path for `backend_engine: duckdb`.**
Once Phase 3 closes the eligibility gaps, `get_property` and the eager
`AggregationEvaluator` (`backend_engine.py:61-64`) and `DuckDBBackend.filter()`
(`duckdb_backend.py:402-408`) stop mattering for DuckDB runs — there's no
query left that needs the pandas fallback. This is the actual "full parity"
milestone; treat Phases 0-3 as necessary but not sufficient on their own.

**Phase 5 — Bounded-RSS acceptance test.**
No test today exercises bounded memory on a full pipeline run (verified:
none found in this session's search). Write one: a dataset sized larger than
a constrained `PYCYPHER_DUCKDB_MEMORY_LIMIT`, a pipeline mixing `MATCH`,
`SET`, aggregation, and a sink, asserting process RSS stays bounded
throughout via `resource.getrusage` or a subprocess memory sampler. This is
the test that actually validates the goal stated at the top of this doc,
as opposed to validating individual primitives in isolation.

## Design decisions (resolved 2026-07-17)

- **Database lifetime: Option A, scratch file deleted at run end.** No
  persistent-path option (Option B) for now — every `nmetl run` starts from
  the source files and ends with nothing persisted beyond the configured
  `output:` sinks, matching current pandas-shadow behavior exactly.
- **Concurrent runs: explicitly deferred, not addressed by this plan.**
  Assume single-run-at-a-time for now. The scratch path should still get a
  run-unique component (PID or UUID via `tempfile.mkdtemp()`) as a matter of
  not-actively-breaking-things, but no locking, contention handling, or
  concurrent-run test coverage is in scope. Revisit if/when concurrent runs
  become a real use case.
- **Crash cleanup: a new run must always start clean, regardless of how the
  previous run ended.** Two mechanisms are both needed, because a graceful
  Python exception and a hard process kill (`SIGKILL`, OOM-killer) require
  different handling:
  - **Graceful failure** (any exception during the run, including a failed
    query or a failed sink write): wrap the run in `try`/`finally` in
    `cli/pipeline.py` so the scratch database is deleted (and any open
    transaction discarded — see below) on the way out, not only on the
    success path where `close()` is called today (`cli/pipeline.py:823`).
  - **Hard crash** (`SIGKILL`, OOM-killer, power loss — nothing Python-level
    can run): `try`/`finally` never executes, so the scratch file is
    orphaned on disk. Since Option A already gives every run a fresh
    run-unique path, an orphaned file from a prior crash cannot corrupt or
    leak into a *new* run's state — but it does leak disk space
    indefinitely. Add a startup sweep in `nmetl run` that removes orphaned
    scratch files (matched by the known scratch-directory + naming
    convention) older than some threshold before starting. This is a
    disk-hygiene measure, not a correctness requirement, since correctness
    is already guaranteed by the unique-path property.
  - Net effect: "starts from the beginning of the pipeline" is guaranteed
    structurally (fresh path per run) even without the cleanup mechanisms;
    the cleanup mechanisms exist so crashed runs don't accumulate scratch
    files on disk over time.
  - What "discarded" means for a run that fails after some mutations have
    already been issued (but before the scratch file is deleted) is governed
    by the transaction-boundary decision below.

- **Scratch file location: a dedicated directory, separate from the spill
  temp dir.** Do not reuse `PYCYPHER_DUCKDB_TEMP_DIRECTORY`
  (`duckdb_backend.py:31`) — that's sized and cleaned for DuckDB's own spill
  files via `PYCYPHER_DUCKDB_MAX_TEMP_DIRECTORY_SIZE`, and mixing table
  storage into it risks that sizing logic evicting or miscounting a live
  database file. Use a separate `tempfile.mkdtemp()`-created directory
  (own env var if override is ever needed, e.g.
  `PYCYPHER_DUCKDB_SCRATCH_DIRECTORY`, defaulting to the platform temp dir)
  for the database file itself.
- **Transaction boundary: one DuckDB transaction per `nmetl run`.** `BEGIN`
  right after Phase 1 materialization, `COMMIT` only after every configured
  query and every `output:` sink has succeeded, `ROLLBACK`-or-just-delete-
  the-scratch-file on any exception. Per-query commits are rejected — they'd
  leave the scratch database in a partially-mutated state that the
  deletion-on-failure guarantee would then need to race against.
  `MutationEngine`'s DuckDB path therefore needs no transaction-aware code
  of its own; the wrapping transaction is owned entirely by
  `cli/pipeline.py`'s run loop.
- **Concurrent pipeline runs — explicitly out of scope for now** (per
  decision above). Revisit if concurrent `nmetl run` becomes a real use
  case; will need file-locking / failure-mode design at that point.

## Non-goals for this plan

`collect()`/list aggregation, `EXISTS` subqueries, and pattern
comprehensions are out of scope here — they weren't verified as blocked
specifically by the backend/storage model in this session and should be
scoped separately once Phase 4 is reached, since some may already work
correctly (just always via pandas).
