# Spark Backend — Design (Phase 0)

Design decisions for adding a `SparkBackend` implementing the `BackendEngine`
protocol (`packages/pycypher/src/pycypher/backend_engine.py`). Written before
implementation (Phases 1–8). Phase 9 (making Spark stay off-driver) is
out of scope for the current effort.

## Goal

A `SparkBackend` that implements all 16 protocol operations, registers in
`_BACKEND_FACTORIES`, is a valid `backend_engine: spark` config value, and
passes the equivalence + e2e acceptance matrices — the same bar pandas and
duckdb already clear. Correctness first; distributed performance is Phase 9.

## Environment

- `pyspark[sql]` 4.1.1 and `delta-spark` are declared as an optional
  dependency group (`pyproject.toml:281-282`), not core deps.
- Verified locally: pyspark 4.1.1 + OpenJDK 21 start a `local[1]` session and
  run count/filter correctly.
- Consequence: Spark tests must **skip cleanly** when pyspark is absent so the
  default `make test` stays green. The `spark`, `cluster`, and `dual_backend`
  pytest markers already exist (`pyproject.toml:214-219`).

## Decision 1 — `filter(frame, mask)` mask alignment

The protocol's `filter` takes a **positional boolean `pd.Series`** aligned to
the frame (`backend_engine.py:147`), and `check_backend_health`
(`backend_engine.py:430`) exercises it. A distributed Spark DataFrame has no
positional index, so aligning an external positional mask is the core
impedance mismatch.

**Decision: boundary materialization (strategy c) for this effort.**
`filter` materializes the Spark frame to pandas (`.toPandas()`), applies the
mask in pandas, and re-creates a Spark DataFrame from the result. This is
trivially correct and unblocks the health check. It collects to the driver,
which is acceptable because property resolution and aggregation already
collect to pandas today (see Non-goals) — so `filter` is not the marginal
bottleneck until Phase 9.

The `filter` implementation is kept behind a single seam so Phase 9 can swap
in predicate push-down (hand backends a column expression instead of a
materialized mask) without touching call sites.

Rejected for now:
- Predicate push-down (a): cleanest, but changes the shared `BindingFrame`
  contract and risks pandas/duckdb regressions — deferred to Phase 9.
- Positional join via `monotonically_increasing_id`/`zipWithIndex` (b): a
  shuffle per filter and `monotonically_increasing_id` is not contiguous, so
  it needs `zipWithIndex` — more complexity than (c) for no correctness gain
  at current scale.

### Return contract (confirmed against the codebase)

`BindingFrame.bindings` is a **pandas DataFrame**, and it calls
`backend.filter(self.bindings, mask.values)` etc. with pandas in
(`binding_frame.py:949`). The existing backends therefore **return pandas**
from every operation (DuckDB: `scan_entity`/`filter`/`join`/`distinct` all
return pandas via `.fetchdf()`; only `sort` returns a lazy frame for
sort→limit fusion, `duckdb_backend.py:247-326`), and DuckDB *delegates*
`filter`/`rename`/`concat`/`assign_column` straight to pandas because SQL adds
nothing there.

`SparkBackend` follows the same contract: **every method returns a pandas
DataFrame** (no lazy wrapper for this effort). Spark is used as the compute
engine for the set-oriented ops — `scan_entity`, `join`, `distinct`,
`aggregate`, `sort` — each materialising with `.toPandas()` at the return
boundary. The positional-mask ops (`filter`, `assign_column`) and the trivial
ops (`rename`, `concat`, `drop_columns`, `limit`, `skip`) delegate to pandas,
exactly as DuckDB does — this is what makes strategy (c) correct with no
ordering hazard. Inspection ops (`row_count`, `is_empty`,
`memory_estimate_bytes`) short-circuit on pandas inputs to avoid a needless
pandas→Spark round-trip. Because bindings round-trips through pandas between
operations, the backend is correct but not end-to-end distributed — the
Phase 9 lazy-wrapper work is what closes that gap.

## Decision 2 — SparkSession lifecycle

A `SparkSession` is a heavy, JVM-backed, effectively-process-singleton
resource, unlike DuckDB's cheap per-instance in-memory connection
(`duckdb_backend.py:140`).

**Decisions:**
- `__init__` does a lazy `import pyspark` (mirrors `import duckdb` at
  `duckdb_backend.py:138`) and obtains the session via
  `SparkSession.builder.getOrCreate()`.
- Track an `_owned` flag: `True` only if this backend created the session
  (no active session existed at construction). `close()` calls
  `session.stop()` **only when `_owned`** — otherwise it is a no-op, so the
  `context.backend.close()` call now in `run_impl` (`cli/pipeline.py`) cannot
  tear down a session shared with the rest of the process or the test suite.
- `close()` is idempotent; `__enter__`/`__exit__`/`__del__` mirror
  `DuckDBBackend` (`duckdb_backend.py:150-176`) but session-aware.
- Config: `master=local[*]` by default via `getOrCreate` (respect any
  existing session/config); tests pin `local[1]` with
  `spark.sql.shuffle.partitions=1` and `spark.ui.enabled=false` for speed.

## Decision 3 — ordering semantics

Spark has no inherent row order, so pandas/duckdb assumptions
(`reset_index`, stable head) do not translate:

- `sort`: `df.orderBy(*cols, ascending=[...])`. Deterministic.
- `limit(n)`: `df.limit(n)`. **Nondeterministic without a preceding sort** —
  documented as a caller requirement; equivalence tests sort before comparing
  (`_QUERIES` carry a `sort_col`, `test_backend_e2e_acceptance.py`).
- `skip(n)`: no native operator — implement with a `row_number()` window over
  a stable ordering, filter `row_number > n`, drop the helper column.
- `sort`→`limit` fusion (as DuckDB does lazily, `duckdb_backend.py:382-432`)
  is **not** replicated initially; keep ordering ops eager for simplicity.

## Decision 4 — auto-selection

`SparkBackend` is registered but **excluded from `_FALLBACK_CHAIN`**
(`backend_engine.py:619`). JVM startup cost makes it a poor automatic choice;
`auto` will never pick Spark. Spark is explicit-only
(`backend_engine: spark`). Documented so this is intentional, not an omission.

## Decision 5 — `to_pandas` and dtype parity

`to_pandas` = `.toPandas()`. The likely equivalence-test failure mode is dtype
drift (Spark long→int64, nullable columns → object/float, string vs object).
Parity fixes land in Phase 7 as the matrices surface them; the reference for
"correct" output is the pandas backend.

## Non-goals (this effort)

- `BindingFrame.get_property` and `AggregationEvaluator` still run on pandas
  regardless of backend (`backend_engine.py:61-64`). For Spark this means
  queries collect to the driver at those points, so **Spark is correct but not
  genuinely distributed** until Phase 9. This is a documented limitation, not
  a bug.
- Delta Lake I/O and `cluster`-marked distributed tests: Phase 9.

## Test infrastructure (built in Phase 0)

- `tests/conftest.py` (or a shared helper): a `requires_pyspark` skip helper
  and a session-scoped `spark_session` fixture (`local[1]`, UI off, 1 shuffle
  partition) so the JVM starts once per test session.
- Spark-specific unit tests carry `@pytest.mark.spark`.
- Cross-backend enrollment (Phase 7): add an import-guarded `"spark"` branch to
  `_available_backends()` (`test_backend_equivalence_comprehensive.py:45`) and
  to the e2e acceptance fixture params (`test_backend_e2e_acceptance.py:80`).

## Definition of done (Phases 1–8)

- All 16 protocol methods implemented; `check_backend_health(SparkBackend())`
  returns `True`.
- Equivalence + e2e acceptance suites include spark and pass (skip without
  pyspark).
- `nmetl run` with `backend_engine: spark` produces correct sinks and does not
  stop a shared session.
- No pandas/duckdb regressions; default `make test` unaffected.
- CLAUDE.md Backends section and the backend-state memory note updated to the
  real status.
