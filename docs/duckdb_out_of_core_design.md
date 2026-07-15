# Out-of-Core DuckDB — Design & Migration Plan (Approach A)

Status: **planning only** (not yet implemented, as of 2026-07-15).

Goal: run ETL jobs over datasets larger than a single machine's RAM by keeping
data in DuckDB relations end-to-end (spilling to disk) instead of materialising
everything to pandas. This document is the living plan for **Approach A** — the
progressive migration of the `BindingFrame` IR to out-of-core relations.

## Background: why the config flag alone doesn't work

Three materialisation boundaries force all data into RAM today, so setting
`backend_engine: duckdb` does **not** exceed local memory:

1. **Ingestion** collapses each source to a full `pa.Table`
   (`ingestion/data_sources.py:585` `to_arrow_table()`; `ingestion/config.py:733`
   `load_with_sources`; `ingestion/context_builder.py:81`). Per-`read()`
   throwaway DuckDB connections, so the relation can't outlive the read.
2. **Every backend op** ends in `.fetchdf()` (`backends/duckdb_backend.py:211-214`),
   pulling the full result back into pandas. `BindingFrame.bindings` is pandas
   between every operation.
3. **The query result** is a pandas DataFrame (`star.py` `execute_query`
   `-> pd.DataFrame`), and the sink takes a full pandas frame
   (`ingestion/output_writer.py:111-116`).

DuckDB the engine is capable of larger-than-RAM processing; the codebase's
integration is what prevents it.

## Two approaches (and why A)

- **Approach B — additive SQL fast-path.** Leave the pandas engine untouched;
  compile the ETL-shaped subset of Cypher (scan → filter → equijoin →
  aggregate → project → sink) into a single streaming DuckDB relation, falling
  back to pandas for everything else. Lower risk, but only covers the subset.
- **Approach A — migrate the `BindingFrame` IR** so *every* query streams
  through DuckDB relations. Universal, but rewrites the engine's core IR.

We are planning for **A** because the expected workload will hit Approach B's
fallback too often. A is structured as **B's foundation phases + a progressive
IR migration behind the same fallback dispatch** — not a big-bang rewrite.

## Target end-state

`BindingFrame` bindings become a **DuckDB relation** rather than a
positionally-indexed pandas DataFrame:

- Variables are relation columns holding entity IDs.
- **Properties are resolved as in-relation `LEFT JOIN`s** (not index-aligned
  `pd.Series`).
- **WHERE is a composed SQL predicate** (not an externally-computed numpy mask).
- Results stream to the sink via `COPY … TO`, spilling to disk, never fully
  resident in pandas.

## Guiding principles (how we avoid a big-bang rewrite)

1. **Dual-representation IR.** `BindingFrame` gets two backing implementations
   behind one interface: `PandasBindings` (today's engine, untouched) and
   `RelationBindings` (new). The engine picks per query.
2. **Whole-query fallback dispatch is the migration scaffold.** An
   **eligibility predicate** over the parsed query decides relation-path vs
   pandas-path. It starts near-empty (everything falls back) and widens one
   feature per phase. Anything not yet migrated runs on the existing engine, so
   the suite stays green throughout.
3. **Never regress the oracle.** Before migrating any feature, capture the
   current pandas-engine outputs for a broad query corpus as a golden
   regression set; every migrated feature must reproduce them exactly.
4. **Small queries stay on pandas.** Gate the relation path on estimated size
   (like `select_backend`'s row threshold) so DuckDB overhead doesn't regress
   latency for small queries even when they're eligible.

## Cross-cutting parity hazards (budget for these every phase)

- **Null semantics:** Cypher `null` vs SQL `NULL` vs pandas `NaN`/`None` differ
  in comparisons, three-valued WHERE logic, and aggregation. The single most
  persistent parity risk (Phases 7 and 9 especially).
- **Dtype drift at the `.fetchdf()` boundary:** int/float widening, nullable →
  object/float, string vs object (same class of issue the Spark backend hit).
- **Ordering:** SQL relations have no inherent row order. RETURN-without-ORDER-BY
  must be compared order-insensitively; the positional-index assumption in
  `binding_frame.py` `filter`/`get_property` disappears on the relation path.
- **Stale index machinery:** `VectorizedPropertyStore` / `index_manager`
  (`binding_frame.py:518`) and `_property_cache` are pandas-path constructs —
  the relation path must **bypass** them, not double-maintain them.

Each phase is TDD (red → green → refactor). The existing engine remains the
fallback and stays green.

---

## Foundation (shared with Approach B — prerequisites for A)

### Phase 0 — Design doc, dual-representation contract, oracle harness
- This document: the `RelationBindings` contract, SQL mapping for
  variables/properties/predicates, eligibility predicate, size gate,
  materialisation boundary.
- Define the `Bindings` interface and the two impls (design only; the empty
  `RelationBindings` shell + interface extraction happen in Phase 4).
- **Oracle:** a golden-output corpus — snapshot today's engine outputs across a
  broad query set; freeze as the equivalence oracle for every later phase.
- **Memory-bounded acceptance harness:** low `memory_limit`, larger-than-RAM
  dataset, assert bounded RSS. Marked `slow` / `large_dataset`.

### Phase 1 — Spill configuration
`SET memory_limit / temp_directory / max_temp_directory_size /
preserve_insertion_order=false` in `DuckDBBackend.__init__`
(`backends/duckdb_backend.py:131-141`), configurable. Tests: config applied; a
single large op spills instead of OOM. Partial relief on its own.

### Phase 2 — Persistent connection + relation-returning ingestion
Context-owned persistent DuckDB connection (lifecycle mirrors the
`backend.close()` already in `run_impl`); `read_relation()` on
`FileDataSource`/`SqlDataSource` returning a view over
`read_parquet`/`read_csv_auto` **without** `to_arrow_table()`
(`data_sources.py:585`). Keep `.read()` for the fallback. Root-cause seam.

### Phase 3 — Streaming sink (`COPY … TO`)
Relation-aware `write_relation_to_uri` emitting `COPY (<sql>) TO`
(`output_writer.py:111-116`; pattern already in
`packages/fastopendata/Snakefile:167`). Streams to disk, no pandas frame.

---

## IR scaffold

### Phase 4 — Dual-representation `BindingFrame` + fallback dispatch
- Extract the `Bindings` interface; add `RelationBindings` (wraps
  `DuckDBLazyFrame`) alongside `PandasBindings`.
- Add the **eligibility predicate** (starts: eligible ≈ nothing) + size gate,
  and dispatch in `star.py` / `QueryAnalyzer`.
- Materialisation boundary: `RelationBindings.to_pandas()` for the
  `-> pd.DataFrame` return / fallback.
- **Tests:** all current queries dispatch to fallback → entire existing suite
  unchanged/green; one trivial eligible query (bare `MATCH (n:Label) RETURN n`)
  runs in-relation. Adds capability with zero behaviour change — the safety
  property that makes the rest incremental.

---

## Feature migration (each phase widens eligibility; fallback covers the rest)

### Phase 5 — Entity scan + simple projection
Single-label MATCH → relation scan (`SELECT __ID__ FROM <view>`); RETURN of
ids/simple columns → `SELECT`. Eligibility += scan/project-only queries.
Equivalence vs oracle + out-of-core assertion.

### Phase 6 — Property resolution as in-relation joins  *(HARD)*
- `get_property` becomes a `LEFT JOIN` against the source view keyed on
  `__ID__`, projecting the property as a relation column — replacing the
  index-aligned Series model (`binding_frame.py:429-602`) and the Arrow→pandas
  entity materialisation (`binding_frame.py:414`).
- Replace the index-aligned `_property_cache` with a per-query projected-columns
  set; bypass `VectorizedPropertyStore`.
- Eligibility += queries with property access in RETURN. One of the two hardest,
  most design-heavy phases.

### Phase 7 — WHERE as SQL predicate  *(HARD — the numpy-mask filter)*
- Build a Cypher-WHERE → DuckDB-SQL expression compiler: comparisons, boolean
  logic, IS NULL / three-valued logic, IN, string ops, supported scalar
  functions. Compose as `WHERE <predicate>` into the relation, replacing the
  numpy mask + positional reindex (`binding_frame.py:934-997`).
- Predicates using non-compilable functions → fall back.
- Null-semantics parity is the main risk. Eligibility += SQL-expressible WHERE.

### Phase 8 — Relationship MATCH, equijoins, multi-pattern
Relationship traversal → SQL joins on `__SOURCE__`/`__TARGET__`; fixed-length
multi-hop → chained joins; OPTIONAL MATCH → `LEFT JOIN`; cross products.
Eligibility += relationship patterns.

### Phase 9 — Aggregation in-relation
Route WITH/RETURN aggregation to `GROUP BY` SQL (the unused
`duckdb_backend.aggregate()` + `_pandas_agg_to_sql` already exist,
`backends/duckdb_backend.py:353`). Handle `collect()`→`list()`, percentiles,
DISTINCT, null-aware counts. Eligibility += aggregating queries. Second
null-semantics hotspot.

### Phase 10 — ORDER BY / SKIP / LIMIT / DISTINCT / WITH chaining
Ordering + pagination (sort→limit fusion already present,
`backends/duckdb_backend.py:382`), DISTINCT, and multi-part `WITH` queries as
chained relations / CTEs. Eligibility += these.

### Phase 11 — Mutations (SET / CREATE / DELETE)
Migrate `MutationEngine` / `binding_frame.mutate*` (`binding_frame.py:1487-1776`)
for ETL that writes derived graph data. Sizable; **may be deferred** if the
out-of-core need is read/transform→sink rather than in-graph mutation — decide
based on workload.

---

## Finalization

### Phase 12 — Streaming result contract + full-pipeline acceptance
`execute_query` keeps eligible results as a relation and streams to sink (no
forced `-> pd.DataFrame`; result cache + row-count metrics at `star.py:749,808`
need relation-aware branches). **Headline test:** `nmetl run` over a
larger-than-`memory_limit` dataset with representative queries, bounded RSS,
correct output. Report eligibility-coverage % against the query corpus.

### Phase 13 — Relegate fallback, hardening, docs
Measure what still falls back; close gaps or accept them. **Variable-length
paths (`PathExpander` BFS) are the genuinely open question** — DuckDB recursive
CTEs can express them but it's complex; they may remain fallback-only (and thus
not out-of-core) indefinitely. Observability (spill/temp metrics), CI
`large_dataset` memory regression test, docs/CLAUDE.md/memory updates.

---

## Seam map (effort ranking, from investigation)

| Seam | Assessment | Location |
|---|---|---|
| Ingestion `.read()` → Arrow | Major (root cause) | `data_sources.py:585`, `config.py:733` |
| `BindingFrame` pandas IR (~14 methods) | Major | `binding_frame.py` |
| `get_property` index-aligned Series | Major (co-dependent) | `binding_frame.py:429-602,414` |
| Aggregation (pandas groupby) | Moderate | `aggregation_evaluator.py` |
| `execute_query` `-> pd.DataFrame` | Moderate | `star.py` |
| `DuckDBLazyFrame` as universal type | Moderate | `backends/duckdb_backend.py:27` |
| Sink `COPY … TO` | Localized | `output_writer.py:111-116` |
| Spill config | Localized | `backends/duckdb_backend.py:140` |

Critical path: ingestion (relation) → `BindingFrame` IR + `get_property` →
`DuckDBLazyFrame` composition. Sink, spill config, aggregation, and the result
contract are comparatively mechanical once that spine exists.

## Honest assessment

- Multi-quarter effort. Phases 0–4 are foundation + scaffold (moderate,
  low-risk, shippable with zero behaviour change). Phases 5, 8, 10 are
  mechanical-ish. **Phases 6 and 7 carry the real architectural risk** (they
  replace the two load-bearing pandas assumptions), and null-semantics parity is
  a recurring tax across 7 and 9.
- The incremental design is the point: every phase ships behind the
  eligibility/fallback dispatch, so the system is always green, out-of-core
  coverage grows monotonically, and the migration can pause at any phase with a
  coherent product.
- Won't fully reach out-of-core without extra work: variable-length paths
  (Phase 13, maybe never) and mutations (Phase 11, maybe deferred).

## Open sequencing question

This is a workload-agnostic order. Once representative queries are available
(pipeline configs, `queries/*.cypher`), reorder Phases 6–9 to unblock the most
common query shapes first, and re-size the Phase 6/7 work against real patterns.
