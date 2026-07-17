ADR-007: Extract Cardinality Estimator from Query Planner
=========================================================

:Status: Accepted
:Date: 2026-03
:Affects: ``packages/pycypher/src/pycypher/cardinality_estimator.py``,
          ``packages/pycypher/src/pycypher/query_planner.py``

Context
-------

The ``query_planner.py`` module (1,360 lines) contained both query planning
logic and cardinality estimation primitives (``ColumnStatistics``,
``TableStatistics``, ``CardinalityFeedbackStore``).  While there was no
actual bidirectional dependency with ``relational_models.py`` (as initially
suspected), the cardinality estimation code was tightly embedded in the
planner, making it difficult to reuse in other contexts (e.g. the query
plan analyser) or test independently.

Decision
--------

Extract cardinality estimation into a dedicated ``cardinality_estimator.py``
module (347 lines) containing:

- **ColumnStatistics** — per-column NDV, null fraction, histograms
- **TableStatistics** — lazily computed column statistics for a DataFrame
- **CardinalityFeedbackStore** — accumulates actual-vs-estimated ratios
  for self-correcting estimates over time

All three classes are re-exported from ``query_planner.py`` for backward
compatibility.  ``query_planner.py`` reduced from 1,360 to 1,061 lines.

Consequences
------------

**Benefits:**

- Cardinality estimation is independently importable and testable
- Other components (query plan analyser, query learning) can import
  statistics primitives without pulling in the full planner
- Clearer separation of concerns: planner decides *what* to do,
  estimator provides *data* for those decisions
- Reduced cognitive load when working on either module

**Trade-offs:**

- One additional module in the package
- Re-exports in ``query_planner.py`` add a small indirection for
  existing callers (no code changes required)
