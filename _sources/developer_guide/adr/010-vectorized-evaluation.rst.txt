ADR-010: Vectorized Scalar Function Evaluation
===============================================

:Status: Accepted
:Date: 2025
:Relates to: ``packages/pycypher/src/pycypher/scalar_function_evaluator.py``

Context
-------

Scalar functions (math, string, type predicates) were initially implemented
using ``pd.Series.apply(func)`` which calls a Python function once per row.
For large DataFrames (100K+ rows), this is 10–100x slower than vectorized
alternatives because of Python function-call overhead per row.

Decision
--------

Replace ``pd.Series.apply()`` with vectorized operations wherever possible:

- **Math functions** (``abs``, ``ceil``, ``floor``, ``sqrt``, ``log``, trig
  functions, etc.) use numpy C-level array operations via
  ``_make_math1_np()`` / ``_make_trig1_np()`` factory functions — one numpy
  call per Series regardless of row count.

- **String predicates** (``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``) use
  the pandas ``.str`` Cython accessor (``.str.startswith()``,
  ``.str.endswith()``, ``.str.contains()``).

- **Aggregations** (``COUNT``, ``SUM``, ``AVG``, ``MIN``, ``MAX``) use
  pandas native grouped Cython paths rather than per-group Python callbacks.

Alternatives Considered
-----------------------

1. **Keep ``pd.Series.apply()``** — Simplest but unacceptable performance
   on large datasets.

2. **Numba JIT compilation** — Maximum performance for numeric operations
   but adds a heavy dependency and compilation latency on first call.

3. **Compile expressions to SQL for DuckDB execution** — Attractive for the
   DuckDB backend but does not help the pandas-only path which is the default.

Consequences
------------

- 3–5x speedup on large frames for math and string operations.
- Null handling requires explicit pre-allocation and masking (numpy operations
  do not propagate pandas NA natively).
- Math functions must be stateless to be vectorizable.
- Tight coupling to numpy/pandas internals — tested against pinned versions
  to prevent breakage on library updates.
