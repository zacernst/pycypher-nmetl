ADR-006: Star Class Decomposition into Specialised Modules
==========================================================

:Status: Accepted
:Date: 2024-09
:Affects: ``packages/pycypher/src/pycypher/star.py``,
          ``packages/pycypher/src/pycypher/pattern_matcher.py``,
          ``packages/pycypher/src/pycypher/mutation_engine.py``,
          ``packages/pycypher/src/pycypher/path_expander.py``

Context
-------

The ``Star`` class was originally a monolithic god object responsible for
parsing, pattern matching, path expansion, mutation handling, result caching,
and query orchestration.  This made the class difficult to test in isolation,
hard to navigate (thousands of lines), and tightly coupled across concerns.

Decision
--------

Extract specialised modules from ``Star`` while keeping it as the single
entry point for query execution:

- **PatternMatcher** — translates MATCH patterns into BindingFrames via node
  scanning, relationship joins, and multi-path merging
- **PathExpander** — handles variable-length BFS expansion for ``[*min..max]``
  relationship patterns
- **MutationEngine** — all write-path operations (CREATE, SET, DELETE, MERGE,
  FOREACH, REMOVE) with shadow-write atomicity

``Star.execute_query()`` remains the top-level orchestrator. Each Cypher
clause type dispatches to the appropriate module. ``Star`` retains:

- Query parsing and clause sequencing
- Result caching (LRU with TTL and thread-safe locking)
- Context management
- RETURN/WITH/ORDER BY/LIMIT projection

Consequences
------------

**Benefits:**

- Each module can be tested independently with mock Contexts
- Clear ownership: mutation bugs go to MutationEngine, pattern bugs go to
  PatternMatcher
- Smaller files are easier to navigate and review
- Reduced risk of unintended coupling between read and write paths

**Trade-offs:**

- Star still has substantial coordination logic and remains the largest
  single file (~2,581 lines)
- Modules need access to Context and sometimes to each other (e.g.
  MutationEngine may need PatternMatcher for MERGE)

**2026-03 Assessment: No Further Decomposition Recommended**

A review (Task #6) confirmed that Star now delegates to **7 specialised
engines**: MutationEngine, PatternMatcher, FrameJoiner, PathExpander,
ProjectionPlanner, AggregationPlanner, and ExpressionRenderer.  The
remaining ~2,581 lines are orchestration logic (``execute_query`` entry
point, clause dispatch, query planning coordination, timeout/cache
management).  This follows the **Mediator pattern** appropriately —
further splitting would scatter tightly-coupled execution flow across
files without improving testability, since each delegate is already
independently testable.

**Current delegates:**

- ``pattern_matcher.py`` — MATCH execution
- ``mutation_engine.py`` — write path (CREATE, SET, DELETE, MERGE, FOREACH, REMOVE)
- ``path_expander.py`` — BFS expansion for variable-length paths
- ``frame_joiner.py`` — BindingFrame join operations
- ``projection_planner.py`` — RETURN/WITH projection
- ``aggregation_planner.py`` — aggregation dispatch
- ``expression_renderer.py`` — expression evaluation for display
