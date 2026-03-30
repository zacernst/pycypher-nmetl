# Advanced Query Optimization Research Report

## Current State Assessment

The PyCypher query engine already has a **remarkably sophisticated** optimization stack:

### Fully Implemented (Production-Ready)
| Component | Technique | Status |
|-----------|-----------|--------|
| Cost Model | I/O + CPU with hardware calibration (EMA α=0.2) | Active |
| Join Enumeration | System R DP for ≤12 relations, greedy fallback | Active |
| Filter Pushdown | Column-aware push below joins | Active |
| Limit Pushdown | LIMIT propagation through compatible ops | Active |
| Lazy Eval | DAG computation graphs with fusion passes | Active |
| Result Cache | LRU with TTL, SHA256 keys, non-determinism detection | Active |
| Plan Cache | Fingerprint-based, cardinality divergence invalidation (3x threshold) | Active |
| Backend Selection | Auto-escalation to DuckDB at 10K row threshold | Active |
| Cardinality Estimation | NDV-based heuristic + learned neural network fallback | Active |
| Statistics Collection | EMA-based online learning from execution outcomes | Active |

### Implemented But Not Fully Integrated
| Component | Technique | Gap |
|-----------|-----------|-----|
| RL Join Optimizer | Thompson Sampling contextual bandit | Not wired into PatternMatcher |
| Predictive Optimizer | Online linear models (time/memory/strategy) | Streaming strategy not executable |
| Advanced Cache | Workload-aware policy + predictive warming | Not replacing default LRU |
| Plan Quality | Multi-dimensional scoring with opportunity detection | Advisory only |
| Adaptive Execution | Trigger-based mid-execution adaptation | Enum framework without execution hooks |

---

## Next-Generation Optimization Opportunities

### Priority 1: Integration of Existing Advanced Components

**The highest ROI is wiring together what already exists.**

#### 1A. Activate RL Join Optimizer in Hot Path
- **Where**: `pattern_matcher.py` → `match_to_binding_frame()`
- **How**: After 50+ executions per pattern fingerprint, use `RLJoinOptimizer.select_join_order()` instead of `CardinalityEstimator.optimal_join_order()`
- **Expected Impact**: 15-30% reduction in join execution time for recurring query patterns
- **Risk**: Low — Thompson Sampling naturally explores, and the existing greedy order is the fallback

#### 1B. Enable Predictive Optimizer Strategy Selection
- **Where**: `query_executor.py` → `execute_query_inner()`
- **How**: Before execution, call `PredictiveOptimizer.predict()`. If `recommended_strategy == "streaming"`, switch to chunk-based execution
- **Expected Impact**: Prevents OOM for large intermediate results
- **Prerequisite**: Implement streaming execution mode (chunked DataFrame processing)

#### 1C. Replace Default LRU Cache with Workload-Aware Policy
- **Where**: `result_cache.py`
- **How**: Swap LRU eviction with `WorkloadAwareCachePolicy.score()` ranking
- **Expected Impact**: Higher cache hit rates for expensive queries (score = frequency × cost / size)

### Priority 2: Adaptive Query Re-Planning

#### 2A. Mid-Execution Cardinality Correction
- **Concept**: After each join in a multi-join plan, compare actual cardinality to estimate. If divergence > 3x, re-plan remaining joins using actual cardinalities
- **Where**: `binding_frame.py` → `join()` method, with callback to `CostAwareJoinPlanner`
- **Algorithm**:
  ```
  for each planned_join in plan:
      result = execute(planned_join)
      if abs(log(result.rows / estimated.rows)) > log(3):
          remaining_plan = re_plan(remaining_joins, actual_cardinalities)
  ```
- **Expected Impact**: Prevents catastrophic plan choices when estimates are wrong
- **Complexity**: Medium — requires plan representation that can be split and re-planned

#### 2B. Adaptive Backend Escalation
- **Concept**: Start with Pandas backend; if intermediate result exceeds threshold (e.g., 100K rows), dynamically escalate to DuckDB or Polars
- **Where**: `backend_protocol.py` → add `AdaptiveBackend` wrapper
- **Algorithm**: Monitor row counts after each operator; escalate when threshold crossed
- **Expected Impact**: Best-of-both-worlds — low overhead for small queries, columnar acceleration for large ones

### Priority 3: Advanced Join Algorithms

#### 3A. Worst-Case Optimal Joins (Leapfrog Triejoin)
- **Concept**: For cyclic graph patterns (triangles, cliques), standard binary joins produce exponential intermediates. WCOJ algorithms process all relations simultaneously
- **When It Matters**: `MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a)` — triangle query
- **Algorithm**: LeapfrogTriejoin iterates over sorted relation columns, "leapfrogging" to valid intersections
- **Expected Impact**: Asymptotically optimal for cyclic queries (no intermediate blowup)
- **Complexity**: High — requires sorted index structures and new execution primitives

#### 3B. Sideways Information Passing (SIP)
- **Concept**: Propagate binding information "sideways" during joins — if joining A⋈B⋈C, use bindings from A⋈B to filter C before materializing C
- **Where**: `pattern_matcher.py` → between join steps
- **Algorithm**: After each join, extract distinct values from join columns and pre-filter next scan
- **Expected Impact**: 2-5x reduction for selective patterns
- **Complexity**: Low-Medium — can be implemented as pre-filter step

### Priority 4: Intelligent Caching Extensions

#### 4A. Subquery Result Materialization
- **Concept**: Cache intermediate binding frames (not just final results) and reuse across queries sharing common subpatterns
- **Example**: If Q1 = `MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.age > 30 RETURN q` and Q2 = `MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.age > 30 RETURN p.name, q.name`, the MATCH+WHERE result is shared
- **Algorithm**: Fingerprint subplans; cache binding frames keyed by subplan hash
- **Expected Impact**: Major speedup for dashboards/reports with overlapping queries

#### 4B. Materialized View Maintenance
- **Concept**: For frequently executed expensive subpatterns, maintain pre-computed binding frames that are incrementally updated on mutations
- **Where**: `result_cache.py` extension + `mutation_engine.py` hooks
- **Algorithm**:
  1. Detect hot subpatterns (frequency × cost threshold)
  2. Materialize and cache
  3. On CREATE/SET/DELETE, incrementally update affected materialized views
- **Expected Impact**: Near-instant response for repeated analytical patterns

### Priority 5: Query Compilation

#### 5A. JIT Compilation of Filter Predicates
- **Concept**: Compile WHERE clause predicates to native code via `numba` or `cffi` for vectorized evaluation
- **Where**: `where_filter.py` → `evaluate_predicate()`
- **How**: Generate Python code string from AST predicate, compile with `numba.jit`
- **Expected Impact**: 3-10x faster predicate evaluation for complex WHERE clauses
- **Complexity**: Medium — AST-to-code generation is straightforward; numba integration needs care

#### 5B. Operator Fusion via Code Generation
- **Concept**: Instead of materializing between operators (scan → filter → project), generate fused code that processes one tuple at a time through the pipeline
- **Where**: `lazy_eval.py` → add `codegen_fused()` pass
- **Algorithm**: Walk computation DAG; for linear chains, emit fused Python function
- **Expected Impact**: Eliminates intermediate DataFrame allocation; 2-5x for simple queries

### Priority 6: Distributed & Parallel Execution

#### 6A. Intra-Query Parallelism
- **Concept**: Execute independent subplans concurrently using Python 3.14's free-threaded capabilities
- **Where**: `query_executor.py` — identify independent plan branches
- **Algorithm**:
  1. Analyze plan DAG for independent subtrees
  2. Execute independent scans/filters in parallel threads
  3. Synchronize at join points
- **Expected Impact**: Near-linear speedup for multi-pattern MATCH clauses
- **Key Advantage**: Python 3.14t (free-threaded) makes this viable without GIL

#### 6B. Partition-Parallel Joins
- **Concept**: Hash-partition both sides of a join and process partitions in parallel
- **Where**: `binding_frame.py` → `join()` method
- **Algorithm**:
  1. Hash join columns into N partitions
  2. Process each partition pair independently (thread pool)
  3. Concatenate results
- **Expected Impact**: Near-linear speedup with thread count for large joins

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 weeks)
1. Wire RL Join Optimizer into PatternMatcher (Priority 1A)
2. Implement Sideways Information Passing pre-filters (Priority 3B)
3. Replace LRU cache with workload-aware policy (Priority 1C)

### Phase 2: Core Enhancements (2-4 weeks)
4. Mid-execution cardinality correction (Priority 2A)
5. Adaptive backend escalation (Priority 2B)
6. Subquery result materialization (Priority 4A)

### Phase 3: Advanced Techniques (4-8 weeks)
7. Intra-query parallelism with free-threaded Python (Priority 6A)
8. JIT compilation of filter predicates (Priority 5A)
9. Operator fusion code generation (Priority 5B)

### Phase 4: Research Frontier (8+ weeks)
10. Worst-case optimal joins for cyclic patterns (Priority 3A)
11. Materialized view maintenance (Priority 4B)
12. Partition-parallel joins (Priority 6B)

---

## Key Architectural Observations

1. **The feedback loop architecture is the crown jewel.** The EMA-based statistics collection → cost model calibration → plan cache invalidation cycle is production-grade. Every new optimization should plug into this loop.

2. **Free-threaded Python 3.14t is a game-changer.** The codebase already uses thread-safe locks throughout. Intra-query parallelism is architecturally ready — it just needs the execution orchestrator.

3. **The backend abstraction is clean and extensible.** Adding adaptive backend selection is straightforward because the `BackendEngine` protocol already defines all necessary operations.

4. **The experimental/ directory contains production-quality code.** The RL optimizer, predictive models, and advanced cache strategies are well-implemented. The main gap is integration, not implementation.

5. **Cypher's graph pattern matching creates unique optimization opportunities** that don't exist in SQL. Worst-case optimal joins and sideways information passing are especially relevant for graph traversal patterns.
