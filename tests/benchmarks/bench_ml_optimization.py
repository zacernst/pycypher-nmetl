"""Benchmark suite for ML query optimization features.

Validates the performance impact of:
- QueryFingerprinter: structural query fingerprinting
- PredicateSelectivityTracker: EMA-based selectivity learning
- JoinPerformanceTracker: adaptive join strategy selection
- AdaptivePlanCache: plan caching keyed by structural fingerprint

Run benchmarks::

    uv run pytest tests/benchmarks/bench_ml_optimization.py -v --benchmark-only
    uv run pytest tests/benchmarks/bench_ml_optimization.py --benchmark-save=ml_opt

Or standalone::

    uv run python tests/benchmarks/bench_ml_optimization.py
"""

from __future__ import annotations

import statistics
import threading
import time
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.query_learning import (
    AdaptivePlanCache,
    JoinPerformanceTracker,
    PredicateSelectivityTracker,
    QueryFingerprint,
    QueryFingerprinter,
    QueryLearningStore,
)
from pycypher.query_planner import QueryPlanAnalyzer
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures: synthetic graph data
# ---------------------------------------------------------------------------


def _build_persons(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
    depts = ["eng", "mktg", "sales", "ops", "hr"]
    return pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n + 1),
            "name": [f"Person_{i}" for i in range(1, n + 1)],
            "age": rng.integers(18, 65, size=n),
            "dept": rng.choice(depts, size=n),
            "salary": rng.integers(40_000, 200_000, size=n),
        },
    )


def _build_knows(
    n_persons: int, *, avg_degree: int = 5, rng: np.random.Generator
) -> pd.DataFrame:
    n_edges = n_persons * avg_degree
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    return pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_actual + 1),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": rng.integers(2000, 2026, size=n_actual),
        },
    )


def _build_context(n_persons: int) -> Context:
    rng = np.random.default_rng(42)
    persons_df = _build_persons(n_persons, rng=rng)
    knows_df = _build_knows(n_persons, rng=rng)
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(persons_df.columns),
        source_obj_attribute_map={
            c: c for c in persons_df.columns if c != ID_COLUMN
        },
        attribute_map={c: c for c in persons_df.columns if c != ID_COLUMN},
        source_obj=persons_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows_df.columns),
        source_obj_attribute_map={
            c: c
            for c in knows_df.columns
            if c not in {ID_COLUMN, "__SOURCE__", "__TARGET__"}
        },
        attribute_map={
            c: c
            for c in knows_df.columns
            if c not in {ID_COLUMN, "__SOURCE__", "__TARGET__"}
        },
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture(scope="module")
def ctx_1k() -> Context:
    return _build_context(1_000)


@pytest.fixture(scope="module")
def ctx_10k() -> Context:
    return _build_context(10_000)


@pytest.fixture(scope="module")
def star_1k(ctx_1k: Context) -> Star:
    return Star(context=ctx_1k)


@pytest.fixture(scope="module")
def star_10k(ctx_10k: Context) -> Star:
    return Star(context=ctx_10k)


# ---------------------------------------------------------------------------
# Test queries for fingerprinting and plan analysis
# ---------------------------------------------------------------------------

_QUERIES = [
    "MATCH (n:Person) RETURN n.name",
    "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
    "MATCH (n:Person) WHERE n.salary > 100000 RETURN n.name, n.salary",
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 25 RETURN a.name, b.name",
    "MATCH (n:Person) RETURN n.dept, count(n) AS cnt",
    "MATCH (n:Person) WHERE n.dept = 'eng' RETURN n.name, n.salary",
    "MATCH (n:Person) RETURN n.name ORDER BY n.salary LIMIT 10",
]

# Queries that share structure but differ in literals (same fingerprint)
_SIMILAR_QUERIES = [
    ("MATCH (n:Person) WHERE n.age > 30 RETURN n", "MATCH (n:Person) WHERE n.age > 50 RETURN n"),
    ("MATCH (n:Person) WHERE n.salary > 100000 RETURN n", "MATCH (n:Person) WHERE n.salary > 200000 RETURN n"),
]


# ---------------------------------------------------------------------------
# Group 1: QueryFingerprinter micro-benchmarks
# ---------------------------------------------------------------------------


class TestQueryFingerprinterBenchmarks:
    """Benchmark structural fingerprinting speed."""

    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.fingerprinter = QueryFingerprinter()

    def test_fingerprint_simple_query(self, benchmark: Any) -> None:
        """Fingerprint a simple MATCH...RETURN query."""
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")
        benchmark(self.fingerprinter.fingerprint, ast)

    def test_fingerprint_filtered_query(self, benchmark: Any) -> None:
        """Fingerprint a query with WHERE clause."""
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
        )
        benchmark(self.fingerprinter.fingerprint, ast)

    def test_fingerprint_relationship_query(self, benchmark: Any) -> None:
        """Fingerprint a relationship pattern query."""
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name"
        )
        benchmark(self.fingerprinter.fingerprint, ast)

    def test_fingerprint_complex_query(self, benchmark: Any) -> None:
        """Fingerprint a complex multi-clause query."""
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.age > 25 AND c.salary > 100000 "
            "RETURN a.name, c.name, c.salary"
        )
        benchmark(self.fingerprinter.fingerprint, ast)

    def test_fingerprint_batch_8_queries(self, benchmark: Any) -> None:
        """Fingerprint a batch of 8 diverse queries."""
        asts = [ASTConverter.from_cypher(q) for q in _QUERIES]

        def batch_fingerprint() -> list:
            return [self.fingerprinter.fingerprint(ast) for ast in asts]

        benchmark(batch_fingerprint)

    def test_fingerprint_stability(self) -> None:
        """Verify structurally similar queries produce same fingerprint."""
        for q1, q2 in _SIMILAR_QUERIES:
            ast1 = ASTConverter.from_cypher(q1)
            ast2 = ASTConverter.from_cypher(q2)
            fp1 = self.fingerprinter.fingerprint(ast1)
            fp2 = self.fingerprinter.fingerprint(ast2)
            assert fp1.digest == fp2.digest, (
                f"Structurally similar queries should have same fingerprint: "
                f"{q1!r} vs {q2!r}"
            )


# ---------------------------------------------------------------------------
# Group 2: PredicateSelectivityTracker benchmarks
# ---------------------------------------------------------------------------


class TestPredicateSelectivityBenchmarks:
    """Benchmark selectivity learning operations."""

    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.tracker = PredicateSelectivityTracker()

    def test_record_selectivity(self, benchmark: Any) -> None:
        """Benchmark recording a single selectivity observation."""
        benchmark(
            self.tracker.record,
            "Person", "age", ">",
            estimated=0.33, actual=0.12,
        )

    def test_record_batch_100(self, benchmark: Any) -> None:
        """Record 100 selectivity observations across patterns."""
        patterns = [
            ("Person", "age", ">"),
            ("Person", "salary", ">"),
            ("Person", "dept", "="),
            ("Person", "name", "="),
            ("Person", "age", "<"),
        ]

        def batch_record() -> None:
            for i in range(100):
                et, prop, op = patterns[i % len(patterns)]
                self.tracker.record(
                    et, prop, op,
                    estimated=0.33, actual=0.1 + (i % 10) * 0.05,
                )

        benchmark(batch_record)

    def test_lookup_after_warmup(self, benchmark: Any) -> None:
        """Benchmark selectivity lookup after warmup with observations."""
        # Warm up with enough observations
        for i in range(10):
            self.tracker.record(
                "Person", "age", ">",
                estimated=0.33, actual=0.12 + i * 0.01,
            )
        benchmark(
            self.tracker.get_learned_selectivity, "Person", "age", ">"
        )

    def test_correction_factor(self, benchmark: Any) -> None:
        """Benchmark correction factor computation."""
        for i in range(10):
            self.tracker.record(
                "Person", "age", ">",
                estimated=0.33, actual=0.12 + i * 0.01,
            )
        benchmark(
            self.tracker.correction_factor,
            "Person", "age", ">",
            heuristic=0.33,
        )

    def test_ema_convergence(self) -> None:
        """Verify EMA converges to actual selectivity over observations."""
        tracker = PredicateSelectivityTracker()
        true_selectivity = 0.15
        for i in range(50):
            noise = (i % 5 - 2) * 0.01  # small noise
            tracker.record(
                "Person", "age", ">",
                estimated=0.33, actual=true_selectivity + noise,
            )
        learned = tracker.get_learned_selectivity("Person", "age", ">")
        assert learned is not None
        assert abs(learned - true_selectivity) < 0.05, (
            f"EMA should converge near true selectivity {true_selectivity}, "
            f"got {learned}"
        )


# ---------------------------------------------------------------------------
# Group 3: JoinPerformanceTracker benchmarks
# ---------------------------------------------------------------------------


class TestJoinPerformanceBenchmarks:
    """Benchmark join strategy learning operations."""

    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.tracker = JoinPerformanceTracker()

    def test_record_join(self, benchmark: Any) -> None:
        """Benchmark recording a single join observation."""
        benchmark(
            self.tracker.record,
            strategy="hash",
            left_rows=1000,
            right_rows=500,
            actual_output_rows=450,
            elapsed_ms=12.3,
        )

    def test_record_batch_100(self, benchmark: Any) -> None:
        """Record 100 join observations across strategies."""
        strategies = ["hash", "merge", "broadcast", "nested_loop"]
        sizes = [(100, 50), (1000, 500), (10000, 5000), (50, 50)]

        def batch_record() -> None:
            for i in range(100):
                strat = strategies[i % len(strategies)]
                left, right = sizes[i % len(sizes)]
                self.tracker.record(
                    strategy=strat,
                    left_rows=left,
                    right_rows=right,
                    actual_output_rows=min(left, right),
                    elapsed_ms=5.0 + (i % 20) * 0.5,
                )

        benchmark(batch_record)

    def test_best_strategy_lookup(self, benchmark: Any) -> None:
        """Benchmark strategy lookup after warmup."""
        # Warm up: hash is fastest for this bucket
        for i in range(10):
            self.tracker.record(
                strategy="hash",
                left_rows=1000, right_rows=500,
                actual_output_rows=450, elapsed_ms=5.0 + i * 0.1,
            )
            self.tracker.record(
                strategy="merge",
                left_rows=1000, right_rows=500,
                actual_output_rows=450, elapsed_ms=15.0 + i * 0.1,
            )
        benchmark(self.tracker.best_strategy, 1000, 500)

    def test_strategy_selection_accuracy(self) -> None:
        """Verify tracker selects the actually fastest strategy."""
        tracker = JoinPerformanceTracker()
        # Hash is consistently faster
        for _ in range(5):
            tracker.record(
                strategy="hash", left_rows=5000, right_rows=3000,
                actual_output_rows=2500, elapsed_ms=8.0,
            )
            tracker.record(
                strategy="merge", left_rows=5000, right_rows=3000,
                actual_output_rows=2500, elapsed_ms=20.0,
            )
            tracker.record(
                strategy="nested_loop", left_rows=5000, right_rows=3000,
                actual_output_rows=2500, elapsed_ms=50.0,
            )
        best = tracker.best_strategy(5000, 3000)
        assert best == "hash", f"Expected hash as best strategy, got {best}"


# ---------------------------------------------------------------------------
# Group 4: AdaptivePlanCache benchmarks
# ---------------------------------------------------------------------------


class TestAdaptivePlanCacheBenchmarks:
    """Benchmark plan cache operations."""

    def _make_fingerprint(self, idx: int) -> QueryFingerprint:
        return QueryFingerprint(
            digest=f"fp_{idx:016x}",
            clause_signature=f"Match -> Return #{idx}",
            entity_types=("Person",),
            relationship_types=(),
        )

    def _make_analysis(self) -> Any:
        """Create a mock AnalysisResult-like object."""
        from pycypher.query_planner import AnalysisResult
        result = AnalysisResult()
        result.clause_cardinalities = [1000, 1000]
        result.estimated_peak_bytes = 80000
        return result

    def test_cache_put(self, benchmark: Any) -> None:
        """Benchmark inserting into plan cache."""
        cache = AdaptivePlanCache(max_entries=1024)
        analysis = self._make_analysis()
        counter = [0]

        def put_one() -> None:
            fp = self._make_fingerprint(counter[0])
            counter[0] += 1
            cache.put(fp, analysis)

        benchmark(put_one)

    def test_cache_hit(self, benchmark: Any) -> None:
        """Benchmark cache hit lookup."""
        cache = AdaptivePlanCache(max_entries=256)
        fp = self._make_fingerprint(0)
        analysis = self._make_analysis()
        cache.put(fp, analysis)
        benchmark(cache.get, fp)

    def test_cache_miss(self, benchmark: Any) -> None:
        """Benchmark cache miss lookup."""
        cache = AdaptivePlanCache(max_entries=256)
        fp = self._make_fingerprint(999)
        benchmark(cache.get, fp)

    def test_cache_hit_rate_under_load(self) -> None:
        """Measure cache hit rate with realistic workload."""
        cache = AdaptivePlanCache(max_entries=64)
        analysis = self._make_analysis()
        rng = np.random.default_rng(42)

        # Populate cache with 64 plans
        fps = [self._make_fingerprint(i) for i in range(64)]
        for fp in fps:
            cache.put(fp, analysis)

        # Simulate workload: 80% repeat queries, 20% new
        hits = 0
        total = 1000
        for _ in range(total):
            if rng.random() < 0.8:
                fp = fps[rng.integers(0, 64)]
            else:
                fp = self._make_fingerprint(rng.integers(1000, 2000))
            result = cache.get(fp)
            if result is not None:
                hits += 1

        hit_rate = hits / total
        assert hit_rate > 0.70, (
            f"Expected >70% hit rate with 80% repeat workload, got {hit_rate:.1%}"
        )

    def test_lru_eviction_performance(self) -> None:
        """Verify LRU eviction maintains bounded memory."""
        cache = AdaptivePlanCache(max_entries=32)
        analysis = self._make_analysis()

        # Insert 100 entries — should evict down to 32
        for i in range(100):
            cache.put(self._make_fingerprint(i), analysis)

        stats = cache.stats
        assert stats["entries"] <= 32, (
            f"Cache should maintain <= 32 entries, has {stats['entries']}"
        )


# ---------------------------------------------------------------------------
# Group 5: Thread safety under contention
# ---------------------------------------------------------------------------


class TestMLOptThreadSafety:
    """Benchmark ML optimization components under concurrent access."""

    def test_concurrent_selectivity_recording(self, benchmark: Any) -> None:
        """Benchmark selectivity tracker under 4-thread contention."""
        tracker = PredicateSelectivityTracker()

        def record_many() -> None:
            threads = []
            for t in range(4):
                def worker(tid: int = t) -> None:
                    for i in range(100):
                        tracker.record(
                            "Person", "age", ">",
                            estimated=0.33, actual=0.1 + tid * 0.05,
                        )
                threads.append(threading.Thread(target=worker))
            for th in threads:
                th.start()
            for th in threads:
                th.join()

        benchmark(record_many)

    def test_concurrent_cache_access(self, benchmark: Any) -> None:
        """Benchmark plan cache under 4-thread concurrent read/write."""
        cache = AdaptivePlanCache(max_entries=128)
        from pycypher.query_planner import AnalysisResult
        analysis = AnalysisResult()

        def concurrent_ops() -> None:
            threads = []
            for t in range(4):
                def worker(tid: int = t) -> None:
                    for i in range(100):
                        fp = QueryFingerprint(
                            digest=f"fp_{tid}_{i:08x}",
                            clause_signature="Match -> Return",
                            entity_types=("Person",),
                            relationship_types=(),
                        )
                        if i % 3 == 0:
                            cache.put(fp, analysis)
                        else:
                            cache.get(fp)
                threads.append(threading.Thread(target=worker))
            for th in threads:
                th.start()
            for th in threads:
                th.join()

        benchmark(concurrent_ops)


# ---------------------------------------------------------------------------
# Group 6: End-to-end QueryPlanAnalyzer with/without learning store
# ---------------------------------------------------------------------------


class TestPlanAnalyzerWithLearning:
    """Benchmark query plan analysis with and without ML learning store."""

    def test_analyze_without_learning(
        self, benchmark: Any, ctx_1k: Context
    ) -> None:
        """Baseline: analyze query without learning store."""
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 30 RETURN a.name, b.name"
        )

        def analyze() -> Any:
            analyzer = QueryPlanAnalyzer(ast, ctx_1k, learning_store=None)
            return analyzer.analyze()

        benchmark(analyze)

    def test_analyze_with_learning_cold(
        self, benchmark: Any, ctx_1k: Context
    ) -> None:
        """Analyze with empty learning store (cold start)."""
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 30 RETURN a.name, b.name"
        )
        store = QueryLearningStore()

        def analyze() -> Any:
            store.clear()
            analyzer = QueryPlanAnalyzer(ast, ctx_1k, learning_store=store)
            return analyzer.analyze()

        benchmark(analyze)

    def test_analyze_with_learning_warm(
        self, benchmark: Any, ctx_1k: Context
    ) -> None:
        """Analyze with warm learning store (plan cache populated)."""
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 30 RETURN a.name, b.name"
        )
        store = QueryLearningStore()

        # Warm up: first analysis populates the cache
        warmup_analyzer = QueryPlanAnalyzer(ast, ctx_1k, learning_store=store)
        warmup_analyzer.analyze()

        def analyze() -> Any:
            analyzer = QueryPlanAnalyzer(ast, ctx_1k, learning_store=store)
            return analyzer.analyze()

        benchmark(analyze)

    def test_plan_cache_speedup(self, ctx_1k: Context) -> None:
        """Measure speedup from plan cache hit vs miss."""
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 30 RETURN a.name, b.name"
        )
        store = QueryLearningStore()

        # Cold: first analysis (cache miss)
        cold_times = []
        for _ in range(10):
            store.clear()
            analyzer = QueryPlanAnalyzer(ast, ctx_1k, learning_store=store)
            t0 = time.perf_counter()
            analyzer.analyze()
            cold_times.append(time.perf_counter() - t0)

        # Warm: subsequent analyses (cache hit)
        store.clear()
        warmup = QueryPlanAnalyzer(ast, ctx_1k, learning_store=store)
        warmup.analyze()

        warm_times = []
        for _ in range(10):
            analyzer = QueryPlanAnalyzer(ast, ctx_1k, learning_store=store)
            t0 = time.perf_counter()
            analyzer.analyze()
            warm_times.append(time.perf_counter() - t0)

        cold_median = statistics.median(cold_times)
        warm_median = statistics.median(warm_times)

        # Warm should be faster (cache hit skips analysis)
        if cold_median > 0:
            speedup = cold_median / warm_median if warm_median > 0 else float("inf")
            # Log the result for visibility
            print(
                f"\nPlan cache speedup: {speedup:.1f}x "
                f"(cold={cold_median*1000:.2f}ms, warm={warm_median*1000:.2f}ms)"
            )
            # Cache hit should be at least somewhat faster
            assert warm_median <= cold_median * 1.5, (
                f"Warm analysis should not be significantly slower than cold: "
                f"warm={warm_median*1000:.2f}ms, cold={cold_median*1000:.2f}ms"
            )


# ---------------------------------------------------------------------------
# Group 7: Full QueryLearningStore integration benchmarks
# ---------------------------------------------------------------------------


class TestQueryLearningStoreIntegration:
    """End-to-end benchmarks for the unified learning store."""

    def test_full_feedback_loop(self, benchmark: Any) -> None:
        """Benchmark a complete feedback loop cycle."""
        store = QueryLearningStore()
        ast = ASTConverter.from_cypher("MATCH (n:Person) WHERE n.age > 30 RETURN n")

        def feedback_cycle() -> None:
            # 1. Fingerprint
            fp = store.fingerprint(ast)
            # 2. Check cache
            store.get_cached_plan(fp)
            # 3. Record selectivity
            store.record_selectivity(
                "Person", "age", ">", estimated=0.33, actual=0.15
            )
            # 4. Record join
            store.record_join_performance(
                strategy="hash", left_rows=1000, right_rows=500,
                actual_output_rows=450, elapsed_ms=8.5,
            )
            # 5. Get learned values
            store.get_learned_selectivity("Person", "age", ">")
            store.get_best_join_strategy(1000, 500)

        benchmark(feedback_cycle)

    def test_diagnostics(self, benchmark: Any) -> None:
        """Benchmark diagnostics reporting."""
        store = QueryLearningStore()
        # Populate some data
        for i in range(20):
            store.record_selectivity(
                "Person", "age", ">", estimated=0.33, actual=0.15
            )
            store.record_join_performance(
                strategy="hash", left_rows=1000, right_rows=500,
                actual_output_rows=450, elapsed_ms=8.5,
            )
        benchmark(store.diagnostics)

    def test_mutation_invalidation(self, benchmark: Any) -> None:
        """Benchmark cache invalidation after mutation."""
        store = QueryLearningStore()
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN n")

        # Pre-populate cache
        fp = store.fingerprint(ast)
        from pycypher.query_planner import AnalysisResult
        store.cache_plan(fp, AnalysisResult())

        benchmark(store.invalidate_on_mutation)


# ---------------------------------------------------------------------------
# Group 8: Selectivity learning improves estimation quality
# ---------------------------------------------------------------------------


class TestSelectivityLearningQuality:
    """Validate that selectivity learning actually improves estimates."""

    def test_selectivity_convergence_speed(self) -> None:
        """Measure how many observations needed for convergence."""
        tracker = PredicateSelectivityTracker()
        true_sel = 0.15
        convergence_point = None

        for i in range(50):
            tracker.record(
                "Person", "age", ">",
                estimated=0.33, actual=true_sel,
            )
            learned = tracker.get_learned_selectivity("Person", "age", ">")
            if learned is not None and abs(learned - true_sel) < 0.02:
                convergence_point = i + 1
                break

        assert convergence_point is not None, (
            "Selectivity should converge within 50 observations"
        )
        assert convergence_point <= 20, (
            f"Expected convergence within 20 observations, took {convergence_point}"
        )
        print(f"\nSelectivity convergence at observation #{convergence_point}")

    def test_selectivity_adapts_to_distribution_shift(self) -> None:
        """Verify tracker adapts when true selectivity changes."""
        tracker = PredicateSelectivityTracker()

        # Phase 1: true selectivity is 0.15
        for _ in range(20):
            tracker.record(
                "Person", "age", ">",
                estimated=0.33, actual=0.15,
            )
        learned_phase1 = tracker.get_learned_selectivity("Person", "age", ">")
        assert learned_phase1 is not None
        assert abs(learned_phase1 - 0.15) < 0.03

        # Phase 2: distribution shifts, true selectivity is now 0.40
        for _ in range(30):
            tracker.record(
                "Person", "age", ">",
                estimated=0.33, actual=0.40,
            )
        learned_phase2 = tracker.get_learned_selectivity("Person", "age", ">")
        assert learned_phase2 is not None
        # Should have adapted toward 0.40
        assert learned_phase2 > 0.30, (
            f"Expected learned selectivity > 0.30 after shift to 0.40, "
            f"got {learned_phase2:.4f}"
        )
        print(
            f"\nAdaptation: phase1={learned_phase1:.4f}, "
            f"phase2={learned_phase2:.4f} (target=0.40)"
        )

    def test_multi_pattern_isolation(self) -> None:
        """Verify different predicate patterns are tracked independently."""
        tracker = PredicateSelectivityTracker()

        patterns = {
            ("Person", "age", ">"): 0.15,
            ("Person", "salary", ">"): 0.25,
            ("Person", "dept", "="): 0.20,
        }

        for (et, prop, op), true_sel in patterns.items():
            for _ in range(10):
                tracker.record(et, prop, op, estimated=0.33, actual=true_sel)

        for (et, prop, op), true_sel in patterns.items():
            learned = tracker.get_learned_selectivity(et, prop, op)
            assert learned is not None
            assert abs(learned - true_sel) < 0.05, (
                f"Pattern ({et}, {prop}, {op}): expected ~{true_sel}, "
                f"got {learned:.4f}"
            )


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== ML Optimization Performance Validation ===\n")

    store = QueryLearningStore()
    ctx = _build_context(1_000)

    # 1. Fingerprinting speed
    print("1. QueryFingerprinter speed:")
    ast = ASTConverter.from_cypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name, b.name")
    fp_times = []
    for _ in range(1000):
        t0 = time.perf_counter()
        store.fingerprint(ast)
        fp_times.append(time.perf_counter() - t0)
    print(f"   Median: {statistics.median(fp_times)*1e6:.1f}us per fingerprint")

    # 2. Plan cache performance
    print("\n2. AdaptivePlanCache:")
    fp = store.fingerprint(ast)
    analyzer = QueryPlanAnalyzer(ast, ctx, learning_store=store)
    analysis = analyzer.analyze()

    hit_times = []
    for _ in range(1000):
        t0 = time.perf_counter()
        store.get_cached_plan(fp)
        hit_times.append(time.perf_counter() - t0)
    print(f"   Cache hit: {statistics.median(hit_times)*1e6:.1f}us")
    print(f"   Stats: {store.diagnostics()['plan_cache']}")

    # 3. Selectivity learning
    print("\n3. PredicateSelectivityTracker:")
    store.clear()
    for i in range(20):
        store.record_selectivity("Person", "age", ">", estimated=0.33, actual=0.15)
    learned = store.get_learned_selectivity("Person", "age", ">")
    print(f"   Learned selectivity: {learned:.4f} (true: 0.15)")
    correction = store.selectivity_tracker.correction_factor(
        "Person", "age", ">", heuristic=0.33
    )
    print(f"   Correction factor: {correction:.4f}")

    # 4. Join strategy learning
    print("\n4. JoinPerformanceTracker:")
    store.clear()
    for _ in range(10):
        store.record_join_performance(
            strategy="hash", left_rows=5000, right_rows=3000,
            actual_output_rows=2500, elapsed_ms=8.0,
        )
        store.record_join_performance(
            strategy="merge", left_rows=5000, right_rows=3000,
            actual_output_rows=2500, elapsed_ms=20.0,
        )
    best = store.get_best_join_strategy(5000, 3000)
    print(f"   Best strategy for 5K x 3K: {best}")
    stats = store.join_tracker.strategy_stats(5000, 3000)
    for strat, data in stats.items():
        print(f"   {strat}: avg={data['avg_ms']:.1f}ms, count={data['count']:.0f}")

    # 5. Cold vs warm plan analysis
    print("\n5. Plan Analysis Cold vs Warm:")
    store.clear()
    cold_times = []
    for _ in range(20):
        store.clear()
        a = QueryPlanAnalyzer(ast, ctx, learning_store=store)
        t0 = time.perf_counter()
        a.analyze()
        cold_times.append(time.perf_counter() - t0)

    store.clear()
    warmup = QueryPlanAnalyzer(ast, ctx, learning_store=store)
    warmup.analyze()
    warm_times = []
    for _ in range(20):
        a = QueryPlanAnalyzer(ast, ctx, learning_store=store)
        t0 = time.perf_counter()
        a.analyze()
        warm_times.append(time.perf_counter() - t0)

    cold_med = statistics.median(cold_times) * 1000
    warm_med = statistics.median(warm_times) * 1000
    speedup = cold_med / warm_med if warm_med > 0 else float("inf")
    print(f"   Cold (no cache): {cold_med:.3f}ms")
    print(f"   Warm (cache hit): {warm_med:.3f}ms")
    print(f"   Speedup: {speedup:.1f}x")

    print(f"\n   Final diagnostics: {store.diagnostics()}")
    print("\n=== Validation Complete ===")
