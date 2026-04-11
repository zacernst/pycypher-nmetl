"""TDD tests validating that all documentation examples are executable.

These tests ensure documentation code examples remain accurate and functional.
Each test corresponds to a specific documentation section and validates that
the example code produces the documented behavior.
"""

import time

import pytest

# ---------------------------------------------------------------------------
# README ML Optimization Section Examples
# ---------------------------------------------------------------------------


class TestReadmeMLOptimizationExamples:
    """Validate examples from the README ML Optimization section."""

    def test_query_learning_store_creation(self):
        """README example: creating a QueryLearningStore."""
        from pycypher.query_learning import QueryLearningStore

        store = QueryLearningStore()
        assert store is not None
        assert hasattr(store, "fingerprinter")
        assert hasattr(store, "selectivity_tracker")
        assert hasattr(store, "join_tracker")
        assert hasattr(store, "plan_cache")

    def test_singleton_access(self):
        """README example: accessing the global singleton."""
        from pycypher.query_learning import get_learning_store

        store = get_learning_store()
        assert store is not None
        # Singleton returns same instance
        store2 = get_learning_store()
        assert store is store2

    def test_fingerprint_query(self):
        """README example: fingerprinting a query for plan reuse."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_learning import QueryLearningStore

        store = QueryLearningStore()
        query = ASTConverter.from_cypher(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        )
        fingerprint = store.fingerprint(query)

        assert fingerprint.digest  # non-empty hex string
        assert "Person" in fingerprint.entity_types
        assert "Match" in fingerprint.clause_signature
        assert "Return" in fingerprint.clause_signature

    def test_literal_invariant_fingerprinting(self):
        """README example: same structure, different literals = same fingerprint."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_learning import QueryLearningStore

        store = QueryLearningStore()
        q1 = ASTConverter.from_cypher(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        )
        q2 = ASTConverter.from_cypher(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name"
        )

        fp1 = store.fingerprint(q1)
        fp2 = store.fingerprint(q2)
        assert fp1.digest == fp2.digest

    def test_selectivity_learning_workflow(self):
        """README example: recording and retrieving learned selectivity."""
        from pycypher.query_learning import QueryLearningStore

        store = QueryLearningStore()

        # Record observed selectivities (system does this automatically)
        for _ in range(5):
            store.record_selectivity(
                "Person", "age", ">", estimated=0.33, actual=0.12
            )

        # Retrieve learned selectivity
        learned = store.get_learned_selectivity("Person", "age", ">")
        assert learned is not None
        # Converges toward actual value of 0.12
        assert 0.08 <= learned <= 0.18

    def test_join_strategy_learning_workflow(self):
        """README example: learning optimal join strategies."""
        from pycypher.query_learning import QueryLearningStore

        store = QueryLearningStore()

        # Record performance: hash join is slower
        for _ in range(5):
            store.record_join_performance(
                strategy="hash",
                left_rows=5000,
                right_rows=3000,
                actual_output_rows=2500,
                elapsed_ms=30.0,
            )

        # Record performance: merge join is faster
        for _ in range(5):
            store.record_join_performance(
                strategy="merge",
                left_rows=5000,
                right_rows=3000,
                actual_output_rows=2500,
                elapsed_ms=10.0,
            )

        best = store.get_best_join_strategy(5000, 3000)
        assert best == "merge"

    def test_plan_caching_workflow(self):
        """README example: caching and reusing query plans."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_learning import QueryLearningStore
        from pycypher.query_planner import AnalysisResult

        store = QueryLearningStore()
        query = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fingerprint = store.fingerprint(query)

        # First execution: cache miss
        cached = store.get_cached_plan(fingerprint)
        assert cached is None

        # After analysis, cache the plan
        analysis = AnalysisResult(clause_cardinalities=[100])
        store.cache_plan(fingerprint, analysis)

        # Subsequent execution: cache hit
        cached = store.get_cached_plan(fingerprint)
        assert cached is not None
        assert cached.clause_cardinalities == [100]

    def test_diagnostics_snapshot(self):
        """README example: getting diagnostics."""
        from pycypher.query_learning import QueryLearningStore

        store = QueryLearningStore()
        diag = store.diagnostics()

        assert "plan_cache" in diag
        assert "selectivity_patterns" in diag
        assert "join_buckets_tracked" in diag
        assert "hits" in diag["plan_cache"]
        assert "hit_rate" in diag["plan_cache"]


# ---------------------------------------------------------------------------
# Tutorial: ML Optimization Walkthrough Examples
# ---------------------------------------------------------------------------


class TestTutorialMLOptimizationExamples:
    """Validate examples from the ML optimization tutorial."""

    def test_cache_effectiveness_measurement(self):
        """Tutorial example: measuring cache hit rates."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_learning import QueryLearningStore
        from pycypher.query_planner import AnalysisResult

        store = QueryLearningStore()

        # Simulate repeated query pattern
        queries = [
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name",
            "MATCH (p:Person) WHERE p.age > 25 RETURN p.name",
        ]

        # First query: cache miss, compute and cache
        q1 = ASTConverter.from_cypher(queries[0])
        fp1 = store.fingerprint(q1)
        assert store.get_cached_plan(fp1) is None
        store.cache_plan(fp1, AnalysisResult(clause_cardinalities=[100]))

        # Subsequent queries with same structure: cache hits
        for cypher in queries[1:]:
            q = ASTConverter.from_cypher(cypher)
            fp = store.fingerprint(q)
            assert store.get_cached_plan(fp) is not None

        # Verify cache effectiveness
        stats = store.plan_cache.stats
        assert stats["hits"] >= 2
        assert stats["hit_rate"] > 0.5

    def test_selectivity_convergence(self):
        """Tutorial example: showing EMA convergence over observations."""
        from pycypher.query_learning import PredicateSelectivityTracker

        tracker = PredicateSelectivityTracker()

        # Simulate observations where actual selectivity is ~0.15
        actuals = [0.12, 0.18, 0.14, 0.16, 0.13, 0.15, 0.14, 0.15, 0.16, 0.14]

        for actual in actuals:
            tracker.record("Person", "age", ">", estimated=0.33, actual=actual)

        learned = tracker.get_learned_selectivity("Person", "age", ">")
        assert learned is not None
        # Should converge near the mean (~0.147)
        assert 0.10 <= learned <= 0.20

    def test_correction_factor_usage(self):
        """Tutorial example: using correction factors to improve estimates."""
        from pycypher.query_learning import PredicateSelectivityTracker

        tracker = PredicateSelectivityTracker()

        # Record: actual selectivity is much lower than heuristic
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)

        # Get correction factor
        factor = tracker.correction_factor(
            "Person", "age", ">", heuristic=0.33
        )
        # factor ~ 0.12/0.33 ~ 0.36
        assert 0.2 <= factor <= 0.6

        # Apply correction: improved_estimate = heuristic * factor
        heuristic = 0.33
        improved = heuristic * factor
        assert 0.08 <= improved <= 0.20  # Much closer to actual 0.12

    def test_join_size_bucketing(self):
        """Tutorial example: understanding join size buckets."""
        from pycypher.query_learning import _size_bucket

        assert _size_bucket(50) == "tiny"      # <= 100 rows
        assert _size_bucket(5000) == "small"    # <= 10K rows
        assert _size_bucket(500_000) == "medium"  # <= 1M rows
        assert _size_bucket(5_000_000) == "large"  # > 1M rows

    def test_mutation_invalidation(self):
        """Tutorial example: cache invalidation on data mutations."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_learning import QueryLearningStore
        from pycypher.query_planner import AnalysisResult

        store = QueryLearningStore()
        query = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fp = store.fingerprint(query)
        store.cache_plan(fp, AnalysisResult(clause_cardinalities=[100]))

        # After a CREATE/SET/DELETE, invalidate stale plans
        store.invalidate_on_mutation()

        # Cache is cleared
        assert store.get_cached_plan(fp) is None

    def test_adaptive_plan_cache_lru_behavior(self):
        """Tutorial example: LRU eviction in plan cache."""
        from pycypher.query_learning import (
            AdaptivePlanCache,
            QueryFingerprint,
        )
        from pycypher.query_planner import AnalysisResult

        cache = AdaptivePlanCache(max_entries=3)

        fps = [
            QueryFingerprint(
                digest=f"query_{i}",
                clause_signature="Match -> Return",
                entity_types=("Person",),
                relationship_types=(),
            )
            for i in range(4)
        ]
        analysis = AnalysisResult(clause_cardinalities=[100])

        # Fill cache
        for fp in fps[:3]:
            cache.put(fp, analysis)

        # Access first entry to keep it fresh
        cache.get(fps[0])

        # Insert fourth — evicts LRU (fps[1])
        cache.put(fps[3], analysis)

        assert cache.get(fps[0]) is not None  # kept (recently accessed)
        assert cache.get(fps[1]) is None       # evicted (LRU)
        assert cache.get(fps[3]) is not None  # newly added

    def test_adaptive_plan_cache_ttl_behavior(self):
        """Tutorial example: TTL expiry in plan cache."""
        from pycypher.query_learning import (
            AdaptivePlanCache,
            QueryFingerprint,
        )
        from pycypher.query_planner import AnalysisResult

        # Short TTL for demonstration
        cache = AdaptivePlanCache(ttl_seconds=0.05)

        fp = QueryFingerprint(
            digest="short_lived",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )
        cache.put(fp, AnalysisResult(clause_cardinalities=[100]))

        # Immediate access: hit
        assert cache.get(fp) is not None

        # After TTL expires: miss
        time.sleep(0.06)
        assert cache.get(fp) is None


# ---------------------------------------------------------------------------
# API Docstring Validation
# ---------------------------------------------------------------------------


class TestAPIDocstringCompleteness:
    """Validate that all public API classes/functions have docstrings."""

    def test_query_learning_module_docstring(self):
        """Module has a docstring."""
        import pycypher.query_learning as mod

        assert mod.__doc__ is not None
        assert "adaptive query optimization" in mod.__doc__.lower()

    def test_all_public_classes_have_docstrings(self):
        """All public classes have non-empty docstrings."""
        from pycypher.query_learning import (
            AdaptivePlanCache,
            JoinPerformanceTracker,
            PredicateSelectivityTracker,
            QueryFingerprinter,
            QueryLearningStore,
        )

        for cls in [
            QueryFingerprinter,
            PredicateSelectivityTracker,
            JoinPerformanceTracker,
            AdaptivePlanCache,
            QueryLearningStore,
        ]:
            assert cls.__doc__, f"{cls.__name__} missing docstring"
            assert len(cls.__doc__) > 20, f"{cls.__name__} docstring too short"

    def test_query_fingerprint_dataclass_docstring(self):
        """QueryFingerprint has documented attributes."""
        from pycypher.query_learning import QueryFingerprint

        assert QueryFingerprint.__doc__ is not None
        assert "digest" in QueryFingerprint.__doc__
        assert "clause_signature" in QueryFingerprint.__doc__

    def test_public_methods_have_docstrings(self):
        """Key public methods have docstrings."""
        from pycypher.query_learning import QueryLearningStore

        methods_to_check = [
            "fingerprint",
            "get_cached_plan",
            "cache_plan",
            "record_selectivity",
            "get_learned_selectivity",
            "record_join_performance",
            "get_best_join_strategy",
            "invalidate_on_mutation",
            "diagnostics",
            "clear",
        ]

        for method_name in methods_to_check:
            method = getattr(QueryLearningStore, method_name)
            assert method.__doc__, (
                f"QueryLearningStore.{method_name} missing docstring"
            )

    def test_get_learning_store_docstring(self):
        """get_learning_store function has a docstring."""
        from pycypher.query_learning import get_learning_store

        assert get_learning_store.__doc__ is not None
        assert "singleton" in get_learning_store.__doc__.lower()


# ---------------------------------------------------------------------------
# Backend Delegation Guide Examples
# ---------------------------------------------------------------------------


class TestBackendDelegationExamples:
    """Validate examples from the backend delegation guide."""

    def test_backend_engine_protocol_exists(self):
        """Backend guide: BackendEngine protocol is importable."""
        from pycypher.backend_engine import BackendEngine

        assert BackendEngine is not None

    def test_pandas_backend_importable(self):
        """Backend guide: default Pandas backend."""
        from pycypher.backend_engine import PandasBackend

        backend = PandasBackend()
        assert backend is not None

    def test_duckdb_backend_importable(self):
        """Backend guide: DuckDB backend for analytical workloads."""
        from pycypher.backends.duckdb_backend import DuckDBBackend

        backend = DuckDBBackend()
        assert backend is not None

    def test_backend_protocol_methods(self):
        """Backend guide: protocol defines expected operations."""
        from pycypher.backend_engine import BackendEngine

        # Verify key protocol methods exist
        expected_methods = [
            "scan_entity",
            "join",
            "filter",
            "rename",
            "concat",
            "distinct",
            "assign_column",
            "drop_columns",
            "aggregate",
            "sort",
            "limit",
            "skip",
            "to_pandas",
            "row_count",
            "is_empty",
            "memory_estimate_bytes",
        ]

        for method_name in expected_methods:
            assert hasattr(BackendEngine, method_name), (
                f"BackendEngine missing {method_name}"
            )
