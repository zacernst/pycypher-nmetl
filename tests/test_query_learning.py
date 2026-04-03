"""Tests for the query_learning module — ML feedback loops for adaptive optimization."""

import time

import pytest

from pycypher.query_learning import (
    AdaptivePlanCache,
    JoinPerformanceTracker,
    PredicateSelectivityTracker,
    QueryFingerprinter,
    QueryLearningStore,
    _size_bucket,
    get_learning_store,
)


# ---------------------------------------------------------------------------
# QueryFingerprinter tests
# ---------------------------------------------------------------------------


class TestQueryFingerprinter:
    """Tests for structural query fingerprinting."""

    def test_identical_queries_same_fingerprint(self):
        """Two identical ASTs produce the same fingerprint digest."""
        from pycypher.ast_converter import ASTConverter

        q1 = ASTConverter.from_cypher("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
        q2 = ASTConverter.from_cypher("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
        fp = QueryFingerprinter()
        assert fp.fingerprint(q1).digest == fp.fingerprint(q2).digest

    def test_different_literals_same_fingerprint(self):
        """Queries differing only in literal values share a fingerprint."""
        from pycypher.ast_converter import ASTConverter

        q1 = ASTConverter.from_cypher("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
        q2 = ASTConverter.from_cypher("MATCH (p:Person) WHERE p.age > 50 RETURN p.name")
        fp = QueryFingerprinter()
        assert fp.fingerprint(q1).digest == fp.fingerprint(q2).digest

    def test_different_structure_different_fingerprint(self):
        """Queries with different clause structures get different fingerprints."""
        from pycypher.ast_converter import ASTConverter

        q1 = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        q2 = ASTConverter.from_cypher("MATCH (c:Company) RETURN c.name")
        fp = QueryFingerprinter()
        assert fp.fingerprint(q1).digest != fp.fingerprint(q2).digest

    def test_fingerprint_captures_entity_types(self):
        """Fingerprint records entity types."""
        from pycypher.ast_converter import ASTConverter

        q = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fp = QueryFingerprinter()
        result = fp.fingerprint(q)
        assert "Person" in result.entity_types

    def test_fingerprint_captures_relationship_types(self):
        """Fingerprint records relationship types."""
        from pycypher.ast_converter import ASTConverter

        q = ASTConverter.from_cypher(
            "MATCH (p:Person)-[r:KNOWS]->(q:Person) RETURN p.name"
        )
        fp = QueryFingerprinter()
        result = fp.fingerprint(q)
        assert "KNOWS" in result.relationship_types

    def test_fingerprint_clause_signature(self):
        """Fingerprint records clause type sequence."""
        from pycypher.ast_converter import ASTConverter

        q = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fp = QueryFingerprinter()
        result = fp.fingerprint(q)
        assert "Match" in result.clause_signature
        assert "Return" in result.clause_signature


# ---------------------------------------------------------------------------
# PredicateSelectivityTracker tests
# ---------------------------------------------------------------------------


class TestPredicateSelectivityTracker:
    """Tests for learned predicate selectivity."""

    def test_no_data_returns_none(self):
        """Returns None when no observations recorded."""
        tracker = PredicateSelectivityTracker()
        assert tracker.get_learned_selectivity("Person", "age", ">") is None

    def test_insufficient_data_returns_none(self):
        """Returns None with fewer than _MIN_OBSERVATIONS."""
        tracker = PredicateSelectivityTracker()
        tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)
        tracker.record("Person", "age", ">", estimated=0.33, actual=0.14)
        # Only 2 observations, need 3
        assert tracker.get_learned_selectivity("Person", "age", ">") is None

    def test_sufficient_data_returns_learned(self):
        """Returns EMA-based selectivity after enough observations."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)
        result = tracker.get_learned_selectivity("Person", "age", ">")
        assert result is not None
        assert 0.10 <= result <= 0.15  # Should converge near 0.12

    def test_ema_weights_recent_more(self):
        """EMA gives more weight to recent observations."""
        tracker = PredicateSelectivityTracker()
        # First: high selectivity
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.50)
        # Then: low selectivity
        for _ in range(10):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.10)
        result = tracker.get_learned_selectivity("Person", "age", ">")
        assert result is not None
        # Should be closer to 0.10 than 0.50
        assert result < 0.25

    def test_correction_factor_no_data(self):
        """Correction factor is 1.0 without data."""
        tracker = PredicateSelectivityTracker()
        factor = tracker.correction_factor(
            "Person", "age", ">", heuristic=0.33
        )
        assert factor == 1.0

    def test_correction_factor_with_data(self):
        """Correction factor adjusts heuristic toward learned value."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)
        factor = tracker.correction_factor(
            "Person", "age", ">", heuristic=0.33
        )
        # factor ≈ 0.12 / 0.33 ≈ 0.36
        assert 0.2 <= factor <= 0.6

    def test_operator_normalization(self):
        """Operators are normalized (case, whitespace)."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", " > ", estimated=0.33, actual=0.12)
        result = tracker.get_learned_selectivity("Person", "age", ">")
        assert result is not None

    def test_tracked_patterns(self):
        """tracked_patterns returns recorded triples."""
        tracker = PredicateSelectivityTracker()
        for _ in range(3):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)
        patterns = tracker.tracked_patterns
        assert ("Person", "age", ">") in patterns

    def test_clear(self):
        """clear() resets all state."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)
        tracker.clear()
        assert tracker.get_learned_selectivity("Person", "age", ">") is None
        assert tracker.tracked_patterns == []


# ---------------------------------------------------------------------------
# JoinPerformanceTracker tests
# ---------------------------------------------------------------------------


class TestJoinPerformanceTracker:
    """Tests for join strategy performance tracking."""

    def test_no_data_returns_none(self):
        """Returns None when no observations."""
        tracker = JoinPerformanceTracker()
        assert tracker.best_strategy(1000, 500) is None

    def test_insufficient_data_returns_none(self):
        """Returns None with too few observations."""
        tracker = JoinPerformanceTracker()
        tracker.record(
            strategy="hash",
            left_rows=1000,
            right_rows=500,
            actual_output_rows=450,
            elapsed_ms=10.0,
        )
        assert tracker.best_strategy(1000, 500) is None

    def test_returns_fastest_strategy(self):
        """Returns the strategy with lowest average elapsed time."""
        tracker = JoinPerformanceTracker()
        # Hash: slower
        for _ in range(5):
            tracker.record(
                strategy="hash",
                left_rows=1000,
                right_rows=500,
                actual_output_rows=450,
                elapsed_ms=20.0,
            )
        # Broadcast: faster
        for _ in range(5):
            tracker.record(
                strategy="broadcast",
                left_rows=1000,
                right_rows=500,
                actual_output_rows=450,
                elapsed_ms=5.0,
            )
        assert tracker.best_strategy(1000, 500) == "broadcast"

    def test_size_bucketing(self):
        """Different size ranges map to different buckets."""
        assert _size_bucket(50) == "tiny"
        assert _size_bucket(5000) == "small"
        assert _size_bucket(500000) == "medium"
        assert _size_bucket(5000000) == "large"

    def test_strategy_stats(self):
        """strategy_stats returns per-strategy metrics."""
        tracker = JoinPerformanceTracker()
        for _ in range(5):
            tracker.record(
                strategy="hash",
                left_rows=1000,
                right_rows=500,
                actual_output_rows=450,
                elapsed_ms=15.0,
            )
        stats = tracker.strategy_stats(1000, 500)
        assert "hash" in stats
        assert stats["hash"]["avg_ms"] == 15.0
        assert stats["hash"]["count"] == 5.0

    def test_clear(self):
        """clear() resets all state."""
        tracker = JoinPerformanceTracker()
        for _ in range(5):
            tracker.record(
                strategy="hash",
                left_rows=1000,
                right_rows=500,
                actual_output_rows=450,
                elapsed_ms=15.0,
            )
        tracker.clear()
        assert tracker.best_strategy(1000, 500) is None


# ---------------------------------------------------------------------------
# AdaptivePlanCache tests
# ---------------------------------------------------------------------------


class TestAdaptivePlanCache:
    """Tests for adaptive plan caching."""

    def _make_fingerprint(self, digest: str = "abc123") -> "QueryFingerprint":
        from pycypher.query_learning import QueryFingerprint

        return QueryFingerprint(
            digest=digest,
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )

    def _make_analysis(self) -> "AnalysisResult":
        from pycypher.query_planner import AnalysisResult

        return AnalysisResult(
            clause_cardinalities=[100, 100],
            estimated_peak_bytes=64000,
        )

    def test_miss_on_empty(self):
        cache = AdaptivePlanCache()
        fp = self._make_fingerprint()
        assert cache.get(fp) is None

    def test_hit_after_put(self):
        cache = AdaptivePlanCache()
        fp = self._make_fingerprint()
        analysis = self._make_analysis()
        cache.put(fp, analysis)
        result = cache.get(fp)
        assert result is not None
        assert result.clause_cardinalities == [100, 100]

    def test_lru_eviction(self):
        """Cache evicts LRU entry when at capacity."""
        cache = AdaptivePlanCache(max_entries=2)
        fp1 = self._make_fingerprint("aaa")
        fp2 = self._make_fingerprint("bbb")
        fp3 = self._make_fingerprint("ccc")
        analysis = self._make_analysis()
        cache.put(fp1, analysis)
        cache.put(fp2, analysis)
        # Access fp1 to make fp2 the LRU
        cache.get(fp1)
        # Insert fp3 — should evict fp2
        cache.put(fp3, analysis)
        assert cache.get(fp1) is not None
        assert cache.get(fp2) is None
        assert cache.get(fp3) is not None

    def test_ttl_expiry(self):
        """Expired entries are not returned."""
        cache = AdaptivePlanCache(ttl_seconds=0.01)
        fp = self._make_fingerprint()
        analysis = self._make_analysis()
        cache.put(fp, analysis)
        time.sleep(0.02)
        assert cache.get(fp) is None

    def test_invalidate(self):
        """invalidate() clears all entries."""
        cache = AdaptivePlanCache()
        fp = self._make_fingerprint()
        cache.put(fp, self._make_analysis())
        cache.invalidate()
        assert cache.get(fp) is None

    def test_stats(self):
        cache = AdaptivePlanCache()
        fp = self._make_fingerprint()
        cache.put(fp, self._make_analysis())
        cache.get(fp)  # hit
        cache.get(self._make_fingerprint("miss"))  # miss
        stats = cache.stats
        assert stats["entries"] == 1
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.5


# ---------------------------------------------------------------------------
# QueryLearningStore tests
# ---------------------------------------------------------------------------


class TestQueryLearningStore:
    """Tests for the unified learning store facade."""

    def test_fingerprint_and_cache_roundtrip(self):
        """Fingerprint → cache → retrieve works end-to-end."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_planner import AnalysisResult

        store = QueryLearningStore()
        query = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fp = store.fingerprint(query)

        analysis = AnalysisResult(clause_cardinalities=[50])
        store.cache_plan(fp, analysis)

        cached = store.get_cached_plan(fp)
        assert cached is not None
        assert cached.clause_cardinalities == [50]

    def test_selectivity_recording_and_retrieval(self):
        """Record selectivity → retrieve learned value."""
        store = QueryLearningStore()
        for _ in range(5):
            store.record_selectivity(
                "Person", "age", ">", estimated=0.33, actual=0.12
            )
        result = store.get_learned_selectivity("Person", "age", ">")
        assert result is not None
        assert 0.08 <= result <= 0.18

    def test_join_performance_recording(self):
        """Record join performance → get best strategy."""
        store = QueryLearningStore()
        for _ in range(5):
            store.record_join_performance(
                strategy="hash",
                left_rows=5000,
                right_rows=3000,
                actual_output_rows=2500,
                elapsed_ms=30.0,
            )
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

    def test_invalidate_on_mutation(self):
        """Mutation invalidation clears plan cache."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_planner import AnalysisResult

        store = QueryLearningStore()
        query = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fp = store.fingerprint(query)
        store.cache_plan(fp, AnalysisResult(clause_cardinalities=[50]))

        store.invalidate_on_mutation()
        assert store.get_cached_plan(fp) is None

    def test_diagnostics(self):
        """diagnostics() returns summary without errors."""
        store = QueryLearningStore()
        diag = store.diagnostics()
        assert "plan_cache" in diag
        assert "selectivity_patterns" in diag
        assert "join_buckets_tracked" in diag

    def test_clear(self):
        """clear() resets all learning state."""
        store = QueryLearningStore()
        for _ in range(5):
            store.record_selectivity(
                "Person", "age", ">", estimated=0.33, actual=0.12
            )
        store.clear()
        assert store.get_learned_selectivity("Person", "age", ">") is None
        diag = store.diagnostics()
        assert diag["selectivity_patterns"] == 0

    def test_singleton(self):
        """get_learning_store returns the same instance."""
        s1 = get_learning_store()
        s2 = get_learning_store()
        assert s1 is s2


# ---------------------------------------------------------------------------
# Thread-safety tests
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Verify thread-safety of learning components."""

    def test_concurrent_selectivity_recording(self):
        """Multiple threads recording selectivity concurrently."""
        import threading

        tracker = PredicateSelectivityTracker()
        errors: list[Exception] = []

        def record_loop():
            try:
                for i in range(100):
                    tracker.record(
                        "Person",
                        "age",
                        ">",
                        estimated=0.33,
                        actual=0.1 + (i % 10) * 0.01,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_loop) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        result = tracker.get_learned_selectivity("Person", "age", ">")
        assert result is not None

    def test_concurrent_plan_cache_access(self):
        """Multiple threads reading/writing plan cache concurrently."""
        import threading

        from pycypher.query_learning import QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        cache = AdaptivePlanCache(max_entries=50)
        errors: list[Exception] = []

        def cache_loop(thread_id: int):
            try:
                for i in range(50):
                    fp = QueryFingerprint(
                        digest=f"t{thread_id}_q{i}",
                        clause_signature="Match -> Return",
                        entity_types=("Person",),
                        relationship_types=(),
                    )
                    analysis = AnalysisResult(
                        clause_cardinalities=[i],
                    )
                    cache.put(fp, analysis)
                    cache.get(fp)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=cache_loop, args=(tid,))
            for tid in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
