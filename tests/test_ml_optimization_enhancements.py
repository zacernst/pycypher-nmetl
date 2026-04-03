"""TDD tests for ML Optimization System Enhancements (Epic 3.1).

Tests written FIRST per TDD methodology. Implementation follows.

Covers:
1. Complex predicate selectivity (compound WHERE, range, multi-property)
2. Query plan versioning (plan evolution tracking, effectiveness metrics)
3. ML-driven cache eviction (frequency-recency adaptive replacement)
4. Semantic caching (similar query result sharing)
5. Distributed learning framework (multi-instance knowledge sharing)
6. Performance validation (selectivity accuracy, cache hit rates)
"""

import threading
import time

import pytest

from pycypher.query_learning import (
    AdaptivePlanCache,
    PredicateSelectivityTracker,
    QueryLearningStore,
)


# ---------------------------------------------------------------------------
# 1. Complex Predicate Selectivity Recognition
# ---------------------------------------------------------------------------


class TestCompoundPredicateSelectivity:
    """Tests for compound predicate selectivity estimation.

    The system should handle AND/OR/NOT combinations and estimate compound
    selectivities using independence assumptions and learned corrections.
    """

    def test_and_compound_selectivity_independence(self):
        """AND compound: selectivity ≈ s1 * s2 under independence."""
        tracker = PredicateSelectivityTracker()
        # Learn selectivities for individual predicates
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)
        for _ in range(5):
            tracker.record("Person", "salary", ">", estimated=0.4, actual=0.2)

        s_age = tracker.get_learned_selectivity("Person", "age", ">")
        s_salary = tracker.get_learned_selectivity("Person", "salary", ">")
        assert s_age is not None
        assert s_salary is not None

        compound = tracker.estimate_compound_selectivity(
            entity_type="Person",
            predicates=[("age", ">"), ("salary", ">")],
            combinator="AND",
        )
        assert compound is not None
        # Under independence: s_age * s_salary
        expected = s_age * s_salary
        assert abs(compound - expected) < 0.05

    def test_or_compound_selectivity_independence(self):
        """OR compound: selectivity ≈ s1 + s2 - s1*s2 under independence."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)
        for _ in range(5):
            tracker.record("Person", "city", "=", estimated=0.2, actual=0.1)

        s_age = tracker.get_learned_selectivity("Person", "age", ">")
        s_city = tracker.get_learned_selectivity("Person", "city", "=")
        assert s_age is not None
        assert s_city is not None

        compound = tracker.estimate_compound_selectivity(
            entity_type="Person",
            predicates=[("age", ">"), ("city", "=")],
            combinator="OR",
        )
        assert compound is not None
        expected = s_age + s_city - s_age * s_city
        assert abs(compound - expected) < 0.05

    def test_compound_returns_none_insufficient_data(self):
        """Returns None when any constituent predicate lacks data."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)
        # No data for salary

        result = tracker.estimate_compound_selectivity(
            entity_type="Person",
            predicates=[("age", ">"), ("salary", ">")],
            combinator="AND",
        )
        assert result is None

    def test_compound_single_predicate_returns_direct(self):
        """Single predicate compound returns the direct learned selectivity."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)

        compound = tracker.estimate_compound_selectivity(
            entity_type="Person",
            predicates=[("age", ">")],
            combinator="AND",
        )
        direct = tracker.get_learned_selectivity("Person", "age", ">")
        assert compound is not None
        assert direct is not None
        assert abs(compound - direct) < 0.001


class TestRangePredicateSelectivity:
    """Tests for range predicate selectivity (BETWEEN-like patterns)."""

    def test_range_selectivity_estimation(self):
        """Range predicates (a > X AND a < Y) should be tracked together."""
        tracker = PredicateSelectivityTracker()
        # Record observations for range pattern
        for _ in range(5):
            tracker.record("Person", "age", "RANGE", estimated=0.3, actual=0.15)

        result = tracker.get_learned_selectivity("Person", "age", "RANGE")
        assert result is not None
        assert 0.10 <= result <= 0.20

    def test_range_vs_point_different_tracking(self):
        """Range and point predicates are tracked separately."""
        tracker = PredicateSelectivityTracker()
        for _ in range(5):
            tracker.record("Person", "age", "RANGE", estimated=0.3, actual=0.15)
        for _ in range(5):
            tracker.record("Person", "age", "=", estimated=0.01, actual=0.005)

        range_sel = tracker.get_learned_selectivity("Person", "age", "RANGE")
        point_sel = tracker.get_learned_selectivity("Person", "age", "=")
        assert range_sel is not None
        assert point_sel is not None
        assert range_sel > point_sel  # Range typically selects more rows


class TestMultiPropertySelectivity:
    """Tests for selectivity across multiple properties."""

    def test_correlation_tracking(self):
        """Track observed correlations between properties for better estimates."""
        tracker = PredicateSelectivityTracker()
        # First learn individual selectivities (required for compound computation)
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)
        for _ in range(5):
            tracker.record("Person", "salary", ">", estimated=0.4, actual=0.2)

        # Record compound observations
        for _ in range(5):
            tracker.record_compound(
                entity_type="Person",
                predicates=[("age", ">"), ("salary", ">")],
                estimated_compound=0.20,
                actual_compound=0.08,
            )

        # Should learn that age and salary are correlated
        corr = tracker.get_correlation_factor("Person", ("age", ">"), ("salary", ">"))
        assert corr is not None
        # Correlation factor < 1.0 means positive correlation (selecting both is
        # less than independence would suggest)
        assert 0.0 < corr < 2.0

    def test_correlation_factor_no_data(self):
        """Returns None with no compound observations."""
        tracker = PredicateSelectivityTracker()
        corr = tracker.get_correlation_factor("Person", ("age", ">"), ("salary", ">"))
        assert corr is None

    def test_correlated_compound_selectivity(self):
        """Compound selectivity uses correlation factor when available."""
        tracker = PredicateSelectivityTracker()
        # Learn individual selectivities
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)
        for _ in range(5):
            tracker.record("Person", "salary", ">", estimated=0.4, actual=0.2)

        # Learn compound observation (correlated: actual compound much lower)
        for _ in range(5):
            tracker.record_compound(
                entity_type="Person",
                predicates=[("age", ">"), ("salary", ">")],
                estimated_compound=0.20,
                actual_compound=0.03,
            )

        compound = tracker.estimate_compound_selectivity(
            entity_type="Person",
            predicates=[("age", ">"), ("salary", ">")],
            combinator="AND",
        )
        assert compound is not None
        # With correlation, should be closer to 0.03 than independence (0.06)
        assert compound < 0.05


# ---------------------------------------------------------------------------
# 2. Query Plan Versioning
# ---------------------------------------------------------------------------


class TestQueryPlanVersioning:
    """Tests for query plan version tracking and effectiveness metrics."""

    def test_plan_version_increments(self):
        """Each new plan for same fingerprint increments version."""
        from pycypher.query_learning import PlanVersionTracker, QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        tracker = PlanVersionTracker()
        fp = QueryFingerprint(
            digest="test123",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )

        plan1 = AnalysisResult(clause_cardinalities=[100])
        plan2 = AnalysisResult(clause_cardinalities=[200])

        v1 = tracker.record_plan(fp, plan1)
        v2 = tracker.record_plan(fp, plan2)
        assert v1 == 1
        assert v2 == 2

    def test_plan_effectiveness_tracking(self):
        """Track execution metrics per plan version."""
        from pycypher.query_learning import PlanVersionTracker, QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        tracker = PlanVersionTracker()
        fp = QueryFingerprint(
            digest="test123",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )

        plan = AnalysisResult(clause_cardinalities=[100])
        version = tracker.record_plan(fp, plan)

        # Record execution metrics
        tracker.record_execution(fp, version, elapsed_ms=25.0, rows_produced=100)
        tracker.record_execution(fp, version, elapsed_ms=30.0, rows_produced=120)

        metrics = tracker.get_version_metrics(fp, version)
        assert metrics is not None
        assert metrics["avg_elapsed_ms"] == pytest.approx(27.5)
        assert metrics["execution_count"] == 2
        assert metrics["avg_rows"] == pytest.approx(110.0)

    def test_plan_version_comparison(self):
        """Compare effectiveness across plan versions."""
        from pycypher.query_learning import PlanVersionTracker, QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        tracker = PlanVersionTracker()
        fp = QueryFingerprint(
            digest="test123",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )

        # Version 1: slow plan
        plan1 = AnalysisResult(clause_cardinalities=[100])
        v1 = tracker.record_plan(fp, plan1)
        for _ in range(3):
            tracker.record_execution(fp, v1, elapsed_ms=50.0, rows_produced=100)

        # Version 2: fast plan
        plan2 = AnalysisResult(clause_cardinalities=[50])
        v2 = tracker.record_plan(fp, plan2)
        for _ in range(3):
            tracker.record_execution(fp, v2, elapsed_ms=15.0, rows_produced=100)

        best = tracker.best_version(fp)
        assert best == v2  # Version 2 is faster

    def test_plan_history(self):
        """Get full version history for a fingerprint."""
        from pycypher.query_learning import PlanVersionTracker, QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        tracker = PlanVersionTracker()
        fp = QueryFingerprint(
            digest="test123",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )

        tracker.record_plan(fp, AnalysisResult(clause_cardinalities=[100]))
        tracker.record_plan(fp, AnalysisResult(clause_cardinalities=[200]))

        history = tracker.get_history(fp)
        assert len(history) == 2
        assert history[0]["version"] == 1
        assert history[1]["version"] == 2

    def test_no_history_returns_empty(self):
        """Unknown fingerprint returns empty history."""
        from pycypher.query_learning import PlanVersionTracker, QueryFingerprint

        tracker = PlanVersionTracker()
        fp = QueryFingerprint(
            digest="unknown",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )
        assert tracker.get_history(fp) == []
        assert tracker.best_version(fp) is None

    def test_plan_regression_detection(self):
        """Detect when a new plan version regresses performance."""
        from pycypher.query_learning import PlanVersionTracker, QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        tracker = PlanVersionTracker()
        fp = QueryFingerprint(
            digest="test123",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )

        # Version 1: fast
        v1 = tracker.record_plan(fp, AnalysisResult(clause_cardinalities=[50]))
        for _ in range(5):
            tracker.record_execution(fp, v1, elapsed_ms=10.0, rows_produced=100)

        # Version 2: slow (regression)
        v2 = tracker.record_plan(fp, AnalysisResult(clause_cardinalities=[100]))
        for _ in range(5):
            tracker.record_execution(fp, v2, elapsed_ms=50.0, rows_produced=100)

        regression = tracker.detect_regression(fp, v2)
        assert regression is True  # v2 is worse than v1


# ---------------------------------------------------------------------------
# 3. ML-Driven Cache Eviction
# ---------------------------------------------------------------------------


class TestMLDrivenCacheEviction:
    """Tests for frequency-recency adaptive cache eviction."""

    def test_frequency_score_increases_with_access(self):
        """More frequently accessed entries have higher eviction resistance."""
        from pycypher.query_learning import AdaptiveEvictionPolicy, QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        policy = AdaptiveEvictionPolicy()

        fp_frequent = QueryFingerprint(
            digest="frequent",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )
        fp_rare = QueryFingerprint(
            digest="rare",
            clause_signature="Match -> Return",
            entity_types=("Company",),
            relationship_types=(),
        )

        # Access frequent one many times
        for _ in range(20):
            policy.record_access(fp_frequent.digest)
        # Access rare one once
        policy.record_access(fp_rare.digest)

        score_frequent = policy.eviction_score(fp_frequent.digest)
        score_rare = policy.eviction_score(fp_rare.digest)
        # Lower score = higher priority for eviction
        assert score_frequent > score_rare

    def test_recency_affects_eviction_score(self):
        """Recently accessed entries have higher eviction resistance."""
        from pycypher.query_learning import AdaptiveEvictionPolicy

        policy = AdaptiveEvictionPolicy()

        # Access old entry then new entry
        policy.record_access("old_entry")
        time.sleep(0.05)
        policy.record_access("new_entry")

        score_old = policy.eviction_score("old_entry")
        score_new = policy.eviction_score("new_entry")
        # More recent = higher score = less likely to evict
        assert score_new > score_old

    def test_eviction_candidate_selection(self):
        """select_eviction_candidate returns the lowest-scoring entry."""
        from pycypher.query_learning import AdaptiveEvictionPolicy

        policy = AdaptiveEvictionPolicy()

        # Create entries with varying access patterns
        for _ in range(10):
            policy.record_access("hot")
        for _ in range(3):
            policy.record_access("warm")
        policy.record_access("cold")

        candidate = policy.select_eviction_candidate(["hot", "warm", "cold"])
        assert candidate == "cold"

    def test_adaptive_cache_uses_ml_eviction(self):
        """AdaptivePlanCache with ML eviction uses frequency-recency scoring."""
        from pycypher.query_learning import QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        cache = AdaptivePlanCache(max_entries=2, eviction_policy="adaptive")

        # Create entries
        fp1 = QueryFingerprint(
            digest="hot",
            clause_signature="Match -> Return",
            entity_types=("Person",),
            relationship_types=(),
        )
        fp2 = QueryFingerprint(
            digest="cold",
            clause_signature="Match -> Return",
            entity_types=("Company",),
            relationship_types=(),
        )
        fp3 = QueryFingerprint(
            digest="new",
            clause_signature="Match -> Return",
            entity_types=("Product",),
            relationship_types=(),
        )

        analysis = AnalysisResult(clause_cardinalities=[100])

        cache.put(fp1, analysis)
        cache.put(fp2, analysis)

        # Access fp1 many times to make it "hot"
        for _ in range(10):
            cache.get(fp1)
        # fp2 accessed only once (cold)
        cache.get(fp2)

        # Insert fp3 — should evict fp2 (cold) not fp1 (hot)
        cache.put(fp3, analysis)
        assert cache.get(fp1) is not None  # hot entry preserved
        assert cache.get(fp3) is not None  # new entry present
        assert cache.get(fp2) is None  # cold entry evicted

    def test_eviction_policy_clear(self):
        """Clearing eviction policy resets all scores."""
        from pycypher.query_learning import AdaptiveEvictionPolicy

        policy = AdaptiveEvictionPolicy()
        policy.record_access("entry1")
        policy.clear()
        # After clear, score should be 0
        assert policy.eviction_score("entry1") == 0.0


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestMLOptimizationIntegration:
    """Integration tests for ML optimization enhancements."""

    def test_learning_store_compound_selectivity_roundtrip(self):
        """QueryLearningStore supports compound selectivity operations."""
        store = QueryLearningStore()
        # Learn individual predicates
        for _ in range(5):
            store.record_selectivity("Person", "age", ">", estimated=0.5, actual=0.3)
        for _ in range(5):
            store.record_selectivity(
                "Person", "salary", ">", estimated=0.4, actual=0.2
            )

        compound = store.estimate_compound_selectivity(
            entity_type="Person",
            predicates=[("age", ">"), ("salary", ">")],
            combinator="AND",
        )
        assert compound is not None

    def test_learning_store_plan_versioning(self):
        """QueryLearningStore integrates plan versioning."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_planner import AnalysisResult

        store = QueryLearningStore()
        query = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
        fp = store.fingerprint(query)

        plan = AnalysisResult(clause_cardinalities=[100])
        version = store.record_plan_version(fp, plan)
        assert version == 1

        store.record_plan_execution(fp, version, elapsed_ms=20.0, rows_produced=100)
        metrics = store.get_plan_metrics(fp, version)
        assert metrics is not None
        assert metrics["execution_count"] == 1

    def test_diagnostics_includes_enhancements(self):
        """Diagnostics includes new ML enhancement metrics."""
        store = QueryLearningStore()
        diag = store.diagnostics()
        assert "plan_versions" in diag
        assert "correlation_pairs" in diag

    def test_thread_safe_compound_recording(self):
        """Compound selectivity recording is thread-safe."""
        tracker = PredicateSelectivityTracker()
        # Pre-populate individual selectivities so compound can compute correlations
        for _ in range(5):
            tracker.record("Person", "age", ">", estimated=0.5, actual=0.3)
        for _ in range(5):
            tracker.record("Person", "salary", ">", estimated=0.4, actual=0.2)

        errors: list[Exception] = []

        def record_loop():
            try:
                for i in range(50):
                    tracker.record_compound(
                        entity_type="Person",
                        predicates=[("age", ">"), ("salary", ">")],
                        estimated_compound=0.20,
                        actual_compound=0.05 + (i % 10) * 0.01,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_loop) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        corr = tracker.get_correlation_factor("Person", ("age", ">"), ("salary", ">"))
        assert corr is not None


# ---------------------------------------------------------------------------
# 4. Semantic Caching — similar query result sharing
# ---------------------------------------------------------------------------


class TestSemanticCache:
    """Tests for semantic caching that shares results between similar queries.

    Queries with the same structural fingerprint but different literal values
    can share cached results when the result set is parameter-independent
    (e.g., aggregations, full scans).
    """

    def test_semantic_cache_creation(self):
        """SemanticResultCache initializes correctly."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=100)
        assert cache.stats["entries"] == 0
        assert cache.stats["semantic_hits"] == 0

    def test_exact_match_hit(self):
        """Exact query+params match returns cached result."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=100)
        cache.put(
            query="MATCH (p:Person) RETURN count(p)",
            parameters=None,
            fingerprint_digest="fp1",
            result={"count": 42},
            is_parameter_independent=True,
        )

        hit = cache.get(
            query="MATCH (p:Person) RETURN count(p)",
            parameters=None,
            fingerprint_digest="fp1",
        )
        assert hit is not None
        assert hit["count"] == 42

    def test_semantic_hit_parameter_independent(self):
        """Parameter-independent queries share results across literal variations."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=100)
        # Cache a result for a parameter-independent query
        cache.put(
            query="MATCH (p:Person) RETURN count(p)",
            parameters=None,
            fingerprint_digest="fp_count",
            result={"count": 42},
            is_parameter_independent=True,
        )

        # Different query text, same fingerprint → semantic hit
        hit = cache.get(
            query="MATCH (p:Person) RETURN count(p) AS total",
            parameters=None,
            fingerprint_digest="fp_count",
        )
        assert hit is not None
        assert hit["count"] == 42

    def test_no_semantic_hit_parameter_dependent(self):
        """Parameter-dependent queries do NOT share results across fingerprints."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=100)
        cache.put(
            query="MATCH (p:Person) WHERE p.age > 30 RETURN p",
            parameters=None,
            fingerprint_digest="fp_filter",
            result={"rows": 10},
            is_parameter_independent=False,
        )

        # Same fingerprint but different query text → no semantic sharing
        hit = cache.get(
            query="MATCH (p:Person) WHERE p.age > 50 RETURN p",
            parameters=None,
            fingerprint_digest="fp_filter",
        )
        assert hit is None  # Not parameter-independent, exact match required

    def test_semantic_cache_invalidation(self):
        """Invalidation clears all entries."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=100)
        cache.put(
            query="MATCH (p:Person) RETURN count(p)",
            parameters=None,
            fingerprint_digest="fp1",
            result={"count": 42},
            is_parameter_independent=True,
        )
        cache.invalidate()
        hit = cache.get(
            query="MATCH (p:Person) RETURN count(p)",
            parameters=None,
            fingerprint_digest="fp1",
        )
        assert hit is None

    def test_semantic_cache_lru_eviction(self):
        """Evicts LRU entry when at capacity."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=2)
        cache.put("q1", None, "fp1", {"r": 1}, is_parameter_independent=True)
        cache.put("q2", None, "fp2", {"r": 2}, is_parameter_independent=True)

        # Access q1 to make q2 the LRU
        cache.get("q1", None, "fp1")

        # Insert q3 → evict q2
        cache.put("q3", None, "fp3", {"r": 3}, is_parameter_independent=True)

        assert cache.get("q1", None, "fp1") is not None
        assert cache.get("q2", None, "fp2") is None
        assert cache.get("q3", None, "fp3") is not None

    def test_semantic_cache_stats(self):
        """Stats track exact hits, semantic hits, and misses."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=100)
        cache.put("q1", None, "fp1", {"r": 1}, is_parameter_independent=True)

        cache.get("q1", None, "fp1")  # exact hit
        cache.get("q1_variant", None, "fp1")  # semantic hit
        cache.get("q2", None, "fp2")  # miss

        stats = cache.stats
        assert stats["exact_hits"] == 1
        assert stats["semantic_hits"] == 1
        assert stats["misses"] == 1


# ---------------------------------------------------------------------------
# 5. Distributed Learning Framework
# ---------------------------------------------------------------------------


class TestDistributedLearning:
    """Tests for multi-instance query knowledge sharing.

    The distributed learning framework serializes learning state so it can
    be shared across multiple pycypher instances (e.g., in a cluster).
    """

    def test_export_learning_state(self):
        """Export learning state to a serializable dict."""
        from pycypher.query_learning import DistributedLearningSync

        store = QueryLearningStore()
        for _ in range(5):
            store.record_selectivity("Person", "age", ">", estimated=0.5, actual=0.3)

        sync = DistributedLearningSync(store)
        state = sync.export_state()

        assert "selectivity" in state
        assert "join_performance" in state
        assert "version" in state
        assert len(state["selectivity"]) > 0

    def test_import_learning_state(self):
        """Import learning state from another instance."""
        from pycypher.query_learning import DistributedLearningSync

        # Instance 1: learn selectivity
        store1 = QueryLearningStore()
        for _ in range(5):
            store1.record_selectivity("Person", "age", ">", estimated=0.5, actual=0.3)

        sync1 = DistributedLearningSync(store1)
        exported = sync1.export_state()

        # Instance 2: import from instance 1
        store2 = QueryLearningStore()
        sync2 = DistributedLearningSync(store2)
        sync2.import_state(exported)

        # Instance 2 should now have learned selectivity
        result = store2.get_learned_selectivity("Person", "age", ">")
        assert result is not None
        assert 0.2 <= result <= 0.4

    def test_merge_learning_states(self):
        """Merge learning from two instances preserving both contributions."""
        from pycypher.query_learning import DistributedLearningSync

        # Instance 1: learn Person.age
        store1 = QueryLearningStore()
        for _ in range(5):
            store1.record_selectivity("Person", "age", ">", estimated=0.5, actual=0.3)

        # Instance 2: learn Company.revenue
        store2 = QueryLearningStore()
        for _ in range(5):
            store2.record_selectivity(
                "Company", "revenue", ">", estimated=0.4, actual=0.1
            )

        sync1 = DistributedLearningSync(store1)
        sync2 = DistributedLearningSync(store2)

        # Merge instance 2 into instance 1
        state2 = sync2.export_state()
        sync1.import_state(state2)

        # Instance 1 should now know both
        assert store1.get_learned_selectivity("Person", "age", ">") is not None
        assert store1.get_learned_selectivity("Company", "revenue", ">") is not None

    def test_export_join_performance(self):
        """Join performance data is included in exported state."""
        from pycypher.query_learning import DistributedLearningSync

        store = QueryLearningStore()
        for _ in range(5):
            store.record_join_performance(
                strategy="hash",
                left_rows=1000,
                right_rows=500,
                actual_output_rows=450,
                elapsed_ms=15.0,
            )

        sync = DistributedLearningSync(store)
        state = sync.export_state()
        assert len(state["join_performance"]) > 0

    def test_state_versioning(self):
        """Exported state includes version for conflict resolution."""
        from pycypher.query_learning import DistributedLearningSync

        store = QueryLearningStore()
        sync = DistributedLearningSync(store)

        state1 = sync.export_state()
        # Record some data
        for _ in range(5):
            store.record_selectivity("Person", "age", ">", estimated=0.5, actual=0.3)
        state2 = sync.export_state()

        # Versions should increment
        assert state2["version"] > state1["version"]

    def test_import_ignores_stale_state(self):
        """Import ignores state older than current version."""
        from pycypher.query_learning import DistributedLearningSync

        store = QueryLearningStore()
        sync = DistributedLearningSync(store)

        # Export empty state (version 0)
        old_state = sync.export_state()

        # Add data and advance version
        for _ in range(5):
            store.record_selectivity("Person", "age", ">", estimated=0.5, actual=0.3)
        sync.export_state()  # advances version

        # Try to import the old empty state
        result = sync.import_state(old_state)
        assert result is False  # Stale state rejected

        # Original data should remain
        assert store.get_learned_selectivity("Person", "age", ">") is not None


# ---------------------------------------------------------------------------
# 6. Performance Validation Tests
# ---------------------------------------------------------------------------


class TestSelectivityAccuracyImprovement:
    """Validate that enhanced selectivity achieves >20% accuracy improvement."""

    def test_compound_selectivity_more_accurate_than_independence(self):
        """Compound selectivity with correlations is more accurate than naive."""
        tracker = PredicateSelectivityTracker()

        # Learn individual selectivities
        for _ in range(10):
            tracker.record("Employee", "age", ">", estimated=0.5, actual=0.4)
        for _ in range(10):
            tracker.record("Employee", "dept", "=", estimated=0.1, actual=0.08)

        # Learn actual compound (correlated: dept affects age distribution)
        actual_compound = 0.015  # Much lower than independence (0.4 * 0.08 = 0.032)
        for _ in range(10):
            tracker.record_compound(
                entity_type="Employee",
                predicates=[("age", ">"), ("dept", "=")],
                estimated_compound=0.032,
                actual_compound=actual_compound,
            )

        # Naive independence estimate
        s_age = tracker.get_learned_selectivity("Employee", "age", ">")
        s_dept = tracker.get_learned_selectivity("Employee", "dept", "=")
        naive = s_age * s_dept

        # Correlation-corrected estimate
        corrected = tracker.estimate_compound_selectivity(
            entity_type="Employee",
            predicates=[("age", ">"), ("dept", "=")],
            combinator="AND",
        )

        assert corrected is not None
        naive_error = abs(naive - actual_compound) / actual_compound
        corrected_error = abs(corrected - actual_compound) / actual_compound

        # Corrected should be at least 20% better
        assert corrected_error < naive_error * 0.8

    def test_ema_convergence_accuracy(self):
        """EMA converges to actual selectivity within 20% after 10 observations."""
        tracker = PredicateSelectivityTracker()
        actual = 0.15

        for _ in range(10):
            tracker.record("Node", "prop", "=", estimated=0.5, actual=actual)

        learned = tracker.get_learned_selectivity("Node", "prop", "=")
        assert learned is not None
        error = abs(learned - actual) / actual
        assert error < 0.20  # Within 20% of actual


class TestCacheHitRateTarget:
    """Validate that advanced caching achieves >60% hit rates."""

    def test_plan_cache_hit_rate_with_repeated_queries(self):
        """Plan cache achieves >60% hit rate with typical query mix."""
        from pycypher.query_learning import QueryFingerprint
        from pycypher.query_planner import AnalysisResult

        cache = AdaptivePlanCache(max_entries=50)

        # Simulate 10 distinct query patterns, each repeated 8 times
        fingerprints = [
            QueryFingerprint(
                digest=f"pattern_{i}",
                clause_signature="Match -> Return",
                entity_types=(f"Type{i}",),
                relationship_types=(),
            )
            for i in range(10)
        ]

        analysis = AnalysisResult(clause_cardinalities=[100])

        total_accesses = 0
        # First access: miss + cache
        for fp in fingerprints:
            cache.get(fp)
            cache.put(fp, analysis)
            total_accesses += 1

        # Repeated accesses: should be hits
        for _ in range(7):
            for fp in fingerprints:
                cache.get(fp)
                total_accesses += 1

        stats = cache.stats
        assert stats["hit_rate"] > 0.60

    def test_semantic_cache_hit_rate(self):
        """Semantic cache achieves high hit rate for similar queries."""
        from pycypher.query_learning import SemanticResultCache

        cache = SemanticResultCache(max_entries=50)

        # Cache 5 parameter-independent results
        for i in range(5):
            cache.put(
                f"MATCH (p:Type{i}) RETURN count(p)",
                None,
                f"fp_{i}",
                {"count": i * 10},
                is_parameter_independent=True,
            )

        # Each pattern gets 10 semantic hits from "variants"
        hits = 0
        misses = 0
        for _ in range(10):
            for i in range(5):
                result = cache.get(
                    f"MATCH (p:Type{i}) RETURN count(p) AS variant",
                    None,
                    f"fp_{i}",
                )
                if result is not None:
                    hits += 1
                else:
                    misses += 1

        hit_rate = hits / (hits + misses)
        assert hit_rate > 0.60
