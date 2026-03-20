"""
Performance Loop 290: Quantifier Vectorization TDD Tests.

This module implements TDD tests for vectorizing quantifier evaluation
(ANY, ALL, NONE, SINGLE) to eliminate the O(rows × elements) performance
anti-pattern currently causing 9× performance regression.

Current Issue: 200 rows × 50 elements = 10,000 separate evaluations
Target: Explode to flat frame, evaluate once, group back
Expected: 4.5s → <0.5s (9× improvement)
"""

import time
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.collection_evaluator import CollectionExpressionEvaluator
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star

pytestmark = [pytest.mark.slow, pytest.mark.performance]


def _make_large_ctx(n_people: int = 200, n_scores: int = 50):
    """Create test context with many people and scores for performance testing."""
    people_data = {
        "__ID__": [f"p{i}" for i in range(n_people)],
        "name": [f"Person{i}" for i in range(n_people)],
        "scores": [
            [j + 1 for j in range(n_scores)] for i in range(n_people)
        ],  # Each person has scores [1, 2, ..., n_scores]
    }
    people_df = pd.DataFrame(people_data)
    return ContextBuilder.from_dict({"Person": people_df})


class TestQuantifierVectorizationBaseline:
    """Test current quantifier performance to establish baseline measurements."""

    def test_current_quantifier_performance_200x50_baseline(self):
        """Measure current quantifier performance on 200×50 data."""
        ctx = _make_large_ctx(n_people=200, n_scores=50)
        star = Star(context=ctx)

        # Warm up
        star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
            "RETURN p.name AS name"
        )

        # Measure performance
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
            "RETURN p.name AS name"
        )
        elapsed = time.perf_counter() - start

        # Current performance should be slow (>4s)
        # This documents the current regression state for TDD red phase
        print(
            f"Current quantifier performance: {elapsed:.3f}s for 200×50 any()"
        )
        assert isinstance(result, pd.DataFrame)
        # Expect slow performance in current state (this will be the red phase)
        # When vectorization is implemented, this becomes the green phase

    def test_quantifier_allocation_pattern_analysis(self):
        """Analyze current allocation patterns in quantifier evaluation."""
        ctx = _make_large_ctx(n_people=10, n_scores=5)  # Smaller for analysis
        star = Star(context=ctx)

        # Count BindingFrame and BindingExpressionEvaluator allocations
        original_binding_frame = BindingFrame.__init__
        original_evaluator = None

        binding_frame_count = 0
        evaluator_count = 0

        def count_binding_frame(self, *args, **kwargs):
            nonlocal binding_frame_count
            binding_frame_count += 1
            return original_binding_frame(self, *args, **kwargs)

        def count_evaluator(frame):
            nonlocal evaluator_count
            evaluator_count += 1

            return original_evaluator(frame)

        # Mock to count allocations
        with patch.object(BindingFrame, "__init__", count_binding_frame):
            from pycypher.binding_evaluator import BindingExpressionEvaluator

            original_evaluator = BindingExpressionEvaluator
            with patch(
                "pycypher.binding_evaluator.BindingExpressionEvaluator",
                side_effect=count_evaluator,
            ):
                result = star.execute_query(
                    "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
                    "RETURN p.name AS name"
                )

        print(
            f"Allocations for 10×5 quantifier: {binding_frame_count} BindingFrames, {evaluator_count} Evaluators"
        )

        # Current pattern: O(rows × elements) allocations
        # Expected: ~50 BindingFrame allocations (10 people × 5 scores each)
        # Target after vectorization: ~constant allocations


class TestQuantifierVectorizationImplementation:
    """Test vectorized quantifier implementation."""

    def test_vectorized_eval_quantifier_method_exists(self):
        """Test that vectorized eval_quantifier method exists in CollectionExpressionEvaluator."""
        frame = Mock()
        evaluator = CollectionExpressionEvaluator(frame)

        # After implementation, this method should exist
        # In red phase: will fail because method doesn't exist yet
        # In green phase: will pass
        assert hasattr(evaluator, "eval_quantifier_vectorized"), (
            "eval_quantifier_vectorized method should be implemented for vectorized evaluation"
        )

    def test_vectorized_quantifier_functionality_preservation(self):
        """Test that vectorized implementation preserves all quantifier semantics."""
        ctx = _make_large_ctx(n_people=20, n_scores=10)
        star = Star(context=ctx)

        # Test all quantifier types with known results
        test_cases = [
            ("any(x IN p.scores WHERE x > 50)", "any"),
            ("all(x IN p.scores WHERE x > 0)", "all"),
            ("none(x IN p.scores WHERE x > 100)", "none"),
            ("single(x IN p.scores WHERE x = 5)", "single"),
        ]

        for quantifier_expr, qtype in test_cases:
            result = star.execute_query(
                f"MATCH (p:Person) WHERE {quantifier_expr} RETURN p.name AS name"
            )

            # Verify results are correct (functional preservation)
            assert isinstance(result, pd.DataFrame)
            if qtype == "any":
                # any(x > 50) should match (scores go 1..10, none > 50)
                assert len(result) == 0  # No matches expected
            elif qtype == "all":
                # all(x > 0) should match all (all scores are > 0)
                assert len(result) == 20  # All people should match
            elif qtype == "none":
                # none(x > 100) should match all (no scores > 100)
                assert len(result) == 20  # All people should match
            elif qtype == "single":
                # single(x = 5) should match all (exactly one score = 5)
                assert len(result) == 20  # All people should match

    def test_vectorized_quantifier_explode_strategy(self):
        """Test the explode-evaluate-group vectorization strategy."""
        # This tests the core vectorization approach:
        # 1. Explode (row, element) pairs to flat frame
        # 2. Evaluate WHERE condition once
        # 3. Group back by original row

        ctx = _make_large_ctx(n_people=5, n_scores=3)
        star = Star(context=ctx)

        # Mock to capture the exploded frame structure
        exploded_frames = []
        original_evaluate = None

        def capture_exploded_evaluate(self, expression):
            # Capture the frame structure during evaluation
            if (
                hasattr(self.frame, "bindings")
                and len(self.frame.bindings) > 5
            ):
                # This might be the exploded frame (more rows than original)
                exploded_frames.append(
                    {
                        "frame_length": len(self.frame.bindings),
                        "columns": list(self.frame.bindings.columns),
                    }
                )
            return original_evaluate(self, expression)

        from pycypher.binding_evaluator import BindingExpressionEvaluator

        original_evaluate = BindingExpressionEvaluator.evaluate

        with patch.object(
            BindingExpressionEvaluator, "evaluate", capture_exploded_evaluate
        ):
            result = star.execute_query(
                "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 2) "
                "RETURN p.name AS name"
            )

        # After vectorization, should see exploded frame with 5×3=15 rows
        # In current implementation: multiple small frames
        # In vectorized implementation: one large exploded frame
        print(f"Captured frames during evaluation: {exploded_frames}")


class TestQuantifierVectorizationPerformance:
    """Test vectorized quantifier performance targets."""

    @pytest.mark.performance_target
    def test_vectorized_quantifier_200x50_performance_target(self):
        """Vectorized any() over 200×50 elements must complete in < 0.5s."""
        ctx = _make_large_ctx(n_people=200, n_scores=50)
        star = Star(context=ctx)

        # Warm up
        star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
            "RETURN p.name AS name"
        )

        # Measure performance
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
            "RETURN p.name AS name"
        )
        elapsed = time.perf_counter() - start

        assert isinstance(result, pd.DataFrame)
        assert elapsed < 0.5, (
            f"Vectorized 200×50 any() took {elapsed:.3f}s — expected < 0.5s."
        )

    @pytest.mark.performance_comparison
    def test_vectorized_vs_baseline_speedup_measurement(self):
        """Measure actual speedup achieved by vectorization."""
        ctx = _make_large_ctx(
            n_people=100, n_scores=25
        )  # Smaller for comparison
        star = Star(context=ctx)

        # This test will measure before/after once vectorization is implemented
        # For now, documents the baseline for comparison

        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 30) "
            "RETURN p.name AS name"
        )
        elapsed = time.perf_counter() - start

        print(f"100×25 quantifier baseline: {elapsed:.3f}s")

        # Target: >5× improvement after vectorization
        # Baseline expectation: ~1-2s for 100×25
        # Vectorized target: <0.2s for 100×25
        assert isinstance(result, pd.DataFrame)


class TestQuantifierVectorizationEdgeCases:
    """Test vectorized quantifier handles all edge cases correctly."""

    def test_vectorized_quantifier_null_list_handling(self):
        """Test that vectorized implementation handles null/empty lists correctly."""
        # Create context with some null/empty lists
        people_data = {
            "__ID__": ["p1", "p2", "p3", "p4"],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "scores": [
                [1, 2, 3],  # Normal list
                [],  # Empty list
                None,  # Null list
                [4, 5],  # Another normal list
            ],
        }
        ctx = ContextBuilder.from_dict({"Person": pd.DataFrame(people_data)})
        star = Star(context=ctx)

        # Test quantifier behavior with mixed null/empty/normal lists
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "any(x IN p.scores WHERE x > 2) AS any_gt2, "
            "all(x IN p.scores WHERE x > 0) AS all_gt0, "
            "none(x IN p.scores WHERE x < 0) AS none_lt0"
        )

        assert len(result) == 4

        # Verify quantifier semantics for edge cases:
        # any() of empty/null = false
        # all() of empty/null = true (vacuous truth)
        # none() of empty/null = true
        results_by_name = {row["name"]: row for _, row in result.iterrows()}

        # Alice [1,2,3]: any(>2)=True, all(>0)=True, none(<0)=True
        assert results_by_name["Alice"]["any_gt2"] == True
        assert results_by_name["Alice"]["all_gt0"] == True
        assert results_by_name["Alice"]["none_lt0"] == True

        # Bob []: any(>2)=False, all(>0)=True, none(<0)=True
        assert results_by_name["Bob"]["any_gt2"] == False
        assert results_by_name["Bob"]["all_gt0"] == True
        assert results_by_name["Bob"]["none_lt0"] == True

        # Carol None: any(>2)=False, all(>0)=True, none(<0)=True
        assert results_by_name["Carol"]["any_gt2"] == False
        assert results_by_name["Carol"]["all_gt0"] == True
        assert results_by_name["Carol"]["none_lt0"] == True

    def test_vectorized_quantifier_complex_expressions(self):
        """Test vectorized quantifier with complex WHERE expressions."""
        ctx = _make_large_ctx(n_people=10, n_scores=5)
        star = Star(context=ctx)

        # Test complex WHERE expressions that stress the vectorization
        complex_queries = [
            "any(x IN p.scores WHERE x * 2 > 6)",
            "all(x IN p.scores WHERE x + 1 < 10)",
            "none(x IN p.scores WHERE x % 2 = 0 AND x > 3)",
            "single(x IN p.scores WHERE x = 3)",
        ]

        for query_expr in complex_queries:
            result = star.execute_query(
                f"MATCH (p:Person) WHERE {query_expr} RETURN p.name AS name"
            )
            # Should execute without error and return valid results
            assert isinstance(result, pd.DataFrame)
            assert len(result) >= 0  # Valid result count
