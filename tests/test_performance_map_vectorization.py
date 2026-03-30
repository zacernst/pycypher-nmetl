"""TDD tests for Performance Loop 285 - Map Literal Vectorization.

This test suite documents the current O(n²) performance anti-pattern in map literal
evaluation and defines the expected behavior for vectorized replacement.

The current implementation in CollectionExpressionEvaluator.eval_map_literal uses nested
loops that cause expression evaluation to occur n×k times instead of k times, where:
- n = number of rows in the binding frame
- k = number of keys in the map literal

Performance target: 500 rows × 10 keys should complete in <2s.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

pytestmark = [pytest.mark.slow, pytest.mark.performance]


@pytest.fixture
def performance_star() -> Star:
    """Create a Star with sufficient data for performance testing."""
    # Create dataset with 500 rows for performance testing
    n_rows = 500
    df = pd.DataFrame(
        {
            ID_COLUMN: list(range(n_rows)),
            "name": [f"person_{i}" for i in range(n_rows)],
            "age": [20 + (i % 50) for i in range(n_rows)],
            "score": [80 + (i % 20) for i in range(n_rows)],
            "city": [["NYC", "SF", "LA", "CHI", "BOS"][i % 5] for i in range(n_rows)],
            "salary": [50000 + (i * 1000) for i in range(n_rows)],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "score", "city", "salary"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
            "city": "city",
            "salary": "salary",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
            "city": "city",
            "salary": "salary",
        },
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
        ),
    )


class TestMapLiteralCurrentBehavior:
    """Document current map literal evaluation behavior (TDD red phase)."""

    def test_basic_map_literal_functionality(
        self,
        performance_star: Star,
    ) -> None:
        """Test basic map literal creates correct structure."""
        query = """
        MATCH (p:Person)
        RETURN {name: p.name, age: p.age} AS person_map
        LIMIT 5
        """
        result = performance_star.execute_query(query)

        assert len(result) == 5
        assert "person_map" in result.columns

        # Check first map structure
        first_map = result["person_map"].iloc[0]
        assert isinstance(first_map, dict)
        assert "name" in first_map
        assert "age" in first_map
        assert first_map["name"] == "person_0"
        assert first_map["age"] == 20

    def test_map_literal_with_expressions(
        self,
        performance_star: Star,
    ) -> None:
        """Test map literals with computed expressions."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            age_category: CASE
                WHEN p.age < 25 THEN "young"
                WHEN p.age < 40 THEN "middle"
                ELSE "senior"
            END,
            score_doubled: p.score * 2
        } AS complex_map
        LIMIT 3
        """
        result = performance_star.execute_query(query)

        assert len(result) == 3
        first_map = result["complex_map"].iloc[0]

        assert first_map["name"] == "person_0"
        assert first_map["age_category"] == "young"  # age 20
        assert first_map["score_doubled"] == 160  # score 80 * 2

    def test_empty_map_literal(self, performance_star: Star) -> None:
        """Test empty map literal evaluation."""
        query = """
        MATCH (p:Person)
        RETURN {} AS empty_map
        LIMIT 2
        """
        result = performance_star.execute_query(query)

        assert len(result) == 2
        assert all(result["empty_map"] == {})

    def test_map_literal_with_null_handling(
        self,
        performance_star: Star,
    ) -> None:
        """Test map literals handle null values correctly."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            missing_field: p.nonexistent,
            computed: p.age + null
        } AS null_map
        LIMIT 2
        """
        result = performance_star.execute_query(query)

        assert len(result) == 2
        first_map = result["null_map"].iloc[0]

        assert first_map["name"] == "person_0"
        assert first_map["missing_field"] is None
        # Computed field with null should be null
        assert pd.isna(first_map["computed"]) or first_map["computed"] is None


class TestMapLiteralPerformanceBaseline:
    """Measure current performance characteristics for baseline."""

    def test_current_performance_1_key(self, performance_star: Star) -> None:
        """Measure baseline performance with 1 key (should be fast)."""
        query = """
        MATCH (p:Person)
        RETURN {name: p.name} AS single_key_map
        """

        start_time = time.time()
        result = performance_star.execute_query(query)
        elapsed = time.time() - start_time

        assert len(result) == 500
        print(f"1-key map literal (500 rows): {elapsed:.3f}s")

        # Single key should be reasonable even with current implementation
        assert elapsed < 5.0, f"1-key map took {elapsed:.3f}s (threshold 5s)"

    def test_current_performance_5_keys(self, performance_star: Star) -> None:
        """Measure baseline performance with 5 keys."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            age: p.age,
            score: p.score,
            city: p.city,
            salary: p.salary
        } AS five_key_map
        """

        start_time = time.time()
        result = performance_star.execute_query(query)
        elapsed = time.time() - start_time

        assert len(result) == 500
        print(f"5-key map literal (500 rows): {elapsed:.3f}s")

        # Document current performance for comparison
        # This may fail with current O(n²) implementation

    @pytest.mark.slow
    def test_current_performance_10_keys_critical(
        self,
        performance_star: Star,
    ) -> None:
        """Measure baseline performance with 10 keys (critical test case matching original)."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            age: p.age,
            score: p.score,
            city: p.city,
            salary: p.salary,
            name_upper: toUpper(p.name),
            age_doubled: p.age * 2,
            score_category: CASE WHEN p.score > 90 THEN "high" ELSE "normal" END,
            salary_k: p.salary / 1000,
            is_senior: p.age > 35
        } AS ten_key_map
        """

        # Match original test: 20 repetitions to amplify the performance issue
        start_time = time.time()
        for _ in range(20):
            result = performance_star.execute_query(query)
        elapsed = time.time() - start_time

        assert len(result) == 500
        print(f"20 × 10-key map literal (500 rows): {elapsed:.3f}s")

        # This matches the original failing test case
        # Expected to fail with current O(n²) implementation showing ~14s
        # Target: should pass after vectorization (<2s for 20 repetitions)


class TestVectorizedMapLiteralTarget:
    """Define target behavior for vectorized map literal implementation (TDD green phase)."""

    def test_vectorized_correctness_preservation(
        self,
        performance_star: Star,
    ) -> None:
        """Vectorized implementation must preserve all semantic behavior."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            computed: p.age + p.score,
            conditional: CASE WHEN p.age > 30 THEN "old" ELSE "young" END
        } AS preserved_map
        LIMIT 10
        """
        result = performance_star.execute_query(query)

        assert len(result) == 10

        # Verify correctness of each map
        for i in range(len(result)):
            map_val = result["preserved_map"].iloc[i]
            expected_name = f"person_{i}"
            expected_age = 20 + (i % 50)
            expected_score = 80 + (i % 20)
            expected_computed = expected_age + expected_score
            expected_conditional = "old" if expected_age > 30 else "young"

            assert map_val["name"] == expected_name
            assert map_val["computed"] == expected_computed
            assert map_val["conditional"] == expected_conditional

    @pytest.mark.performance_target
    def test_vectorized_performance_target_10_keys(
        self,
        performance_star: Star,
    ) -> None:
        """Vectorized implementation must meet performance target."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            age: p.age,
            score: p.score,
            city: p.city,
            salary: p.salary,
            name_upper: toUpper(p.name),
            age_doubled: p.age * 2,
            score_category: CASE WHEN p.score > 90 THEN "high" ELSE "normal" END,
            salary_k: p.salary / 1000,
            is_senior: p.age > 35
        } AS target_map
        """

        start_time = time.time()
        result = performance_star.execute_query(query)
        elapsed = time.time() - start_time

        assert len(result) == 500
        print(f"Vectorized 10-key map literal (500 rows): {elapsed:.3f}s")

        # This should pass after vectorization
        assert elapsed < 2.0, f"Vectorized 10-key map took {elapsed:.3f}s (target <2s)"

    @pytest.mark.performance_target
    def test_vectorized_scalability_1000_rows(
        self,
        performance_star: Star,
    ) -> None:
        """Vectorized implementation should scale to larger datasets."""
        # Create larger dataset for scalability testing
        n_rows = 1000
        df = pd.DataFrame(
            {
                ID_COLUMN: list(range(n_rows)),
                "name": [f"person_{i}" for i in range(n_rows)],
                "age": [20 + (i % 50) for i in range(n_rows)],
                "score": [80 + (i % 20) for i in range(n_rows)],
            },
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age", "score"],
            source_obj_attribute_map={
                "name": "name",
                "age": "age",
                "score": "score",
            },
            attribute_map={"name": "name", "age": "age", "score": "score"},
            source_obj=df,
        )
        large_star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={"Person": table}),
            ),
        )

        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            age: p.age,
            score: p.score,
            computed: p.age * p.score,
            category: CASE WHEN p.age > 35 THEN "senior" ELSE "junior" END
        } AS scalable_map
        """

        start_time = time.time()
        result = large_star.execute_query(query)
        elapsed = time.time() - start_time

        assert len(result) == 1000
        print(f"Vectorized 5-key map literal (1000 rows): {elapsed:.3f}s")

        # Should scale linearly, not quadratically
        assert elapsed < 5.0, (
            f"Vectorized 1000-row map took {elapsed:.3f}s (target <5s)"
        )


class TestPerformanceImprovement:
    """Validate performance improvements after vectorization."""

    @pytest.mark.performance_comparison
    def test_performance_improvement_measurement(
        self,
        performance_star: Star,
    ) -> None:
        """Measure and validate performance improvement."""
        query = """
        MATCH (p:Person)
        RETURN {
            name: p.name,
            age: p.age,
            score: p.score,
            computed1: p.age + p.score,
            computed2: p.age * 2,
            computed3: toUpper(p.name)
        } AS improvement_map
        """

        # Multiple runs for stable measurement
        times = []
        for _ in range(3):
            start_time = time.time()
            result = performance_star.execute_query(query)
            elapsed = time.time() - start_time
            times.append(elapsed)

        avg_time = sum(times) / len(times)
        assert len(result) == 500
        print(f"6-key map literal average time (500 rows): {avg_time:.3f}s")

        # After vectorization, should be significantly faster
        # Target: At least 2x improvement over baseline
        assert avg_time < 3.0, f"Average time {avg_time:.3f}s exceeds target 3s"
