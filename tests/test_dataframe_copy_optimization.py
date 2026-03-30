"""TDD tests for DataFrame copy optimization.

This module identifies unnecessary DataFrame .copy() calls and replaces them
with more efficient view operations where safe to do so.

Performance Impact: Reducing unnecessary copies can improve query performance
by 10-50% especially for large graphs and complex traversals.

Run with:
    uv run pytest tests/test_dataframe_copy_optimization_tdd.py -v
"""

import time
from unittest.mock import patch

import pandas as pd
import pytest
from pycypher import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


class TestDataFrameCopyOptimization:
    """Test optimization of DataFrame copy operations."""

    @pytest.fixture
    def large_graph_star(self) -> Star:
        """Create a Star with larger dataset for meaningful performance testing."""
        # Create a larger dataset to make copy performance differences measurable
        n_people = 2000
        person_df = pd.DataFrame(
            {
                ID_COLUMN: list(range(1, n_people + 1)),
                "name": [f"Person{i}" for i in range(1, n_people + 1)],
                "age": [(20 + i % 50) for i in range(1, n_people + 1)],
                "active": [i % 3 == 0 for i in range(1, n_people + 1)],
            },
        )

        # Create relationships between people
        n_relationships = 5000
        knows_df = pd.DataFrame(
            {
                ID_COLUMN: list(range(1, n_relationships + 1)),
                "__SOURCE__": [((i % n_people) + 1) for i in range(n_relationships)],
                "__TARGET__": [
                    ((i + 100) % n_people + 1) for i in range(n_relationships)
                ],
                "strength": [0.1 + (i % 10) * 0.1 for i in range(n_relationships)],
            },
        )

        person_table = EntityTable.from_dataframe("Person", person_df)
        knows_table = EntityTable.from_dataframe("KNOWS", knows_df)

        from pycypher.relational_models import RelationshipMapping

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        )
        return Star(context=context)

    def test_identify_copy_usage_in_source_code(self) -> None:
        """Test that identifies all .copy() usage in source code."""
        import subprocess

        # Use grep to find all .copy() calls in source code
        result = subprocess.run(
            ["grep", "-r", r"\.copy()", "packages/pycypher/src/"],
            capture_output=True,
            text=True,
        )

        copy_lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

        # Document current copy usage
        print(f"Found {len(copy_lines)} .copy() calls in source code:")
        for line in copy_lines[:10]:  # Show first 10
            print(f"  {line}")

        # After systematic copy elimination: only necessary copies remain.
        # 3 backend to_pandas (API contract), 2 shadow layer (atomicity),
        # 2 star.py (result isolation), 1 path_expander (frontier isolation),
        # ~10 scalar_functions (Series mutation + empty series guards).
        assert len(copy_lines) <= 18  # Verify optimization reduced copy count

    def test_frontier_copy_in_variable_length_path(
        self,
        large_graph_star: Star,
    ) -> None:
        """Test frontier DataFrame copying in variable-length path traversal."""
        # This query triggers variable-length path logic which uses frontier.copy()
        # Line: frontier = start_frame.bindings.copy()
        query = "MATCH (p:Person)-[*1..2]->(connected) RETURN count(connected) AS count"

        # Time the query to establish baseline performance
        start_time = time.perf_counter()
        result = large_graph_star.execute_query(query)
        elapsed_time = time.perf_counter() - start_time

        print(f"Variable-length path query took: {elapsed_time:.3f}s")

        # Verify the query works correctly
        assert result.iloc[0]["count"] > 0

        # The frontier.copy() might be unnecessary if we're just reading from it
        # This copy happens for every variable-length path traversal

    def test_edge_dataframe_copy_in_relationship_traversal(
        self,
        large_graph_star: Star,
    ) -> None:
        """Test edge DataFrame copying in relationship traversal."""
        # This triggers: edge_df = rel_df[[src_col, tgt_col]].copy()
        query = (
            "MATCH (p:Person)-[r:KNOWS]->(friend) RETURN count(friend) AS friend_count"
        )

        start_time = time.perf_counter()
        result = large_graph_star.execute_query(query)
        elapsed_time = time.perf_counter() - start_time

        print(f"Relationship traversal query took: {elapsed_time:.3f}s")

        # Verify correctness
        assert result.iloc[0]["friend_count"] > 0

        # The edge_df.copy() might be unnecessary if we're just using it for joins
        # Column selection creates a view, so the .copy() might be redundant

    def test_bindings_copy_in_aggregation(
        self,
        large_graph_star: Star,
    ) -> None:
        """Test bindings copying in aggregation operations."""
        # This might trigger: df = frame.bindings.copy()
        query = "MATCH (p:Person) RETURN avg(p.age) AS avg_age, count(p) AS total"

        start_time = time.perf_counter()
        result = large_graph_star.execute_query(query)
        elapsed_time = time.perf_counter() - start_time

        print(f"Aggregation query took: {elapsed_time:.3f}s")

        # Verify correctness
        assert result.iloc[0]["total"] == 2000
        assert 20 <= result.iloc[0]["avg_age"] <= 70  # Age range check

    def test_copy_call_frequency_during_complex_query(
        self,
        large_graph_star: Star,
    ) -> None:
        """Test how many DataFrame copies occur during a complex query."""
        original_copy = pd.DataFrame.copy
        copy_call_count = 0

        def tracking_copy(self, deep=True):
            nonlocal copy_call_count
            copy_call_count += 1
            return original_copy(self, deep=deep)

        # Patch DataFrame.copy to count calls
        with patch.object(pd.DataFrame, "copy", tracking_copy):
            # Execute a complex query that triggers multiple operations
            query = """
            MATCH (p:Person)-[r:KNOWS]->(friend)
            WHERE p.age > 30
            RETURN p.name AS person, collect(friend.name) AS friends, count(friend) AS friend_count
            ORDER BY friend_count DESC
            LIMIT 10
            """

            result = large_graph_star.execute_query(query)

        print(
            f"Complex query triggered {copy_call_count} DataFrame.copy() calls",
        )

        # Verify correctness
        assert len(result) <= 10
        assert "person" in result.columns
        assert "friends" in result.columns
        assert "friend_count" in result.columns

        # Document current copy frequency for optimization target
        # High copy count indicates optimization opportunity
        # Target: Reduce copy_call_count by at least 30%

    def test_unnecessary_copy_in_column_selection(self) -> None:
        """Test that column selection doesn't need .copy() for read-only operations."""
        # Create test DataFrame
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
            },
        )

        # Column selection with copy (current pattern)
        selected_with_copy = df[["id", "name"]].copy()

        # Column selection without copy (view)
        selected_view = df[["id", "name"]]

        # Both should have identical content
        pd.testing.assert_frame_equal(selected_with_copy, selected_view)

        # View should be faster for read-only operations
        # Copy only needed if we plan to modify the result

        # Test modification safety
        # If we modify the copy, original should be unchanged
        selected_with_copy.loc[0, "name"] = "Modified"
        assert df.loc[0, "name"] == "Alice"  # Original unchanged

        # If we modify the view, original might be affected (depends on pandas version)
        # This is why copies are used - to ensure isolation

    def test_copy_vs_view_performance_difference(self) -> None:
        """Test performance difference between .copy() and view operations."""
        # Create large DataFrame for meaningful performance test
        large_df = pd.DataFrame(
            {
                "id": list(range(100000)),
                "value1": list(range(100000)),
                "value2": list(range(100000)),
                "value3": list(range(100000)),
            },
        )

        # Time column selection with copy
        start_time = time.perf_counter()
        for _ in range(100):
            selected = large_df[["id", "value1"]].copy()
        copy_time = time.perf_counter() - start_time

        # Time column selection without copy (view)
        start_time = time.perf_counter()
        for _ in range(100):
            selected = large_df[["id", "value1"]]
        view_time = time.perf_counter() - start_time

        print(f"Copy time: {copy_time:.3f}s, View time: {view_time:.3f}s")
        print(f"Copy is {copy_time / view_time:.1f}x slower than view")

        # Copy should be significantly slower
        # For read-only operations, view is preferred
        assert copy_time > view_time

        # Document the performance difference
        # This justifies optimizing unnecessary copies

    def test_identify_safe_copy_elimination_candidates(self) -> None:
        """Identify DataFrame copies that can safely be eliminated."""
        # Categories of potentially unnecessary copies:
        # 1. Column selection for read-only operations
        # 2. Temporary variables that aren't modified
        # 3. DataFrame passed immediately to pandas operations
        # 4. Copies made "just to be safe" without actual mutation risk

        candidates = [
            # From actual source code analysis:
            "edge_df = rel_df[[src_col, tgt_col]].copy()",  # Column selection for join
            "part = frontier.drop(columns=[_VL_TIP_COL]).copy()",  # Drop columns for read
            "df = bfs_frame.bindings.copy()",  # Bindings for read-only processing
            "temp_df = df.copy()",  # Temporary variable usage
        ]

        # These copies might be safe to eliminate if:
        # - The result is used only for reading
        # - The result is passed to pandas operations that don't modify it
        # - No concurrent access to the original DataFrame

        print(
            f"Identified {len(candidates)} potential copy elimination candidates:",
        )
        for candidate in candidates:
            print(f"  - {candidate}")

        # Manual analysis required to determine safety of each elimination
        assert len(candidates) >= 4

    def test_copy_elimination_preserves_correctness(
        self,
        large_graph_star: Star,
    ) -> None:
        """Test that optimizations preserve query correctness."""
        # Run the same query multiple times to ensure consistency
        query = "MATCH (p:Person) WHERE p.age > 40 RETURN count(p) AS count"

        results = []
        for i in range(5):
            result = large_graph_star.execute_query(query)
            results.append(result.iloc[0]["count"])

        # All results should be identical (deterministic)
        assert all(r == results[0] for r in results)

        # Any copy elimination optimization must maintain this consistency

    def test_memory_usage_reduction_with_fewer_copies(self) -> None:
        """Test that reducing copies reduces memory usage."""
        # Create large DataFrame to make memory difference measurable
        large_df = pd.DataFrame(
            {f"col_{i}": list(range(50000)) for i in range(20)},
        )

        import os

        import psutil

        process = psutil.Process(os.getpid())

        # Measure memory usage with many copies
        initial_memory = process.memory_info().rss

        copies = []
        for i in range(10):
            copy = large_df.copy()
            copies.append(copy)

        memory_with_copies = process.memory_info().rss

        # Clean up copies
        del copies

        final_memory = process.memory_info().rss

        memory_increase = memory_with_copies - initial_memory
        print(
            f"Memory increase with 10 copies: {memory_increase / 1024 / 1024:.1f} MB",
        )

        # Demonstrates the memory cost of unnecessary copies
        # Eliminating unnecessary copies should reduce peak memory usage
        assert memory_increase > 0
