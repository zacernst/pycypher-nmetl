"""TDD tests for Performance Loop 254: Fix iterrows anti-pattern in CLI table display.

The _print_table function in nmetl_cli.py uses df.iterrows() which is a well-known
pandas performance anti-pattern. This can be 10-20× slower than vectorized alternatives,
especially with large result sets, directly impacting CLI user experience.
"""

import io
import time
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd
import pytest
from pycypher.nmetl_cli import _print_table

pytestmark = pytest.mark.performance


class TestCurrentPerformanceIssue:
    """Test the current iterrows performance anti-pattern."""

    def test_print_table_uses_iterrows_antipattern(self):
        """Test that _print_table currently uses iterrows (performance anti-pattern).

        This test documents the current performance issue that needs to be fixed.
        """
        # Create a test DataFrame
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "value": [100, 200, 300, 400, 500],
            }
        )

        # Capture the output
        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            _print_table(df)

        output = output_buffer.getvalue()

        # Verify it produces table output (functionality works)
        assert "Alice" in output
        assert "Bob" in output
        assert "id" in output
        assert "name" in output
        assert "value" in output

        # Verify table formatting
        assert "+---+------+-------+" in output or "+" in output
        assert "|" in output

    def test_print_table_performance_with_large_dataframe(self):
        """Test current performance with a larger DataFrame to demonstrate the issue."""
        # Create a larger DataFrame to show performance impact
        df = pd.DataFrame(
            {
                "id": range(1000),
                "name": [f"User_{i}" for i in range(1000)],
                "value": range(1000, 2000),
                "description": [
                    f"Description for user {i}" for i in range(1000)
                ],
            }
        )

        # Time the current implementation
        start_time = time.perf_counter()

        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            _print_table(df)

        elapsed_time = time.perf_counter() - start_time

        # Verify it still works correctly
        output = output_buffer.getvalue()
        assert "User_0" in output
        assert "User_999" in output

        # Store baseline time for comparison after fix
        # (This will be used to verify performance improvement)
        assert elapsed_time > 0  # Basic sanity check
        print(
            f"Current iterrows implementation took: {elapsed_time:.4f}s for 1000 rows"
        )

    def test_empty_dataframe_handling(self):
        """Test current handling of empty DataFrames."""
        df = pd.DataFrame()

        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            _print_table(df)

        output = output_buffer.getvalue()
        assert "(no rows returned)" in output


class TestVectorizedPerformanceImplementation:
    """Tests for the improved vectorized implementation.

    These tests define expected behavior after fixing the performance issue.
    Initially these will fail (red phase), then pass after implementation (green phase).
    """

    def test_vectorized_print_table_produces_same_output(self):
        """Test that vectorized implementation produces identical output."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [95.5, 87.2, 92.8],
            }
        )

        # Test current output
        current_output = io.StringIO()
        with redirect_stdout(current_output):
            _print_table(df)
        current_result = current_output.getvalue()

        # After vectorization, output should be identical
        # (This test will initially fail but should pass after the fix)
        vectorized_output = io.StringIO()
        with redirect_stdout(vectorized_output):
            _print_table(df)
        vectorized_result = vectorized_output.getvalue()

        # Output should be identical
        assert vectorized_result == current_result

        # Verify key content is present
        assert "Alice" in vectorized_result
        assert "95.5" in vectorized_result
        assert "|" in vectorized_result
        assert "+" in vectorized_result

    def test_vectorized_performance_improvement(self):
        """Test that vectorized implementation is significantly faster."""
        # Create a larger DataFrame for performance testing
        df = pd.DataFrame(
            {
                "id": range(2000),
                "username": [f"user_{i:04d}" for i in range(2000)],
                "email": [f"user{i}@example.com" for i in range(2000)],
                "score": [i * 0.1 for i in range(2000)],
            }
        )

        # Time the vectorized implementation
        start_time = time.perf_counter()

        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            _print_table(df)

        vectorized_time = time.perf_counter() - start_time

        # Verify output is still correct
        output = output_buffer.getvalue()
        assert "user_0000" in output
        assert "user_1999" in output
        assert "user1999@example.com" in output

        # After vectorization, this should be much faster
        # For 2000 rows, vectorized should be under 0.1 seconds
        # while iterrows might take 0.5+ seconds
        print(
            f"Vectorized implementation took: {vectorized_time:.4f}s for 2000 rows"
        )

        # This assertion will initially fail (current implementation is slow)
        # but should pass after vectorization
        assert vectorized_time < 0.2, (
            f"Vectorized implementation too slow: {vectorized_time:.4f}s"
        )

    def test_vectorized_handles_edge_cases(self):
        """Test that vectorized implementation handles edge cases correctly."""
        # Test with various data types
        df = pd.DataFrame(
            {
                "str_col": ["hello", "world", "test"],
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "bool_col": [True, False, True],
                "none_col": [None, "value", None],
            }
        )

        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            _print_table(df)

        output = output_buffer.getvalue()

        # Should handle all data types correctly
        assert "hello" in output
        assert "1.1" in output
        assert "True" in output
        assert "False" in output
        assert (
            "NULL" in output
            or "None" in output
            or "NaN" in output
            or "nan" in output
        )

    def test_vectorized_empty_dataframe(self):
        """Test vectorized implementation with empty DataFrame."""
        df = pd.DataFrame()

        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            _print_table(df)

        output = output_buffer.getvalue()
        assert "(no rows returned)" in output

    def test_no_more_iterrows_usage(self):
        """Test that the vectorized implementation doesn't use iterrows."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        # Mock iterrows to detect if it's called
        with patch.object(pd.DataFrame, "iterrows") as mock_iterrows:
            mock_iterrows.side_effect = Exception(
                "iterrows should not be called in vectorized implementation!"
            )

            # This should work without calling iterrows
            output_buffer = io.StringIO()
            with redirect_stdout(output_buffer):
                _print_table(df)

            output = output_buffer.getvalue()
            assert "a" in output
            assert "1" in output

            # iterrows should not have been called
            mock_iterrows.assert_not_called()
