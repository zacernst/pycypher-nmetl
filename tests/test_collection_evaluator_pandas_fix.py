"""
Testing Loop 288 - Collection Evaluator Pandas Array Ambiguity Fix TDD

Comprehensive test suite for fixing the critical pandas array boolean evaluation
error in _is_null_raw_list function that is blocking list comprehension operations.

Root Issue: pd.isna(value) returns pandas array when value is pandas array/Series,
causing "The truth value of an array with more than one element is ambiguous" error
when used in boolean context with 'or' operator.
"""

from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star


class TestPandasArrayAmbiguityReproduction:
    """Reproduce the exact pandas array ambiguity error from the failing test."""

    def test_pandas_array_ambiguity_error_reproduction(self):
        """Reproduce the specific error that's causing test failures."""

        # Simulate the problematic function from collection_evaluator.py
        def _is_null_raw_list_original(value: Any) -> bool:
            return (
                value is None
                or pd.isna(value)
                or (isinstance(value, list) and len(value) == 0)
            )

        # Test cases that should work fine
        assert _is_null_raw_list_original(None) is True
        assert _is_null_raw_list_original("test") is False

        # In Python 3.14, pd.isna() on lists returns numpy arrays, causing ambiguity errors
        # This demonstrates the problem the fix is meant to address
        with pytest.raises(
            ValueError,
            match="The truth value of an (empty )?array is ambiguous",
        ):
            _is_null_raw_list_original([])

        with pytest.raises(
            ValueError, match="The truth value of an array.*is ambiguous"
        ):
            _is_null_raw_list_original([1, 2, 3])

        # The problematic case: pandas array/Series
        pandas_array = pd.array([1, 2, 3])
        pandas_series = pd.Series([1, 2, 3])

        # These should cause the ambiguity error
        with pytest.raises(ValueError, match="The truth value.*is ambiguous"):
            _is_null_raw_list_original(pandas_array)

        with pytest.raises(ValueError, match="The truth value.*is ambiguous"):
            _is_null_raw_list_original(pandas_series)

    def test_pandas_isna_behavior_analysis(self):
        """Analyze how pd.isna behaves with different value types."""

        # Scalar values - pd.isna returns boolean
        assert pd.isna(None) is True
        assert pd.isna(pd.NA) is True
        assert pd.isna(5) is False
        assert pd.isna("test") is False

        # Array/Series values - pd.isna returns array of booleans
        pandas_array = pd.array([1, 2, None])
        isna_result = pd.isna(pandas_array)
        assert isinstance(
            isna_result, np.ndarray
        )  # Returns array, not boolean
        assert list(isna_result) == [False, False, True]

        pandas_series = pd.Series([1, None, 3])
        isna_result_series = pd.isna(pandas_series)
        assert isinstance(
            isna_result_series, pd.Series
        )  # Returns Series, not boolean
        assert list(isna_result_series) == [False, True, False]

        # Empty arrays/series
        empty_array = pd.array([])
        empty_series = pd.Series([])
        assert len(pd.isna(empty_array)) == 0  # Empty array of booleans
        assert len(pd.isna(empty_series)) == 0  # Empty Series of booleans


class TestNullRawListFunctionFixed:
    """Test the fixed version of _is_null_raw_list function."""

    def _is_null_raw_list_fixed(self, value: Any) -> bool:
        """Fixed version that handles pandas arrays correctly."""
        # Handle None first
        if value is None:
            return True

        # Handle pandas arrays/Series explicitly before boolean context
        if hasattr(value, "dtype") and hasattr(value, "__len__"):
            # This is likely a pandas array or Series
            if len(value) == 0:
                return True
            # For non-empty pandas arrays, return False (not considered null list)
            # Only empty arrays are considered null lists
            return False

        # Handle regular Python lists
        if isinstance(value, list):
            return len(value) == 0

        # Handle scalar pandas null values
        try:
            # This should work for scalar values without array ambiguity
            return pd.isna(value)
        except ValueError:
            # If pd.isna fails with ValueError, it's likely not a pandas-compatible type
            return False

    def test_fixed_function_with_none(self):
        """Test fixed function handles None correctly."""
        assert self._is_null_raw_list_fixed(None) is True

    def test_fixed_function_with_empty_lists(self):
        """Test fixed function handles empty lists correctly."""
        assert self._is_null_raw_list_fixed([]) is True

    def test_fixed_function_with_non_empty_lists(self):
        """Test fixed function handles non-empty lists correctly."""
        assert self._is_null_raw_list_fixed([1, 2, 3]) is False
        assert self._is_null_raw_list_fixed(["a", "b"]) is False

    def test_fixed_function_with_pandas_arrays(self):
        """Test fixed function handles pandas arrays without ambiguity error."""
        # Non-empty pandas arrays should return False (not considered null)
        pandas_array = pd.array([1, 2, 3])
        assert self._is_null_raw_list_fixed(pandas_array) is False

        pandas_series = pd.Series([1, 2, 3])
        assert self._is_null_raw_list_fixed(pandas_series) is False

        # Empty pandas arrays should return True
        empty_array = pd.array([])
        assert self._is_null_raw_list_fixed(empty_array) is True

        empty_series = pd.Series([])
        assert self._is_null_raw_list_fixed(empty_series) is True

        # Arrays with null values should return False (not empty, so not null list)
        array_with_nulls = pd.array([1, None, 3])
        assert self._is_null_raw_list_fixed(array_with_nulls) is False

    def test_fixed_function_with_scalar_pandas_nulls(self):
        """Test fixed function handles scalar pandas null values correctly."""
        assert self._is_null_raw_list_fixed(pd.NA) is True
        assert self._is_null_raw_list_fixed(pd.NaT) is True

    def test_fixed_function_with_regular_scalars(self):
        """Test fixed function handles regular scalar values correctly."""
        assert self._is_null_raw_list_fixed(5) is False
        assert self._is_null_raw_list_fixed("test") is False
        assert self._is_null_raw_list_fixed(3.14) is False


class TestCollectionEvaluatorIntegration:
    """Test integration of the fixed function with collection evaluator."""

    def test_list_comprehension_with_mixed_data_types(self):
        """Test list comprehension evaluation with various data types."""
        # Create test data that might include pandas arrays
        entities_df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2", "p3"],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
            }
        )

        context = ContextBuilder.from_dict({"Person": entities_df})
        star = Star(context=context)

        # This query uses list comprehension that should work without pandas array issues
        query = "MATCH (p:Person) RETURN [x IN [1,2,3,4,5] | x * 2] AS doubled_list"

        # This should execute without pandas array ambiguity error
        result = star.execute_query(query)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3  # One row per person
        assert "doubled_list" in result.columns

        # Verify the list comprehension results
        for doubled_list in result["doubled_list"]:
            assert doubled_list == [2, 4, 6, 8, 10]

    def test_quantifier_expressions_integration(self):
        """Test quantifier expressions work with fixed pandas handling."""
        entities_df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2", "p3"],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
            }
        )

        context = ContextBuilder.from_dict({"Person": entities_df})
        star = Star(context=context)

        # Query that uses quantifier evaluation
        query = "MATCH (p:Person) WHERE p.age IN [25, 30, 35, 40] RETURN count(p) AS specific_ages"

        # Should execute without pandas array issues
        result = star.execute_query(query)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["specific_ages"].iloc[0] == 3  # All three people match


class TestOriginalTestCaseExecution:
    """Test that the original failing test case now passes."""

    def test_architecture_loop_276_collection_baseline_reproduction(self):
        """Reproduce the exact test case that was failing."""
        # Create test data similar to the original failing test
        entities_df = pd.DataFrame(
            {
                "__ID__": list(range(1, 101)),
                "name": [f"Person_{i}" for i in range(1, 101)],
                "city": ["NYC", "LA", "Chicago"] * 33 + ["Boston"],
                "age": list(range(20, 120)),
            }
        )

        context = ContextBuilder.from_dict({"Person": entities_df})
        star = Star(context=context)

        # The exact queries from the failing test
        collection_queries = [
            # _eval_list_comprehension - list comprehensions
            "MATCH (p:Person) RETURN [x IN [1,2,3,4,5] | x * 2] AS doubled_list",
            # _eval_quantifier - quantifier expressions
            "MATCH (p:Person) WHERE p.age IN [25, 30, 35, 40] RETURN count(p) AS specific_ages",
            # List operations
            "MATCH (p:Person) RETURN [p.name, p.city] AS person_info LIMIT 10",
            # Complex list expressions
            "MATCH (p:Person) RETURN size([x IN [1,2,3] WHERE x > 1]) AS filtered_size",
        ]

        # All queries should execute without pandas array ambiguity errors
        for query in collection_queries:
            result = star.execute_query(query)

            # Basic validation
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert len(result.columns) > 0


# TDD Approach Summary:
# 1. Reproduce the pandas array ambiguity error (RED phase)
# 2. Analyze pandas behavior with different value types
# 3. Design fixed function that handles pandas arrays explicitly
# 4. Test fixed function with all value types (GREEN phase)
# 5. Validate integration with collection evaluator
# 6. Confirm original failing test case now passes
