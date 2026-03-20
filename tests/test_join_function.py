"""TDD tests for the missing join() scalar function.

The join() function is the inverse of split() - it takes a list of strings
and a delimiter and joins them into a single string. This is a standard
Neo4j/openCypher function that is currently missing from the registry.

Written in TDD red phase before implementation.

Run with:
    uv run pytest tests/test_join_function_tdd.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry


class TestJoinFunctionExists:
    """Test that join function is registered and callable."""

    @pytest.fixture
    def registry(self) -> ScalarFunctionRegistry:
        """Get the scalar function registry singleton."""
        return ScalarFunctionRegistry.get_instance()

    def test_join_function_registered(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join function must be registered in the scalar function registry."""
        assert "join" in registry._functions, (
            "join function not found in registry"
        )

    def test_join_function_callable(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join function must be callable through registry.execute."""
        # Basic test - should not raise an error about unknown function
        list_series = pd.Series([["a", "b", "c"]], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])
        assert result is not None, "join function should return a result"


class TestJoinFunctionCorrectness:
    """Test join function correctness with various inputs."""

    @pytest.fixture
    def registry(self) -> ScalarFunctionRegistry:
        """Get the scalar function registry singleton."""
        return ScalarFunctionRegistry.get_instance()

    def test_basic_join(self, registry: ScalarFunctionRegistry) -> None:
        """join(['a', 'b', 'c'], ',') should return 'a,b,c'."""
        list_series = pd.Series([["a", "b", "c"]], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] == "a,b,c"

    def test_empty_list(self, registry: ScalarFunctionRegistry) -> None:
        """join([], ',') should return empty string."""
        list_series = pd.Series([[]], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] == ""

    def test_single_element_list(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join(['hello'], ',') should return 'hello' (no delimiter needed)."""
        list_series = pd.Series([["hello"]], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] == "hello"

    def test_null_list_input(self, registry: ScalarFunctionRegistry) -> None:
        """join(null, ',') should return null."""
        list_series = pd.Series([None], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] is None

    def test_null_delimiter(self, registry: ScalarFunctionRegistry) -> None:
        """join(['a', 'b'], null) should return null."""
        list_series = pd.Series([["a", "b"]], dtype="object")
        delimiter_series = pd.Series([None], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] is None

    def test_different_delimiters(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """Test join with various delimiters."""
        # Space delimiter
        list_series = pd.Series([["hello", "world"]], dtype="object")
        delimiter_series = pd.Series([" "], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])
        assert result.iloc[0] == "hello world"

        # Empty string delimiter
        list_series = pd.Series([["a", "b", "c"]], dtype="object")
        delimiter_series = pd.Series([""], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])
        assert result.iloc[0] == "abc"

        # Multi-character delimiter
        list_series = pd.Series([["one", "two", "three"]], dtype="object")
        delimiter_series = pd.Series([" | "], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])
        assert result.iloc[0] == "one | two | three"

    def test_list_with_null_elements(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join(['a', null, 'c'], ',') should skip null elements and return 'a,c'."""
        list_series = pd.Series([["a", None, "c"]], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] == "a,c"

    def test_list_with_numeric_elements(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join([1, 2, 3], ',') should convert numbers to strings and return '1,2,3'."""
        list_series = pd.Series([[1, 2, 3]], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 1
        assert result.iloc[0] == "1,2,3"

    def test_multiple_rows(self, registry: ScalarFunctionRegistry) -> None:
        """Test join function with multiple rows of data."""
        list_series = pd.Series(
            [["a", "b"], ["x", "y", "z"], [], None], dtype="object"
        )
        delimiter_series = pd.Series(["-", "-", "-", "-"], dtype="object")

        result = registry.execute("join", [list_series, delimiter_series])

        assert len(result) == 4
        assert result.iloc[0] == "a-b"
        assert result.iloc[1] == "x-y-z"
        assert result.iloc[2] == ""  # empty list
        assert result.iloc[3] is None  # null list


class TestJoinFunctionIntegration:
    """Test join function integration with split function."""

    @pytest.fixture
    def registry(self) -> ScalarFunctionRegistry:
        """Get the scalar function registry singleton."""
        return ScalarFunctionRegistry.get_instance()

    def test_join_split_roundtrip(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join(split(s, d), d) should return the original string s."""
        original = "hello,world,test"
        delimiter = ","

        # Split the string
        string_series = pd.Series([original], dtype="object")
        delim_series = pd.Series([delimiter], dtype="object")

        split_result = registry.execute("split", [string_series, delim_series])

        # Join it back
        join_result = registry.execute("join", [split_result, delim_series])

        assert join_result.iloc[0] == original

    def test_split_join_preserves_empty_elements(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """Test that split/join roundtrip preserves empty elements."""
        original = "a,,c"  # empty element in the middle
        delimiter = ","

        string_series = pd.Series([original], dtype="object")
        delim_series = pd.Series([delimiter], dtype="object")

        split_result = registry.execute("split", [string_series, delim_series])
        join_result = registry.execute("join", [split_result, delim_series])

        assert join_result.iloc[0] == original


class TestJoinFunctionErrors:
    """Test join function error handling."""

    @pytest.fixture
    def registry(self) -> ScalarFunctionRegistry:
        """Get the scalar function registry singleton."""
        return ScalarFunctionRegistry.get_instance()

    def test_non_list_first_argument(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join('not_a_list', ',') should handle non-list input gracefully."""
        # Depending on implementation, this might return null or raise an error
        non_list_series = pd.Series(["not_a_list"], dtype="object")
        delimiter_series = pd.Series([","], dtype="object")

        # Should not crash - either return null or handle gracefully
        result = registry.execute("join", [non_list_series, delimiter_series])
        assert result is not None

    def test_wrong_number_of_arguments(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """join() with wrong number of arguments should raise appropriate error."""
        list_series = pd.Series([["a", "b"]], dtype="object")

        # Too few arguments
        with pytest.raises(
            Exception
        ):  # Specific exception type depends on implementation
            registry.execute("join", [list_series])

        # Too many arguments
        extra_series = pd.Series(["extra"], dtype="object")
        with pytest.raises(Exception):
            registry.execute("join", [list_series, extra_series, extra_series])
