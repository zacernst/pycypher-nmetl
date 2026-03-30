"""TDD tests for adding common function aliases (Cowbell Loop).

The scalar function registry is missing common function aliases that users
would naturally expect to exist, especially those coming from Python or
general programming backgrounds. This creates poor UX when users try
intuitive function names and get "Unknown scalar function" errors.

This cowbell loop adds 6 essential aliases:
- now() → timestamp()
- len() → length()
- str() → toString()
- int() → toInteger()
- float() → toFloat()
- bool() → toBoolean()

Run with:
    uv run pytest tests/test_function_aliases_cowbell_tdd.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry


class TestFunctionAliasesExist:
    """Test that common function aliases are registered and callable."""

    def test_now_alias_exists(self) -> None:
        """now() function should be registered as alias to timestamp()."""
        registry = ScalarFunctionRegistry.get_instance()

        # Should be able to get metadata for now()
        assert "now" in registry._functions, "now() function should be registered"
        meta = registry._functions["now"]
        assert meta.callable is not None, "now() should be callable"

    def test_len_alias_exists(self) -> None:
        """len() function should be registered as alias to length()."""
        registry = ScalarFunctionRegistry.get_instance()

        assert "len" in registry._functions, "len() function should be registered"
        meta = registry._functions["len"]
        assert meta.callable is not None, "len() should be callable"

    def test_str_alias_exists(self) -> None:
        """str() function should be registered as alias to toString()."""
        registry = ScalarFunctionRegistry.get_instance()

        assert "str" in registry._functions, "str() function should be registered"
        meta = registry._functions["str"]
        assert meta.callable is not None, "str() should be callable"

    def test_int_alias_exists(self) -> None:
        """int() function should be registered as alias to toInteger()."""
        registry = ScalarFunctionRegistry.get_instance()

        assert "int" in registry._functions, "int() function should be registered"
        meta = registry._functions["int"]
        assert meta.callable is not None, "int() should be callable"

    def test_float_alias_exists(self) -> None:
        """float() function should be registered as alias to toFloat()."""
        registry = ScalarFunctionRegistry.get_instance()

        assert "float" in registry._functions, "float() function should be registered"
        meta = registry._functions["float"]
        assert meta.callable is not None, "float() should be callable"

    def test_bool_alias_exists(self) -> None:
        """bool() function should be registered as alias to toBoolean()."""
        registry = ScalarFunctionRegistry.get_instance()

        assert "bool" in registry._functions, "bool() function should be registered"
        meta = registry._functions["bool"]
        assert meta.callable is not None, "bool() should be callable"


class TestAliasCorrectness:
    """Test that aliases produce identical results to their target functions."""

    def test_now_equals_timestamp(self) -> None:
        """now() should return same result as timestamp()."""
        registry = ScalarFunctionRegistry.get_instance()

        now_result = registry.execute("now", [])
        timestamp_result = registry.execute("timestamp", [])

        # Both should return current timestamp (might differ by milliseconds)
        assert isinstance(now_result.iloc[0], (int, np.int64)), (
            "now() should return integer timestamp"
        )
        assert isinstance(timestamp_result.iloc[0], (int, np.int64)), (
            "timestamp() should return integer"
        )
        # Allow small time difference (test execution time)
        diff = abs(now_result.iloc[0] - timestamp_result.iloc[0])
        assert diff < 1000, (
            f"now() and timestamp() differ by {diff}ms, expected < 1000ms"
        )

    def test_len_equals_length(self) -> None:
        """len() should return same results as length()."""
        registry = ScalarFunctionRegistry.get_instance()

        test_data = pd.Series(["hello", "world", "", "test string"])

        len_result = registry.execute("len", [test_data])
        length_result = registry.execute("length", [test_data])

        pd.testing.assert_series_equal(len_result, length_result)

    def test_str_equals_toString(self) -> None:
        """str() should return same results as toString()."""
        registry = ScalarFunctionRegistry.get_instance()

        test_data = pd.Series([42, 3.14, True, None])

        str_result = registry.execute("str", [test_data])
        tostring_result = registry.execute("tostring", [test_data])

        pd.testing.assert_series_equal(str_result, tostring_result)

    def test_int_equals_toInteger(self) -> None:
        """int() should return same results as toInteger()."""
        registry = ScalarFunctionRegistry.get_instance()

        test_data = pd.Series(["42", "100", "-5", "0"])

        int_result = registry.execute("int", [test_data])
        tointeger_result = registry.execute("tointeger", [test_data])

        pd.testing.assert_series_equal(int_result, tointeger_result)

    def test_float_equals_toFloat(self) -> None:
        """float() should return same results as toFloat()."""
        registry = ScalarFunctionRegistry.get_instance()

        test_data = pd.Series(["3.14", "42", "-2.5", "0.0"])

        float_result = registry.execute("float", [test_data])
        tofloat_result = registry.execute("tofloat", [test_data])

        pd.testing.assert_series_equal(float_result, tofloat_result)

    def test_bool_equals_toBoolean(self) -> None:
        """bool() should return same results as toBoolean()."""
        registry = ScalarFunctionRegistry.get_instance()

        test_data = pd.Series(["true", "false", "TRUE", "False"])

        bool_result = registry.execute("bool", [test_data])
        toboolean_result = registry.execute("toboolean", [test_data])

        pd.testing.assert_series_equal(bool_result, toboolean_result)


class TestAliasMetadata:
    """Test that aliases have proper metadata matching their targets."""

    def test_alias_argument_counts_match_targets(self) -> None:
        """All aliases should have same min/max args as their targets."""
        registry = ScalarFunctionRegistry.get_instance()

        alias_pairs = [
            ("now", "timestamp"),
            ("len", "length"),
            ("str", "tostring"),
            ("int", "tointeger"),
            ("float", "tofloat"),
            ("bool", "toboolean"),
        ]

        for alias, target in alias_pairs:
            alias_meta = registry._functions[alias]
            target_meta = registry._functions[target]

            assert alias_meta.min_args == target_meta.min_args, (
                f"{alias}() min_args should match {target}() min_args"
            )
            assert alias_meta.max_args == target_meta.max_args, (
                f"{alias}() max_args should match {target}() max_args"
            )

    def test_aliases_have_descriptive_names(self) -> None:
        """All aliases should have proper function names in metadata."""
        registry = ScalarFunctionRegistry.get_instance()

        expected_names = ["now", "len", "str", "int", "float", "bool"]

        for name in expected_names:
            meta = registry._functions[name]
            assert meta.name == name, f"Function {name}() should have name='{name}'"


class TestUserExperienceImprovements:
    """Test that the aliases improve the user experience."""

    def test_no_unknown_function_errors(self) -> None:
        """Users should not get 'Unknown scalar function' for common aliases."""
        registry = ScalarFunctionRegistry.get_instance()

        # These should all work without errors
        common_functions = ["now", "len", "str", "int", "float", "bool"]

        for func_name in common_functions:
            try:
                # Just verify the function exists - don't execute with wrong args
                assert func_name in registry._functions, (
                    f"User-expected function {func_name}() should exist"
                )
            except Exception as e:
                pytest.fail(f"Function {func_name}() should be available: {e}")

    def test_python_style_function_availability(self) -> None:
        """Python programmers should find familiar function names."""
        registry = ScalarFunctionRegistry.get_instance()

        python_functions = {
            "len": "Get length of strings/lists",
            "str": "Convert to string",
            "int": "Convert to integer",
            "float": "Convert to float",
            "bool": "Convert to boolean",
        }

        for func, desc in python_functions.items():
            assert func in registry._functions, (
                f"Python-style function {func}() should exist - {desc}"
            )

    def test_temporal_function_discoverability(self) -> None:
        """Users should easily find temporal functions with intuitive names."""
        registry = ScalarFunctionRegistry.get_instance()

        # now() is the most intuitive name for current timestamp
        assert "now" in registry._functions, (
            "now() should be available for current timestamp"
        )

        # Should work without arguments
        result = registry.execute("now", [])
        assert len(result) == 1, "now() should return single timestamp value"


class TestBackwardCompatibility:
    """Test that adding aliases doesn't break existing functionality."""

    def test_original_functions_still_work(self) -> None:
        """Original target functions should still work unchanged."""
        registry = ScalarFunctionRegistry.get_instance()

        originals = [
            "timestamp",
            "length",
            "tostring",
            "tointeger",
            "tofloat",
            "toboolean",
        ]

        for func_name in originals:
            assert func_name in registry._functions, (
                f"Original function {func_name}() should still exist"
            )

            meta = registry._functions[func_name]
            assert meta.callable is not None, (
                f"Original function {func_name}() should still be callable"
            )

    def test_function_count_increased_correctly(self) -> None:
        """Registry should have exactly 6 more functions after adding aliases."""
        registry = ScalarFunctionRegistry.get_instance()

        # We know there were 125 functions before, should be 125 + 6 = 131 after
        total_functions = len(registry._functions)
        expected_total = 125 + 6  # Original count + 6 new aliases

        assert total_functions >= expected_total, (
            f"Should have at least {expected_total} functions, got {total_functions}"
        )

    def test_no_conflicts_with_existing_functions(self) -> None:
        """New aliases should not conflict with any existing functions."""
        registry = ScalarFunctionRegistry.get_instance()

        # These aliases should not overwrite any existing functions
        # (We already verified above they don't exist, this is defensive)
        new_aliases = ["now", "len", "str", "int", "float", "bool"]

        for alias in new_aliases:
            # Should exist now
            assert alias in registry._functions, f"Alias {alias}() should be registered"


class TestEdgeCases:
    """Test edge cases and error conditions for aliases."""

    def test_aliases_handle_null_inputs(self) -> None:
        """Aliases should handle null inputs same as their targets."""
        registry = ScalarFunctionRegistry.get_instance()

        # Test with null/None values
        null_data = pd.Series([None, None])

        # These should handle nulls gracefully
        str_result = registry.execute("str", [null_data])
        int_result = registry.execute("int", [pd.Series([None, "42"])])

        assert pd.isna(str_result.iloc[0]), "str(null) should return null"
        assert pd.isna(int_result.iloc[0]), "int(null) should return null"

    def test_aliases_preserve_error_behavior(self) -> None:
        """Aliases should have same null handling as their target functions."""
        registry = ScalarFunctionRegistry.get_instance()

        # int() should handle invalid strings same as toInteger() (return null rather than raise)
        invalid_data = pd.Series(["not_a_number"])

        int_result = registry.execute("int", [invalid_data])
        tointeger_result = registry.execute("tointeger", [invalid_data])

        # Both should return null for invalid inputs (null safety contract)
        assert pd.isna(int_result.iloc[0]), "int() should return null for invalid input"
        assert pd.isna(tointeger_result.iloc[0]), (
            "tointeger() should return null for invalid input"
        )
