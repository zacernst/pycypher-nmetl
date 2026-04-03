"""Unit tests for scalar function registry and implementations.

Tests cover:
- ScalarFunctionRegistry mechanics (singleton, registration, execution)
- String functions (toUpper, toLower, trim, substring, size)
- Type conversion functions (toString, toInteger, toFloat, toBoolean)
- Utility functions (coalesce)
- Integration with ExpressionEvaluator and WITH clauses
- Error handling for invalid inputs
"""

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry


class TestScalarFunctionRegistry:
    """Test scalar function registry mechanics."""

    def test_singleton_pattern(self):
        """Registry follows singleton pattern."""
        r1 = ScalarFunctionRegistry.get_instance()
        r2 = ScalarFunctionRegistry.get_instance()
        assert r1 is r2, "Registry should return same instance"

    def test_register_custom_function(self):
        """Can register and execute custom function."""
        registry = ScalarFunctionRegistry.get_instance()

        # Register custom function
        registry.register_function(
            name="double",
            callable=lambda s: s * 2,
            min_args=1,
            max_args=1,
            description="Double the value",
        )

        # Execute it
        input_series = pd.Series([1, 2, 3])
        result = registry.execute("double", [input_series])

        assert result.tolist() == [2, 4, 6]

    def test_case_insensitive_function_names(self):
        """Function names are case-insensitive."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["hello"])

        result1 = registry.execute("toUpper", [input_series])
        result2 = registry.execute("TOUPPER", [input_series])
        result3 = registry.execute("ToUpPeR", [input_series])

        assert result1.iloc[0] == result2.iloc[0] == result3.iloc[0] == "HELLO"

    def test_has_function(self):
        """has_function() correctly identifies registered functions."""
        registry = ScalarFunctionRegistry.get_instance()

        assert registry.has_function("toUpper")
        assert registry.has_function("TOUPPER")
        assert registry.has_function("toLower")
        assert not registry.has_function("nonExistentFunction")

    def test_argument_count_validation_min(self):
        """Validates minimum argument count."""
        from pycypher.exceptions import FunctionArgumentError

        registry = ScalarFunctionRegistry.get_instance()

        # toUpper requires at least 1 argument
        with pytest.raises(FunctionArgumentError, match="toUpper"):
            registry.execute("toUpper", [])

    def test_argument_count_validation_max(self):
        """Validates maximum argument count."""
        from pycypher.exceptions import FunctionArgumentError

        registry = ScalarFunctionRegistry.get_instance()

        # toUpper accepts at most 1 argument
        input_series = pd.Series(["a", "b"])
        with pytest.raises(FunctionArgumentError, match="toUpper"):
            registry.execute("toUpper", [input_series, input_series])

    def test_unknown_function_error(self):
        """Raises error for unknown function with helpful message."""
        from pycypher.exceptions import UnsupportedFunctionError

        registry = ScalarFunctionRegistry.get_instance()

        with pytest.raises(UnsupportedFunctionError, match="foobar"):
            registry.execute("foobar", [])


class TestStringFunctions:
    """Test built-in string functions."""

    def test_toupper_basic(self):
        """ToUpper converts strings to uppercase."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["hello", "WORLD", "MiXeD"])
        result = registry.execute("toUpper", [input_series])

        assert result.tolist() == ["HELLO", "WORLD", "MIXED"]

    def test_toupper_empty_string(self):
        """ToUpper handles empty strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["", "hello", ""])
        result = registry.execute("toUpper", [input_series])

        assert result.tolist() == ["", "HELLO", ""]

    def test_tolower_basic(self):
        """ToLower converts strings to lowercase."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["HELLO", "world", "MiXeD"])
        result = registry.execute("toLower", [input_series])

        assert result.tolist() == ["hello", "world", "mixed"]

    def test_tolower_numbers(self):
        """ToLower handles strings with numbers."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["ABC123", "def456"])
        result = registry.execute("toLower", [input_series])

        assert result.tolist() == ["abc123", "def456"]

    def test_trim_whitespace(self):
        """Trim removes leading and trailing whitespace."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(
            ["  hello  ", "world", "  spaces  ", "\t\ntabs\n"],
        )
        result = registry.execute("trim", [input_series])

        assert result.tolist() == ["hello", "world", "spaces", "tabs"]

    def test_trim_no_whitespace(self):
        """Trim leaves strings without whitespace unchanged."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["hello", "world"])
        result = registry.execute("trim", [input_series])

        assert result.tolist() == ["hello", "world"]

    def test_substring_with_length(self):
        """Substring extracts substring with specified length."""
        registry = ScalarFunctionRegistry.get_instance()

        input_str = pd.Series(["hello world"])
        start = pd.Series([0])
        length = pd.Series([5])

        result = registry.execute("substring", [input_str, start, length])
        assert result.iloc[0] == "hello"

    def test_substring_without_length(self):
        """Substring extracts from start to end without length."""
        registry = ScalarFunctionRegistry.get_instance()

        input_str = pd.Series(["hello world"])
        start = pd.Series([6])

        result = registry.execute("substring", [input_str, start])
        assert result.iloc[0] == "world"

    def test_substring_zero_start(self):
        """Substring with start=0 extracts from beginning."""
        registry = ScalarFunctionRegistry.get_instance()

        input_str = pd.Series(["hello"])
        start = pd.Series([0])
        length = pd.Series([3])

        result = registry.execute("substring", [input_str, start, length])
        assert result.iloc[0] == "hel"

    def test_substring_multiple_rows(self):
        """Substring works on multiple rows."""
        registry = ScalarFunctionRegistry.get_instance()

        input_str = pd.Series(["hello", "world", "testing"])
        start = pd.Series([1])
        length = pd.Series([3])

        result = registry.execute("substring", [input_str, start, length])
        assert result.tolist() == ["ell", "orl", "est"]

    def test_size_string(self):
        """Size returns string length."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["hello", "hi", "", "testing"])
        result = registry.execute("size", [input_series])

        assert result.tolist() == [5, 2, 0, 7]

    def test_size_list(self):
        """Size returns list length."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series([[1, 2, 3], [4, 5], []])
        result = registry.execute("size", [input_series])

        assert result.tolist() == [3, 2, 0]


class TestConversionFunctions:
    """Test type conversion functions."""

    def test_tostring_integer(self):
        """ToString converts integers to strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series([42, 0, -17])
        result = registry.execute("toString", [input_series])

        assert result.tolist() == ["42", "0", "-17"]

    def test_tostring_float(self):
        """ToString converts floats to strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series([3.14, 2.71, 0.0])
        result = registry.execute("toString", [input_series])

        assert result.tolist() == ["3.14", "2.71", "0.0"]

    def test_tostring_boolean(self):
        """ToString converts booleans to strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series([True, False])
        result = registry.execute("toString", [input_series])

        assert result.tolist() == ["true", "false"]

    def test_tostring_preserves_nulls(self):
        """ToString preserves null values."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series([42, None, 17])
        result = registry.execute("toString", [input_series])

        assert result.iloc[0] == "42"
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == "17"

    def test_tointeger_valid_strings(self):
        """ToInteger converts valid string integers."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["42", "0", "-17"])
        result = registry.execute("toInteger", [input_series])

        assert result.tolist() == [42, 0, -17]

    def test_tointeger_float_strings(self):
        """ToInteger truncates float strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["3.14", "2.99"])
        result = registry.execute("toInteger", [input_series])

        assert result.iloc[0] == 3
        assert result.iloc[1] == 2

    def test_tointeger_invalid_strings(self):
        """ToInteger returns null for invalid strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["abc", "hello", "123abc"])
        result = registry.execute("toInteger", [input_series])

        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])

    def test_tointeger_mixed(self):
        """ToInteger handles mixed valid/invalid inputs."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["42", "invalid", "17"])
        result = registry.execute("toInteger", [input_series])

        assert result.iloc[0] == 42
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 17

    def test_tofloat_valid_strings(self):
        """ToFloat converts valid float strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["3.14", "2.71", "0.0"])
        result = registry.execute("toFloat", [input_series])

        assert result.tolist() == [3.14, 2.71, 0.0]

    def test_tofloat_integer_strings(self):
        """ToFloat converts integer strings to floats."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["42", "17"])
        result = registry.execute("toFloat", [input_series])

        assert result.tolist() == [42.0, 17.0]

    def test_tofloat_invalid_strings(self):
        """ToFloat returns null for invalid strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["abc", "not_a_number"])
        result = registry.execute("toFloat", [input_series])

        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])

    def test_toboolean_true_values(self):
        """ToBoolean converts 'true' strings to True."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["true", "TRUE", "True", "1"])
        result = registry.execute("toBoolean", [input_series])

        assert result.tolist() == [True, True, True, True]

    def test_toboolean_false_values(self):
        """ToBoolean converts 'false' strings to False."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["false", "FALSE", "False", "0"])
        result = registry.execute("toBoolean", [input_series])

        assert result.tolist() == [False, False, False, False]

    def test_toboolean_invalid_values(self):
        """ToBoolean returns null for invalid strings."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["yes", "no", "maybe"])
        result = registry.execute("toBoolean", [input_series])

        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])

    def test_toboolean_preserves_nulls(self):
        """ToBoolean preserves null inputs."""
        registry = ScalarFunctionRegistry.get_instance()

        input_series = pd.Series(["true", None, "false"])
        result = registry.execute("toBoolean", [input_series])

        assert result.iloc[0] is True
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] is False


class TestUtilityFunctions:
    """Test utility functions."""

    def test_coalesce_first_non_null(self):
        """Coalesce returns first non-null value."""
        registry = ScalarFunctionRegistry.get_instance()

        s1 = pd.Series([None, None, "a"])
        s2 = pd.Series([None, "b", "b"])
        s3 = pd.Series(["c", "c", "c"])

        result = registry.execute("coalesce", [s1, s2, s3])

        assert result.tolist() == ["c", "b", "a"]

    def test_coalesce_all_nulls(self):
        """Coalesce returns null when all values are null."""
        registry = ScalarFunctionRegistry.get_instance()

        s1 = pd.Series([None, None])
        s2 = pd.Series([None, None])

        result = registry.execute("coalesce", [s1, s2])

        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])

    def test_coalesce_single_argument(self):
        """Coalesce with single argument returns that argument."""
        registry = ScalarFunctionRegistry.get_instance()

        s1 = pd.Series(["a", None, "c"])

        result = registry.execute("coalesce", [s1])

        assert result.iloc[0] == "a"
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == "c"

    def test_coalesce_multiple_arguments(self):
        """Coalesce handles many arguments."""
        registry = ScalarFunctionRegistry.get_instance()

        s1 = pd.Series([None])
        s2 = pd.Series([None])
        s3 = pd.Series([None])
        s4 = pd.Series(["found"])

        result = registry.execute("coalesce", [s1, s2, s3, s4])

        assert result.iloc[0] == "found"

    def test_coalesce_mixed_types(self):
        """Coalesce works with mixed types."""
        registry = ScalarFunctionRegistry.get_instance()

        s1 = pd.Series([None, None])
        s2 = pd.Series([42, None])
        s3 = pd.Series(["default", "default"])

        result = registry.execute("coalesce", [s1, s2, s3])

        # First row: 42 (from s2)
        # Second row: "default" (from s3)
        assert result.iloc[0] == 42
        assert result.iloc[1] == "default"


class TestFunctionErrorHandling:
    """Test error handling for scalar functions."""

    def test_unknown_function(self):
        """Unknown function produces clear error message."""
        from pycypher.exceptions import UnsupportedFunctionError

        registry = ScalarFunctionRegistry.get_instance()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            registry.execute("unknownFunction", [pd.Series([1])])

        assert exc_info.value.function_name == "unknownFunction"
        assert exc_info.value.category == "scalar"

    def test_too_few_arguments(self):
        """Too few arguments produces clear error."""
        from pycypher.exceptions import FunctionArgumentError

        registry = ScalarFunctionRegistry.get_instance()

        with pytest.raises(FunctionArgumentError) as exc_info:
            registry.execute("toUpper", [])

        assert exc_info.value.function_name == "toUpper"
        assert exc_info.value.actual_args == 0

    def test_too_many_arguments(self):
        """Too many arguments produces clear error."""
        from pycypher.exceptions import FunctionArgumentError

        registry = ScalarFunctionRegistry.get_instance()

        s1 = pd.Series(["a"])
        s2 = pd.Series(["b"])

        with pytest.raises(FunctionArgumentError) as exc_info:
            registry.execute("toUpper", [s1, s2])

        assert exc_info.value.function_name == "toUpper"
        assert exc_info.value.actual_args == 2

    def test_return_type_validation(self):
        """Functions must return pd.Series — wrong return type raises TypeError."""
        registry = ScalarFunctionRegistry.get_instance()

        # Register a badly behaved function that returns wrong type
        registry.register_function(
            name="badFunction",
            callable=lambda s: (
                "wrong_type"
            ),  # Returns string instead of Series
            min_args=1,
            max_args=1,
        )

        with pytest.raises(TypeError) as exc_info:
            registry.execute("badFunction", [pd.Series([1])])

        assert "must return pd.Series" in str(exc_info.value)


class TestScalarFunctionEdgeCases:
    """Edge cases for additional coverage."""

    def test_overwrite_existing_function_warning(self):
        """Re-registering a function by name triggers a warning log."""
        registry = ScalarFunctionRegistry.get_instance()

        # Register a custom function
        registry.register_function(
            name="myCustom",
            callable=lambda s: s * 2,
            min_args=1,
            max_args=1,
            description="first version",
        )
        # Register again with same name — should overwrite (hits warning branch)
        registry.register_function(
            name="myCustom",
            callable=lambda s: s * 3,
            min_args=1,
            max_args=1,
            description="second version",
        )
        # Verify the second version is active
        result = registry.execute("mycustom", [pd.Series([5])])
        assert result.tolist() == [15]

    def test_size_numeric_works(self):
        """size() on a numeric Series converts to string and returns length."""
        registry = ScalarFunctionRegistry.get_instance()
        input_series = pd.Series([10, 200, 3000])
        result = registry.execute("size", [input_series])
        # "10" → 2, "200" → 3, "3000" → 4
        expected = pd.Series([2, 3, 4])
        pd.testing.assert_series_equal(result, expected)
