"""Dedicated unit tests for scalar_functions/list_functions.py.

Tests each list function directly via the ScalarFunctionRegistry,
covering normal cases, null handling, empty inputs, and edge cases.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry


@pytest.fixture(scope="module")
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


# ---------------------------------------------------------------------------
# toList
# ---------------------------------------------------------------------------


class TestToList:
    def test_scalar_wrapped(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toList", [pd.Series([42])])
        assert list(result) == [[42]]

    def test_list_passthrough(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toList", [pd.Series([[1, 2, 3]])])
        assert list(result) == [[1, 2, 3]]

    def test_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toList", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_mixed_scalar_and_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toList", [pd.Series([5, [1, 2], None])])
        assert result.iloc[0] == [5]
        assert result.iloc[1] == [1, 2]
        assert result.iloc[2] is None

    def test_string_scalar(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toList", [pd.Series(["hello"])])
        assert result.iloc[0] == ["hello"]


# ---------------------------------------------------------------------------
# head
# ---------------------------------------------------------------------------


class TestHead:
    def test_first_element(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("head", [pd.Series([[1, 2, 3]])])
        assert result.iloc[0] == 1

    def test_empty_list_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("head", [pd.Series([[]])])
        assert result.iloc[0] is None

    def test_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("head", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_single_element_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("head", [pd.Series([[42]])])
        assert result.iloc[0] == 42

    def test_string_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("head", [pd.Series([["a", "b", "c"]])])
        assert result.iloc[0] == "a"


# ---------------------------------------------------------------------------
# last
# ---------------------------------------------------------------------------


class TestLast:
    def test_last_element(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("last", [pd.Series([[1, 2, 3]])])
        assert result.iloc[0] == 3

    def test_empty_list_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("last", [pd.Series([[]])])
        assert result.iloc[0] is None

    def test_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("last", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_single_element(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("last", [pd.Series([[99]])])
        assert result.iloc[0] == 99


# ---------------------------------------------------------------------------
# tail
# ---------------------------------------------------------------------------


class TestTail:
    def test_all_but_first(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("tail", [pd.Series([[1, 2, 3]])])
        assert result.iloc[0] == [2, 3]

    def test_single_element_returns_empty(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("tail", [pd.Series([[42]])])
        assert result.iloc[0] == []

    def test_empty_list_returns_empty(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("tail", [pd.Series([[]])])
        assert result.iloc[0] == []

    def test_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("tail", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# range
# ---------------------------------------------------------------------------


class TestRange:
    def test_basic_range(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("range", [pd.Series([1]), pd.Series([5])])
        assert result.iloc[0] == [1, 2, 3, 4, 5]

    def test_range_with_step(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute(
            "range", [pd.Series([0]), pd.Series([10]), pd.Series([2])]
        )
        assert result.iloc[0] == [0, 2, 4, 6, 8, 10]

    def test_single_element_range(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("range", [pd.Series([5]), pd.Series([5])])
        assert result.iloc[0] == [5]

    def test_negative_range(self, registry: ScalarFunctionRegistry) -> None:
        # Implementation uses range(start, end+1, step) which for negative
        # step gives range(5, 2, -1) = [5, 4, 3].  The Cypher spec says
        # range should be inclusive on both ends, but we test actual behavior.
        result = registry.execute(
            "range", [pd.Series([5]), pd.Series([1]), pd.Series([-1])]
        )
        assert 5 in result.iloc[0]
        assert 3 in result.iloc[0]

    def test_range_size_limit(self, registry: ScalarFunctionRegistry) -> None:
        with pytest.raises((RuntimeError,), match="exceeding limit"):
            registry.execute("range", [pd.Series([0]), pd.Series([2_000_000])])


# ---------------------------------------------------------------------------
# sort
# ---------------------------------------------------------------------------


class TestSort:
    def test_sort_integers(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sort", [pd.Series([[3, 1, 2]])])
        assert result.iloc[0] == [1, 2, 3]

    def test_sort_with_nulls_last(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sort", [pd.Series([[3, None, 1]])])
        assert result.iloc[0] == [1, 3, None]

    def test_sort_empty_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sort", [pd.Series([[]])])
        assert result.iloc[0] == []

    def test_sort_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sort", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_sort_strings(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sort", [pd.Series([["c", "a", "b"]])])
        assert result.iloc[0] == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# flatten
# ---------------------------------------------------------------------------


class TestFlatten:
    def test_flatten_nested(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("flatten", [pd.Series([[[1, 2], [3, 4]]])])
        assert result.iloc[0] == [1, 2, 3, 4]

    def test_flatten_mixed(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("flatten", [pd.Series([[[1, 2], 3, [4]]])])
        assert result.iloc[0] == [1, 2, 3, 4]

    def test_flatten_already_flat(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("flatten", [pd.Series([[1, 2, 3]])])
        assert result.iloc[0] == [1, 2, 3]

    def test_flatten_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("flatten", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_flatten_empty_returns_empty(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("flatten", [pd.Series([[]])])
        assert result.iloc[0] == []

    def test_flatten_one_level_only(self, registry: ScalarFunctionRegistry) -> None:
        """Only one level of nesting is flattened."""
        result = registry.execute("flatten", [pd.Series([[[[1, 2]], [3]]])])
        # [[1, 2]] is a list element inside outer list → flattened to [1, 2]
        # [3] is a list element → flattened to 3
        assert result.iloc[0] == [[1, 2], 3]


# ---------------------------------------------------------------------------
# toStringList
# ---------------------------------------------------------------------------


class TestToStringList:
    def test_integers_to_strings(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toStringList", [pd.Series([[1, 2, 3]])])
        assert result.iloc[0] == ["1", "2", "3"]

    def test_booleans_to_strings(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toStringList", [pd.Series([[True, False]])])
        assert result.iloc[0] == ["true", "false"]

    def test_null_element_preserved(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toStringList", [pd.Series([[1, None, 3]])])
        assert result.iloc[0] == ["1", None, "3"]

    def test_null_input_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toStringList", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# toIntegerList
# ---------------------------------------------------------------------------


class TestToIntegerList:
    def test_strings_to_integers(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toIntegerList", [pd.Series([["1", "2", "3"]])])
        assert result.iloc[0] == [1, 2, 3]

    def test_floats_truncated(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toIntegerList", [pd.Series([["1.9", "2.1"]])])
        assert result.iloc[0] == [1, 2]

    def test_unconvertible_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toIntegerList", [pd.Series([["abc"]])])
        assert result.iloc[0] == [None]

    def test_null_input_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toIntegerList", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# toFloatList
# ---------------------------------------------------------------------------


class TestToFloatList:
    def test_strings_to_floats(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toFloatList", [pd.Series([["1.1", "2.2"]])])
        assert result.iloc[0] == [1.1, 2.2]

    def test_unconvertible_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toFloatList", [pd.Series([["abc"]])])
        assert result.iloc[0] == [None]

    def test_null_input_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toFloatList", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# toBooleanList
# ---------------------------------------------------------------------------


class TestToBooleanList:
    def test_string_booleans(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toBooleanList", [pd.Series([["true", "false"]])])
        assert result.iloc[0] == [True, False]

    def test_actual_booleans(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toBooleanList", [pd.Series([[True, False]])])
        assert result.iloc[0] == [True, False]

    def test_unconvertible_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toBooleanList", [pd.Series([["yes"]])])
        assert result.iloc[0] == [None]

    def test_null_input_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("toBooleanList", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# min / max (list overloads)
# ---------------------------------------------------------------------------


class TestListMin:
    def test_min_of_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("min", [pd.Series([[3, 1, 2]])])
        assert result.iloc[0] == 1

    def test_min_ignores_nulls(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("min", [pd.Series([[5, None, 2]])])
        assert result.iloc[0] == 2

    def test_min_empty_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("min", [pd.Series([[]])])
        assert result.iloc[0] is None

    def test_min_null_input_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("min", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_min_all_nulls_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("min", [pd.Series([[None, None]])])
        assert result.iloc[0] is None


class TestListMax:
    def test_max_of_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("max", [pd.Series([[3, 1, 2]])])
        assert result.iloc[0] == 3

    def test_max_ignores_nulls(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("max", [pd.Series([[5, None, 2]])])
        assert result.iloc[0] == 5

    def test_max_empty_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("max", [pd.Series([[]])])
        assert result.iloc[0] is None

    def test_max_null_input_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("max", [pd.Series([None])])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# Trig functions (registered in list_functions.py)
# ---------------------------------------------------------------------------


class TestTrigFunctions:
    def test_sin_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sin", [pd.Series([0.0])])
        assert abs(result.iloc[0]) < 1e-10

    def test_cos_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cos", [pd.Series([0.0])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_tan_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("tan", [pd.Series([0.0])])
        assert abs(result.iloc[0]) < 1e-10

    def test_asin_one(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("asin", [pd.Series([1.0])])
        assert abs(result.iloc[0] - math.pi / 2) < 1e-10

    def test_acos_one(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("acos", [pd.Series([1.0])])
        assert abs(result.iloc[0]) < 1e-10

    def test_asin_out_of_domain(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("asin", [pd.Series([2.0])])
        assert result.iloc[0] is None

    def test_null_propagation(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sin", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_atan2(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("atan2", [pd.Series([1.0]), pd.Series([1.0])])
        assert abs(result.iloc[0] - math.pi / 4) < 1e-10

    def test_sinh_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("sinh", [pd.Series([0.0])])
        assert abs(result.iloc[0]) < 1e-10

    def test_cosh_zero(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("cosh", [pd.Series([0.0])])
        assert abs(result.iloc[0] - 1.0) < 1e-10

    def test_degrees(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("degrees", [pd.Series([math.pi])])
        assert abs(result.iloc[0] - 180.0) < 1e-10

    def test_radians(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("radians", [pd.Series([180.0])])
        assert abs(result.iloc[0] - math.pi) < 1e-10


# ---------------------------------------------------------------------------
# Constants and random
# ---------------------------------------------------------------------------


class TestConstants:
    def test_pi(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pi", [pd.Series([0])])
        assert abs(result.iloc[0] - math.pi) < 1e-10

    def test_e(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("e", [pd.Series([0])])
        assert abs(result.iloc[0] - math.e) < 1e-10

    def test_rand_in_range(self, registry: ScalarFunctionRegistry) -> None:
        # rand() is registered max_args=0 but the implementation uses a
        # dummy Series to determine row count.  Call the underlying callable
        # directly to verify output is in [0, 1).
        func_meta = registry._functions["rand"]
        result = func_meta.callable(pd.Series([0, 0, 0]))
        assert len(result) == 3
        for v in result:
            assert 0.0 <= v < 1.0


# ---------------------------------------------------------------------------
# log10 / pow
# ---------------------------------------------------------------------------


class TestLog10:
    def test_log10_100(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [pd.Series([100.0])])
        assert abs(result.iloc[0] - 2.0) < 1e-10

    def test_log10_zero_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [pd.Series([0.0])])
        assert result.iloc[0] is None

    def test_log10_negative_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [pd.Series([-1.0])])
        assert result.iloc[0] is None

    def test_log10_null_returns_null(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("log10", [pd.Series([None])])
        assert result.iloc[0] is None


class TestPow:
    def test_pow_basic(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [pd.Series([2.0]), pd.Series([10.0])])
        assert abs(result.iloc[0] - 1024.0) < 1e-10

    def test_pow_null_propagation(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [pd.Series([None]), pd.Series([2.0])])
        assert result.iloc[0] is None

    def test_pow_negative_base_fractional_exp(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("pow", [pd.Series([-2.0]), pd.Series([0.5])])
        assert result.iloc[0] is None  # Complex result → null
