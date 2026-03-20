"""TDD tests for Neo4j 5.x type predicate functions.

isString(x), isInteger(x), isFloat(x), isBoolean(x), isList(x), isMap(x)
all return a boolean (or null for null input).

Written before the implementation (TDD red phase).

Run with:
    uv run pytest tests/test_type_predicate_functions.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pycypher import Star
from pycypher.ingestion import ContextBuilder
from pycypher.scalar_functions import ScalarFunctionRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg() -> ScalarFunctionRegistry:
    ScalarFunctionRegistry._instance = None
    return ScalarFunctionRegistry.get_instance()


def _call(fn_name: str, value: object) -> object:
    """Call a type predicate on a single value."""
    reg = _reg()
    result = reg.execute(fn_name.lower(), [pd.Series([value])])
    return result.iloc[0]


def _star() -> Star:
    # Use object dtype to avoid DuckDB type-inference conflicts with mixed columns
    str_df = pd.DataFrame(
        {
            "__ID__": ["r1", "r2"],
            "code": pd.array(["hello", "world"], dtype=object),
        }
    )
    int_df = pd.DataFrame(
        {"__ID__": ["i1", "i2"], "count": pd.array([10, 20], dtype=object)}
    )
    ctx = ContextBuilder.from_dict({"Str": str_df, "Int": int_df})
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# isString
# ---------------------------------------------------------------------------


class TestIsString:
    def test_registered(self) -> None:
        assert _reg().has_function("isstring")

    def test_string_returns_true(self) -> None:
        assert (
            _call("isString", "hello") is True
            or _call("isString", "hello") == True
        )  # noqa: E712

    def test_integer_returns_false(self) -> None:
        assert _call("isString", 42) == False  # noqa: E712

    def test_float_returns_false(self) -> None:
        assert _call("isString", 3.14) == False  # noqa: E712

    def test_bool_returns_false(self) -> None:
        assert _call("isString", True) == False  # noqa: E712

    def test_list_returns_false(self) -> None:
        assert _call("isString", [1, 2]) == False  # noqa: E712

    def test_null_returns_null(self) -> None:
        result = _call("isString", None)
        assert result is None or pd.isna(result)

    def test_empty_string_is_string(self) -> None:
        assert _call("isString", "") == True  # noqa: E712


# ---------------------------------------------------------------------------
# isInteger
# ---------------------------------------------------------------------------


class TestIsInteger:
    def test_registered(self) -> None:
        assert _reg().has_function("isinteger")

    def test_int_returns_true(self) -> None:
        assert _call("isInteger", 42) == True  # noqa: E712

    def test_numpy_int64_returns_true(self) -> None:
        assert _call("isInteger", np.int64(99)) == True  # noqa: E712

    def test_float_returns_false(self) -> None:
        assert _call("isInteger", 3.14) == False  # noqa: E712

    def test_bool_returns_false(self) -> None:
        """bool is a subclass of int — isInteger(True) must return false."""
        assert _call("isInteger", True) == False  # noqa: E712

    def test_string_returns_false(self) -> None:
        assert _call("isInteger", "42") == False  # noqa: E712

    def test_null_returns_null(self) -> None:
        result = _call("isInteger", None)
        assert result is None or pd.isna(result)


# ---------------------------------------------------------------------------
# isFloat
# ---------------------------------------------------------------------------


class TestIsFloat:
    def test_registered(self) -> None:
        assert _reg().has_function("isfloat")

    def test_float_returns_true(self) -> None:
        assert _call("isFloat", 3.14) == True  # noqa: E712

    def test_numpy_float64_returns_true(self) -> None:
        assert _call("isFloat", np.float64(2.5)) == True  # noqa: E712

    def test_nan_returns_true(self) -> None:
        """float('nan') is a float value — isFloat(nan) must return true."""
        assert _call("isFloat", float("nan")) == True  # noqa: E712

    def test_integer_returns_false(self) -> None:
        assert _call("isFloat", 42) == False  # noqa: E712

    def test_string_returns_false(self) -> None:
        assert _call("isFloat", "3.14") == False  # noqa: E712

    def test_bool_returns_false(self) -> None:
        assert _call("isFloat", True) == False  # noqa: E712

    def test_null_returns_null(self) -> None:
        result = _call("isFloat", None)
        assert result is None or pd.isna(result)


# ---------------------------------------------------------------------------
# isBoolean
# ---------------------------------------------------------------------------


class TestIsBoolean:
    def test_registered(self) -> None:
        assert _reg().has_function("isboolean")

    def test_true_returns_true(self) -> None:
        assert _call("isBoolean", True) == True  # noqa: E712

    def test_false_returns_true(self) -> None:
        assert _call("isBoolean", False) == True  # noqa: E712

    def test_numpy_bool_returns_true(self) -> None:
        assert _call("isBoolean", np.bool_(True)) == True  # noqa: E712

    def test_integer_1_returns_false(self) -> None:
        """1 is not a boolean in Cypher — only true/false literals are."""
        assert _call("isBoolean", 1) == False  # noqa: E712

    def test_string_returns_false(self) -> None:
        assert _call("isBoolean", "true") == False  # noqa: E712

    def test_null_returns_null(self) -> None:
        result = _call("isBoolean", None)
        assert result is None or pd.isna(result)


# ---------------------------------------------------------------------------
# isList
# ---------------------------------------------------------------------------


class TestIsList:
    def test_registered(self) -> None:
        assert _reg().has_function("islist")

    def test_list_returns_true(self) -> None:
        assert _call("isList", [1, 2, 3]) == True  # noqa: E712

    def test_empty_list_returns_true(self) -> None:
        assert _call("isList", []) == True  # noqa: E712

    def test_dict_returns_false(self) -> None:
        assert _call("isList", {"a": 1}) == False  # noqa: E712

    def test_string_returns_false(self) -> None:
        assert _call("isList", "abc") == False  # noqa: E712

    def test_integer_returns_false(self) -> None:
        assert _call("isList", 42) == False  # noqa: E712

    def test_null_returns_null(self) -> None:
        result = _call("isList", None)
        assert result is None or pd.isna(result)


# ---------------------------------------------------------------------------
# isMap
# ---------------------------------------------------------------------------


class TestIsMap:
    def test_registered(self) -> None:
        assert _reg().has_function("ismap")

    def test_dict_returns_true(self) -> None:
        assert _call("isMap", {"a": 1, "b": 2}) == True  # noqa: E712

    def test_empty_dict_returns_true(self) -> None:
        assert _call("isMap", {}) == True  # noqa: E712

    def test_list_returns_false(self) -> None:
        assert _call("isMap", [1, 2]) == False  # noqa: E712

    def test_string_returns_false(self) -> None:
        assert _call("isMap", "hello") == False  # noqa: E712

    def test_integer_returns_false(self) -> None:
        assert _call("isMap", 42) == False  # noqa: E712

    def test_null_returns_null(self) -> None:
        result = _call("isMap", None)
        assert result is None or pd.isna(result)


# ---------------------------------------------------------------------------
# Vectorized series
# ---------------------------------------------------------------------------


class TestTypePredicateVectorized:
    def test_isstring_mixed_series(self) -> None:
        reg = _reg()
        s = pd.Series(["hello", 42, None, True, []])
        result = reg.execute("isstring", [s])
        assert list(result) == [True, False, None, False, False]

    def test_isinteger_mixed_series(self) -> None:
        reg = _reg()
        s = pd.Series([1, "x", None, True, 3.0])
        result = reg.execute("isinteger", [s])
        assert list(result) == [True, False, None, False, False]

    def test_isfloat_mixed_series(self) -> None:
        reg = _reg()
        s = pd.Series([1.5, 1, None, "x", float("nan")])
        result = reg.execute("isfloat", [s])
        assert list(result) == [True, False, None, False, True]

    def test_isboolean_mixed_series(self) -> None:
        reg = _reg()
        s = pd.Series([True, False, None, 1, "true"])
        result = reg.execute("isboolean", [s])
        assert list(result) == [True, True, None, False, False]


# ---------------------------------------------------------------------------
# Cypher integration
# ---------------------------------------------------------------------------


class TestTypePredicateCypherIntegration:
    def test_isstring_where_filter(self) -> None:
        """WHERE isString(n.code) keeps rows where code is a string."""
        star = _star()
        result = star.execute_query(
            "MATCH (n:Str) WHERE isString(n.code) RETURN n.code AS v"
        )
        assert set(result["v"].tolist()) == {"hello", "world"}

    def test_isinteger_where_filter(self) -> None:
        """WHERE isInteger(n.count) keeps rows where count is an integer."""
        star = _star()
        result = star.execute_query(
            "MATCH (n:Int) WHERE isInteger(n.count) RETURN n.count AS v"
        )
        assert set(result["v"].tolist()) == {10, 20}

    def test_isstring_false_filters_out_non_strings(self) -> None:
        """WHERE NOT isString(n.count) when count holds integers → all rows pass."""
        star = _star()
        result = star.execute_query(
            "MATCH (n:Int) WHERE NOT isString(n.count) RETURN n.count AS v"
        )
        assert len(result) == 2
