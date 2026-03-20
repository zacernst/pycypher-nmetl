"""TDD tests for the valueType() scalar function (Neo4j 5.x).

Written before the implementation (TDD red phase).

Run with:
    uv run pytest tests/test_value_type_function.py -v
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
    ScalarFunctionRegistry._instance = None  # reset singleton between tests
    return ScalarFunctionRegistry.get_instance()


def _vt(value: object) -> str:
    """Call valueType() on a single scalar value and return the string result."""
    reg = _reg()
    result = reg.execute("valueType", [pd.Series([value])])
    return result.iloc[0]


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestValueTypeRegistration:
    def test_is_registered(self) -> None:
        """valueType is in the registry."""
        assert _reg().has_function("valueType")


# ---------------------------------------------------------------------------
# Scalar types
# ---------------------------------------------------------------------------


class TestValueTypeScalars:
    def test_null_returns_null_string(self) -> None:
        assert _vt(None) == "NULL"

    def test_python_none_null(self) -> None:
        assert _vt(None) == "NULL"

    def test_integer_python_int(self) -> None:
        assert _vt(42) == "INTEGER"

    def test_integer_numpy_int64(self) -> None:
        assert _vt(np.int64(42)) == "INTEGER"

    def test_float_python_float(self) -> None:
        assert _vt(3.14) == "FLOAT"

    def test_float_numpy_float64(self) -> None:
        assert _vt(np.float64(3.14)) == "FLOAT"

    def test_string(self) -> None:
        assert _vt("hello") == "STRING"

    def test_empty_string(self) -> None:
        assert _vt("") == "STRING"

    def test_boolean_true(self) -> None:
        assert _vt(True) == "BOOLEAN"

    def test_boolean_false(self) -> None:
        assert _vt(False) == "BOOLEAN"


# ---------------------------------------------------------------------------
# List types
# ---------------------------------------------------------------------------


class TestValueTypeLists:
    def test_empty_list(self) -> None:
        assert _vt([]) == "LIST<NOTHING>"

    def test_homogeneous_int_list_no_nulls(self) -> None:
        assert _vt([1, 2, 3]) == "LIST<INTEGER NOT NULL>"

    def test_homogeneous_float_list_no_nulls(self) -> None:
        assert _vt([1.1, 2.2]) == "LIST<FLOAT NOT NULL>"

    def test_homogeneous_string_list_no_nulls(self) -> None:
        assert _vt(["a", "b"]) == "LIST<STRING NOT NULL>"

    def test_homogeneous_int_list_with_null(self) -> None:
        assert _vt([1, None, 3]) == "LIST<INTEGER>"

    def test_mixed_type_list(self) -> None:
        assert _vt([1, "a"]) == "LIST<ANY>"

    def test_list_of_all_nulls(self) -> None:
        # A list containing only nulls has no discernible element type
        assert _vt([None, None]) == "LIST<NULL>"


# ---------------------------------------------------------------------------
# Map type
# ---------------------------------------------------------------------------


class TestValueTypeMap:
    def test_empty_dict(self) -> None:
        assert _vt({}) == "MAP"

    def test_non_empty_dict(self) -> None:
        assert _vt({"a": 1, "b": "x"}) == "MAP"


# ---------------------------------------------------------------------------
# Series of mixed values
# ---------------------------------------------------------------------------


class TestValueTypeSeriesVectorized:
    def test_mixed_series(self) -> None:
        reg = _reg()
        result = reg.execute(
            "valueType", [pd.Series([1, "hello", None, 3.14, True])]
        )
        assert list(result) == [
            "INTEGER",
            "STRING",
            "NULL",
            "FLOAT",
            "BOOLEAN",
        ]


# ---------------------------------------------------------------------------
# Cypher integration
# ---------------------------------------------------------------------------


class TestValueTypeCypherIntegration:
    def test_returns_type_string_for_property(self) -> None:
        """MATCH (n:Person) RETURN valueType(n.age) returns INTEGER for int ages."""
        people = pd.DataFrame(
            {
                "__ID__": ["p1", "p2"],
                "name": ["Alice", "Bob"],
                "age": [30, 25],
            }
        )
        star = Star(context=ContextBuilder.from_dict({"Person": people}))
        result = star.execute_query(
            "MATCH (n:Person) RETURN valueType(n.age) AS t"
        )
        assert set(result["t"].tolist()) == {"INTEGER"}

    def test_returns_string_type_for_string_property(self) -> None:
        """valueType(n.name) returns STRING for string properties."""
        people = pd.DataFrame(
            {
                "__ID__": ["p1"],
                "name": ["Alice"],
            }
        )
        star = Star(context=ContextBuilder.from_dict({"Person": people}))
        result = star.execute_query(
            "MATCH (n:Person) RETURN valueType(n.name) AS t"
        )
        assert result["t"].iloc[0] == "STRING"
