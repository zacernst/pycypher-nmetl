"""TDD tests for infinity(), isInfinite(), isFinite() scalar functions.

Neo4j/openCypher spec:
  - infinity()            — positive infinity (Float)
  - isInfinite(x)         — true iff x is +Inf or -Inf
  - isFinite(x)           — true iff x is finite (not Inf, not NaN)
  - isNaN(x)              — already implemented (regression test only)

These functions are particularly useful for validating graph data after
arithmetic operations and for writing robust WHERE predicates.

All tests written before the fix (TDD step 1).
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


@pytest.fixture()
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture()
def numeric_star() -> Star:
    """Entities with finite, infinite, and NaN float values."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "score": [10.0, float("inf"), float("-inf"), float("nan")],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "score"],
        source_obj_attribute_map={"name": "name", "score": "score"},
        attribute_map={"name": "name", "score": "score"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ---------------------------------------------------------------------------
# infinity()
# ---------------------------------------------------------------------------


class TestInfinity:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("infinity")

    def test_returns_positive_infinity(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        # zero-arg: the evaluator calls func_meta.callable(dummy) directly.
        # Replicate that here by accessing the internal callable.
        fn = reg._functions["infinity"].callable
        result = fn(pd.Series([0, 1, 2]))  # dummy series of length 3
        assert all(math.isinf(v) and v > 0 for v in result)

    def test_returns_float_dtype(self, reg: ScalarFunctionRegistry) -> None:
        fn = reg._functions["infinity"].callable
        result = fn(pd.Series([0]))
        assert pd.api.types.is_float_dtype(result)

    def test_callable_produces_n_values(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        """The callable produces one +inf per element of the dummy series."""
        fn = reg._functions["infinity"].callable
        result = fn(pd.Series(range(5)))
        assert len(result) == 5
        assert all(math.isinf(v) and v > 0 for v in result)

    def test_in_where_clause(self, numeric_star: Star) -> None:
        """Filter by explicit arithmetic infinity (1.0/0.0 evaluates to inf)."""
        r = numeric_star.execute_query(
            "MATCH (p:Person) WHERE p.score = 1.0/0.0 RETURN p.name"
        )
        assert list(r["name"]) == ["Bob"]


# ---------------------------------------------------------------------------
# isInfinite()
# ---------------------------------------------------------------------------


class TestIsInfinite:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("isInfinite")

    def test_positive_infinity(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([float("inf")])
        assert reg.execute("isInfinite", [s]).iloc[0] == True  # noqa: E712

    def test_negative_infinity(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([float("-inf")])
        assert reg.execute("isInfinite", [s]).iloc[0] == True  # noqa: E712

    def test_finite_value(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([42.0])
        result = reg.execute("isInfinite", [s]).iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_nan_is_not_infinite(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([float("nan")])
        result = reg.execute("isInfinite", [s]).iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_null_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([None])
        assert pd.isna(reg.execute("isInfinite", [s]).iloc[0])

    def test_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([10.0, float("inf"), float("-inf"), float("nan")])
        result = list(reg.execute("isInfinite", [s]))
        assert result[0] is False or result[0] == False  # noqa: E712
        assert result[1] is True
        assert result[2] is True
        assert result[3] is False or result[3] == False  # noqa: E712

    def test_in_where_clause(self, numeric_star: Star) -> None:
        r = numeric_star.execute_query(
            "MATCH (p:Person) WHERE isInfinite(p.score) RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Bob", "Carol"]


# ---------------------------------------------------------------------------
# isFinite()
# ---------------------------------------------------------------------------


class TestIsFinite:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("isFinite")

    def test_finite_value(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([42.0])
        assert reg.execute("isFinite", [s]).iloc[0] == True  # noqa: E712

    def test_positive_infinity_not_finite(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series([float("inf")])
        result = reg.execute("isFinite", [s]).iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_negative_infinity_not_finite(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series([float("-inf")])
        result = reg.execute("isFinite", [s]).iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_nan_not_finite(self, reg: ScalarFunctionRegistry) -> None:
        """NaN is not finite per IEEE 754."""
        s = pd.Series([float("nan")])
        result = reg.execute("isFinite", [s]).iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_null_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([None])
        assert pd.isna(reg.execute("isFinite", [s]).iloc[0])

    def test_zero_is_finite(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.0])
        assert reg.execute("isFinite", [s]).iloc[0] == True  # noqa: E712

    def test_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([10.0, float("inf"), float("-inf"), float("nan")])
        result = list(reg.execute("isFinite", [s]))
        assert result[0] == True  # noqa: E712
        assert result[1] is False or result[1] == False  # noqa: E712
        assert result[2] is False or result[2] == False  # noqa: E712
        assert result[3] is False or result[3] == False  # noqa: E712

    def test_in_where_clause(self, numeric_star: Star) -> None:
        r = numeric_star.execute_query(
            "MATCH (p:Person) WHERE isFinite(p.score) RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Alice"]

    def test_isfinite_isinfinite_partition(self, numeric_star: Star) -> None:
        """A finite value is NOT infinite, and vice versa (NaN is neither)."""
        r_fin = numeric_star.execute_query(
            "MATCH (p:Person) WHERE isFinite(p.score) RETURN p.name ORDER BY p.name"
        )
        r_inf = numeric_star.execute_query(
            "MATCH (p:Person) WHERE isInfinite(p.score) RETURN p.name ORDER BY p.name"
        )
        # Alice is finite, Bob and Carol are infinite, Dave (NaN) is neither
        assert set(r_fin["name"]) & set(r_inf["name"]) == set()


# ---------------------------------------------------------------------------
# isNaN regression
# ---------------------------------------------------------------------------


class TestIsNaNRegression:
    def test_isnan_still_works(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([float("nan"), 1.0, None])
        result = reg.execute("isNaN", [s])
        assert result.iloc[0] == True  # noqa: E712
        assert result.iloc[1] == False  # noqa: E712
