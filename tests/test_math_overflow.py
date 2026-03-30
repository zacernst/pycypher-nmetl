"""Consolidated tests for math function overflow handling.

Covers exp() and pow() overflow behavior:
- exp(1000) -> +infinity (not RuntimeError)
- exp(-1000) -> 0.0 (underflow)
- pow(10, 400) -> +infinity (not RuntimeError)
- No RuntimeWarning emissions
- Null propagation through both functions

Consolidated from: test_exp_overflow.py, test_pow_overflow.py
"""

from __future__ import annotations

import math
import warnings

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


@pytest.fixture
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def exp_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "val": [1.0, 1000.0, -1000.0],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "val"],
        source_obj_attribute_map={"name": "name", "val": "val"},
        attribute_map={"name": "name", "val": "val"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# ===========================================================================
# exp() overflow
# ===========================================================================


class TestExpOverflow:
    def test_overflow_does_not_crash(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        reg.execute("exp", [pd.Series([1000.0])])

    def test_overflow_returns_positive_infinity(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("exp", [pd.Series([1000.0])])
        assert math.isinf(result.iloc[0]) and result.iloc[0] > 0

    def test_large_negative_returns_zero(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("exp", [pd.Series([-1000.0])])
        assert result.iloc[0] == pytest.approx(0.0, abs=1e-100)

    def test_normal_values_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("exp", [pd.Series([1.0])])
        assert result.iloc[0] == pytest.approx(math.e)

    def test_zero_returns_one(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("exp", [pd.Series([0.0])])
        assert result.iloc[0] == pytest.approx(1.0)

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("exp", [pd.Series([None])])
        assert pd.isna(result.iloc[0])

    def test_mixed_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.0, 1000.0, None, 1.0])
        result = reg.execute("exp", [s])
        assert result.iloc[0] == pytest.approx(1.0)
        assert math.isinf(result.iloc[1]) and result.iloc[1] > 0
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] == pytest.approx(math.e)

    def test_in_return_clause(self, exp_star: Star) -> None:
        r = exp_star.execute_query(
            "MATCH (p:Person) RETURN exp(p.val) AS e ORDER BY p.name",
        )
        vals = list(r["e"])
        assert vals[0] == pytest.approx(math.e)
        assert math.isinf(vals[1]) and vals[1] > 0
        assert vals[2] == pytest.approx(0.0, abs=1e-100)

    def test_isfinite_of_overflow_is_false(self, exp_star: Star) -> None:
        r = exp_star.execute_query(
            "MATCH (p:Person) WHERE p.val = 1000.0 RETURN isFinite(exp(p.val)) AS r",
        )
        result_val = r["r"].iloc[0]
        assert result_val is False or result_val == False

    def test_overflow_emits_no_runtime_warning(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            reg.execute("exp", [pd.Series([1000.0])])

    def test_mixed_column_emits_no_runtime_warning(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            reg.execute("exp", [pd.Series([0.0, 1000.0, None, 1.0])])


# ===========================================================================
# pow() overflow
# ===========================================================================


class TestPowOverflow:
    def test_overflow_does_not_crash(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        reg.execute("pow", [pd.Series([10.0]), pd.Series([400.0])])

    def test_overflow_returns_infinity(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("pow", [pd.Series([10.0]), pd.Series([400.0])])
        assert math.isinf(result.iloc[0]) and result.iloc[0] > 0

    def test_normal_values_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("pow", [pd.Series([2.0]), pd.Series([10.0])])
        assert result.iloc[0] == pytest.approx(1024.0)

    def test_null_base_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("pow", [pd.Series([None]), pd.Series([2.0])])
        assert pd.isna(result.iloc[0])

    def test_null_exponent_propagates(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("pow", [pd.Series([2.0]), pd.Series([None])])
        assert pd.isna(result.iloc[0])

    def test_zero_exponent_returns_one(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("pow", [pd.Series([5.0]), pd.Series([0.0])])
        assert result.iloc[0] == pytest.approx(1.0)

    def test_negative_base_positive_exponent(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("pow", [pd.Series([-2.0]), pd.Series([3.0])])
        assert result.iloc[0] == pytest.approx(-8.0)
