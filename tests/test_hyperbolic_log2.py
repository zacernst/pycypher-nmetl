"""TDD tests for sinh(), cosh(), tanh(), log2() scalar functions.

All four are standard Neo4j/openCypher math functions absent from the registry.

  sinh(x)  — hyperbolic sine
  cosh(x)  — hyperbolic cosine
  tanh(x)  — hyperbolic tangent
  log2(x)  — base-2 logarithm (real, x > 0; null for x ≤ 0)

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
def math_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "x": [0.0, 1.0, -1.0],
            "pos": [1.0, 4.0, 8.0],  # for log2 (must be > 0)
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "x", "pos"],
        source_obj_attribute_map={"name": "name", "x": "x", "pos": "pos"},
        attribute_map={"name": "name", "x": "x", "pos": "pos"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ---------------------------------------------------------------------------
# sinh
# ---------------------------------------------------------------------------


class TestSinh:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("sinh")

    def test_zero(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("sinh", [pd.Series([0.0])]).iloc[
            0
        ] == pytest.approx(0.0)

    def test_one(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("sinh", [pd.Series([1.0])]).iloc[
            0
        ] == pytest.approx(math.sinh(1.0))

    def test_negative(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("sinh", [pd.Series([-1.0])]).iloc[
            0
        ] == pytest.approx(math.sinh(-1.0))

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        assert pd.isna(reg.execute("sinh", [pd.Series([None])]).iloc[0])

    def test_returns_numeric(self, reg: ScalarFunctionRegistry) -> None:
        # Result is object dtype (consistent with sin/cos/tan) but values are float
        val = reg.execute("sinh", [pd.Series([0.0])]).iloc[0]
        assert isinstance(float(val), float)

    def test_in_return_clause(self, math_star: Star) -> None:
        r = math_star.execute_query(
            "MATCH (p:Person) RETURN sinh(p.x) AS s ORDER BY p.name"
        )
        vals = list(r["s"])
        assert vals[0] == pytest.approx(math.sinh(0.0))  # Alice  x=0.0
        assert vals[1] == pytest.approx(math.sinh(1.0))  # Bob   x=1.0

    def test_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.0, 1.0, -1.0])
        result = reg.execute("sinh", [s])
        assert result.iloc[0] == pytest.approx(math.sinh(0.0))
        assert result.iloc[1] == pytest.approx(math.sinh(1.0))
        assert result.iloc[2] == pytest.approx(math.sinh(-1.0))


# ---------------------------------------------------------------------------
# cosh
# ---------------------------------------------------------------------------


class TestCosh:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("cosh")

    def test_zero(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("cosh", [pd.Series([0.0])]).iloc[
            0
        ] == pytest.approx(1.0)

    def test_one(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("cosh", [pd.Series([1.0])]).iloc[
            0
        ] == pytest.approx(math.cosh(1.0))

    def test_negative(self, reg: ScalarFunctionRegistry) -> None:
        # cosh is even: cosh(-x) == cosh(x)
        r1 = reg.execute("cosh", [pd.Series([1.0])]).iloc[0]
        r2 = reg.execute("cosh", [pd.Series([-1.0])]).iloc[0]
        assert r1 == pytest.approx(r2)

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        assert pd.isna(reg.execute("cosh", [pd.Series([None])]).iloc[0])

    def test_returns_numeric(self, reg: ScalarFunctionRegistry) -> None:
        # Result is object dtype (consistent with sin/cos/tan) but values are float
        val = reg.execute("cosh", [pd.Series([0.0])]).iloc[0]
        assert isinstance(float(val), float)


# ---------------------------------------------------------------------------
# tanh
# ---------------------------------------------------------------------------


class TestTanh:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("tanh")

    def test_zero(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("tanh", [pd.Series([0.0])]).iloc[
            0
        ] == pytest.approx(0.0)

    def test_one(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("tanh", [pd.Series([1.0])]).iloc[
            0
        ] == pytest.approx(math.tanh(1.0))

    def test_range(self, reg: ScalarFunctionRegistry) -> None:
        """tanh values are always in (-1, 1)."""
        result = reg.execute("tanh", [pd.Series([0.0, 10.0, -10.0])])
        for v in result:
            assert -1.0 < v < 1.0

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        assert pd.isna(reg.execute("tanh", [pd.Series([None])]).iloc[0])

    def test_returns_numeric(self, reg: ScalarFunctionRegistry) -> None:
        # Result is object dtype (consistent with sin/cos/tan) but values are float
        val = reg.execute("tanh", [pd.Series([0.0])]).iloc[0]
        assert isinstance(float(val), float)


# ---------------------------------------------------------------------------
# log2
# ---------------------------------------------------------------------------


class TestLog2:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("log2")

    def test_one(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("log2", [pd.Series([1.0])]).iloc[
            0
        ] == pytest.approx(0.0)

    def test_two(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("log2", [pd.Series([2.0])]).iloc[
            0
        ] == pytest.approx(1.0)

    def test_eight(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.execute("log2", [pd.Series([8.0])]).iloc[
            0
        ] == pytest.approx(3.0)

    def test_non_positive_returns_null(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        """log2(0) and log2(-1) → null (domain error)."""
        assert pd.isna(reg.execute("log2", [pd.Series([0.0])]).iloc[0])
        assert pd.isna(reg.execute("log2", [pd.Series([-1.0])]).iloc[0])

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        assert pd.isna(reg.execute("log2", [pd.Series([None])]).iloc[0])

    def test_returns_float(self, reg: ScalarFunctionRegistry) -> None:
        val = reg.execute("log2", [pd.Series([2.0])]).iloc[0]
        assert isinstance(float(val), float)

    def test_in_return_clause(self, math_star: Star) -> None:
        r = math_star.execute_query(
            "MATCH (p:Person) RETURN log2(p.pos) AS l ORDER BY p.name"
        )
        vals = list(r["l"])
        assert vals[0] == pytest.approx(
            math.log2(1.0)
        )  # Alice pos=1 → log2(1)=0
        assert vals[1] == pytest.approx(math.log2(4.0))  # Bob pos=4 → 2.0
        assert vals[2] == pytest.approx(math.log2(8.0))  # Carol pos=8 → 3.0

    def test_in_where_clause(self, math_star: Star) -> None:
        """Filter by log2 threshold."""
        r = math_star.execute_query(
            "MATCH (p:Person) WHERE log2(p.pos) >= 2.0 RETURN p.name ORDER BY p.name"
        )
        # pos=4 (log2=2), pos=8 (log2=3) both qualify
        assert list(r["name"]) == ["Bob", "Carol"]
