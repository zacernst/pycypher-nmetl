"""TDD tests for Loop 189 — Cowbell: hypot(), fmod(), toList() scalar functions.

All three are missing from the scalar function registry.

  hypot(x, y)   — Euclidean distance sqrt(x² + y²); null if either arg null.
                  Neo4j 5.x built-in; vectorizable with np.hypot.
  fmod(x, y)    — IEEE 754 floating-point modulo (remainder); null if either
                  arg null or y = 0.  Different from the integer ``%`` operator.
                  Vectorizable with np.fmod.
  toList(x)     — Wrap a scalar in a single-element list; return lists unchanged;
                  null → [null] (wrapped, not null itself).
                  Useful for homogenising mixed-type columns.

All tests written BEFORE the implementation (TDD red phase).
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

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture()
def geo_star() -> Star:
    """A Star with a small GeoPoint table for integration tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["A", "B", "C"],
            "dx": [3.0, 0.0, -5.0],
            "dy": [4.0, 0.0, 12.0],
            "x": [5.0, 3.0, 7.0],
            "y": [2.5, 3.0, 0.0],
        }
    )
    table = EntityTable(
        entity_type="Point",
        identifier="Point",
        column_names=[ID_COLUMN, "name", "dx", "dy", "x", "y"],
        source_obj_attribute_map={
            "name": "name",
            "dx": "dx",
            "dy": "dy",
            "x": "x",
            "y": "y",
        },
        attribute_map={
            "name": "name",
            "dx": "dx",
            "dy": "dy",
            "x": "x",
            "y": "y",
        },
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Point": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


def _s(*values: object) -> pd.Series:
    return pd.Series(list(values))


def _null(v: object) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


# ===========================================================================
# hypot(x, y)
# ===========================================================================


class TestHypot:
    """hypot(x, y) = sqrt(x² + y²), vectorised via np.hypot."""

    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("hypot"), "'hypot' must be registered."

    def test_3_4_5(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("hypot", [_s(3.0), _s(4.0)])
        assert result.iloc[0] == pytest.approx(5.0)

    def test_zero_zero(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("hypot", [_s(0.0), _s(0.0)])
        assert result.iloc[0] == pytest.approx(0.0)

    def test_negative_inputs(self, reg: ScalarFunctionRegistry) -> None:
        """hypot is symmetric — sign does not matter."""
        result = reg.execute("hypot", [_s(-3.0), _s(-4.0)])
        assert result.iloc[0] == pytest.approx(5.0)

    def test_null_x_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        assert _null(reg.execute("hypot", [_s(None), _s(4.0)]).iloc[0])

    def test_null_y_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        assert _null(reg.execute("hypot", [_s(3.0), _s(None)]).iloc[0])

    def test_both_null_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        assert _null(reg.execute("hypot", [_s(None), _s(None)]).iloc[0])

    def test_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        x = pd.Series([3.0, 0.0, 5.0])
        y = pd.Series([4.0, 0.0, 12.0])
        result = reg.execute("hypot", [x, y])
        assert result.iloc[0] == pytest.approx(5.0)
        assert result.iloc[1] == pytest.approx(0.0)
        assert result.iloc[2] == pytest.approx(13.0)

    def test_mixed_null_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        x = pd.Series([3.0, None, 5.0])
        y = pd.Series([4.0, 4.0, None])
        result = reg.execute("hypot", [x, y])
        assert result.iloc[0] == pytest.approx(5.0)
        assert _null(result.iloc[1])
        assert _null(result.iloc[2])

    def test_in_return_clause(self, geo_star: Star) -> None:
        r = geo_star.execute_query(
            "MATCH (p:Point) RETURN hypot(p.dx, p.dy) AS dist ORDER BY p.name"
        )
        vals = list(r["dist"])
        assert vals[0] == pytest.approx(5.0)  # A: 3,4 → 5
        assert vals[1] == pytest.approx(0.0)  # B: 0,0 → 0
        assert vals[2] == pytest.approx(13.0)  # C: -5,12 → 13

    def test_in_where_clause(self, geo_star: Star) -> None:
        """Filter points whose distance from origin > 10."""
        r = geo_star.execute_query(
            "MATCH (p:Point) WHERE hypot(p.dx, p.dy) > 10.0 RETURN p.name"
        )
        assert list(r["name"]) == ["C"]

    def test_requires_two_args(self, reg: ScalarFunctionRegistry) -> None:
        with pytest.raises(ValueError, match="at least 2"):
            reg.execute("hypot", [_s(3.0)])

    def test_returns_numeric(self, reg: ScalarFunctionRegistry) -> None:
        val = reg.execute("hypot", [_s(3.0), _s(4.0)]).iloc[0]
        assert isinstance(float(val), float)


# ===========================================================================
# fmod(x, y)
# ===========================================================================


class TestFmod:
    """fmod(x, y) = IEEE 754 floating-point remainder.

    fmod(x, y) = x - trunc(x/y)*y, which differs from Python ``%`` for negative args.
    fmod(x, 0) → null (domain error).
    """

    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("fmod"), "'fmod' must be registered."

    def test_basic(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("fmod", [_s(10.0), _s(3.0)])
        assert result.iloc[0] == pytest.approx(1.0)

    def test_float_remainder(self, reg: ScalarFunctionRegistry) -> None:
        """5.5 mod 2.3 = 0.9 (floating-point)."""
        result = reg.execute("fmod", [_s(5.5), _s(2.3)])
        assert result.iloc[0] == pytest.approx(0.9)

    def test_negative_dividend(self, reg: ScalarFunctionRegistry) -> None:
        """fmod preserves sign of dividend (IEEE 754 definition)."""
        result = reg.execute("fmod", [_s(-10.0), _s(3.0)])
        assert result.iloc[0] == pytest.approx(-1.0)

    def test_zero_divisor_returns_null(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        assert _null(reg.execute("fmod", [_s(5.0), _s(0.0)]).iloc[0])

    def test_null_x_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        assert _null(reg.execute("fmod", [_s(None), _s(3.0)]).iloc[0])

    def test_null_y_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        assert _null(reg.execute("fmod", [_s(5.0), _s(None)]).iloc[0])

    def test_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        x = pd.Series([10.0, 7.0, -5.0])
        y = pd.Series([3.0, 2.0, 3.0])
        result = reg.execute("fmod", [x, y])
        assert result.iloc[0] == pytest.approx(1.0)
        assert result.iloc[1] == pytest.approx(1.0)
        assert result.iloc[2] == pytest.approx(-2.0)

    def test_in_return_clause(self, geo_star: Star) -> None:
        """fmod available in RETURN expressions."""
        r = geo_star.execute_query(
            "MATCH (p:Point) RETURN fmod(p.x, 3.0) AS rem ORDER BY p.name"
        )
        vals = list(r["rem"])
        assert vals[0] == pytest.approx(2.0)  # A: 5 mod 3 = 2
        assert vals[1] == pytest.approx(0.0)  # B: 3 mod 3 = 0
        assert vals[2] == pytest.approx(1.0)  # C: 7 mod 3 = 1

    def test_requires_two_args(self, reg: ScalarFunctionRegistry) -> None:
        with pytest.raises(ValueError, match="at least 2"):
            reg.execute("fmod", [_s(5.0)])

    def test_returns_numeric(self, reg: ScalarFunctionRegistry) -> None:
        val = reg.execute("fmod", [_s(10.0), _s(3.0)]).iloc[0]
        assert isinstance(float(val), float)


# ===========================================================================
# toList(x)
# ===========================================================================


class TestToList:
    """toList(x) wraps a scalar in a single-element list; passes lists through.

    Cypher semantics:
      toList(42)       → [42]
      toList('hello')  → ['hello']
      toList([1, 2])   → [1, 2]   (list passed through unchanged)
      toList(null)     → null     (null propagated)
    """

    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("toList"), "'toList' must be registered."

    def test_integer_wrapped(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("toList", [_s(42)])
        assert result.iloc[0] == [42]

    def test_string_wrapped(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("toList", [_s("hello")])
        assert result.iloc[0] == ["hello"]

    def test_float_wrapped(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("toList", [_s(3.14)])
        assert result.iloc[0] == pytest.approx([3.14])

    def test_list_passed_through(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("toList", [_s([1, 2, 3])])
        assert result.iloc[0] == [1, 2, 3]

    def test_null_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        assert _null(reg.execute("toList", [_s(None)]).iloc[0])

    def test_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([1, "a", None, [4, 5]])
        result = reg.execute("toList", [s])
        assert result.iloc[0] == [1]
        assert result.iloc[1] == ["a"]
        assert _null(result.iloc[2])
        assert result.iloc[3] == [4, 5]

    def test_in_return_clause(self, geo_star: Star) -> None:
        """toList wraps scalar properties in a list per row."""
        r = geo_star.execute_query(
            "MATCH (p:Point) RETURN toList(p.dx) AS wrapped ORDER BY p.name"
        )
        assert r["wrapped"].iloc[0] == [3.0]
        assert r["wrapped"].iloc[1] == [0.0]
        assert r["wrapped"].iloc[2] == [-5.0]

    def test_result_is_series_of_lists(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series([1, 2, 3])
        result = reg.execute("toList", [s])
        for val in result:
            assert isinstance(val, list)
