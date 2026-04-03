"""Consolidated tests for round() rounding semantics.

Covers:
- Default HALF_UP rounding (1-arg and 2-arg forms)
- All 7 rounding modes in the 3-arg form: round(value, precision, mode)
  HALF_UP    — ties go away from zero    (2.5 → 3, -2.5 → -3)
  HALF_DOWN  — ties go toward zero       (2.5 → 2, -2.5 → 2)
  HALF_EVEN  — ties go to nearest even   (2.5 → 2, 3.5 → 4)  banker's
  CEILING    — always toward +∞          (2.1 → 3, -2.9 → -2)
  FLOOR      — always toward -∞          (2.9 → 2, -2.1 → -3)
  UP         — always away from zero     (0.1 → 1, -0.1 → -1)
  DOWN       — always toward zero        (2.9 → 2, -2.9 → -2)  truncation
- Vectorized multi-row behavior
- Cypher query integration

Consolidated from: test_round_half_up.py, test_round_mode.py
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
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
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def ctx():
    return ContextBuilder().from_dict(
        {
            "Num": pd.DataFrame(
                {
                    "__ID__": ["a", "b", "c", "d", "e"],
                    "val": [2.5, -2.5, 2.1, -2.1, 0.0],
                },
            ),
        },
    )


def _exec(
    reg: ScalarFunctionRegistry,
    mode: str,
    value: float,
    prec: int = 0,
) -> float:
    """Shortcut: execute round(value, prec, mode) and return scalar result."""
    result = reg.execute(
        "round",
        [pd.Series([value]), pd.Series([prec]), pd.Series([mode])],
    )
    return result.iloc[0]


@pytest.fixture
def round_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "val": [2.5, 3.5, -0.5, -2.5],
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


# ---------------------------------------------------------------------------
# Default round() (1-arg) — HALF_UP semantics
# ---------------------------------------------------------------------------


class TestRoundDefaultHalfUp:
    """round() must use HALF_UP (tie away from zero) by default."""

    def test_two_point_five_rounds_to_three(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("round", [pd.Series([2.5])])
        assert result.iloc[0] == pytest.approx(3.0)

    def test_three_point_five_rounds_to_four(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("round", [pd.Series([3.5])])
        assert result.iloc[0] == pytest.approx(4.0)

    def test_negative_half_rounds_away_from_zero(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("round", [pd.Series([-0.5])])
        assert result.iloc[0] == pytest.approx(-1.0)

    def test_negative_two_point_five_rounds_to_minus_three(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("round", [pd.Series([-2.5])])
        assert result.iloc[0] == pytest.approx(-3.0)

    def test_exact_integer_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("round", [pd.Series([3.0])])
        assert result.iloc[0] == pytest.approx(3.0)

    def test_below_half_rounds_down(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("round", [pd.Series([2.4])])
        assert result.iloc[0] == pytest.approx(2.0)

    def test_above_half_rounds_up(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("round", [pd.Series([2.6])])
        assert result.iloc[0] == pytest.approx(3.0)

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("round", [pd.Series([None])])
        assert pd.isna(result.iloc[0])

    def test_returns_float(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("round", [pd.Series([2.5])])
        assert pd.api.types.is_float_dtype(result)

    def test_column_half_up(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.5, 1.5, 2.5, 3.5])
        result = reg.execute("round", [s])
        assert list(result) == pytest.approx([1.0, 2.0, 3.0, 4.0])


class TestRoundDefaultPrecision:
    """round(n, precision) with 2-arg form also uses HALF_UP."""

    def test_precision_one_half_up(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("round", [pd.Series([2.55]), pd.Series([1])])
        assert result.iloc[0] == pytest.approx(2.6)

    def test_precision_zero_same_as_no_precision(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("round", [pd.Series([2.5]), pd.Series([0])])
        assert result.iloc[0] == pytest.approx(3.0)

    def test_precision_two(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("round", [pd.Series([3.145]), pd.Series([2])])
        assert result.iloc[0] == pytest.approx(3.15)


class TestRoundInCypherQuery:
    def test_round_in_return_clause(self, round_star: Star) -> None:
        r = round_star.execute_query(
            "MATCH (p:Person) RETURN round(p.val) AS r ORDER BY p.name",
        )
        vals = list(r["r"])
        assert vals[0] == pytest.approx(3.0)  # Alice val=2.5 → 3
        assert vals[1] == pytest.approx(4.0)  # Bob   val=3.5 → 4
        assert vals[2] == pytest.approx(-1.0)  # Carol val=-0.5 → -1
        assert vals[3] == pytest.approx(-3.0)  # Dave  val=-2.5 → -3

    def test_round_in_where_clause(self, round_star: Star) -> None:
        r = round_star.execute_query(
            "MATCH (p:Person) WHERE round(p.val) = 3.0 RETURN p.name",
        )
        assert list(r["name"]) == ["Alice"]


# ---------------------------------------------------------------------------
# Smoke test: 3rd argument is accepted
# ---------------------------------------------------------------------------


class TestRoundModeAccepted:
    def test_three_args_no_longer_raises_arity_error(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """round(x, n, mode) must not raise 'accepts at most 2 arguments'."""
        result = reg.execute(
            "round",
            [pd.Series([2.5]), pd.Series([0]), pd.Series(["HALF_UP"])],
        )
        assert len(result) == 1

    def test_max_args_is_three(self, reg: ScalarFunctionRegistry) -> None:
        """Confirm the function registry entry allows 3 arguments."""
        fn = reg._functions["round"]
        assert fn.max_args >= 3


# ---------------------------------------------------------------------------
# HALF_UP (away from zero for ties) — default Neo4j behaviour
# ---------------------------------------------------------------------------


class TestHalfUp:
    def test_positive_tie_rounds_up(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "HALF_UP", 2.5) == pytest.approx(3.0)

    def test_negative_tie_rounds_down(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """Ties go away from zero, so -2.5 → -3."""
        assert _exec(reg, "HALF_UP", -2.5) == pytest.approx(-3.0)

    def test_non_tie_rounds_to_nearest(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "HALF_UP", 2.4) == pytest.approx(2.0)
        assert _exec(reg, "HALF_UP", 2.6) == pytest.approx(3.0)

    def test_with_precision(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "HALF_UP", 1.555, prec=2) == pytest.approx(1.56)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["HALF_UP"])],
        )
        assert result.iloc[0] is None or (
            result.iloc[0] != result.iloc[0]
        )  # None or NaN


# ---------------------------------------------------------------------------
# HALF_DOWN (toward zero for ties)
# ---------------------------------------------------------------------------


class TestHalfDown:
    def test_positive_tie_rounds_down(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "HALF_DOWN", 2.5) == pytest.approx(2.0)

    def test_negative_tie_rounds_up(self, reg: ScalarFunctionRegistry) -> None:
        """Ties go toward zero, so -2.5 → -2."""
        assert _exec(reg, "HALF_DOWN", -2.5) == pytest.approx(-2.0)

    def test_non_tie_rounds_to_nearest(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "HALF_DOWN", 2.6) == pytest.approx(3.0)

    def test_with_precision(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "HALF_DOWN", 1.555, prec=2) == pytest.approx(1.55)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["HALF_DOWN"])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# HALF_EVEN (banker's rounding)
# ---------------------------------------------------------------------------


class TestHalfEven:
    def test_tie_to_even_two_point_five(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """2.5 → 2 (nearest even)."""
        assert _exec(reg, "HALF_EVEN", 2.5) == pytest.approx(2.0)

    def test_tie_to_even_three_point_five(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """3.5 → 4 (nearest even)."""
        assert _exec(reg, "HALF_EVEN", 3.5) == pytest.approx(4.0)

    def test_negative_tie_banker(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "HALF_EVEN", -2.5) == pytest.approx(-2.0)

    def test_non_tie_rounds_to_nearest(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "HALF_EVEN", 2.6) == pytest.approx(3.0)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["HALF_EVEN"])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# CEILING (toward +∞)
# ---------------------------------------------------------------------------


class TestCeiling:
    def test_positive_fractional_rounds_up(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """2.1 → 3.0."""
        assert _exec(reg, "CEILING", 2.1) == pytest.approx(3.0)

    def test_negative_fractional_rounds_toward_pos_inf(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """-2.9 → -2.0."""
        assert _exec(reg, "CEILING", -2.9) == pytest.approx(-2.0)

    def test_exact_integer_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "CEILING", 3.0) == pytest.approx(3.0)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["CEILING"])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# FLOOR (toward -∞)
# ---------------------------------------------------------------------------


class TestFloor:
    def test_positive_fractional_rounds_down(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """2.9 → 2.0."""
        assert _exec(reg, "FLOOR", 2.9) == pytest.approx(2.0)

    def test_negative_fractional_rounds_toward_neg_inf(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """-2.1 → -3.0."""
        assert _exec(reg, "FLOOR", -2.1) == pytest.approx(-3.0)

    def test_exact_integer_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "FLOOR", 3.0) == pytest.approx(3.0)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["FLOOR"])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# UP (away from zero, regardless of value)
# ---------------------------------------------------------------------------


class TestUp:
    def test_small_positive_rounds_to_one(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """0.1 → 1.0."""
        assert _exec(reg, "UP", 0.1) == pytest.approx(1.0)

    def test_small_negative_rounds_to_neg_one(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """-0.1 → -1.0."""
        assert _exec(reg, "UP", -0.1) == pytest.approx(-1.0)

    def test_exact_integer_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "UP", 2.0) == pytest.approx(2.0)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["UP"])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# DOWN (toward zero = truncation)
# ---------------------------------------------------------------------------


class TestDown:
    def test_positive_fractional_truncates(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """2.9 → 2.0."""
        assert _exec(reg, "DOWN", 2.9) == pytest.approx(2.0)

    def test_negative_fractional_truncates(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """-2.9 → -2.0."""
        assert _exec(reg, "DOWN", -2.9) == pytest.approx(-2.0)

    def test_exact_integer_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        assert _exec(reg, "DOWN", 3.0) == pytest.approx(3.0)

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "round",
            [pd.Series([None]), pd.Series([0]), pd.Series(["DOWN"])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# Case-insensitive mode names
# ---------------------------------------------------------------------------


class TestModeNameCaseFolding:
    def test_lowercase_half_up(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "half_up", 2.5) == pytest.approx(3.0)

    def test_mixed_case_half_even(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "Half_Even", 2.5) == pytest.approx(2.0)

    def test_lowercase_ceiling(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec(reg, "ceiling", 2.1) == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Invalid mode raises ValueError
# ---------------------------------------------------------------------------


class TestInvalidMode:
    def test_unknown_mode_raises(self, reg: ScalarFunctionRegistry) -> None:
        with pytest.raises(ValueError, match="Unknown rounding mode"):
            reg.execute(
                "round",
                [pd.Series([2.5]), pd.Series([0]), pd.Series(["BANKERS"])],
            )


# ---------------------------------------------------------------------------
# Vectorised multi-row behaviour
# ---------------------------------------------------------------------------


class TestVectorised:
    def test_half_up_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.5, 1.5, 2.5, 3.5])
        result = reg.execute(
            "round",
            [s, pd.Series([0] * 4), pd.Series(["HALF_UP"] * 4)],
        )
        assert list(result) == pytest.approx([1.0, 2.0, 3.0, 4.0])

    def test_half_even_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.5, 1.5, 2.5, 3.5])
        result = reg.execute(
            "round",
            [s, pd.Series([0] * 4), pd.Series(["HALF_EVEN"] * 4)],
        )
        # 0.5→0, 1.5→2, 2.5→2, 3.5→4 (nearest even)
        assert list(result) == pytest.approx([0.0, 2.0, 2.0, 4.0])

    def test_mixed_null_multi_row(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([2.5, None, 1.5])
        result = reg.execute(
            "round",
            [s, pd.Series([0] * 3), pd.Series(["HALF_UP"] * 3)],
        )
        assert result.iloc[0] == pytest.approx(3.0)
        assert result.iloc[1] is None or (result.iloc[1] != result.iloc[1])
        assert result.iloc[2] == pytest.approx(2.0)

    def test_precision_with_mode_multi_row(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        # Mode is treated as a constant (first element used for all rows),
        # like precision — this matches Neo4j's semantics where mode is always
        # a string literal constant, not a per-row computed value.
        s = pd.Series([1.555, 2.545])
        result_up = reg.execute(
            "round",
            [s, pd.Series([2, 2]), pd.Series(["HALF_UP", "HALF_UP"])],
        )
        assert result_up.iloc[0] == pytest.approx(
            1.56,
        )  # HALF_UP: 1.555 → 1.56
        assert result_up.iloc[1] == pytest.approx(
            2.55,
        )  # HALF_UP: 2.545 → 2.55
        result_even = reg.execute(
            "round",
            [s, pd.Series([2, 2]), pd.Series(["HALF_EVEN", "HALF_EVEN"])],
        )
        assert (
            result_even.iloc[0]
            == pytest.approx(
                1.56,
            )
        )  # HALF_EVEN: 1.555 → 1.56 (5 is even? no: 5 rounds to 6 to make it even)
        assert result_even.iloc[1] == pytest.approx(
            2.54,
        )  # HALF_EVEN: 2.545 → 2.54 (4 is even)


# ---------------------------------------------------------------------------
# Cypher integration tests
# ---------------------------------------------------------------------------


class TestCypherIntegration:
    def test_half_up_in_return(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Num) WHERE n.val = 2.5 RETURN round(n.val, 0, "HALF_UP") AS r',
        )
        assert result["r"].iloc[0] == pytest.approx(3.0)

    def test_half_even_in_return(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Num) WHERE n.val = 2.5 RETURN round(n.val, 0, "HALF_EVEN") AS r',
        )
        assert result["r"].iloc[0] == pytest.approx(2.0)

    def test_ceiling_in_return(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Num) WHERE n.val = 2.1 RETURN round(n.val, 0, "CEILING") AS r',
        )
        assert result["r"].iloc[0] == pytest.approx(3.0)

    def test_floor_negative_in_return(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Num) WHERE n.val = -2.1 RETURN round(n.val, 0, "FLOOR") AS r',
        )
        assert result["r"].iloc[0] == pytest.approx(-3.0)

    def test_down_in_where(self, ctx: ContextBuilder) -> None:
        """round(val, 0, 'DOWN') = 2.0 for val=2.5 or -2.5."""
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Num) WHERE round(n.val, 0, "DOWN") = 2.0 RETURN n.val ORDER BY n.val',
        )
        # 2.5 truncates to 2, -2.5 truncates to -2 (toward zero)
        assert len(result) == 2  # both 2.5 and -2.5

    def test_mode_is_case_insensitive_in_cypher(
        self,
        ctx: ContextBuilder,
    ) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Num) WHERE n.val = 2.5 RETURN round(n.val, 0, "half_up") AS r',
        )
        assert result["r"].iloc[0] == pytest.approx(3.0)
