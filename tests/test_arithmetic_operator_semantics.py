"""Consolidated tests for openCypher arithmetic operator semantics.

Covers exponentiation (^) and modulo (%) operator behavior:
- Exponentiation: negative exponents, null propagation
- Modulo: truncating-toward-zero semantics (not Python floored), division by zero

Consolidated from: test_exponentiation.py, test_modulo_semantics.py
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture()
def star() -> Star:
    df = pd.DataFrame({ID_COLUMN: [1], "n": [1]})
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "n"],
        source_obj_attribute_map={"n": "n"},
        attribute_map={"n": "n"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ===========================================================================
# Exponentiation (^)
# ===========================================================================


class TestExponentiationNegativeExponent:
    """integer ^ negative_integer should return a float, not raise."""

    def test_two_to_neg_one(self, star: Star) -> None:
        r = star.execute_query("RETURN 2 ^ -1 AS r")
        assert abs(float(r["r"].iloc[0]) - 0.5) < 1e-9

    def test_two_to_neg_two(self, star: Star) -> None:
        r = star.execute_query("RETURN 2 ^ -2 AS r")
        assert abs(float(r["r"].iloc[0]) - 0.25) < 1e-9

    def test_ten_to_neg_three(self, star: Star) -> None:
        r = star.execute_query("RETURN 10 ^ -3 AS r")
        assert abs(float(r["r"].iloc[0]) - 0.001) < 1e-9

    def test_integer_to_zero_still_works(self, star: Star) -> None:
        r = star.execute_query("RETURN 5 ^ 0 AS r")
        assert r["r"].iloc[0] == 1

    def test_float_to_negative_still_works(self, star: Star) -> None:
        r = star.execute_query("RETURN 2.0 ^ -1 AS r")
        assert abs(float(r["r"].iloc[0]) - 0.5) < 1e-9

    def test_positive_integer_exponent_unchanged(self, star: Star) -> None:
        r = star.execute_query("RETURN 2 ^ 10 AS r")
        assert r["r"].iloc[0] == 1024

    def test_negative_base(self, star: Star) -> None:
        r = star.execute_query("RETURN (-2) ^ 3 AS r")
        assert r["r"].iloc[0] == -8


class TestExponentiationNullPropagation:
    """null ^ anything = null; anything ^ null = null."""

    def test_null_base_is_null(self, star: Star) -> None:
        r = star.execute_query("RETURN null ^ 2 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_exponent_is_null(self, star: Star) -> None:
        r = star.execute_query("RETURN 2 ^ null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_base_in_where_excludes_row(self, star: Star) -> None:
        r = star.execute_query("MATCH (n:N) WHERE null ^ 2 > 0 RETURN n.n")
        assert len(r) == 0

    def test_null_exponent_in_where_excludes_row(self, star: Star) -> None:
        r = star.execute_query("MATCH (n:N) WHERE 2 ^ null > 0 RETURN n.n")
        assert len(r) == 0


# ===========================================================================
# Modulo (%) — truncating-toward-zero semantics
# ===========================================================================


class TestModuloTruncatingSemantics:
    """Modulo must use truncating-toward-zero, not Python floored division."""

    def test_positive_positive(self, star: Star) -> None:
        r = star.execute_query("RETURN 5 % 3 AS r")
        assert r["r"].iloc[0] == 2

    def test_negative_positive_dividend_sign(self, star: Star) -> None:
        """-5 % 3 = -2 (Neo4j), not 1 (Python)."""
        r = star.execute_query("RETURN -5 % 3 AS r")
        assert r["r"].iloc[0] == -2

    def test_positive_negative_dividend_sign(self, star: Star) -> None:
        """5 % -3 = 2 (Neo4j), not -1 (Python)."""
        r = star.execute_query("RETURN 5 % -3 AS r")
        assert r["r"].iloc[0] == 2

    def test_negative_negative(self, star: Star) -> None:
        r = star.execute_query("RETURN -5 % -3 AS r")
        assert r["r"].iloc[0] == -2

    def test_exact_divisor(self, star: Star) -> None:
        r = star.execute_query("RETURN 10 % 5 AS r")
        assert r["r"].iloc[0] == 0

    def test_dividend_smaller_than_divisor(self, star: Star) -> None:
        r = star.execute_query("RETURN 2 % 5 AS r")
        assert r["r"].iloc[0] == 2

    def test_float_modulo(self, star: Star) -> None:
        r = star.execute_query("RETURN 7.5 % 2.0 AS r")
        assert abs(r["r"].iloc[0] - 1.5) < 1e-9

    def test_negative_float(self, star: Star) -> None:
        r = star.execute_query("RETURN -7.5 % 3.0 AS r")
        result = float(r["r"].iloc[0])
        assert abs(result - (-1.5)) < 1e-9


class TestModuloByZero:
    """x % 0 should return null (not nan, not crash)."""

    def test_integer_mod_zero_is_null(self, star: Star) -> None:
        r = star.execute_query("RETURN 10 % 0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_negative_mod_zero_is_null(self, star: Star) -> None:
        r = star.execute_query("RETURN -5 % 0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_float_mod_zero_is_null(self, star: Star) -> None:
        r = star.execute_query("RETURN 3.14 % 0.0 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_mod_zero_in_where_excludes_row(self, star: Star) -> None:
        r = star.execute_query("MATCH (n:N) WHERE (10 % 0) = 0 RETURN n.n")
        assert len(r) == 0
