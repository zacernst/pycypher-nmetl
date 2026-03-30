"""Tests for null property normalization: get_property must return Python None.

Root cause: ``pd.Series.map()`` converts ``None``/``NaN``-valued property cells to
``float('nan')``, and ``get_property`` previously returned ``pd.NA`` for missing
property columns.  Both representations are unrecognised by the ``_strictly_null``
guards in scalar functions (``x is None``), so functions like ``isString``,
``isFloat``, and ``isNaN`` misclassify null-valued properties.

Expected behaviour (Cypher semantics):
- ``isString(null)`` → ``null``
- ``isFloat(null)`` → ``null``
- ``isNaN(null)`` → ``null``
- ``isInfinite(null)`` → ``null``
- ``isFinite(null)`` → ``null``

TDD: all tests in this file were written before the fix.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mixed_null_ctx() -> Context:
    """Context with a mix of null and non-null string and float properties."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", None, "Bob"],
            "score": [1.5, None, 3.0],
        },
    )
    table = EntityTable(
        entity_type="Item",
        identifier="Item",
        column_names=[ID_COLUMN, "name", "score"],
        source_obj_attribute_map={"name": "name", "score": "score"},
        attribute_map={"name": "name", "score": "score"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Item": table}))


@pytest.fixture
def star(mixed_null_ctx: Context) -> Star:
    return Star(context=mixed_null_ctx)


# ---------------------------------------------------------------------------
# Class 1: Direct property lookup returns Python None
# ---------------------------------------------------------------------------


class TestNullPropertyReturnsNone:
    """get_property must return Python None (not NaN or pd.NA) for null values."""

    def test_null_string_property_is_python_none(self, star: Star) -> None:
        """Null string property → property value is Python None, not float NaN."""
        result = star.execute_query("MATCH (i:Item) RETURN i.name AS v")
        null_val = result["v"].iloc[1]  # Row with name=None
        assert null_val is None, (
            f"Expected None for null string property, got {null_val!r} "
            f"(type={type(null_val).__name__})"
        )

    def test_null_float_property_is_python_none(self, star: Star) -> None:
        """Null float property → property value is Python None, not float NaN."""
        result = star.execute_query("MATCH (i:Item) RETURN i.score AS v")
        null_val = result["v"].iloc[1]  # Row with score=None
        assert null_val is None, (
            f"Expected None for null float property, got {null_val!r} "
            f"(type={type(null_val).__name__})"
        )

    def test_missing_property_is_python_none(self, star: Star) -> None:
        """Missing (non-existent) property column → Python None, not pd.NA."""
        result = star.execute_query("MATCH (i:Item) RETURN i.age AS v")
        for i, val in enumerate(result["v"]):
            assert val is None, (
                f"Row {i}: expected None for missing property, got {val!r} "
                f"(type={type(val).__name__})"
            )

    def test_non_null_string_property_unchanged(self, star: Star) -> None:
        """Non-null string properties must not be affected by the normalisation."""
        result = star.execute_query("MATCH (i:Item) RETURN i.name AS v")
        assert result["v"].iloc[0] == "Alice"
        assert result["v"].iloc[2] == "Bob"

    def test_non_null_float_property_unchanged(self, star: Star) -> None:
        """Non-null float properties must not be affected by the normalisation."""
        result = star.execute_query("MATCH (i:Item) RETURN i.score AS v")
        assert result["v"].iloc[0] == pytest.approx(1.5)
        assert result["v"].iloc[2] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Class 2: Type predicates on null properties → null
# ---------------------------------------------------------------------------


class TestTypePredicatesOnNullProperty:
    """Type predicate functions must return null for null-valued properties."""

    def _assert_null(self, val: object, fn_name: str, scenario: str) -> None:
        """Helper: assert val is null (None or pd.NA)."""
        assert val is None or (hasattr(pd, "NA") and val is pd.NA), (
            f"{fn_name}({scenario}) expected null, got {val!r} "
            f"(type={type(val).__name__})"
        )

    def test_isstring_on_null_string_property_returns_null(
        self,
        star: Star,
    ) -> None:
        """isString(null_string_prop) must return null, not False."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN isString(i.name) AS v",
        )
        self._assert_null(result["v"].iloc[1], "isString", "null_string")

    def test_isstring_on_missing_property_returns_null(
        self,
        star: Star,
    ) -> None:
        """isString(missing_prop) must return null, not False."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN isString(i.nonexistent) AS v",
        )
        for i, val in enumerate(result["v"]):
            self._assert_null(val, "isString", "missing")

    def test_isfloat_on_null_float_property_returns_null(
        self,
        star: Star,
    ) -> None:
        """isFloat(null_float_prop) must return null, not True.

        Bug: float('nan') passes isinstance(x, float) → isFloat was returning True.
        """
        result = star.execute_query(
            "MATCH (i:Item) RETURN isFloat(i.score) AS v",
        )
        self._assert_null(result["v"].iloc[1], "isFloat", "null_float")

    def test_isinteger_on_null_returns_null(self, star: Star) -> None:
        """isInteger(null_prop) must return null."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN isInteger(i.age) AS v",
        )
        for i, val in enumerate(result["v"]):
            self._assert_null(val, "isInteger", "missing")

    def test_isnan_on_null_string_property_returns_null(
        self,
        star: Star,
    ) -> None:
        """isNaN(null_string_prop) must return null, not True.

        Bug: float('nan') from null-as-NaN passes ``isinstance(x, float) and isnan(x)``
        so isNaN was returning True for null string properties.
        """
        result = star.execute_query("MATCH (i:Item) RETURN isNaN(i.name) AS v")
        self._assert_null(result["v"].iloc[1], "isNaN", "null_string")

    def test_isnan_on_null_float_property_returns_null(
        self,
        star: Star,
    ) -> None:
        """isNaN(null_float_prop) must return null, not True."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN isNaN(i.score) AS v",
        )
        self._assert_null(result["v"].iloc[1], "isNaN", "null_float")

    def test_isinfinite_on_null_property_returns_null(
        self,
        star: Star,
    ) -> None:
        """isInfinite(null_prop) must return null, not False."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN isInfinite(i.score) AS v",
        )
        self._assert_null(result["v"].iloc[1], "isInfinite", "null_float")

    def test_isfinite_on_null_property_returns_null(self, star: Star) -> None:
        """isFinite(null_prop) must return null, not False."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN isFinite(i.score) AS v",
        )
        self._assert_null(result["v"].iloc[1], "isFinite", "null_float")


# ---------------------------------------------------------------------------
# Class 3: Non-null type predicate behaviour is unaffected
# ---------------------------------------------------------------------------


class TestTypePredicatesNonNullUnaffected:
    """Non-null properties must still produce correct True/False results."""

    def test_isstring_true_for_string(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (i:Item) RETURN isString(i.name) AS v",
        )
        assert result["v"].iloc[0] == True
        assert result["v"].iloc[2] == True

    def test_isfloat_true_for_float(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (i:Item) RETURN isFloat(i.score) AS v",
        )
        assert result["v"].iloc[0] == True
        assert result["v"].iloc[2] == True

    def test_isnan_false_for_non_nan(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (i:Item) RETURN isNaN(i.score) AS v",
        )
        assert result["v"].iloc[0] == False
        assert result["v"].iloc[2] == False


# ---------------------------------------------------------------------------
# Class 4: Null-semantics regression guards
# ---------------------------------------------------------------------------


class TestNullSemanticsRegressionGuards:
    """Existing null-handling behaviour must still work after the fix."""

    def test_is_null_check_still_returns_true_for_null_props(
        self,
        star: Star,
    ) -> None:
        """``n.name IS NULL`` must still return True where name is null."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN i.name IS NULL AS v",
        )
        assert result["v"].iloc[0] == False  # Alice is not null
        assert result["v"].iloc[1] == True  # None is null
        assert result["v"].iloc[2] == False  # Bob is not null

    def test_where_is_null_still_filters(self, star: Star) -> None:
        """``WHERE i.name IS NULL`` must still select only null-name rows."""
        result = star.execute_query(
            "MATCH (i:Item) WHERE i.name IS NULL RETURN i.name AS v",
        )
        assert len(result) == 1

    def test_coalesce_null_string_still_substitutes(self, star: Star) -> None:
        """``coalesce(null_string_prop, 'fallback')`` must still return 'fallback'."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN coalesce(i.name, 'fallback') AS v",
        )
        assert result["v"].iloc[1] == "fallback"

    def test_coalesce_missing_prop_still_substitutes(self, star: Star) -> None:
        """``coalesce(missing_prop, 99)`` must still return 99."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN coalesce(i.age, 99) AS v",
        )
        for val in result["v"]:
            assert val == 99

    def test_null_in_return_column_is_null(self, star: Star) -> None:
        """RETURN null_prop must produce a null value detectable by pd.isna()."""
        result = star.execute_query("MATCH (i:Item) RETURN i.name AS v")
        null_val = result["v"].iloc[1]
        assert pd.isna(null_val), f"Expected null (pd.isna), got {null_val!r}"
