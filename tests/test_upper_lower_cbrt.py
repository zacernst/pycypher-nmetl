"""TDD tests for upper(), lower() aliases and cbrt() scalar function.

Neo4j supports ``upper(str)`` and ``lower(str)`` as aliases for the
canonical ``toUpper(str)`` / ``toLower(str)`` functions.  Users coming from
SQL or Neo4j commonly write ``upper()`` / ``lower()`` and are surprised
when they get ``ValueError: Unknown scalar function: upper``.

``cbrt(n)`` — cube root — is a standard Neo4j math function absent from
the registry.  Neo4j spec: ``cbrt(27.0) → 3.0``.

All tests written before the fix (TDD step 1).
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
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


@pytest.fixture
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def name_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "BOB", "carol"],
            "val": [8.0, 27.0, 64.0],
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
# upper() alias
# ---------------------------------------------------------------------------


class TestUpperAlias:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("upper")

    def test_agrees_with_toupper(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series(["hello", "World"])
        assert list(reg.execute("upper", [s])) == list(
            reg.execute("toUpper", [s]),
        )

    def test_value(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series(["hello"])
        assert reg.execute("upper", [s]).iloc[0] == "HELLO"

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([None])
        assert pd.isna(reg.execute("upper", [s]).iloc[0])

    def test_in_return_clause(self, name_star: Star) -> None:
        r = name_star.execute_query(
            "MATCH (p:Person) RETURN upper(p.name) AS u ORDER BY p.name",
        )
        assert list(r["u"]) == ["ALICE", "BOB", "CAROL"]

    def test_in_where_clause(self, name_star: Star) -> None:
        r = name_star.execute_query(
            "MATCH (p:Person) WHERE upper(p.name) = 'BOB' RETURN p.name",
        )
        assert list(r["name"]) == ["BOB"]


# ---------------------------------------------------------------------------
# lower() alias
# ---------------------------------------------------------------------------


class TestLowerAlias:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("lower")

    def test_agrees_with_tolower(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series(["HELLO", "World"])
        assert list(reg.execute("lower", [s])) == list(
            reg.execute("toLower", [s]),
        )

    def test_value(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series(["HELLO"])
        assert reg.execute("lower", [s]).iloc[0] == "hello"

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([None])
        assert pd.isna(reg.execute("lower", [s]).iloc[0])

    def test_in_return_clause(self, name_star: Star) -> None:
        r = name_star.execute_query(
            "MATCH (p:Person) RETURN lower(p.name) AS l ORDER BY p.name",
        )
        assert list(r["l"]) == ["alice", "bob", "carol"]

    def test_in_where_clause(self, name_star: Star) -> None:
        r = name_star.execute_query(
            "MATCH (p:Person) WHERE lower(p.name) = 'alice' RETURN p.name",
        )
        assert list(r["name"]) == ["Alice"]


# ---------------------------------------------------------------------------
# cbrt()
# ---------------------------------------------------------------------------


class TestCbrt:
    def test_registered(self, reg: ScalarFunctionRegistry) -> None:
        assert reg.has_function("cbrt")

    def test_perfect_cube(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([27.0])
        assert reg.execute("cbrt", [s]).iloc[0] == pytest.approx(3.0)

    def test_one(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([1.0])
        assert reg.execute("cbrt", [s]).iloc[0] == pytest.approx(1.0)

    def test_zero(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.0])
        assert reg.execute("cbrt", [s]).iloc[0] == pytest.approx(0.0)

    def test_negative(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([-8.0])
        assert reg.execute("cbrt", [s]).iloc[0] == pytest.approx(-2.0)

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([None])
        result = reg.execute("cbrt", [s])
        assert pd.isna(result.iloc[0])

    def test_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([8.0, 27.0, 64.0])
        result = reg.execute("cbrt", [s])
        assert result.iloc[0] == pytest.approx(2.0)
        assert result.iloc[1] == pytest.approx(3.0)
        assert result.iloc[2] == pytest.approx(4.0)

    def test_returns_float(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([8.0])
        result = reg.execute("cbrt", [s])
        assert isinstance(float(result.iloc[0]), float)

    def test_in_return_clause(self, name_star: Star) -> None:
        r = name_star.execute_query(
            "MATCH (p:Person) RETURN cbrt(p.val) AS c ORDER BY p.name",
        )
        vals = list(r["c"])
        assert vals[0] == pytest.approx(2.0)  # cbrt(8)
        assert vals[1] == pytest.approx(3.0)  # cbrt(27)
        assert vals[2] == pytest.approx(4.0)  # cbrt(64)
