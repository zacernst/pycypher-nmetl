"""TDD tests for abs() null propagation.

abs(null) → TypeError crash instead of null.  openCypher spec: abs(null) → null.

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
def abs_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "val": [-5.0, None, 3.0],
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


class TestAbsNullPropagation:
    def test_null_does_not_crash(self, reg: ScalarFunctionRegistry) -> None:
        """abs(null) must not raise TypeError."""
        reg.execute("abs", [pd.Series([None])])  # should not raise

    def test_null_returns_null(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("abs", [pd.Series([None])])
        assert pd.isna(result.iloc[0])

    def test_null_in_mixed_series(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([None, -5.0, None, 3.0])
        result = reg.execute("abs", [s])
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(5.0)
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] == pytest.approx(3.0)

    def test_positive_value_unchanged(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("abs", [pd.Series([7.5])])
        assert result.iloc[0] == pytest.approx(7.5)

    def test_negative_value_inverted(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("abs", [pd.Series([-7.5])])
        assert result.iloc[0] == pytest.approx(7.5)

    def test_zero_unchanged(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("abs", [pd.Series([0.0])])
        assert result.iloc[0] == pytest.approx(0.0)

    def test_integer_abs(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("abs", [pd.Series([-10])])
        assert result.iloc[0] == 10

    def test_in_return_clause(self, abs_star: Star) -> None:
        r = abs_star.execute_query(
            "MATCH (p:Person) RETURN abs(p.val) AS a ORDER BY p.name",
        )
        vals = list(r["a"])
        assert vals[0] == pytest.approx(5.0)  # Alice  val=-5
        assert pd.isna(vals[1])  # Bob    val=null
        assert vals[2] == pytest.approx(3.0)  # Carol  val=3

    def test_in_where_clause(self, abs_star: Star) -> None:
        r = abs_star.execute_query(
            "MATCH (p:Person) WHERE abs(p.val) > 4.0 RETURN p.name",
        )
        assert list(r["name"]) == ["Alice"]
