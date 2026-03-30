"""TDD tests for asin()/acos() domain error → null.

asin(x) and acos(x) for x outside [-1, 1] raise ValueError ("expected a
number in range from -1 up to 1") instead of returning null.
openCypher spec: domain error → null (consistent with sqrt, log, log2, log10).

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


@pytest.fixture
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def trig_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "val": [0.5, 1.5, -0.5, -1.5],
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


class TestAsinDomain:
    def test_out_of_range_positive_does_not_crash(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        reg.execute("asin", [pd.Series([2.0])])

    def test_out_of_range_positive_returns_null(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("asin", [pd.Series([2.0])])
        assert pd.isna(result.iloc[0])

    def test_out_of_range_negative_returns_null(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("asin", [pd.Series([-2.0])])
        assert pd.isna(result.iloc[0])

    def test_boundary_one_valid(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("asin", [pd.Series([1.0])])
        assert result.iloc[0] == pytest.approx(math.pi / 2)

    def test_boundary_minus_one_valid(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("asin", [pd.Series([-1.0])])
        assert result.iloc[0] == pytest.approx(-math.pi / 2)

    def test_zero_returns_zero(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("asin", [pd.Series([0.0])])
        assert result.iloc[0] == pytest.approx(0.0)

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("asin", [pd.Series([None])])
        assert pd.isna(result.iloc[0])

    def test_mixed_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([0.5, 2.0, None, -0.5])
        result = reg.execute("asin", [s])
        assert result.iloc[0] == pytest.approx(math.asin(0.5))
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] == pytest.approx(math.asin(-0.5))

    def test_in_where_clause_domain_errors_excluded(
        self,
        trig_star: Star,
    ) -> None:
        """Asin of out-of-range values → null, excluded from WHERE filter."""
        r = trig_star.execute_query(
            "MATCH (p:Person) WHERE asin(p.val) >= 0.0 RETURN p.name ORDER BY p.name",
        )
        # val=0.5 → asin≈0.524 ✓, val=1.5 → null, val=-0.5 → asin≈-0.524, val=-1.5 → null
        assert list(r["name"]) == ["Alice"]


class TestAcosDomain:
    def test_out_of_range_positive_does_not_crash(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        reg.execute("acos", [pd.Series([2.0])])

    def test_out_of_range_positive_returns_null(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("acos", [pd.Series([2.0])])
        assert pd.isna(result.iloc[0])

    def test_out_of_range_negative_returns_null(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("acos", [pd.Series([-2.0])])
        assert pd.isna(result.iloc[0])

    def test_boundary_one_returns_zero(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("acos", [pd.Series([1.0])])
        assert result.iloc[0] == pytest.approx(0.0)

    def test_zero_returns_pi_over_two(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("acos", [pd.Series([0.0])])
        assert result.iloc[0] == pytest.approx(math.pi / 2)

    def test_null_propagation(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("acos", [pd.Series([None])])
        assert pd.isna(result.iloc[0])
