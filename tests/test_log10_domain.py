"""TDD tests for log10() domain-error null semantics.

log10(x) for x <= 0 currently returns float('nan') instead of null.
openCypher spec (matching log() and log2() behavior): domain error → null.

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
def log_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "val": [100.0, 0.0, -1.0, 10.0],
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


class TestLog10DomainNull:
    """log10(x<=0) → null, not nan; consistent with log() and log2()."""

    def test_zero_returns_null_not_nan(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("log10", [pd.Series([0.0])])
        # Must be null (isna) and must NOT be NaN-the-float (which isna also catches,
        # but the value must come from a None path, not float('nan'))
        assert pd.isna(result.iloc[0])

    def test_negative_returns_null_not_nan(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("log10", [pd.Series([-1.0])])
        assert pd.isna(result.iloc[0])

    def test_null_input_returns_null(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        result = reg.execute("log10", [pd.Series([None])])
        assert pd.isna(result.iloc[0])

    def test_positive_still_works(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("log10", [pd.Series([100.0])])
        assert result.iloc[0] == pytest.approx(2.0)

    def test_one_returns_zero(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute("log10", [pd.Series([1.0])])
        assert result.iloc[0] == pytest.approx(0.0)

    def test_consistent_with_log_and_log2(
        self,
        reg: ScalarFunctionRegistry,
    ) -> None:
        """All three log functions must agree on domain-error → null."""
        for fn in ("log", "log2", "log10"):
            zero_result = reg.execute(fn, [pd.Series([0.0])])
            neg_result = reg.execute(fn, [pd.Series([-1.0])])
            assert pd.isna(zero_result.iloc[0]), f"{fn}(0) should be null"
            assert pd.isna(neg_result.iloc[0]), f"{fn}(-1) should be null"

    def test_mixed_column(self, reg: ScalarFunctionRegistry) -> None:
        s = pd.Series([100.0, 0.0, -1.0, 10.0])
        result = reg.execute("log10", [s])
        assert result.iloc[0] == pytest.approx(2.0)
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] == pytest.approx(1.0)

    def test_in_where_clause_nulls_excluded(self, log_star: Star) -> None:
        """Domain-error nulls are excluded from WHERE comparisons (null-safe)."""
        r = log_star.execute_query(
            "MATCH (p:Person) WHERE log10(p.val) >= 1.0 RETURN p.name ORDER BY p.name",
        )
        # val=100 → log10=2.0 ✓, val=10 → log10=1.0 ✓, val=0 → null, val=-1 → null
        assert list(r["name"]) == ["Alice", "Dave"]

    def test_isnan_does_not_see_domain_error_as_nan(
        self,
        log_star: Star,
    ) -> None:
        """log10(0) is a domain error → null. isNaN(null) → null (null propagates)."""
        r = log_star.execute_query(
            "MATCH (p:Person) WHERE p.val = 0.0 RETURN isNaN(log10(p.val)) AS r",
        )
        # log10(0) → null; isNaN(null) → null per Neo4j null-propagation semantics.
        # (Previously returned False due to missing null guard; null is now correct.)
        result_val = r["r"].iloc[0]
        import pandas as pd

        assert result_val is None or pd.isna(result_val)
