"""Tests for the _coerce_join helper extracted from star.py.

_coerce_join(frame_a, frame_b) joins two BindingFrames:
  - If they share a variable name, keyed inner-join on that variable.
  - If they share no variable, Cartesian cross-join.

This pattern previously appeared verbatim in _match_to_binding_frame and
_merge_frames_for_match; the helper eliminates the duplication.

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def make_frame(
    bindings: dict, type_registry: dict, ctx: Context
) -> BindingFrame:
    """Build a BindingFrame directly for white-box tests."""
    return BindingFrame(
        bindings=pd.DataFrame(bindings),
        type_registry=type_registry,
        context=ctx,
    )


@pytest.fixture()
def empty_ctx() -> Context:
    """Empty context — no entities or relationships."""
    return Context(entity_mapping=EntityMapping(mapping={}))


# ---------------------------------------------------------------------------
# _coerce_join unit tests (white-box)
# ---------------------------------------------------------------------------


class TestCoerceJoinUnit:
    """Direct tests for Star._coerce_join()."""

    def test_shared_variable_triggers_inner_join(
        self, empty_ctx: Context
    ) -> None:
        """When frames share a variable, _coerce_join performs a keyed inner join."""
        star = Star(context=empty_ctx)
        fa = make_frame({"p": [1, 2, 3]}, {"p": "Person"}, empty_ctx)
        fb = make_frame(
            {"p": [2, 3, 4], "q": [20, 30, 40]},
            {"p": "Person", "q": "Company"},
            empty_ctx,
        )
        result = star._coerce_join(fa, fb)
        # Inner join on 'p': only rows with p in {2, 3} survive
        assert set(result.bindings["p"].tolist()) == {2, 3}

    def test_no_shared_variable_triggers_cross_join(
        self, empty_ctx: Context
    ) -> None:
        """When frames share no variable, _coerce_join performs a Cartesian product."""
        star = Star(context=empty_ctx)
        fa = make_frame({"p": [1, 2]}, {"p": "Person"}, empty_ctx)
        fb = make_frame({"q": [10, 20]}, {"q": "Company"}, empty_ctx)
        result = star._coerce_join(fa, fb)
        # Cross product: 2 × 2 = 4 rows
        assert len(result.bindings) == 4

    def test_cross_join_contains_all_combinations(
        self, empty_ctx: Context
    ) -> None:
        """Cross-join result has every combination from both frames."""
        star = Star(context=empty_ctx)
        fa = make_frame({"p": [1, 2]}, {"p": "Person"}, empty_ctx)
        fb = make_frame({"q": [10, 20]}, {"q": "Company"}, empty_ctx)
        result = star._coerce_join(fa, fb)
        # All 4 combinations must be present (order may vary)
        p_vals = sorted(result.bindings["p"].tolist())
        q_vals = sorted(result.bindings["q"].tolist())
        assert p_vals == [1, 1, 2, 2]
        assert q_vals == [10, 10, 20, 20]

    def test_single_row_frames_join_correctly(
        self, empty_ctx: Context
    ) -> None:
        """Single-row frames with shared variable produce single-row output."""
        star = Star(context=empty_ctx)
        fa = make_frame({"x": [5]}, {"x": "T"}, empty_ctx)
        fb = make_frame({"x": [5], "y": [99]}, {"x": "T", "y": "U"}, empty_ctx)
        result = star._coerce_join(fa, fb)
        assert len(result.bindings) == 1
        assert result.bindings["y"].iloc[0] == 99

    def test_mismatched_shared_variable_produces_empty(
        self, empty_ctx: Context
    ) -> None:
        """When shared variable has no common ID values, inner-join returns empty."""
        star = Star(context=empty_ctx)
        fa = make_frame({"p": [1, 2]}, {"p": "Person"}, empty_ctx)
        fb = make_frame({"p": [3, 4]}, {"p": "Person"}, empty_ctx)
        result = star._coerce_join(fa, fb)
        assert len(result.bindings) == 0


# ---------------------------------------------------------------------------
# Integration tests — _coerce_join is used inside _match_to_binding_frame
# ---------------------------------------------------------------------------


@pytest.fixture()
def two_table_ctx() -> Context:
    """Context with Person and Company entity types."""
    persons_df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]})
    companies_df = pd.DataFrame(
        {ID_COLUMN: [10, 20], "industry": ["Tech", "Finance"]}
    )
    persons_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=persons_df,
    )
    companies_table = EntityTable(
        entity_type="Company",
        identifier="Company",
        column_names=[ID_COLUMN, "industry"],
        source_obj_attribute_map={"industry": "industry"},
        attribute_map={"industry": "industry"},
        source_obj=companies_df,
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": persons_table, "Company": companies_table}
        )
    )


class TestCoerceJoinIntegration:
    """Integration tests confirming _coerce_join governs MATCH cross-products."""

    def test_two_independent_matches_produce_cross_product(
        self, two_table_ctx: Context
    ) -> None:
        """Two MATCH clauses with no shared variables → cross product."""
        star = Star(context=two_table_ctx)
        result = star.execute_query(
            "MATCH (p:Person) MATCH (c:Company) "
            "RETURN p.name AS person, c.industry AS industry"
        )
        # 2 persons × 2 companies = 4 rows
        assert len(result) == 4

    def test_query_still_works_after_refactor(
        self, two_table_ctx: Context
    ) -> None:
        """Basic MATCH + WHERE + RETURN works correctly after helper extraction."""
        star = Star(context=two_table_ctx)
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert set(result["name"].tolist()) == {"Alice", "Bob"}
