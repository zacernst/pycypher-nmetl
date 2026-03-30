"""Tests for MATCH clause cardinality-based reordering.

Verifies that the query executor reorders consecutive MATCH clauses
by estimated cardinality (smallest first) to minimize intermediate
result sizes and cross-join explosion risk.
"""

from __future__ import annotations

import copy

import pandas as pd
import pytest
from pycypher.ast_converter import _parse_cypher_cached
from pycypher.ast_models import Match
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


def _make_entity_table(
    label: str,
    n_rows: int,
    extra_cols: dict | None = None,
) -> EntityTable:
    """Create an EntityTable with n_rows synthetic rows."""
    data = {ID_COLUMN: list(range(1, n_rows + 1))}
    attr_map = {}
    if extra_cols:
        data.update(extra_cols)
        attr_map = {c: c for c in extra_cols}
    df = pd.DataFrame(data)
    return EntityTable(
        entity_type=label,
        identifier=label,
        column_names=list(df.columns),
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=df,
    )


@pytest.fixture
def asymmetric_context() -> Context:
    """Context with asymmetric table sizes: Small(5), Medium(50), Large(500)."""
    small = _make_entity_table("Small", 5, {"val": list(range(5))})
    medium = _make_entity_table("Medium", 50, {"val": list(range(50))})
    large = _make_entity_table("Large", 500, {"val": list(range(500))})
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Small": small, "Medium": medium, "Large": large},
        ),
    )


def _parse_fresh(cypher: str):
    """Parse a Cypher query and return a mutable deep copy of the AST."""
    return copy.deepcopy(_parse_cypher_cached(cypher))


class TestMatchReorderingCorrectness:
    """Verify reordering doesn't change query results."""

    def test_two_match_clauses_same_result_either_order(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Cross-join result is the same regardless of MATCH order."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Small) MATCH (b:Medium) RETURN count(*) AS cnt",
        )
        # 5 x 50 = 250
        assert result["cnt"].iloc[0] == 250

    def test_three_match_clauses_correct_count(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Three-way cross-join produces correct cardinality."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Medium) MATCH (b:Small) RETURN count(*) AS cnt",
        )
        assert result["cnt"].iloc[0] == 250

    def test_reordering_preserves_variable_binding(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Variables from reordered MATCH clauses are still accessible."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Large) MATCH (b:Small) "
            "RETURN a.val AS a_val, b.val AS b_val "
            "LIMIT 5",
        )
        assert "a_val" in result.columns
        assert "b_val" in result.columns
        assert len(result) == 5


class TestMatchReorderingStrategy:
    """Verify the optimizer reorders as expected."""

    def test_large_then_small_gets_reordered(
        self,
        asymmetric_context: Context,
    ) -> None:
        """MATCH (a:Large) MATCH (b:Small) should be reordered to Small first."""
        ast = _parse_fresh(
            "MATCH (a:Large) MATCH (b:Small) RETURN count(*) AS c",
        )
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        first_label = matches[0].pattern.paths[0].elements[0].labels[0]
        assert first_label == "Small"

    def test_already_optimal_not_changed(
        self,
        asymmetric_context: Context,
    ) -> None:
        """MATCH (a:Small) MATCH (b:Large) is already optimal -- no change."""
        ast = _parse_fresh(
            "MATCH (a:Small) MATCH (b:Large) RETURN count(*) AS c",
        )
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        first_label = matches[0].pattern.paths[0].elements[0].labels[0]
        assert first_label == "Small"

    def test_optional_match_not_reordered(
        self,
        asymmetric_context: Context,
    ) -> None:
        """OPTIONAL MATCH clauses should not be included in reordering."""
        ast = _parse_fresh(
            "MATCH (a:Large) OPTIONAL MATCH (b:Small) RETURN a.val AS v LIMIT 5",
        )
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        first_label = matches[0].pattern.paths[0].elements[0].labels[0]
        assert first_label == "Large"

    def test_single_match_no_reordering(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Single MATCH clause needs no reordering."""
        ast = _parse_fresh("MATCH (a:Large) RETURN count(*) AS c")
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        assert len(ast.clauses) == 2  # MATCH + RETURN

    def test_three_way_reorder_smallest_first(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Three MATCH clauses reordered: Large, Medium, Small -> Small, Medium, Large."""
        ast = _parse_fresh(
            "MATCH (a:Large) MATCH (b:Medium) MATCH (c:Small) RETURN count(*) AS cnt",
        )
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        labels = [m.pattern.paths[0].elements[0].labels[0] for m in matches]
        assert labels == ["Small", "Medium", "Large"]
