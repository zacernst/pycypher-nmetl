"""TDD tests for vectorised list-comprehension and quantifier execution (Loop 177).

Problem: ``_eval_list_comprehension`` and ``_eval_quantifier`` in
``binding_evaluator.py`` allocate one ``BindingFrame`` + ``BindingExpressionEvaluator``
**per element** across all rows:

    for raw_list in list_series:           # O(n_rows)
        for item in items:                 # O(m_elements_per_row)
            elem_evaluator = self._make_single_row_evaluator({var_name: item})
            ...                            # BindingFrame + evaluator allocation!

For a 500-row frame each with 20-element lists, this means 10 000 BindingFrame
allocations — even though the WHERE and map expressions are identical for every
element.

Fix: explode all (row_idx, element) pairs into a single flat DataFrame, build
**one** BindingFrame, evaluate WHERE/map once over all elements, then re-group
back to per-row lists via a lightweight Python loop.

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import time

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

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def _make_ctx(n_people: int = 5, *, scores_factor: int = 10) -> Context:
    """Return a Person context where each person has a ``scores`` list.

    ``scores`` = [id * scores_factor, id * scores_factor + 1, ...] with
    ``n_scores`` elements per person. Tags = [«P{id}», «all»].
    """
    ids = list(range(1, n_people + 1))
    people_df = pd.DataFrame(
        {
            ID_COLUMN: ids,
            "name": [f"P{i}" for i in ids],
            "age": [i * 10 for i in ids],
            "scores": [
                [i * scores_factor, i * scores_factor + 5] for i in ids
            ],
            "tags": [[f"P{i}", "all"] for i in ids],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "scores", "tags"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "scores": "scores",
            "tags": "tags",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "scores": "scores",
            "tags": "tags",
        },
        source_obj=people_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture()
def ctx() -> Context:
    return _make_ctx()


# ---------------------------------------------------------------------------
# Category 1 — List comprehension correctness: no WHERE, no map
# ---------------------------------------------------------------------------


class TestListComprehensionBasic:
    """[x IN list] with no filter or transform must return the list unchanged."""

    def test_identity_comprehension_returns_all_elements(
        self, ctx: Context
    ) -> None:
        """[x IN p.scores | x] returns the same list for each person."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P2' RETURN [x IN p.scores | x] AS vals"
        )
        assert len(result) == 1
        vals = result["vals"].iloc[0]
        assert list(vals) == [20, 25]

    def test_identity_comprehension_multi_row(self, ctx: Context) -> None:
        """[x IN p.scores | x] evaluated over multiple rows is correct for each."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, [x IN p.scores | x] AS vals "
            "ORDER BY p.name"
        )
        assert len(result) == 5
        # P1: [10, 15], P2: [20, 25], P3: [30, 35], P4: [40, 45], P5: [50, 55]
        assert list(result["vals"].iloc[0]) == [10, 15]
        assert list(result["vals"].iloc[1]) == [20, 25]
        assert list(result["vals"].iloc[4]) == [50, 55]


# ---------------------------------------------------------------------------
# Category 2 — List comprehension correctness: WHERE filter
# ---------------------------------------------------------------------------


class TestListComprehensionWhere:
    """[x IN list WHERE cond] must filter elements correctly."""

    def test_where_filters_below_threshold(self, ctx: Context) -> None:
        """[x IN p.scores WHERE x > 22] for P2 (scores=[20,25]) → [25]."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P2' "
            "RETURN [x IN p.scores WHERE x > 22] AS vals"
        )
        assert len(result) == 1
        assert list(result["vals"].iloc[0]) == [25]

    def test_where_all_filtered_out_returns_empty_list(
        self, ctx: Context
    ) -> None:
        """[x IN p.scores WHERE x > 9999] → [] for every person."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN [x IN p.scores WHERE x > 9999] AS vals "
            "ORDER BY p.name"
        )
        assert len(result) == 5
        for _, row in result.iterrows():
            assert list(row["vals"]) == []

    def test_where_keeps_all_elements(self, ctx: Context) -> None:
        """[x IN p.scores WHERE x > 0] keeps all elements for P1 (10, 15)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [x IN p.scores WHERE x > 0] AS vals"
        )
        assert list(result["vals"].iloc[0]) == [10, 15]

    def test_where_multi_row_different_filters(self, ctx: Context) -> None:
        """WHERE x >= 30 filters differently per row when evaluated in batch."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, [x IN p.scores WHERE x >= 30] AS vals "
            "ORDER BY p.name"
        )
        # P1 (10,15) → [], P2 (20,25) → [], P3 (30,35) → [30,35]
        assert list(result["vals"].iloc[0]) == []  # P1
        assert list(result["vals"].iloc[1]) == []  # P2
        assert list(result["vals"].iloc[2]) == [30, 35]  # P3


# ---------------------------------------------------------------------------
# Category 3 — List comprehension correctness: map expression
# ---------------------------------------------------------------------------


class TestListComprehensionMap:
    """[x IN list | expr] must transform elements correctly."""

    def test_map_doubles_elements(self, ctx: Context) -> None:
        """[x IN p.scores | x * 2] for P1 → [20, 30]."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [x IN p.scores | x * 2] AS vals"
        )
        assert list(result["vals"].iloc[0]) == [20, 30]

    def test_map_with_where(self, ctx: Context) -> None:
        """[x IN p.scores WHERE x > 12 | x + 100] for P1 → [115]."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [x IN p.scores WHERE x > 12 | x + 100] AS vals"
        )
        assert list(result["vals"].iloc[0]) == [115]


# ---------------------------------------------------------------------------
# Category 4 — List comprehension: edge cases
# ---------------------------------------------------------------------------


class TestListComprehensionEdgeCases:
    """Empty and null-equivalent lists must not crash and return []."""

    def test_empty_list_literal(self, ctx: Context) -> None:
        """[x IN [] WHERE x > 0] → []."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' RETURN [x IN [] WHERE x > 0] AS vals"
        )
        assert list(result["vals"].iloc[0]) == []


# ---------------------------------------------------------------------------
# Category 5 — Quantifier correctness: ANY
# ---------------------------------------------------------------------------


class TestQuantifierAny:
    """any(x IN list WHERE cond) must implement existential quantification."""

    def test_any_true_when_element_matches(self, ctx: Context) -> None:
        """any(x IN p.scores WHERE x > 12) is True for P1 (15 > 12)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND any(x IN p.scores WHERE x > 12) "
            "RETURN p.name AS name"
        )
        assert len(result) == 1

    def test_any_false_when_no_element_matches(self, ctx: Context) -> None:
        """any(x IN p.scores WHERE x > 9999) is False for all persons."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 9999) "
            "RETURN p.name AS name"
        )
        assert len(result) == 0

    def test_any_multi_row(self, ctx: Context) -> None:
        """any(x IN p.scores WHERE x >= 50) true only for P5 (50,55)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x >= 50) "
            "RETURN p.name AS name"
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "P5"


# ---------------------------------------------------------------------------
# Category 6 — Quantifier correctness: ALL
# ---------------------------------------------------------------------------


class TestQuantifierAll:
    """all(x IN list WHERE cond) must implement universal quantification."""

    def test_all_true_when_all_match(self, ctx: Context) -> None:
        """all(x IN p.scores WHERE x > 0) is True for all persons."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE all(x IN p.scores WHERE x > 0) "
            "RETURN p.name AS name "
            "ORDER BY p.name"
        )
        assert len(result) == 5

    def test_all_false_when_one_fails(self, ctx: Context) -> None:
        """all(x IN p.scores WHERE x > 12) is False for P1 (10 fails)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND all(x IN p.scores WHERE x > 12) "
            "RETURN p.name AS name"
        )
        assert len(result) == 0

    def test_all_vacuously_true_for_empty_list(self, ctx: Context) -> None:
        """all(x IN [] WHERE x > 0) is vacuously True."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND all(x IN [] WHERE x > 0) "
            "RETURN p.name AS name"
        )
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Category 7 — Quantifier correctness: NONE
# ---------------------------------------------------------------------------


class TestQuantifierNone:
    """none(x IN list WHERE cond) must reject all matching."""

    def test_none_true_when_no_element_matches(self, ctx: Context) -> None:
        """none(x IN p.scores WHERE x > 9999) is True for all persons."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE none(x IN p.scores WHERE x > 9999) "
            "RETURN p.name AS name "
            "ORDER BY p.name"
        )
        assert len(result) == 5

    def test_none_false_when_element_matches(self, ctx: Context) -> None:
        """none(x IN p.scores WHERE x > 12) is False for P1 (15 matches)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND none(x IN p.scores WHERE x > 12) "
            "RETURN p.name AS name"
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Category 8 — Quantifier correctness: SINGLE
# ---------------------------------------------------------------------------


class TestQuantifierSingle:
    """single(x IN list WHERE cond) must require exactly one match."""

    def test_single_true_when_exactly_one_matches(self, ctx: Context) -> None:
        """single(x IN p.scores WHERE x > 12) is True for P1 (only 15 matches, 10 doesn't)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND single(x IN p.scores WHERE x > 12) "
            "RETURN p.name AS name"
        )
        assert len(result) == 1

    def test_single_false_when_two_match(self, ctx: Context) -> None:
        """single(x IN p.scores WHERE x > 0) is False for P1 (both 10 and 15 match)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND single(x IN p.scores WHERE x > 0) "
            "RETURN p.name AS name"
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Category 9 — Performance: batch is substantially faster
# ---------------------------------------------------------------------------


def _make_large_ctx(n_people: int = 200, n_scores: int = 50) -> Context:
    """Create a context with ``n_people`` persons, each with ``n_scores`` scores."""
    ids = list(range(1, n_people + 1))
    people_df = pd.DataFrame(
        {
            ID_COLUMN: ids,
            "name": [f"P{i}" for i in ids],
            "age": [(i * 7) % 90 + 1 for i in ids],
            "scores": [list(range(i, i + n_scores)) for i in ids],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "scores"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "scores": "scores",
        },
        attribute_map={"name": "name", "age": "age", "scores": "scores"},
        source_obj=people_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestListComprehensionPerformance:
    """Batch execution must complete under tight absolute thresholds.

    Old per-element baseline: O(n_rows * n_elements) BindingFrame allocations.
    Batch target: one BindingFrame for all (row, element) pairs → vectorized
    WHERE + map.
    """

    def test_list_comprehension_200_rows_50_elements_fast(self) -> None:
        """200 rows × 50 elements = 10 000 elements; must complete in < 0.5s."""
        ctx = _make_large_ctx(n_people=200, n_scores=50)
        star = Star(context=ctx)
        # Warm-up
        star.execute_query(
            "MATCH (p:Person) RETURN [x IN p.scores WHERE x > 100 | x * 2] AS vals"
        )
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) RETURN [x IN p.scores WHERE x > 100 | x * 2] AS vals"
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 200
        assert elapsed < 0.5, (
            f"200×50 list comprehension took {elapsed:.3f}s "
            f"— expected < 0.5s with batch execution."
        )

    def test_quantifier_any_200_rows_50_elements_fast(self) -> None:
        """any() over 200×50 elements must complete in < 0.5s."""
        ctx = _make_large_ctx(n_people=200, n_scores=50)
        star = Star(context=ctx)
        star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
            "RETURN p.name AS name"
        )
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE any(x IN p.scores WHERE x > 100) "
            "RETURN p.name AS name"
        )
        elapsed = time.perf_counter() - start
        assert isinstance(result, pd.DataFrame)
        assert elapsed < 0.5, (
            f"200×50 any() took {elapsed:.3f}s — expected < 0.5s."
        )

    def test_quantifier_all_200_rows_50_elements_fast(self) -> None:
        """all() over 200×50 elements must complete in < 0.5s."""
        ctx = _make_large_ctx(n_people=200, n_scores=50)
        star = Star(context=ctx)
        star.execute_query(
            "MATCH (p:Person) WHERE all(x IN p.scores WHERE x > 0) "
            "RETURN p.name AS name"
        )
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE all(x IN p.scores WHERE x > 0) "
            "RETURN p.name AS name"
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 200  # all elements > 0
        assert elapsed < 0.5, (
            f"200×50 all() took {elapsed:.3f}s — expected < 0.5s."
        )
