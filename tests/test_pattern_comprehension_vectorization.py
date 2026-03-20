"""TDD tests for vectorised PatternComprehension evaluation (Loop 175).

Problem: ``_eval_pattern_comprehension`` in ``binding_evaluator.py`` uses a
nested Python loop:

    for anchor_id in anchor_ids:                         # O(n_rows)
        for other_id in rel_df filtered by anchor_id:    # O(m_matches)
            sub_eval = self._make_single_row_evaluator(...)  # BindingFrame+Evaluator per match
            if pc.where: sub_eval.evaluate(pc.where)     # full expression eval per match
            if pc.map_expr: sub_eval.evaluate(pc.map_expr)

For a 1 000-row anchor frame with 50 matches each, this creates 50 000
BindingFrame+Evaluator objects and evaluates the WHERE expression 50 000 times
on single-row frames — the dominant remaining hot path in complex queries.

Fix: replace the nested loop with a vectorised merge + single-pass evaluation:

    1. ``pd.merge(anchor_df, rel_df, ...)``  — one vectorised join for all rows
    2. Build a single BindingFrame from all pairs
    3. Evaluate WHERE once over all pairs  (vectorised, uses Kleene operators)
    4. Evaluate map_expr once over surviving pairs
    5. Groupby ``__row_idx__`` and collect into per-anchor lists

This eliminates O(total_matches) BindingFrame+Evaluator allocations and
replaces O(total_matches) single-row WHERE evaluations with one vectorised pass.

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
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_knows_context(
    n_people: int = 3,
    edges: list[tuple[int, int]] | None = None,
) -> Context:
    """Create a Person/KNOWS context with *n_people* people.

    If *edges* is None a triangle is used: 1→2, 1→3, 2→3.
    Each person has ``age = id * 10``.
    """
    people_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_people + 1)),
            "name": [f"P{i}" for i in range(1, n_people + 1)],
            "age": [i * 10 for i in range(1, n_people + 1)],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )

    if edges is None:
        edges = [(1, 2), (1, 3), (2, 3)]
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(100, 100 + len(edges))),
            "__SOURCE__": [s for s, _ in edges],
            "__TARGET__": [t for _, t in edges],
        }
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


@pytest.fixture()
def ctx() -> Context:
    return _make_knows_context()


# ---------------------------------------------------------------------------
# Category 1 — Correctness: basic comprehension (no WHERE, no map_expr)
# ---------------------------------------------------------------------------


class TestPatternComprehensionCorrectness:
    """Vectorised implementation must produce the same results as the row-by-row one."""

    def test_basic_returns_other_ids_as_list(self, ctx: Context) -> None:
        """``[(p)-[:KNOWS]->(f:Person)]`` returns a list of target IDs, one list per row."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends"
        )
        assert len(result) == 1
        friends = result["friends"].iloc[0]
        assert set(friends) == {"P2", "P3"}

    def test_no_outgoing_edges_gives_empty_list(self, ctx: Context) -> None:
        """A node with no outgoing KNOWS edges gets an empty list."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P3' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends"
        )
        assert result["friends"].iloc[0] == []

    def test_multiple_anchor_rows_each_get_correct_list(
        self, ctx: Context
    ) -> None:
        """All three people get the correct friend list in a single multi-row query."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS person, "
            "       [(p)-[:KNOWS]->(f:Person) | f.name] AS friends "
            "ORDER BY p.name"
        )
        assert len(result) == 3
        by_name = result.set_index("person")["friends"].to_dict()
        assert set(by_name["P1"]) == {"P2", "P3"}
        assert by_name["P2"] == ["P3"]
        assert by_name["P3"] == []

    def test_result_is_series_of_lists(self, ctx: Context) -> None:
        """The result column must be a Series where every element is a list."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends"
        )
        for cell in result["friends"]:
            assert isinstance(cell, list), (
                f"Expected list, got {type(cell)}: {cell!r}"
            )


# ---------------------------------------------------------------------------
# Category 2 — Correctness: WHERE inside the comprehension
# ---------------------------------------------------------------------------


class TestPatternComprehensionWithWhere:
    """WHERE predicate inside the comprehension must be evaluated vectorised."""

    def test_where_filters_by_age(self, ctx: Context) -> None:
        """WHERE f.age > 15 keeps P2 (age=20) and P3 (age=30); excludes no one."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 15 | f.name] AS friends"
        )
        assert set(result["friends"].iloc[0]) == {"P2", "P3"}

    def test_where_filters_out_all_matches(self, ctx: Context) -> None:
        """WHERE f.age > 9999 excludes every target — result is empty list."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 9999 | f.name] AS friends"
        )
        assert result["friends"].iloc[0] == []

    def test_where_filters_some_matches(self, ctx: Context) -> None:
        """WHERE f.age > 20 keeps only P3 (age=30) from P1's two friends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 20 | f.name] AS friends"
        )
        assert result["friends"].iloc[0] == ["P3"]

    def test_where_applied_independently_per_anchor_row(
        self, ctx: Context
    ) -> None:
        """WHERE is applied per-match globally — each anchor gets its correctly filtered list."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS person, "
            "       [(p)-[:KNOWS]->(f:Person) WHERE f.age >= 20 | f.name] AS friends "
            "ORDER BY p.name"
        )
        by_name = result.set_index("person")["friends"].to_dict()
        # P1 knows P2 (age=20, included) and P3 (age=30, included)
        assert set(by_name["P1"]) == {"P2", "P3"}
        # P2 knows P3 (age=30, included)
        assert by_name["P2"] == ["P3"]
        # P3 has no outgoing KNOWS
        assert by_name["P3"] == []

    def test_where_with_string_predicate(self, ctx: Context) -> None:
        """WHERE f.name = 'P2' keeps only P2 from P1's friends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.name = 'P2' | f.name] AS friends"
        )
        assert result["friends"].iloc[0] == ["P2"]


# ---------------------------------------------------------------------------
# Category 3 — Correctness: map expression
# ---------------------------------------------------------------------------


class TestPatternComprehensionMapExpr:
    """Map expression is evaluated once over all matches (vectorised)."""

    def test_map_expr_returns_property_values(self, ctx: Context) -> None:
        """``| f.name`` returns friend name strings, not IDs."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS names"
        )
        names = result["names"].iloc[0]
        assert set(names) == {"P2", "P3"}
        for name in names:
            assert isinstance(name, str)

    def test_map_expr_returns_age_integers(self, ctx: Context) -> None:
        """``| f.age`` returns numeric ages."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.age] AS ages"
        )
        ages = result["ages"].iloc[0]
        assert set(ages) == {20, 30}

    def test_map_expr_with_where_evaluates_over_filtered_rows(
        self, ctx: Context
    ) -> None:
        """After WHERE filtering, map_expr is evaluated only over survivors."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 20 | f.age] AS ages"
        )
        ages = result["ages"].iloc[0]
        # Only P3 (age=30) survives the WHERE
        assert ages == [30]


# ---------------------------------------------------------------------------
# Category 4 — Correctness: large graph (stress test for vector correctness)
# ---------------------------------------------------------------------------


class TestPatternComprehensionLargeGraph:
    """Correctness at scale — ensures vectorised path handles many rows/matches."""

    def _make_large_context(
        self, n_people: int = 200, edges_per_person: int = 10
    ) -> Context:
        """Create context with *n_people* persons and *edges_per_person* outgoing edges each."""
        person_ids = list(range(1, n_people + 1))
        people_df = pd.DataFrame(
            {
                ID_COLUMN: person_ids,
                "name": [f"P{i}" for i in person_ids],
                "age": [i * 10 % 100 + 1 for i in person_ids],
            }
        )
        people_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age"],
            source_obj_attribute_map={"name": "name", "age": "age"},
            attribute_map={"name": "name", "age": "age"},
            source_obj=people_df,
        )
        # Each person i knows persons (i % n + 1), ((i+1) % n + 1), etc.
        sources, targets, rel_ids = [], [], []
        rid = 1000
        for src in person_ids:
            for k in range(edges_per_person):
                tgt = (src + k) % n_people + 1
                if tgt != src:
                    sources.append(src)
                    targets.append(tgt)
                    rel_ids.append(rid)
                    rid += 1
        knows_df = pd.DataFrame(
            {
                ID_COLUMN: rel_ids,
                "__SOURCE__": sources,
                "__TARGET__": targets,
            }
        )
        knows_table = RelationshipTable(
            relationship_type="KNOWS",
            identifier="KNOWS",
            column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=knows_df,
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table}
            ),
        )

    def test_large_graph_produces_list_per_row(self) -> None:
        """200 anchor rows × 10 edges each — every row gets a non-empty list."""
        ctx = self._make_large_context(n_people=200, edges_per_person=10)
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends"
        )
        assert len(result) == 200
        for cell in result["friends"]:
            assert isinstance(cell, list)
            assert len(cell) > 0, (
                "Every person should have at least one friend"
            )

    def test_large_graph_with_where_filter(self) -> None:
        """WHERE applied across 200×10 = 2 000 matches in one vectorised pass."""
        ctx = self._make_large_context(n_people=200, edges_per_person=10)
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 50 | f.name] AS rich_friends"
        )
        assert len(result) == 200
        # Every entry must be a list (may be empty for some anchors)
        for cell in result["rich_friends"]:
            assert isinstance(cell, list)


# ---------------------------------------------------------------------------
# Category 5 — Performance: vectorised path must be substantially faster
# ---------------------------------------------------------------------------


class TestPatternComprehensionPerformance:
    """Absolute-threshold performance tests (no baseline comparison needed)."""

    def _make_perf_context(self) -> Context:
        """500 persons × 20 outgoing edges = 10 000 relationship matches to process."""
        n = 500
        edges_per = 20
        person_ids = list(range(1, n + 1))
        people_df = pd.DataFrame(
            {
                ID_COLUMN: person_ids,
                "name": [f"P{i}" for i in person_ids],
                "age": [(i * 7) % 90 + 1 for i in person_ids],
            }
        )
        people_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age"],
            source_obj_attribute_map={"name": "name", "age": "age"},
            attribute_map={"name": "name", "age": "age"},
            source_obj=people_df,
        )
        sources, targets, rel_ids = [], [], []
        rid = 5000
        for src in person_ids:
            for k in range(1, edges_per + 1):
                tgt = (src + k - 1) % n + 1
                sources.append(src)
                targets.append(tgt)
                rel_ids.append(rid)
                rid += 1
        knows_df = pd.DataFrame(
            {
                ID_COLUMN: rel_ids,
                "__SOURCE__": sources,
                "__TARGET__": targets,
            }
        )
        knows_table = RelationshipTable(
            relationship_type="KNOWS",
            identifier="KNOWS",
            column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=knows_df,
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table}
            ),
        )

    def test_basic_comprehension_500_rows_fast(self) -> None:
        """500 anchors × 20 edges: basic comprehension must complete in < 3s."""
        ctx = self._make_perf_context()
        star = Star(context=ctx)
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends"
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 500
        assert elapsed < 3.0, (
            f"500-row pattern comprehension took {elapsed:.3f}s — expected < 3s. "
            "Vectorised implementation should be much faster than per-row loop."
        )

    def test_where_comprehension_500_rows_fast(self) -> None:
        """500 anchors × 20 edges + WHERE: must complete in < 3s."""
        ctx = self._make_perf_context()
        star = Star(context=ctx)
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 45 | f.name] AS friends"
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 500
        assert elapsed < 3.0, (
            f"500-row WHERE comprehension took {elapsed:.3f}s — expected < 3s."
        )

    def test_map_expr_comprehension_500_rows_fast(self) -> None:
        """500 anchors × 20 edges + map_expr: must complete in < 3s."""
        ctx = self._make_perf_context()
        star = Star(context=ctx)
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) RETURN [(p)-[:KNOWS]->(f:Person) | f.age] AS ages"
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 500
        assert elapsed < 3.0, (
            f"500-row map_expr comprehension took {elapsed:.3f}s — expected < 3s."
        )
