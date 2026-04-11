"""TDD tests for vectorised EXISTS subquery execution (Loop 176).

Problem: ``_exists_via_query_execution`` in ``binding_evaluator.py`` executes
a subquery once **per row** of the outer frame:

    for row_idx in range(len(self.frame.bindings)):
        row_frame = BindingFrame(single-row)
        result_df = temp_star._execute_query_binding_frame_inner(subquery, initial_frame=row_frame)
        results.append(len(result_df) > 0)

For a 100-row outer frame, this invokes ``_execute_query_binding_frame_inner``
100 times — each with its own Lark parse cache hit, pattern-to-frame
translation, MATCH join, WHERE evaluation, and RETURN projection.  Even if
the parse is cached, the relational algebra (join + filter + project) runs
100 × independently with single-row seeds.

Fix: batch all rows into a single execution using a sentinel column:

    1. Add ``__exists_row_idx__ = range(n_rows)`` to the initial frame.
    2. Strip the subquery's RETURN clause; add ``RETURN __exists_row_idx__``
       so we can trace which input rows produced at least one match.
    3. Execute once — ``_execute_query_binding_frame_inner`` with the full
       multi-row initial frame naturally propagates the sentinel through the
       MATCH, WHERE, and RETURN stages.
    4. Collect the unique sentinel values from the result; build a boolean
       Series (True where the index appears, False elsewhere).
    5. Fall back to per-row execution when the subquery contains WITH clauses
       (those can drop variables from scope, including the sentinel).

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
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


def _make_context(
    n_people: int = 5,
    edges: list[tuple[int, int]] | None = None,
) -> Context:
    """Return a Person/KNOWS context with ``n_people`` persons.

    Default triangle: 1→2, 1→3, 2→3.  Age = id * 10.
    """
    ids = list(range(1, n_people + 1))
    people_df = pd.DataFrame(
        {
            ID_COLUMN: ids,
            "name": [f"P{i}" for i in ids],
            "age": [i * 10 for i in ids],
        },
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
        edges = [(1, 2), (1, 3), (2, 3)]  # 1→2, 1→3, 2→3

    knows_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(100, 100 + len(edges))),
            "__SOURCE__": [s for s, _ in edges],
            "__TARGET__": [t for _, t in edges],
        },
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
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture
def ctx() -> Context:
    return _make_context()


# ---------------------------------------------------------------------------
# Category 1 — Correctness: basic EXISTS { MATCH ... } with no WHERE
# ---------------------------------------------------------------------------


class TestExistsQueryCorrectnessBasic:
    """Batch implementation must produce correct True/False per row."""

    def test_exists_subquery_returns_true_for_row_with_match(
        self,
        ctx: Context,
    ) -> None:
        """P1 has outgoing KNOWS → EXISTS returns True for P1."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P1' "
            "AND EXISTS { MATCH (p)-[:KNOWS]->(q) } "
            "RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "P1"

    def test_exists_subquery_returns_false_for_row_without_match(
        self,
        ctx: Context,
    ) -> None:
        """P3 has no outgoing KNOWS → WHERE EXISTS returns no rows."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'P3' "
            "AND EXISTS { MATCH (p)-[:KNOWS]->(q) } "
            "RETURN p.name AS name",
        )
        assert len(result) == 0

    def test_exists_subquery_multi_row_all_rows_correct(
        self,
        ctx: Context,
    ) -> None:
        """All three persons evaluated in one query: P1 and P2 match; P3 does not."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q) } "
            "RETURN p.name AS name "
            "ORDER BY p.name",
        )
        names = list(result["name"])
        assert "P1" in names
        assert "P2" in names
        assert "P3" not in names

    def test_not_exists_subquery_inverts_result(self) -> None:
        """NOT EXISTS returns True only for P3 (no outgoing edges) in a 3-person graph."""
        ctx3 = _make_context(
            n_people=3,
        )  # P1→P2, P1→P3, P2→P3 — only P3 has no outgoing
        star = Star(context=ctx3)
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(q) } "
            "RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "P3"


# ---------------------------------------------------------------------------
# Category 2 — Correctness: EXISTS { MATCH ... WHERE ... }
# ---------------------------------------------------------------------------


class TestExistsQueryCorrectnessWithWhere:
    """WHERE inside EXISTS subquery must be applied correctly in batch mode."""

    def test_exists_with_where_age_filter(self) -> None:
        """EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 25 }

        P1 knows P2 (age=20) and P3 (age=30). q.age > 25 → P3 qualifies → P1 True.
        P2 knows P3 (age=30). q.age > 25 → P3 qualifies → P2 True.
        P3 has no outgoing KNOWS → False.
        """
        ctx3 = _make_context(n_people=3)
        star = Star(context=ctx3)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 25 } "
            "RETURN p.name AS name "
            "ORDER BY p.name",
        )
        names = set(result["name"])
        assert names == {"P1", "P2"}

    def test_exists_with_where_name_filter(self) -> None:
        """EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.name = 'P3' }

        P1 knows P3 → True.  P2 knows P3 → True.  P3 has no outgoing → False.
        """
        ctx3 = _make_context(n_people=3)
        star = Star(context=ctx3)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.name = 'P3' } "
            "RETURN p.name AS name "
            "ORDER BY p.name",
        )
        names = set(result["name"])
        assert names == {"P1", "P2"}

    def test_exists_with_impossible_where_returns_false_for_all(self) -> None:
        """WHERE q.age > 9999 never matches → EXISTS returns False for every row."""
        ctx3 = _make_context(n_people=3)
        star = Star(context=ctx3)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 9999 } "
            "RETURN p.name AS name",
        )
        assert len(result) == 0

    def test_exists_all_rows_have_matches_returns_all(self) -> None:
        """Using edges that make every person have an outgoing KNOWS.

        Context: 1→2, 2→3, 3→1 (triangle) — all have outgoing edge.
        """
        ctx_ring = _make_context(n_people=3, edges=[(1, 2), (2, 3), (3, 1)])
        star = Star(context=ctx_ring)
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) } "
            "RETURN p.name AS name "
            "ORDER BY p.name",
        )
        assert len(result) == 3  # all three


# ---------------------------------------------------------------------------
# Category 3 — Correctness: EXISTS in conjunction with other predicates
# ---------------------------------------------------------------------------


class TestExistsQueryWithConjunction:
    """EXISTS combined with other WHERE predicates must evaluate correctly."""

    def test_exists_and_age_predicate(self) -> None:
        """WHERE p.age >= 20 AND EXISTS { MATCH (p)-[:KNOWS]->(q:Person) }

        P1 (age=10) — EXISTS True, age >= 20 False → excluded.
        P2 (age=20) — EXISTS True, age >= 20 True → included.
        P3 (age=30) — EXISTS False (no outgoing edges), age >= 20 True → excluded.
        """
        ctx3 = _make_context(n_people=3)
        star = Star(context=ctx3)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE p.age >= 20 AND EXISTS { MATCH (p)-[:KNOWS]->(q:Person) } "
            "RETURN p.name AS name",
        )
        names = set(result["name"])
        assert "P2" in names
        assert "P1" not in names
        assert "P3" not in names

    def test_exists_or_age_predicate(self) -> None:
        """WHERE p.age >= 30 OR EXISTS { MATCH (p)-[:KNOWS]->(q:Person) }

        P1 (age=10) — EXISTS True → included.
        P2 (age=20) — EXISTS True → included.
        P3 (age=30) — EXISTS False, age >= 30 True → included.
        """
        ctx3 = _make_context(n_people=3)
        star = Star(context=ctx3)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE p.age >= 30 OR EXISTS { MATCH (p)-[:KNOWS]->(q:Person) } "
            "RETURN p.name AS name "
            "ORDER BY p.name",
        )
        assert len(result) == 3  # all three


# ---------------------------------------------------------------------------
# Category 4 — Performance: batch execution is substantially faster
# ---------------------------------------------------------------------------


def _make_large_context(n: int = 500, edges_per: int = 10) -> Context:
    """Create a context with *n* persons and *edges_per* outgoing edges each."""
    ids = list(range(1, n + 1))
    people_df = pd.DataFrame(
        {
            ID_COLUMN: ids,
            "name": [f"P{i}" for i in ids],
            "age": [(i * 7) % 90 + 1 for i in ids],
        },
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
    rid = 1000
    for src in ids:
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
        },
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
            mapping={"KNOWS": knows_table},
        ),
    )


class TestExistsQueryPerformance:
    """EXISTS batch execution must complete within tight absolute thresholds.

    The old per-row implementation takes ~0.94s for 500 rows × 10 edges.
    The batch implementation should complete in < 0.5s for the same workload
    by executing the subquery once for all rows rather than 500 times.
    """

    def test_basic_exists_500_rows_fast(self) -> None:
        """500-row EXISTS { MATCH ... } must complete in < 0.5s (batch path).

        Old per-row baseline: ~0.94s.  Target: < 0.5s after vectorisation.
        """
        ctx = _make_large_context(n=500, edges_per=10)
        star = Star(context=ctx)
        # Warm up parse cache so we measure only the relational algebra cost.
        star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) } "
            "RETURN p.name AS name",
        )
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) } "
            "RETURN p.name AS name",
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 500  # all persons have edges
        assert elapsed < perf_threshold(0.5), (
            f"500-row EXISTS took {elapsed:.3f}s — expected < 0.5s with batch execution "
            f"(old per-row baseline was ~0.94s)."
        )

    def test_where_exists_500_rows_fast(self) -> None:
        """500-row EXISTS { MATCH ... WHERE ... } must complete in < 0.5s."""
        ctx = _make_large_context(n=500, edges_per=10)
        star = Star(context=ctx)
        star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 50 } "
            "RETURN p.name AS name",
        )
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 50 } "
            "RETURN p.name AS name",
        )
        elapsed = time.perf_counter() - start
        assert isinstance(result, pd.DataFrame)
        assert elapsed < perf_threshold(0.5), (
            f"500-row WHERE EXISTS took {elapsed:.3f}s — expected < 0.5s."
        )

    def test_not_exists_500_rows_fast(self) -> None:
        """NOT EXISTS on 500 rows must complete in < 0.5s."""
        ctx = _make_large_context(n=500, edges_per=10)
        star = Star(context=ctx)
        star.execute_query(
            "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 9999 } "
            "RETURN p.name AS name",
        )
        start = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 9999 } "
            "RETURN p.name AS name",
        )
        elapsed = time.perf_counter() - start
        assert len(result) == 500  # impossible WHERE → all persons qualify
        assert elapsed < perf_threshold(0.5), (
            f"500-row NOT EXISTS took {elapsed:.3f}s — expected < 0.5s."
        )
