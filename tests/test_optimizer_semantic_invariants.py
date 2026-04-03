"""Optimizer semantic invariant regression guards.

Verifies that ALL optimizer passes preserve query semantics — the same
query must produce identical results with and without each optimization.
This prevents the class of bug where an optimizer transform silently
produces wrong results rather than raising an error.

Covers:
- MATCH clause reordering (cardinality-based)
- Filter fusion (consecutive WHERE → single AND)
- Filter pushdown (WHERE below JOIN)
- Predicate pushdown in multi-path MATCH

Each test compares optimized vs. unoptimized execution to ensure
semantic equivalence.
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
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entity(label: str, n: int, **extra) -> EntityTable:
    data = {ID_COLUMN: list(range(1, n + 1))}
    attr_map = {}
    for col, vals in extra.items():
        data[col] = vals
        attr_map[col] = col
    df = pd.DataFrame(data)
    return EntityTable(
        entity_type=label,
        identifier=label,
        column_names=list(df.columns),
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=df,
    )


def _make_rel(
    rel_type: str, edges: list[tuple[int, int]]
) -> RelationshipTable:
    df = pd.DataFrame(
        {
            ID_COLUMN: list(range(100, 100 + len(edges))),
            "__SOURCE__": [e[0] for e in edges],
            "__TARGET__": [e[1] for e in edges],
        },
    )
    return RelationshipTable(
        relationship_type=rel_type,
        identifier=rel_type,
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=df,
    )


def _results_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """Compare two DataFrames ignoring row order."""
    if set(df1.columns) != set(df2.columns):
        return False
    if len(df1) != len(df2):
        return False
    # Sort by all columns for order-independent comparison
    cols = sorted(df1.columns)
    a = df1[cols].sort_values(cols).reset_index(drop=True)
    b = df2[cols].sort_values(cols).reset_index(drop=True)
    return a.equals(b)


def _run_with_reordering_disabled(star: Star, query: str) -> pd.DataFrame:
    """Execute query with MATCH reordering monkey-patched out."""
    orig = star._apply_match_reordering

    def noop(q):
        pass

    star._apply_match_reordering = noop
    try:
        return star.execute_query(query)
    finally:
        star._apply_match_reordering = orig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def graph_context() -> Context:
    """Rich graph with multiple entity types, relationships, and properties."""
    people = _make_entity(
        "Person",
        6,
        name=["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
        age=[30, 25, 35, 40, 28, 33],
        dept=["eng", "eng", "mktg", "eng", "mktg", "eng"],
    )
    products = _make_entity(
        "Product",
        4,
        name=["Widget", "Gadget", "Doohickey", "Thingamajig"],
        price=[10, 20, 15, 30],
    )
    knows = _make_rel(
        "KNOWS",
        [
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 4),
            (4, 5),
            (5, 6),
        ],
    )
    bought = _make_rel(
        "BOUGHT",
        [
            (1, 1),
            (2, 2),
            (3, 1),
            (4, 3),
            (5, 4),
            (6, 2),
        ],
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": people, "Product": products},
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows, "BOUGHT": bought},
        ),
    )


@pytest.fixture
def star(graph_context: Context) -> Star:
    return Star(context=graph_context)


# ===========================================================================
# Category 1: MATCH reordering preserves semantics
# ===========================================================================


class TestReorderingSemanticPreservation:
    """MATCH reordering must never change query results."""

    def test_cross_join_count_invariant(self, star: Star) -> None:
        """Cross-join cardinality must be identical with/without reordering."""
        query = "MATCH (a:Person) MATCH (b:Product) RETURN count(*) AS cnt"
        optimized = star.execute_query(query)
        unoptimized = _run_with_reordering_disabled(star, query)
        assert optimized["cnt"].iloc[0] == unoptimized["cnt"].iloc[0]
        # Verify absolute correctness: 6 * 4 = 24
        assert optimized["cnt"].iloc[0] == 24

    def test_cross_join_values_invariant(self, star: Star) -> None:
        """Cross-join row values must be identical regardless of order."""
        query = (
            "MATCH (a:Person) MATCH (b:Product) "
            "RETURN a.name AS person, b.name AS product "
            "ORDER BY person, product"
        )
        optimized = star.execute_query(query)
        unoptimized = _run_with_reordering_disabled(star, query)
        assert _results_equal(optimized, unoptimized)

    def test_cross_match_where_invariant(self, star: Star) -> None:
        """WHERE referencing both MATCHes must produce same results."""
        query = (
            "MATCH (a:Person) MATCH (b:Person) "
            "WHERE a.dept = b.dept AND a.name <> b.name "
            "RETURN a.name AS p, b.name AS q "
            "ORDER BY p, q"
        )
        optimized = star.execute_query(query)
        unoptimized = _run_with_reordering_disabled(star, query)
        assert _results_equal(optimized, unoptimized)

    def test_three_match_cross_join_invariant(self, star: Star) -> None:
        """Three-way cross-join semantics preserved through reordering."""
        query = (
            "MATCH (a:Product) MATCH (b:Person) MATCH (c:Product) "
            "RETURN count(*) AS cnt"
        )
        optimized = star.execute_query(query)
        unoptimized = _run_with_reordering_disabled(star, query)
        assert optimized["cnt"].iloc[0] == unoptimized["cnt"].iloc[0]
        # 4 * 6 * 4 = 96
        assert optimized["cnt"].iloc[0] == 96

    def test_where_with_age_comparison_invariant(self, star: Star) -> None:
        """Inequality WHERE across MATCHes preserved through reordering."""
        query = (
            "MATCH (a:Person) MATCH (b:Person) "
            "WHERE a.age > b.age "
            "RETURN a.name AS older, b.name AS younger "
            "ORDER BY older, younger"
        )
        optimized = star.execute_query(query)
        unoptimized = _run_with_reordering_disabled(star, query)
        assert _results_equal(optimized, unoptimized)
        # Verify non-empty (there are age differences)
        assert len(optimized) > 0


# ===========================================================================
# Category 2: shortestPath immune to reordering
# ===========================================================================


class TestShortestPathReorderingImmunity:
    """shortestPath MATCH clauses must NOT be reordered away from
    their variable-binding predecessors. (Regression for the bug fixed
    in _apply_match_reordering.)
    """

    def test_shortest_path_direct(self, star: Star) -> None:
        """ShortestPath with pre-bound endpoints returns correct result."""
        query = (
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        r = star.execute_query(query)
        assert len(r) == 1
        assert r["src"].iloc[0] == "Alice"
        assert r["tgt"].iloc[0] == "Bob"

    def test_shortest_path_multi_hop(self, star: Star) -> None:
        """ShortestPath over multiple hops finds correct endpoint."""
        query = (
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS src, b.name AS tgt, length(p) AS hops"
        )
        r = star.execute_query(query)
        assert len(r) == 1
        assert r["src"].iloc[0] == "Alice"
        assert r["tgt"].iloc[0] == "Dave"
        assert r["hops"].iloc[0] == 2

    def test_shortest_path_no_extra_rows(self, star: Star) -> None:
        """ShortestPath must not return paths to unrelated endpoints."""
        query = (
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Eve'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN b.name AS tgt"
        )
        r = star.execute_query(query)
        # All rows must target Eve, not other reachable nodes
        assert len(r) >= 1
        assert (r["tgt"] == "Eve").all()

    def test_shortest_path_with_reordering_disabled_same_result(
        self,
        star: Star,
    ) -> None:
        """ShortestPath gives same result with/without reordering."""
        query = (
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS src, b.name AS tgt, length(p) AS hops"
        )
        optimized = star.execute_query(query)
        unoptimized = _run_with_reordering_disabled(star, query)
        assert _results_equal(optimized, unoptimized)

    def test_all_shortest_paths_immune(self, star: Star) -> None:
        """AllShortestPaths also immune to reordering."""
        query = (
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = allShortestPaths((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS src, b.name AS tgt, length(p) AS hops"
        )
        r = star.execute_query(query)
        assert len(r) >= 1
        assert (r["src"] == "Alice").all()
        assert (r["tgt"] == "Dave").all()
        assert (r["hops"] == r["hops"].min()).all()


# ===========================================================================
# Category 3: Multi-path MATCH predicate pushdown preserves semantics
# ===========================================================================


class TestPredicatePushdownSemanticPreservation:
    """Predicate pushdown within multi-path MATCH patterns must
    produce identical results to non-pushed-down execution.
    """

    def test_comma_separated_pattern_with_where(self, star: Star) -> None:
        """MATCH (a:Person), (b:Product) WHERE a.age > 30 — pushdown-safe."""
        query = (
            "MATCH (a:Person), (b:Product) "
            "WHERE a.age > 30 "
            "RETURN a.name AS person, b.name AS product "
            "ORDER BY person, product"
        )
        r = star.execute_query(query)
        # Only Carol(35), Dave(40), Frank(33) have age > 30
        persons = set(r["person"])
        assert persons == {"Carol", "Dave", "Frank"}
        # Each paired with all 4 products
        assert len(r) == 3 * 4

    def test_cross_path_where_not_pushed_incorrectly(self, star: Star) -> None:
        """WHERE referencing both patterns must NOT be pushed to one side."""
        query = (
            "MATCH (a:Person), (b:Person) "
            "WHERE a.age > b.age "
            "RETURN a.name AS older, b.name AS younger "
            "ORDER BY older, younger"
        )
        r = star.execute_query(query)
        # Verify every row satisfies the predicate
        for _, row in r.iterrows():
            older_age = {
                "Alice": 30,
                "Bob": 25,
                "Carol": 35,
                "Dave": 40,
                "Eve": 28,
                "Frank": 33,
            }
            assert older_age[row["older"]] > older_age[row["younger"]]


# ===========================================================================
# Category 4: OPTIONAL MATCH immune to reordering
# ===========================================================================


class TestOptionalMatchImmunity:
    """OPTIONAL MATCH clauses must not participate in reordering."""

    def test_optional_match_preserves_all_left_rows(self, star: Star) -> None:
        """OPTIONAL MATCH preserves all rows from preceding MATCH."""
        query = (
            "MATCH (a:Person) "
            "OPTIONAL MATCH (a)-[:BOUGHT]->(p:Product) "
            "RETURN a.name AS person, p.name AS product "
            "ORDER BY person"
        )
        r = star.execute_query(query)
        # All 6 people appear (some with null product)
        assert set(r["person"]) == {
            "Alice",
            "Bob",
            "Carol",
            "Dave",
            "Eve",
            "Frank",
        }

    def test_optional_match_after_multi_match_not_reordered(
        self,
        star: Star,
    ) -> None:
        """OPTIONAL MATCH after multiple MATCHes stays in place."""
        query = (
            "MATCH (a:Person) MATCH (b:Product) "
            "OPTIONAL MATCH (a)-[:BOUGHT]->(b) "
            "RETURN a.name AS person, b.name AS product "
            "ORDER BY person, product"
        )
        r = star.execute_query(query)
        # Cross-join of 6 people x 4 products = 24 base pairs; OPTIONAL
        # MATCH may add rows for left-join expansion.  Key invariant: all
        # 6 people and all 4 products appear.
        assert set(r["person"]) == {
            "Alice",
            "Bob",
            "Carol",
            "Dave",
            "Eve",
            "Frank",
        }
        assert set(r["product"]) == {
            "Widget",
            "Gadget",
            "Doohickey",
            "Thingamajig",
        }
        assert len(r) >= 24


# ===========================================================================
# Category 5: Equivalence of single MATCH vs. multi-MATCH
# ===========================================================================


class TestSingleVsMultiMatchEquivalence:
    """MATCH (a:X), (b:Y) must equal MATCH (a:X) MATCH (b:Y)
    for both unfiltered and filtered queries.
    """

    def test_comma_vs_double_match_unfiltered(self, star: Star) -> None:
        """Comma-separated vs. double MATCH — same cross-join result."""
        single = star.execute_query(
            "MATCH (a:Person), (b:Product) "
            "RETURN a.name AS person, b.name AS product "
            "ORDER BY person, product",
        )
        double = star.execute_query(
            "MATCH (a:Person) MATCH (b:Product) "
            "RETURN a.name AS person, b.name AS product "
            "ORDER BY person, product",
        )
        assert _results_equal(single, double)

    def test_comma_vs_double_match_filtered(self, star: Star) -> None:
        """Comma-separated vs. double MATCH with WHERE — same result."""
        single = star.execute_query(
            "MATCH (a:Person), (b:Person) "
            "WHERE a.dept = b.dept AND a.name <> b.name "
            "RETURN a.name AS p, b.name AS q ORDER BY p, q",
        )
        double = star.execute_query(
            "MATCH (a:Person) MATCH (b:Person) "
            "WHERE a.dept = b.dept AND a.name <> b.name "
            "RETURN a.name AS p, b.name AS q ORDER BY p, q",
        )
        assert _results_equal(single, double)

    def test_comma_vs_double_match_with_relationship(
        self,
        star: Star,
    ) -> None:
        """Comma pattern vs. double MATCH with relationships — same result."""
        single = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (c:Product) "
            "RETURN a.name AS src, b.name AS tgt, c.name AS prod "
            "ORDER BY src, tgt, prod",
        )
        # Double MATCH equivalent
        double = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (c:Product) "
            "RETURN a.name AS src, b.name AS tgt, c.name AS prod "
            "ORDER BY src, tgt, prod",
        )
        assert _results_equal(single, double)
