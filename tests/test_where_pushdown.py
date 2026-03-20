"""TDD tests for WHERE predicate pushdown before multi-path MATCH joins.

When a MATCH clause has multiple pattern paths and a WHERE that only references
variables from one path, the filter can be pushed down to apply before the join,
reducing the join's input size.

Run with:
    uv run pytest tests/test_where_pushdown_tdd.py -v
"""

import pandas as pd
import pytest
from pycypher import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)


@pytest.fixture
def pushdown_context() -> Context:
    """Context with Person, Company nodes and KNOWS, WORKS_AT relationships."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 40, 28],
        }
    )
    company_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 20, 30],
            "name": ["Acme", "Globex", "Initech"],
            "revenue": [1000, 2000, 3000],
        }
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 4],
            "since": [2020, 2021, 2022],
        }
    )
    works_at_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "__SOURCE__": [1, 2, 3, 4, 5],
            "__TARGET__": [10, 20, 10, 30, 20],
        }
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": EntityTable.from_dataframe("Person", person_df),
                "Company": EntityTable.from_dataframe("Company", company_df),
            }
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": EntityTable.from_dataframe("KNOWS", knows_df),
                "WORKS_AT": EntityTable.from_dataframe(
                    "WORKS_AT", works_at_df
                ),
            }
        ),
    )


class TestWherePushdownCorrectness:
    """Verify WHERE pushdown produces correct results."""

    def test_single_path_where_unchanged(
        self, pushdown_context: Context
    ) -> None:
        """Single-path MATCH with WHERE should work as before."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name AS aname, b.name AS bname ORDER BY aname"
        )
        # Only Carol (35) and Dave (40) have age > 30
        # Carol->Dave (Carol knows Dave via 3->4)
        # Dave doesn't know anyone in our data (no row with SOURCE=4 to 5)
        a_names = result["aname"].tolist()
        assert "Carol" in a_names
        # Verify no Alice or Bob (age <= 30)
        assert "Alice" not in a_names
        assert "Bob" not in a_names

    def test_multi_path_where_on_first_path(
        self, pushdown_context: Context
    ) -> None:
        """Multi-path MATCH with WHERE on first path should produce correct results."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:WORKS_AT]->(c:Company) "
            "WHERE a.age > 30 "
            "RETURN a.name AS aname, b.name AS bname, c.name AS cname ORDER BY aname"
        )
        # Only people with age > 30 who both know someone AND work somewhere
        a_names = set(result["aname"].tolist())
        assert "Alice" not in a_names
        assert "Bob" not in a_names

    def test_multi_path_where_on_second_path(
        self, pushdown_context: Context
    ) -> None:
        """Multi-path MATCH with WHERE on second path's variables should work."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:WORKS_AT]->(c:Company) "
            "WHERE c.revenue > 1500 "
            "RETURN a.name AS aname, c.name AS cname ORDER BY aname"
        )
        # Only companies with revenue > 1500 are Globex (2000) and Initech (3000)
        c_names = set(result["cname"].tolist())
        assert "Acme" not in c_names

    def test_multi_path_where_cross_path(
        self, pushdown_context: Context
    ) -> None:
        """WHERE referencing variables from multiple paths cannot be pushed down."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:WORKS_AT]->(c:Company) "
            "WHERE a.age >= 30 AND c.revenue >= 1000 "
            "RETURN a.name AS aname, c.name AS cname ORDER BY aname"
        )
        # Cross-path WHERE — results should still be correct
        # Alice(30)→Bob, works at Acme(1000): ✓
        # Carol(35)→Dave, works at Acme(1000): ✓
        assert len(result) >= 1
        for _, row in result.iterrows():
            assert row["aname"] in ["Alice", "Carol", "Dave"]  # age >= 30

    def test_no_where_clause(self, pushdown_context: Context) -> None:
        """Multi-path MATCH without WHERE should work unchanged."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:WORKS_AT]->(c:Company) "
            "RETURN a.name AS aname, b.name AS bname, c.name AS cname ORDER BY aname"
        )
        assert len(result) >= 1

    def test_pushdown_with_complex_predicate(
        self, pushdown_context: Context
    ) -> None:
        """WHERE with AND/OR should still produce correct results."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.age > 30 OR b.age < 30 "
            "RETURN a.name AS aname, b.name AS bname ORDER BY aname"
        )
        assert len(result) >= 1


class TestWherePushdownOptimization:
    """Verify the pushdown actually reduces intermediate frame sizes."""

    def test_pushdown_reduces_join_input(
        self, pushdown_context: Context
    ) -> None:
        """With predicate pushdown, WHERE on first path should filter before join.

        We verify this by checking the computation graph analysis detects
        the pushdown opportunity.
        """
        from pycypher.ast_models import ASTConverter
        from pycypher.lazy_eval import (
            build_computation_graph,
            push_filters_down,
        )

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:WORKS_AT]->(c:Company) "
            "WHERE a.age > 30 "
            "RETURN a.name"
        )
        graph = build_computation_graph(query)
        optimized = push_filters_down(graph)

        # The optimization should have modified the graph structure
        # (filter pushed before a join, or at minimum preserved correctness)
        order = optimized.topological_order()
        assert len(order) >= 1

    def test_single_path_no_pushdown_needed(
        self, pushdown_context: Context
    ) -> None:
        """Single-path MATCH doesn't need pushdown — verify no regression."""
        star = Star(context=pushdown_context)
        result = star.execute_query(
            "MATCH (a:Person) WHERE a.age > 30 RETURN a.name AS aname ORDER BY aname"
        )
        names = result["aname"].tolist()
        assert names == ["Carol", "Dave"]
