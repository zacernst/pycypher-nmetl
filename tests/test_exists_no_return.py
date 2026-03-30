"""TDD tests for EXISTS { MATCH ... WHERE ... } without RETURN clause.

openCypher allows EXISTS subqueries without a RETURN clause:

    EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Bob' }

Without RETURN, the semantics are: "does at least one row match the pattern
and WHERE condition?"  The current implementation returns False for all rows
because _execute_query_binding_frame_inner returns pd.DataFrame() when no
RETURN clause is present.

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
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def social_star() -> Star:
    """Simple social graph: Alice -KNOWS-> Bob and Alice -KNOWS-> Carol."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        },
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11],
            "__SOURCE__": [1, 1],
            "__TARGET__": [2, 3],
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
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        ),
    )


# ---------------------------------------------------------------------------
# EXISTS without RETURN — basic correctness
# ---------------------------------------------------------------------------


class TestExistsNoReturn:
    """EXISTS subqueries without RETURN should match on pattern + WHERE."""

    def test_exists_without_return_true_match(self, social_star: Star) -> None:
        """Alice KNOWS Bob → EXISTS returns True for Alice."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Bob' } "
            "RETURN p.name",
        )
        assert "Alice" in list(r["name"]), (
            f"Expected Alice (who knows Bob), got {list(r['name'])}"
        )

    def test_exists_without_return_excludes_non_match(
        self,
        social_star: Star,
    ) -> None:
        """Bob and Carol don't know anyone → EXISTS returns False for them."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Bob' } "
            "RETURN p.name",
        )
        names = list(r["name"])
        assert "Bob" not in names, f"Bob should not be in results, got {names}"
        assert "Carol" not in names, f"Carol should not be in results, got {names}"

    def test_exists_without_return_correct_count(
        self,
        social_star: Star,
    ) -> None:
        """Exactly one person (Alice) matches EXISTS without RETURN."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Bob' } "
            "RETURN p.name",
        )
        assert len(r) == 1, f"Expected 1 row, got {len(r)}"

    def test_exists_without_return_no_where(self, social_star: Star) -> None:
        """EXISTS { MATCH (p)-[:KNOWS]->(f) } (no WHERE) works too."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) } "
            "RETURN p.name",
        )
        # Alice knows two people, Bob and Carol know nobody
        assert list(r["name"]) == ["Alice"], (
            f"Only Alice should match, got {list(r['name'])}"
        )

    def test_not_exists_without_return(self, social_star: Star) -> None:
        """NOT EXISTS { MATCH ... } (no RETURN) correctly inverts."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(f:Person) } "
            "RETURN p.name ORDER BY p.name",
        )
        names = list(r["name"])
        assert "Bob" in names, f"Expected Bob in results, got {names}"
        assert "Carol" in names, f"Expected Carol in results, got {names}"
        assert "Alice" not in names, f"Alice should not be in results, got {names}"

    def test_exists_with_return_still_works(self, social_star: Star) -> None:
        """Existing behaviour (EXISTS with RETURN) must not regress."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Bob' RETURN f } "
            "RETURN p.name",
        )
        assert "Alice" in list(r["name"]), (
            f"Regression: EXISTS with RETURN should still work, got {list(r['name'])}"
        )

    def test_exists_without_return_no_match_returns_empty(
        self,
        social_star: Star,
    ) -> None:
        """EXISTS { MATCH ... WHERE ... } with impossible condition returns empty."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Zara' } "
            "RETURN p.name",
        )
        assert len(r) == 0, f"Expected empty result, got {list(r['name'])}"
