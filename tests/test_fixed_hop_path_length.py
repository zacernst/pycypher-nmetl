"""TDD tests for length() on fixed-hop path variables.

openCypher supports binding the traversed path to a variable:

    MATCH p = (a)-[:KNOWS]->(b) RETURN length(p)   -- always 1

The current engine only creates the `_path_hops_*` column during
variable-length BFS expansion.  Fixed-hop patterns (1 hop, 2 hops, etc.)
never create that column, so `length(p)` raises ValueError.

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


@pytest.fixture()
def chain_star() -> Star:
    """Four-node chain: Alice -KNOWS-> Bob -KNOWS-> Carol -KNOWS-> Dave."""
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Dave"]}
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
            ID_COLUMN: [10, 11, 12],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 4],
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
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table}
            ),
        )
    )


class TestFixedHopPathLength:
    """length(p) for fixed-hop path variables must return the hop count."""

    def test_single_hop_length_is_one(self, chain_star: Star) -> None:
        """length(p) == 1 for a single-relationship pattern."""
        r = chain_star.execute_query(
            "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN length(p) AS hops"
        )
        assert list(r["hops"]) == [1, 1, 1], (
            f"Expected [1, 1, 1], got {list(r['hops'])}"
        )

    def test_single_hop_does_not_raise(self, chain_star: Star) -> None:
        """length(p) with a single hop must not raise ValueError."""
        chain_star.execute_query(
            "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN length(p) AS hops"
        )

    def test_single_hop_row_count(self, chain_star: Star) -> None:
        """Single-hop MATCH returns one row per traversal."""
        r = chain_star.execute_query(
            "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, length(p) AS hops"
        )
        assert len(r) == 3

    def test_path_length_in_where_clause(self, chain_star: Star) -> None:
        """WHERE length(p) = 1 filters correctly (all rows match)."""
        r = chain_star.execute_query(
            "MATCH p = (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE length(p) = 1 RETURN a.name ORDER BY a.name"
        )
        assert list(r["name"]) == ["Alice", "Bob", "Carol"]

    def test_var_length_still_works(self, chain_star: Star) -> None:
        """Variable-length path length() must not regress."""
        r = chain_star.execute_query(
            "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) "
            "RETURN b.name, length(p) AS hops ORDER BY hops, b.name"
        )
        # Alice->Bob (hops=1), Alice->Bob->Carol (hops=2)
        assert list(r["hops"]) == [1, 2]
