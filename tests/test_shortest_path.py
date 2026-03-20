"""TDD tests for shortestPath() and allShortestPaths() execution (Loop 168).

openCypher supports:
    MATCH p = shortestPath((a:Person)-[:KNOWS*]->(b:Person)) RETURN a, b, length(p)
    MATCH p = allShortestPaths((a:Person)-[:KNOWS*]->(b:Person)) RETURN a, b

The current engine raises a Pydantic ValidationError:
    PatternPath elements[0] must be NodePattern — ShortestPath(pattern=None) is placed
    directly in elements without conversion.

All tests are written before the fix (TDD red phase).
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
# Shared fixture: diamond graph
#
#   Alice -KNOWS-> Bob   -KNOWS-> Dave
#         -KNOWS-> Carol -KNOWS-> Dave
#
# Shortest path from Alice to Dave = 2 hops (two equal-length options)
# ---------------------------------------------------------------------------


@pytest.fixture()
def diamond_star() -> Star:
    """Diamond graph with two equal-length shortest paths Alice→Dave."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    # Edges: Alice→Bob, Alice→Carol, Bob→Dave, Carol→Dave
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12, 13],
            "__SOURCE__": [1, 1, 2, 3],
            "__TARGET__": [2, 3, 4, 4],
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
    entity_mapping = EntityMapping(mapping={"Person": people_table})
    rel_mapping = RelationshipMapping(mapping={"KNOWS": knows_table})
    context = Context(
        entity_mapping=entity_mapping,
        relationship_mapping=rel_mapping,
    )
    return Star(context=context)


@pytest.fixture()
def chain_star() -> Star:
    """Linear chain: Alice -KNOWS-> Bob -KNOWS-> Carol.

    Shortest path Alice→Carol is 2 hops; Alice→Bob is 1 hop.
    """
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        }
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
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
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
    entity_mapping = EntityMapping(mapping={"Person": people_table})
    rel_mapping = RelationshipMapping(mapping={"KNOWS": knows_table})
    context = Context(
        entity_mapping=entity_mapping,
        relationship_mapping=rel_mapping,
    )
    return Star(context=context)


# ===========================================================================
# Category 1 — AST conversion sanity check (must pass first)
# ===========================================================================


class TestShortestPathASTConversion:
    """The ASTConverter must not raise ValidationError on shortestPath."""

    def test_ast_converts_without_validation_error(self) -> None:
        """shortestPath query must parse to a valid AST without ValidationError."""
        from pycypher.ast_models import ASTConverter

        ast = ASTConverter.from_cypher(
            "MATCH p = shortestPath((a:Person)-[:KNOWS*]->(b:Person)) RETURN a, b"
        )
        assert ast is not None

    def test_all_shortest_paths_ast_converts(self) -> None:
        """allShortestPaths query must parse without ValidationError."""
        from pycypher.ast_models import ASTConverter

        ast = ASTConverter.from_cypher(
            "MATCH p = allShortestPaths((a:Person)-[:KNOWS*]->(b:Person)) RETURN a, b"
        )
        assert ast is not None


# ===========================================================================
# Category 2 — Basic shortestPath execution
# ===========================================================================


class TestShortestPathBasic:
    """shortestPath() returns one row per (start, end) pair with min hops."""

    def test_shortest_path_direct_connection(self, chain_star: Star) -> None:
        """Alice-KNOWS->Bob: shortestPath returns single row, no error."""
        r = chain_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS start_name, b.name AS end_name"
        )
        assert len(r) == 1
        assert r["start_name"].iloc[0] == "Alice"
        assert r["end_name"].iloc[0] == "Bob"

    def test_shortest_path_two_hops(self, chain_star: Star) -> None:
        """Alice→Carol via Bob: shortest path is 2 hops, returns one row."""
        r = chain_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS start_name, b.name AS end_name"
        )
        assert len(r) == 1
        assert r["start_name"].iloc[0] == "Alice"
        assert r["end_name"].iloc[0] == "Carol"

    def test_shortest_path_no_connection_returns_empty(
        self, chain_star: Star
    ) -> None:
        """Carol→Alice: no forward path, returns empty result."""
        r = chain_star.execute_query(
            "MATCH (a:Person {name: 'Carol'}), (b:Person {name: 'Alice'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS start_name, b.name AS end_name"
        )
        assert len(r) == 0

    def test_shortest_path_diamond_one_row_per_pair(
        self, diamond_star: Star
    ) -> None:
        """Diamond graph: Alice→Dave has two equal-length paths.

        shortestPath must return exactly ONE row for the (Alice, Dave) pair.
        """
        r = diamond_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS start_name, b.name AS end_name"
        )
        assert len(r) == 1
        assert r["start_name"].iloc[0] == "Alice"
        assert r["end_name"].iloc[0] == "Dave"


# ===========================================================================
# Category 3 — length() on the path variable
# ===========================================================================


class TestShortestPathLength:
    """length(p) returns hop count of shortest path."""

    def test_length_of_one_hop_path(self, chain_star: Star) -> None:
        """shortestPath from Alice to Bob is 1 hop."""
        r = chain_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN length(p) AS hops"
        )
        assert len(r) == 1
        assert r["hops"].iloc[0] == 1

    def test_length_of_two_hop_path(self, chain_star: Star) -> None:
        """shortestPath from Alice to Carol is 2 hops."""
        r = chain_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN length(p) AS hops"
        )
        assert len(r) == 1
        assert r["hops"].iloc[0] == 2

    def test_length_diamond_shortest_is_two(self, diamond_star: Star) -> None:
        """Diamond: shortest Alice→Dave is 2 hops."""
        r = diamond_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN length(p) AS hops"
        )
        assert len(r) == 1
        assert r["hops"].iloc[0] == 2


# ===========================================================================
# Category 4 — allShortestPaths
# ===========================================================================


class TestAllShortestPaths:
    """allShortestPaths() returns at least one minimum-hop path per pair."""

    def test_all_shortest_paths_direct(self, chain_star: Star) -> None:
        """Alice→Bob: single path, allShortestPaths also returns at least one row."""
        r = chain_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "MATCH p = allShortestPaths((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS start_name, b.name AS end_name"
        )
        assert len(r) >= 1
        assert (r["start_name"] == "Alice").all()
        assert (r["end_name"] == "Bob").all()

    def test_all_shortest_paths_diamond_at_least_one_row(
        self, diamond_star: Star
    ) -> None:
        """Diamond: Alice→Dave allShortestPaths returns >= 1 row at min-length."""
        r = diamond_star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = allShortestPaths((a)-[:KNOWS*]->(b)) "
            "RETURN a.name AS start_name, b.name AS end_name, length(p) AS hops"
        )
        assert len(r) >= 1
        # All returned rows must be at the minimum hop count
        assert (r["hops"] == r["hops"].min()).all()
        assert r["hops"].iloc[0] == 2
