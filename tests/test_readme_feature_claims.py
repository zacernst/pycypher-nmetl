"""README feature-claim CI gate.

Each test corresponds to a feature bullet in the README that was historically
misreported as ``NotImplementedError``.  A failure here means either the code
regressed or the README was updated without testing.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def empty_star() -> Star:
    """Star with no entities — sufficient for mutation and standalone tests."""
    ctx = ContextBuilder.from_dict({})
    return Star(context=ctx)


@pytest.fixture()
def people_star() -> Star:
    """Star with Person entities for read tests."""
    ctx = ContextBuilder.from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": ["p1", "p2", "p3"],
                    "name": ["Alice", "Bob", "Carol"],
                    "age": [30, 25, 35],
                }
            ),
            "KNOWS": pd.DataFrame(
                {
                    "__SOURCE__": ["p1", "p2"],
                    "__TARGET__": ["p2", "p3"],
                }
            ),
        }
    )
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# CREATE
# ---------------------------------------------------------------------------


class TestCreateSupported:
    """README: CREATE — insert nodes and relationships."""

    def test_create_node_does_not_raise(self, empty_star: Star) -> None:
        """CREATE a node must not raise NotImplementedError."""
        empty_star.execute_query("CREATE (p:Person {name: 'Dave'})")

    def test_create_node_adds_row(self, empty_star: Star) -> None:
        """After CREATE, MATCH returns the new node."""
        empty_star.execute_query("CREATE (p:Person {name: 'Dave', age: 40})")
        result = empty_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert "Dave" in result["name"].tolist()


# ---------------------------------------------------------------------------
# DELETE / DETACH DELETE
# ---------------------------------------------------------------------------


class TestDeleteSupported:
    """README: DELETE — remove matched entity rows."""

    def test_delete_does_not_raise(self, people_star: Star) -> None:
        """DELETE must not raise NotImplementedError."""
        people_star.execute_query("MATCH (p:Person {name: 'Bob'}) DELETE p")

    def test_delete_removes_row(self, people_star: Star) -> None:
        """After DELETE, the entity no longer appears in MATCH results."""
        people_star.execute_query("MATCH (p:Person {name: 'Bob'}) DELETE p")
        result = people_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert "Bob" not in result["name"].tolist()

    def test_detach_delete_does_not_raise(self, people_star: Star) -> None:
        """DETACH DELETE must not raise NotImplementedError."""
        people_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) DETACH DELETE p"
        )


# ---------------------------------------------------------------------------
# MERGE
# ---------------------------------------------------------------------------


class TestMergeSupported:
    """README: MERGE — upsert; ON CREATE SET / ON MATCH SET."""

    def test_merge_does_not_raise(self, empty_star: Star) -> None:
        """MERGE must not raise NotImplementedError."""
        empty_star.execute_query("MERGE (p:Person {name: 'Eve'})")

    def test_merge_creates_when_absent(self, empty_star: Star) -> None:
        """MERGE creates the node when none exists."""
        empty_star.execute_query("MERGE (p:Person {name: 'Eve'})")
        result = empty_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert "Eve" in result["name"].tolist()

    def test_merge_on_create_set_does_not_raise(self) -> None:
        """MERGE … ON CREATE SET must not raise (uses int IDs for compat)."""
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {
                        "__ID__": [1, 2],
                        "name": ["Alice", "Bob"],
                        "age": [30, 25],
                    }
                )
            }
        )
        star = Star(context=ctx)
        star.execute_query(
            "MERGE (p:Person {name: 'Eve'}) ON CREATE SET p.age = 99"
        )

    def test_merge_on_match_set_does_not_raise(
        self, people_star: Star
    ) -> None:
        """MERGE … ON MATCH SET must not raise."""
        people_star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON MATCH SET p.seen = true"
        )


# ---------------------------------------------------------------------------
# FOREACH
# ---------------------------------------------------------------------------


class TestForeachSupported:
    """README: FOREACH — iterate over a list and apply inner clauses."""

    def test_foreach_does_not_raise(self, empty_star: Star) -> None:
        """FOREACH must not raise NotImplementedError."""
        empty_star.execute_query(
            "FOREACH (name IN ['X', 'Y'] | CREATE (:Tag {value: name}))"
        )

    def test_foreach_creates_rows(self, empty_star: Star) -> None:
        """FOREACH creates one row per list element."""
        empty_star.execute_query(
            "FOREACH (name IN ['X', 'Y'] | CREATE (:Tag {value: name}))"
        )
        result = empty_star.execute_query("MATCH (t:Tag) RETURN t.value AS v")
        assert set(result["v"].tolist()) == {"X", "Y"}


# ---------------------------------------------------------------------------
# CALL (built-in procedures)
# ---------------------------------------------------------------------------


class TestCallSupported:
    """README: CALL procedure YIELD … — built-in db.* procedures."""

    def test_call_db_labels_does_not_raise(self, people_star: Star) -> None:
        """CALL db.labels() must not raise NotImplementedError."""
        people_star.execute_query("CALL db.labels() YIELD label RETURN label")

    def test_call_db_labels_returns_entity_types(
        self, people_star: Star
    ) -> None:
        """db.labels() returns the registered entity type names."""
        result = people_star.execute_query(
            "CALL db.labels() YIELD label RETURN label"
        )
        assert "Person" in result["label"].tolist()

    def test_call_db_relationship_types(self, people_star: Star) -> None:
        """db.relationshipTypes() returns registered relationship types."""
        result = people_star.execute_query(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
        )
        assert "KNOWS" in result["relationshipType"].tolist()


# ---------------------------------------------------------------------------
# Relationship type union  (Loop 97)
# ---------------------------------------------------------------------------


class TestRelTypeUnion:
    """README: [:A|B] and [:A|:B] both supported."""

    @pytest.fixture()
    def multi_rel_star(self) -> Star:
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": ["p1", "p2", "p3"], "name": ["A", "B", "C"]}
                ),
                "KNOWS": pd.DataFrame(
                    {"__SOURCE__": ["p1"], "__TARGET__": ["p2"]}
                ),
                "LIKES": pd.DataFrame(
                    {"__SOURCE__": ["p1"], "__TARGET__": ["p3"]}
                ),
            }
        )
        return Star(context=ctx)

    def test_colon_pipe_union(self, multi_rel_star: Star) -> None:
        """[:KNOWS|:LIKES] returns targets from both relationship types."""
        result = multi_rel_star.execute_query(
            "MATCH (a:Person)-[:KNOWS|:LIKES]->(b:Person) RETURN b.name AS name"
        )
        assert set(result["name"].tolist()) == {"B", "C"}

    def test_no_colon_pipe_union(self, multi_rel_star: Star) -> None:
        """[:KNOWS|LIKES] (pipe-only form) also works."""
        result = multi_rel_star.execute_query(
            "MATCH (a:Person)-[:KNOWS|LIKES]->(b:Person) RETURN b.name AS name"
        )
        assert set(result["name"].tolist()) == {"B", "C"}


# ---------------------------------------------------------------------------
# Standalone RETURN / WITH (no preceding MATCH)
# ---------------------------------------------------------------------------


class TestStandaloneReturnWith:
    """README: standalone RETURN/WITH evaluates literals directly."""

    def test_standalone_return_literal(self, empty_star: Star) -> None:
        """RETURN 42 AS n with no MATCH returns 42."""
        result = empty_star.execute_query("RETURN 42 AS n")
        assert int(result["n"].iloc[0]) == 42

    def test_standalone_with_then_return(self, empty_star: Star) -> None:
        """WITH 1 AS x RETURN x with no MATCH returns 1."""
        result = empty_star.execute_query("WITH 1 AS x RETURN x")
        assert int(result["x"].iloc[0]) == 1
