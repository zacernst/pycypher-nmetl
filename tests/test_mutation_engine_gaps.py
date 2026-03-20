"""Tests targeting uncovered lines in mutation_engine.py.

Exercises CREATE, DELETE, DETACH DELETE, MERGE (ON CREATE SET, matching
existing), FOREACH (empty and non-empty lists), REMOVE, and CREATE with
relationships -- all driven through ``Star.execute_query()``.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.star import Star


class TestCreateBasicEntity:
    """CREATE a new entity with properties and verify it exists."""

    def test_create_new_person(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "CREATE (n:NewPerson {name: 'Alice', age: 30}) RETURN n.name AS name, n.age AS age",
        )
        assert result is not None
        assert "Alice" in result["name"].tolist()
        assert 30 in result["age"].tolist()

    def test_create_node_appears_in_subsequent_match(
        self, social_star: Star
    ) -> None:
        social_star.execute_query(
            "CREATE (n:Scientist {name: 'Marie', field: 'physics'})",
        )
        result = social_star.execute_query(
            "MATCH (s:Scientist) RETURN s.name AS name",
        )
        assert result is not None
        assert "Marie" in result["name"].tolist()


class TestDeleteNoMatchingIds:
    """DELETE with no matching IDs should be a no-op (line 569)."""

    def test_delete_nonexistent_person(self, social_star: Star) -> None:
        before = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        cnt_before = before["cnt"].iloc[0]

        social_star.execute_query(
            "MATCH (n:Person {name: 'NonExistent'}) DELETE n",
        )

        after = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        cnt_after = after["cnt"].iloc[0]
        assert cnt_before == cnt_after


class TestDetachDelete:
    """DETACH DELETE removes entities and their relationships (line 620)."""

    def test_detach_delete_removes_relationships(
        self, social_star: Star
    ) -> None:
        social_star.execute_query("MATCH (n:Person) DETACH DELETE n")

        result = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        assert result is not None
        assert result["cnt"].iloc[0] == 0


class TestMergeOnCreateSet:
    """MERGE with ON CREATE SET when entity does not exist (lines 714, 723)."""

    def test_merge_on_create_set_property(self, social_star: Star) -> None:
        social_star.execute_query(
            "MERGE (n:Person {name: 'NewPerson'}) ON CREATE SET n.age = 25",
        )
        result = social_star.execute_query(
            "MATCH (p:Person {name: 'NewPerson'}) RETURN p.age AS age",
        )
        assert result is not None
        assert len(result) == 1
        assert result["age"].iloc[0] == 25


class TestMergeMatchesExisting:
    """MERGE that matches an existing entity (line 677)."""

    def test_merge_existing_does_not_create(self, social_star: Star) -> None:
        before = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        cnt_before = before["cnt"].iloc[0]

        social_star.execute_query("MERGE (p:Person {name: 'Alice'})")

        after = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        cnt_after = after["cnt"].iloc[0]
        assert cnt_before == cnt_after

    def test_merge_existing_with_on_match_set(self, social_star: Star) -> None:
        social_star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON MATCH SET p.age = 99",
        )
        result = social_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age",
        )
        assert result is not None
        assert result["age"].iloc[0] == 99


class TestForeachEmptyList:
    """FOREACH over an empty list should be a no-op (line 838)."""

    def test_foreach_empty_list(self, social_star: Star) -> None:
        before = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        cnt_before = before["cnt"].iloc[0]

        social_star.execute_query(
            "FOREACH (x IN [] | CREATE (n:Temp {val: x}))",
        )

        after = social_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        assert before["cnt"].iloc[0] == after["cnt"].iloc[0]


class TestForeachOverList:
    """FOREACH over a non-empty list creates entities (lines 788-816)."""

    def test_foreach_creates_three_nodes(self, social_star: Star) -> None:
        social_star.execute_query(
            "FOREACH (x IN [1, 2, 3] | CREATE (n:Temp {val: x}))",
        )
        result = social_star.execute_query(
            "MATCH (t:Temp) RETURN t.val AS val",
        )
        assert result is not None
        assert set(result["val"].tolist()) == {1, 2, 3}


class TestRemoveClause:
    """REMOVE clause sets properties to null (line 957)."""

    def test_remove_property(self, social_star: Star) -> None:
        social_star.execute_query("MATCH (n:Person) REMOVE n.age")

        result = social_star.execute_query(
            "MATCH (p:Person) RETURN p.age AS age",
        )
        assert result is not None
        assert all(pd.isna(v) for v in result["age"].tolist())


class TestCreateWithRelationship:
    """CREATE a path with relationship (shadow relationship creation)."""

    def test_create_path_with_relationship(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'}) "
            "RETURN a.name AS a_name, b.name AS b_name",
        )
        assert result is not None
        assert result["a_name"].iloc[0] == "X"
        assert result["b_name"].iloc[0] == "Y"

    def test_created_relationship_is_queryable(
        self, social_star: Star
    ) -> None:
        social_star.execute_query(
            "CREATE (a:Person {name: 'P1'})-[:KNOWS]->(b:Person {name: 'P2'})",
        )
        result = social_star.execute_query(
            "MATCH (a:Person {name: 'P1'})-[:KNOWS]->(b:Person {name: 'P2'}) "
            "RETURN a.name AS src, b.name AS tgt",
        )
        assert result is not None
        assert len(result) >= 1
        assert result["src"].iloc[0] == "P1"
        assert result["tgt"].iloc[0] == "P2"
