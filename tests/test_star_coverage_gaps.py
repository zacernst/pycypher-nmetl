"""Tests targeting uncovered lines in star.py."""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.star import Star


class TestExplainQuery:
    """Cover explain_query() lines 683-777."""

    def test_explain_simple_match_return(self, social_star: Star) -> None:
        result = social_star.explain_query(
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m",
        )
        assert isinstance(result, str)
        assert "Execution Plan" in result
        assert "Clauses:" in result
        assert "Data Context:" in result
        assert "Person" in result

    def test_explain_empty_query(self, social_star: Star) -> None:
        result = social_star.explain_query("   ")
        assert result == "Error: empty query"

    def test_explain_union_query(self, social_star: Star) -> None:
        result = social_star.explain_query(
            "MATCH (n:Person) RETURN n.name AS name "
            "UNION MATCH (m:Person) RETURN m.name AS name",
        )
        assert "UNION" in result
        assert "sub-quer" in result.lower()

    def test_explain_shows_entity_row_counts(self, social_star: Star) -> None:
        result = social_star.explain_query("MATCH (n:Person) RETURN n.name")
        assert "4 rows" in result  # people_df has 4 rows

    def test_explain_shows_relationship_row_counts(
        self,
        social_star: Star,
    ) -> None:
        result = social_star.explain_query(
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n.name",
        )
        assert "KNOWS" in result


class TestUnionQueries:
    """Cover UNION paths including empty results (lines 2118-2120)."""

    def test_union_with_results(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN n.name AS name "
            "UNION MATCH (n:Person) RETURN n.name AS name",
        )
        assert isinstance(result, pd.DataFrame)
        assert "name" in result.columns
        # UNION deduplicates, so we should get unique names
        assert len(result) == len(result.drop_duplicates())

    def test_union_with_nonexistent_labels(self, social_star: Star) -> None:
        """UNION where sub-queries reference unknown types raises an error."""
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            social_star.execute_query(
                "MATCH (n:NonExistent) RETURN n.name AS name "
                "UNION MATCH (m:AlsoNonExistent) RETURN m.name AS name",
            )


class TestMutationClausesWithoutMatch:
    """Cover SET/REMOVE/DELETE without preceding MATCH (lines 2606-2658)."""

    def test_set_without_match_raises(self, social_star: Star) -> None:
        with pytest.raises(
            ValueError,
            match="SET clause requires a preceding MATCH",
        ):
            social_star.execute_query("SET n.age = 30")

    def test_remove_without_match_raises(self, social_star: Star) -> None:
        with pytest.raises(
            ValueError,
            match="REMOVE clause requires a preceding MATCH",
        ):
            social_star.execute_query("REMOVE n.prop")

    def test_delete_without_match_raises(self, social_star: Star) -> None:
        with pytest.raises(
            ValueError,
            match="DELETE clause requires a preceding MATCH",
        ):
            social_star.execute_query("DELETE n")


class TestOptionalMatchNoResults:
    """Cover OPTIONAL MATCH with no results (line 2273)."""

    def test_optional_match_nonexistent_relationship(
        self,
        social_star: Star,
    ) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[:NONEXISTENT]->(m) RETURN n.name, m",
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        # m should be null since NONEXISTENT rel type does not exist
        m_col = [c for c in result.columns if "m" in c.lower()]
        assert len(m_col) > 0


class TestAggregationInBinaryExpression:
    """Cover aggregation detection in binary expressions (line 1666)."""

    def test_aggregation_in_addition(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "MATCH (n:Person) RETURN 1 + count(n) AS result",
        )
        assert isinstance(result, pd.DataFrame)
        assert "result" in result.columns
        assert result["result"].iloc[0] == 5  # 1 + 4 people


class TestCreateClause:
    """Cover CREATE clause paths (lines 1999, 2003, 2012, 2022, 412-425)."""

    def test_create_node_with_properties(self, social_star: Star) -> None:
        result = social_star.execute_query(
            "CREATE (n:TestNode {name: 'test', age: 42, score: 3.14, active: true})",
        )
        assert isinstance(result, pd.DataFrame)
