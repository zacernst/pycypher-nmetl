"""Tests for ContextBuilder.from_dict() auto-detecting relationship DataFrames.

When a DataFrame passed to from_dict() has both '__SOURCE__' and '__TARGET__'
columns it should automatically be treated as a relationship table and routed
through add_relationship().  This eliminates the need to use the verbose
ContextBuilder().add_entity(...).add_relationship(...).build() pattern for
quick scripting and tests.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    """Alice→Bob, Alice→Carol."""
    return pd.DataFrame(
        {
            "__ID__": [10, 11],
            "__SOURCE__": [1, 1],
            "__TARGET__": [2, 3],
        },
    )


@pytest.fixture
def likes_df() -> pd.DataFrame:
    """Bob→Alice (age difference direction)."""
    return pd.DataFrame(
        {
            "__ID__": [20],
            "__SOURCE__": [2],
            "__TARGET__": [1],
        },
    )


# ===========================================================================
# from_dict() auto-detection unit tests
# ===========================================================================


class TestFromDictAutoDetectsRelationships:
    """from_dict() routes relationship-shaped DataFrames to RelationshipMapping."""

    def test_entity_df_goes_to_entity_mapping(
        self,
        people_df: pd.DataFrame,
    ) -> None:
        ctx = ContextBuilder.from_dict({"Person": people_df})
        assert "Person" in ctx.entity_mapping.mapping
        assert "Person" not in ctx.relationship_mapping.mapping

    def test_relationship_df_goes_to_relationship_mapping(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
    ) -> None:
        ctx = ContextBuilder.from_dict(
            {"Person": people_df, "KNOWS": knows_df},
        )
        assert "Person" in ctx.entity_mapping.mapping
        assert "KNOWS" not in ctx.entity_mapping.mapping
        assert "KNOWS" in ctx.relationship_mapping.mapping

    def test_multiple_relationship_types(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
        likes_df: pd.DataFrame,
    ) -> None:
        ctx = ContextBuilder.from_dict(
            {"Person": people_df, "KNOWS": knows_df, "LIKES": likes_df},
        )
        assert "Person" in ctx.entity_mapping.mapping
        assert "KNOWS" in ctx.relationship_mapping.mapping
        assert "LIKES" in ctx.relationship_mapping.mapping

    def test_only_source_column_treated_as_entity(
        self,
        people_df: pd.DataFrame,
    ) -> None:
        """A DataFrame with __SOURCE__ but not __TARGET__ is still an entity."""
        partial_df = pd.DataFrame(
            {"__ID__": [1], "__SOURCE__": [99], "name": ["X"]},
        )
        ctx = ContextBuilder.from_dict(
            {"Thing": partial_df, "Person": people_df},
        )
        assert "Thing" in ctx.entity_mapping.mapping
        assert "Thing" not in ctx.relationship_mapping.mapping

    def test_only_target_column_treated_as_entity(self) -> None:
        """A DataFrame with __TARGET__ but not __SOURCE__ is still an entity."""
        partial_df = pd.DataFrame(
            {"__ID__": [1], "__TARGET__": [99], "val": [0]},
        )
        ctx = ContextBuilder.from_dict({"Orphan": partial_df})
        assert "Orphan" in ctx.entity_mapping.mapping
        assert "Orphan" not in ctx.relationship_mapping.mapping


# ===========================================================================
# Integration tests — MATCH queries using from_dict() with relationships
# ===========================================================================


class TestFromDictRelationshipQueryIntegration:
    """MATCH queries work when from_dict() auto-detects relationships."""

    def test_single_hop_match(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
    ) -> None:
        star = Star(
            context=ContextBuilder.from_dict(
                {"Person": people_df, "KNOWS": knows_df},
            ),
        )
        result = star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS src, q.name AS tgt ORDER BY q.name ASC",
        )
        assert list(result["src"]) == ["Alice", "Alice"]
        assert list(result["tgt"]) == ["Bob", "Carol"]

    def test_relationship_filter_by_target_property(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
    ) -> None:
        star = Star(
            context=ContextBuilder.from_dict(
                {"Person": people_df, "KNOWS": knows_df},
            ),
        )
        result = star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE q.name = 'Bob' "
            "RETURN p.name AS src, q.name AS tgt",
        )
        assert list(result["src"]) == ["Alice"]
        assert list(result["tgt"]) == ["Bob"]

    def test_variable_length_path(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
    ) -> None:
        """Variable-length paths work after from_dict() auto-detection."""
        star = Star(
            context=ContextBuilder.from_dict(
                {"Person": people_df, "KNOWS": knows_df},
            ),
        )
        result = star.execute_query(
            "MATCH (p:Person)-[:KNOWS*1..2]->(q:Person) "
            "RETURN DISTINCT q.name AS name ORDER BY name ASC",
        )
        names = list(result["name"])
        # Direct: Alice→Bob, Alice→Carol
        assert "Bob" in names
        assert "Carol" in names

    def test_optional_match_with_from_dict(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
    ) -> None:
        star = Star(
            context=ContextBuilder.from_dict(
                {"Person": people_df, "KNOWS": knows_df},
            ),
        )
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS name, q.name AS knows_name "
            "ORDER BY p.name ASC",
        )
        # Carol has no outgoing KNOWS — appears once with null knows_name
        carol_rows = result[result["name"] == "Carol"]
        assert len(carol_rows) == 1
        assert carol_rows["knows_name"].iloc[0] is None or pd.isna(
            carol_rows["knows_name"].iloc[0],
        )

    def test_multiple_relationship_types_from_dict(
        self,
        people_df: pd.DataFrame,
        knows_df: pd.DataFrame,
        likes_df: pd.DataFrame,
    ) -> None:
        star = Star(
            context=ContextBuilder.from_dict(
                {"Person": people_df, "KNOWS": knows_df, "LIKES": likes_df},
            ),
        )
        knows_result = star.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p.name AS src",
        )
        likes_result = star.execute_query(
            "MATCH (p:Person)-[:LIKES]->(q:Person) RETURN p.name AS src",
        )
        assert "Alice" in list(knows_result["src"])
        assert "Bob" in list(likes_result["src"])
