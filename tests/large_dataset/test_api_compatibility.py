"""API compatibility tests for large dataset implementation.

Ensures zero breaking changes to the existing PyCypher public API.
These tests capture the current API contract and must pass after
any large dataset changes are introduced.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
    Star,
)

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _person_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )


@pytest.fixture
def _knows_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [101, 102],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
            "since": [2020, 2021],
        },
    )


@pytest.fixture
def _star(_person_df: pd.DataFrame, _knows_df: pd.DataFrame) -> Star:
    person_table = EntityTable.from_dataframe("Person", _person_df)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=_knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# API contract: Star.execute_query returns pd.DataFrame
# ---------------------------------------------------------------------------


class TestStarExecuteQueryContract:
    """Verify Star.execute_query() returns pandas DataFrame with expected shape."""

    def test_returns_dataframe(self, _star: Star) -> None:
        result = _star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert isinstance(result, pd.DataFrame)

    def test_column_names_match_aliases(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
        )
        assert list(result.columns) == ["name", "age"]

    def test_row_count_matches_data(self, _star: Star) -> None:
        result = _star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert len(result) == 3

    def test_where_clause_filters(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_relationship_traversal(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt",
        )
        assert len(result) == 2
        assert isinstance(result, pd.DataFrame)

    def test_order_by(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC",
        )
        assert list(result["name"]) == ["Bob", "Alice", "Carol"]

    def test_limit(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name LIMIT 2",
        )
        assert len(result) == 2

    def test_distinct(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) RETURN DISTINCT p.age > 28 AS over28",
        )
        assert len(result) == 2

    def test_aggregation_count(self, _star: Star) -> None:
        result = _star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert result["cnt"].iloc[0] == 3

    def test_with_clause(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.age AS age "
            "WHERE age > 28 RETURN name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_parameters(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name",
            parameters={"min_age": 28},
        )
        assert set(result["name"]) == {"Alice", "Carol"}


# ---------------------------------------------------------------------------
# API contract: EntityTable construction
# ---------------------------------------------------------------------------


class TestEntityTableContract:
    """Verify EntityTable factory methods and properties."""

    def test_from_dataframe_factory(self, _person_df: pd.DataFrame) -> None:
        table = EntityTable.from_dataframe("Person", _person_df)
        assert table.entity_type == "Person"

    def test_entity_type_attribute(self, _person_df: pd.DataFrame) -> None:
        table = EntityTable.from_dataframe("Person", _person_df)
        assert hasattr(table, "entity_type")
        assert table.entity_type == "Person"

    def test_source_obj_preserved(self, _person_df: pd.DataFrame) -> None:
        table = EntityTable.from_dataframe("Person", _person_df)
        assert table.source_obj is not None


# ---------------------------------------------------------------------------
# API contract: Context construction
# ---------------------------------------------------------------------------


class TestContextContract:
    """Verify Context construction patterns remain stable."""

    def test_context_accepts_entity_mapping(
        self,
        _person_df: pd.DataFrame,
    ) -> None:
        table = EntityTable.from_dataframe("Person", _person_df)
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        assert ctx is not None

    def test_star_accepts_context(self, _person_df: pd.DataFrame) -> None:
        table = EntityTable.from_dataframe("Person", _person_df)
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=ctx)
        assert star is not None


# ---------------------------------------------------------------------------
# API contract: Return type consistency
# ---------------------------------------------------------------------------


class TestReturnTypeConsistency:
    """Verify that return types are consistent across query patterns."""

    def test_empty_result_is_dataframe(self, _star: Star) -> None:
        result = _star.execute_query(
            "MATCH (p:Person) WHERE p.age > 100 RETURN p.name AS name",
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_single_row_result(self, _star: Star) -> None:
        result = _star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_null_values_in_optional_match(self, _star: Star) -> None:
        result = _star.execute_query(
            "OPTIONAL MATCH (p:Person) WHERE p.age > 100 RETURN p.name AS name",
        )
        assert isinstance(result, pd.DataFrame)
