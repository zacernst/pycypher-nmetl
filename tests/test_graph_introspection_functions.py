"""Tests for Cypher graph introspection functions.

Covers: id(), labels(), type(), keys(), exists()
All tests run through Star.execute_query() to ensure full integration.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "nickname": [None, "Bobby", None],  # Some nulls for exists() tests
        },
    )


@pytest.fixture
def products_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [10, 20],
            "title": ["Widget", "Gadget"],
            "price": [9.99, 49.99],
        },
    )


@pytest.fixture
def purchases_df() -> pd.DataFrame:
    """Relationship table: Person BOUGHT Product."""
    return pd.DataFrame(
        {
            "__ID__": [100, 200],
            "__SOURCE__": [1, 2],
            "__TARGET__": [10, 20],
        },
    )


@pytest.fixture
def star_people(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


@pytest.fixture
def star_with_rels(
    people_df: pd.DataFrame,
    products_df: pd.DataFrame,
    purchases_df: pd.DataFrame,
) -> Star:
    context = (
        ContextBuilder()
        .add_entity("Person", people_df)
        .add_entity("Product", products_df)
        .add_relationship(
            "BOUGHT",
            purchases_df,
            source_col="__SOURCE__",
            target_col="__TARGET__",
        )
        .build()
    )
    return Star(context=context)


# ===========================================================================
# id() — returns entity / relationship IDs
# ===========================================================================


class TestIdFunction:
    def test_id_returns_integer_ids(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN id(p) AS pid",
        )
        assert "pid" in result.columns
        assert set(result["pid"]) == {1, 2, 3}

    def test_id_in_where_filters_correctly(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) WHERE id(p) = 1 RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result.loc[0, "name"] == "Alice"

    def test_id_return_type_is_numeric(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN id(p) AS pid",
        )
        # IDs should be numeric — not strings
        for pid in result["pid"]:
            assert isinstance(pid, (int, float))

    def test_id_matches_bound_variable_value(self, star_people: Star) -> None:
        """id(p) should equal the raw binding value for p."""
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN id(p) AS pid, p.name AS name ORDER BY pid ASC",
        )
        assert list(result["pid"]) == [1, 2, 3]


# ===========================================================================
# labels() — returns a list containing the entity type label
# ===========================================================================


class TestLabelsFunction:
    def test_labels_returns_list_per_row(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS lbls",
        )
        assert "lbls" in result.columns
        for lbls in result["lbls"]:
            assert isinstance(lbls, list)

    def test_labels_contains_entity_type(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS lbls",
        )
        for lbls in result["lbls"]:
            assert "Person" in lbls

    def test_labels_is_list_not_string(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS lbls",
        )
        for lbls in result["lbls"]:
            assert not isinstance(lbls, str)

    def test_labels_same_for_all_rows_of_same_entity_type(
        self,
        star_people: Star,
    ) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS lbls",
        )
        assert len(result) == 3
        # All rows should have the same label list
        all_labels = [tuple(x) for x in result["lbls"]]
        assert len(set(all_labels)) == 1

    def test_labels_different_for_different_entity_types(
        self,
        star_with_rels: Star,
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS lbls",
        )
        assert all("Person" in row for row in result["lbls"])

        result2 = star_with_rels.execute_query(
            "MATCH (p:Product) RETURN labels(p) AS lbls",
        )
        assert all("Product" in row for row in result2["lbls"])


# ===========================================================================
# type() — returns the relationship type string
# ===========================================================================


class TestTypeFunction:
    def test_type_returns_relationship_type_string(
        self,
        star_with_rels: Star,
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person)-[r:BOUGHT]->(pr:Product) RETURN type(r) AS rt",
        )
        assert "rt" in result.columns
        for rt in result["rt"]:
            assert rt == "BOUGHT"

    def test_type_returns_string_not_list(self, star_with_rels: Star) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person)-[r:BOUGHT]->(pr:Product) RETURN type(r) AS rt",
        )
        for rt in result["rt"]:
            assert isinstance(rt, str)

    def test_type_same_for_all_rows_of_same_relationship_type(
        self,
        star_with_rels: Star,
    ) -> None:
        result = star_with_rels.execute_query(
            "MATCH (p:Person)-[r:BOUGHT]->(pr:Product) RETURN type(r) AS rt",
        )
        assert len(result) == 2
        assert set(result["rt"]) == {"BOUGHT"}


# ===========================================================================
# keys() — returns list of property names (excluding __ID__)
# ===========================================================================


class TestKeysFunction:
    def test_keys_returns_list_per_row(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS k",
        )
        assert "k" in result.columns
        for k in result["k"]:
            assert isinstance(k, list)

    def test_keys_excludes_id_column(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS k",
        )
        for k in result["k"]:
            assert "__ID__" not in k

    def test_keys_includes_all_properties(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS k",
        )
        # The person table has name, age, nickname
        for k in result["k"]:
            assert "name" in k
            assert "age" in k
            assert "nickname" in k

    def test_keys_same_for_all_rows(self, star_people: Star) -> None:
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS k",
        )
        # Each row's keys list should be identical (schema is the same)
        all_key_tuples = [tuple(sorted(k)) for k in result["k"]]
        assert len(set(all_key_tuples)) == 1

    def test_keys_different_for_different_entity_types(
        self,
        star_with_rels: Star,
    ) -> None:
        result_person = star_with_rels.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS k",
        )
        result_product = star_with_rels.execute_query(
            "MATCH (p:Product) RETURN keys(p) AS k",
        )
        # Person has name/age/nickname; Product has title/price
        person_keys = set(result_person["k"].iloc[0])
        product_keys = set(result_product["k"].iloc[0])
        assert "name" in person_keys
        assert "title" in product_keys
        assert person_keys != product_keys


# ===========================================================================
# exists() — null-check predicate for property access
# ===========================================================================


class TestExistsFunction:
    def test_exists_true_for_non_null_property(
        self,
        star_people: Star,
    ) -> None:
        # All people have 'name' — exists() should be True for all
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, exists(p.name) AS has_name",
        )
        assert result["has_name"].all()

    def test_exists_false_for_null_property(self, star_people: Star) -> None:
        # Only Bob has 'nickname'; Alice and Carol have None
        result = star_people.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, exists(p.nickname) AS has_nick "
            "ORDER BY p.name ASC",
        )
        # Alice → False, Bob → True, Carol → False
        nick_map = dict(zip(result["name"], result["has_nick"]))
        assert nick_map["Bob"] is True or nick_map["Bob"] == True
        assert nick_map["Alice"] is False or nick_map["Alice"] == False
        assert nick_map["Carol"] is False or nick_map["Carol"] == False

    def test_exists_in_where_clause_filters(self, star_people: Star) -> None:
        # WHERE exists(p.nickname) keeps only Bob
        result = star_people.execute_query(
            "MATCH (p:Person) WHERE exists(p.nickname) RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result.loc[0, "name"] == "Bob"

    def test_exists_where_false_keeps_non_null(
        self,
        star_people: Star,
    ) -> None:
        # WHERE NOT exists(p.nickname) keeps Alice and Carol
        result = star_people.execute_query(
            "MATCH (p:Person) WHERE NOT exists(p.nickname) RETURN p.name AS name "
            "ORDER BY p.name ASC",
        )
        assert len(result) == 2
        assert set(result["name"]) == {"Alice", "Carol"}
