"""Integration tests for the modern relational translation stack.

These tests validate the replacement for the legacy ``pycypher.relational_algebra``
module by exercising ``pycypher.relational_models`` and the ``Star`` translator.
"""

from __future__ import annotations

import pandas as pd
from typing import cast
import pytest

from pycypher.ast_models import (
    ASTConverter,
    Match,
    NodePattern,
    IntegerLiteral,
    StringLiteral,
    Pattern,
    PatternPath,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Variable,
)
from pycypher.star import Star
from pycypher.relational_models import (
    AttributeEqualsValue,
    Context,
    EntityMapping,
    EntityTable,
    FilterRows,
    ID_COLUMN,
    Join,
    JoinType,
    Projection,
    Relation,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    RelationshipMapping,
    RelationshipTable,
)
# from pycypher.prenex_models import Context


@pytest.fixture()
def people_dataframe() -> pd.DataFrame:
    """Sample entity data representing Person nodes."""

    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 24, 31],
            "city": ["NYC", "SF", "SEA"],
        }
    )


@pytest.fixture()
def knows_dataframe() -> pd.DataFrame:
    """Sample KNOWS relationship data."""

    return pd.DataFrame(
        {
            ID_COLUMN: [11, 12, 13],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 1],
            "since": [2020, 2019, 2018],
        }
    )


@pytest.fixture()
def city_dataframe() -> pd.DataFrame:
    """Sample entity data representing City nodes."""

    return pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103],
            "name": ["NYC", "SF", "SEA"],
        }
    )


@pytest.fixture()
def lives_in_dataframe() -> pd.DataFrame:
    """Sample LIVES_IN relationship data from Person to City."""

    return pd.DataFrame(
        {
            ID_COLUMN: [21, 22, 23],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [101, 102, 103],
            "since": [2015, 2016, 2017],
        }
    )


@pytest.fixture()
def person_table(people_dataframe: pd.DataFrame) -> EntityTable:
    """Construct an EntityTable backed by the sample Person dataframe."""

    identifier = "Person"
    return EntityTable(
        entity_type="Person",
        identifier=identifier,
        column_names=[
            ID_COLUMN, 'name', 'age', 'city'],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        source_obj=people_dataframe,
    )


@pytest.fixture()
def city_table(city_dataframe: pd.DataFrame) -> EntityTable:
    """Construct an EntityTable backed by the sample City dataframe."""

    identifier = "City"
    return EntityTable(
        entity_type="City",
        identifier=identifier,
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={
            "name": "name",
        },
        attribute_map={
            "name": "name"
        },
        source_obj=city_dataframe,
    )


@pytest.fixture()
def knows_table(knows_dataframe: pd.DataFrame) -> RelationshipTable:
    """Construct a RelationshipTable backed by sample KNOWS data."""

    identifier = "KNOWS"
    return RelationshipTable(
        relationship_type="KNOWS",
        identifier=identifier,
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN, "since"],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        source_obj=knows_dataframe,
    )


@pytest.fixture()
def lives_in_table(lives_in_dataframe: pd.DataFrame) -> RelationshipTable:
    """Construct a RelationshipTable backed by sample LIVES_IN data."""

    identifier = "LIVES_IN"
    return RelationshipTable(
        relationship_type="LIVES_IN",
        identifier=identifier,
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN,"since"],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        source_obj=lives_in_dataframe,
    )


@pytest.fixture()
def relational_context(
    person_table: EntityTable,
    city_table: EntityTable,
    knows_table: RelationshipTable,
    lives_in_table: RelationshipTable,
) -> Context:
    """Build a translation context composed of the sample tables."""

    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "City": city_table}
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table, "LIVES_IN": lives_in_table}
        ),
    )


def test_entity_table_to_pandas_disambiguates_columns(
    person_table: EntityTable, relational_context: Context
) -> None:
    df = person_table.to_pandas(context=relational_context)

    expected_columns = {
        "Person____ID__",
        "Person__name",
        "Person__age",
        "Person__city",
    }
    assert set(df.columns) == expected_columns
    assert df.loc[df["Person____ID__"] == 1, "Person__name"].item() == "Alice"


def test_relationship_table_to_pandas_disambiguates_columns(
    knows_table: RelationshipTable, relational_context: Context
) -> None:
    df = knows_table.to_pandas(context=relational_context)

    expected_columns = {
        "KNOWS____ID__",
        "KNOWS__%s" % RELATIONSHIP_SOURCE_COLUMN,
        "KNOWS__%s" % RELATIONSHIP_TARGET_COLUMN,
        "KNOWS__since",
    }
    assert set(df.columns) == expected_columns
    assert df.loc[df["KNOWS____ID__"] == 11, "KNOWS__since"].item() == 2020


def test_star_translates_node_without_properties(relational_context: Context) -> None:
    star = Star(context=relational_context)
    node = NodePattern(variable=Variable(name="n"), labels=["Person"], properties={})

    relation = star.to_relation(obj=node)

    assert isinstance(relation, Projection)
    assert Variable(name="n") in relation.variable_map


def test_star_applies_filter_for_node_properties(relational_context: Context) -> None:
    star = Star(context=relational_context)
    node = NodePattern(
        variable=Variable(name="n"),
        labels=["Person"],
        properties={"age": IntegerLiteral(value=30)},
    )

    relation = star.to_relation(obj=node)

    assert isinstance(relation, FilterRows)
    assert isinstance(relation.condition, AttributeEqualsValue)
    assert relation.condition.left == "age"
    assert relation.condition.right == IntegerLiteral(value=30) 

def test_star_applies_filter_for_two_node_properties(relational_context: Context) -> None:
    star = Star(context=relational_context)
    node = NodePattern(
        variable=Variable(name="n"),
        labels=["Person"],
        properties={"age": IntegerLiteral(value=30), "city": StringLiteral(value="NYC")},
    )

    relation = star.to_relation(obj=node)

    assert isinstance(relation, FilterRows)
    assert isinstance(relation.condition, AttributeEqualsValue)
    assert relation.condition.left == "age"
    assert relation.condition.right == IntegerLiteral(value=30) 
    assert cast(AttributeEqualsValue, cast(typ=FilterRows, val=relation.relation).condition).left == "city"
    assert cast(AttributeEqualsValue, cast(typ=FilterRows, val=relation.relation).condition).right == StringLiteral(value="NYC")

def test_star_translates_relationship_without_properties(
    relational_context: Context,
) -> None:
    star = Star(context=relational_context)
    relationship = RelationshipPattern(
        variable=Variable(name="r"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )
    relation: Relation = star.to_relation(obj=relationship)
    assert isinstance(relation, Projection)
    assert Variable(name="r") in relation.variable_map


@pytest.mark.xfail(
    reason="Relationship property filtering not implemented",
    raises=NotImplementedError,
)
def test_star_applies_filter_for_relationship_properties(
    relational_context: Context,
) -> None:
    star = Star(context=relational_context)
    relationship = RelationshipPattern(
        variable=Variable(name="r"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={"since": 2020},
    )

    relation: Relation = star.to_relation(obj=relationship)

    assert isinstance(relation, FilterRows)
    assert isinstance(relation.relation, RelationshipTable)
    assert relation.condition.left == "since"
    assert relation.condition.right == 2020


def test_star_joins_pattern_path(relational_context: Context) -> None:
    star = Star(context=relational_context)

    node_a = NodePattern(variable=Variable(name="a"), labels=["Person"], properties={})
    node_b = NodePattern(variable=Variable(name="b"), labels=["Person"], properties={})
    relationship = RelationshipPattern(
        variable=Variable(name="r"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )
    path = PatternPath(variable=Variable(name="p"), elements=[node_a, relationship, node_b])

    relation: Relation = star.to_relation(obj=path)

    assert isinstance(relation, Join)
    assert relation.join_type == JoinType.INNER
    assert Variable(name="a") in relation.variable_map
    assert Variable(name="b") in relation.variable_map
    assert Variable(name="r") in relation.variable_map

def test_from_node_relationship_tail(relational_context: Context) -> None:
    star = Star(context=relational_context)

    node = NodePattern(variable=Variable(name="a"), labels=["Person"], properties={'age': IntegerLiteral(value=30)})
    relationship = RelationshipPattern(
        variable=Variable(name="r"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )

    relation = star._from_node_relationship_tail(node=node, relationship=relationship)

    assert isinstance(relation, Join)
    assert relation.join_type == JoinType.INNER
    assert Variable(name="a") in relation.variable_map
    assert Variable(name="r") in relation.variable_map


def test_from_node_relationship_tail_to_df(relational_context: Context) -> None:
    star = Star(context=relational_context)

    node = NodePattern(variable=Variable(name="a"), labels=["Person"], properties={'age': IntegerLiteral(value=30)})
    relationship = RelationshipPattern(
        variable=Variable(name="r"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )

    relation = star._from_node_relationship_tail(node=node, relationship=relationship)

    # Duplicates previous test
    assert isinstance(relation, Join)
    assert relation.join_type == JoinType.INNER
    assert Variable(name="a") in relation.variable_map
    assert Variable(name="r") in relation.variable_map

    df = relation.to_pandas(context=relational_context)
    assert len(df) == 1
    assert df.iloc[0][relation.variable_map[Variable(name="a")]] == 1

def test_filter_rows_to_pandas_filters_values(
    person_table: EntityTable, relational_context: Context
) -> None:
    filtered = FilterRows(
        relation=person_table,
        condition=AttributeEqualsValue(left="age", right=30),
        column_names=person_table.column_names,
    )

    df: pd.DataFrame = filtered.to_pandas(context=relational_context)

    assert len(df) == 1
    assert df.iloc[0]["Person__name"] == "Alice"


def test_from_pattern_path_to_relation(relational_context: Context) -> None:
    star = Star(context=relational_context)

    node_a = NodePattern(variable=Variable(name="a"), labels=["Person"], properties={})
    node_b = NodePattern(variable=Variable(name="b"), labels=["Person"], properties={})
    relationship = RelationshipPattern(
        variable=Variable(name="r"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )
    path = PatternPath(variable=Variable(name="p"), elements=[node_a, relationship, node_b])

    relation: Relation = star.to_relation(obj=path)

    assert isinstance(relation, Join)
    assert relation.join_type == JoinType.INNER
    assert Variable(name="a") in relation.variable_map
    assert Variable(name="b") in relation.variable_map
    assert Variable(name="r") in relation.variable_map


def test_integration_1(relational_context: Context) -> None:
    star = Star(context=relational_context)
    cypher: str = 'MATCH (n:Person {age: 30})-[k:KNOWS]->(m:Person) RETURN n, m'
    pattern_obj: Pattern = ASTConverter().from_cypher(cypher).clauses[0].pattern
    relation: Relation = star.to_relation(obj=pattern_obj)
    df = relation.to_pandas(context=relational_context)


def test_integration_2(relational_context: Context) -> None:
    star = Star(context=relational_context)
    cypher: str = 'MATCH (n:Person)-[k:KNOWS]->(m:Person)-[l:LIVES_IN]->(c:City) RETURN n, m, c'
    # cypher: str = 'MATCH (m:Person {age: 30})-[l:LIVES_IN]->(c:City) RETURN n, m, c'
    pattern_obj: Pattern = ASTConverter().from_cypher(cypher).clauses[0].pattern
    relation: Relation = star.to_relation(obj=pattern_obj)
    # import pdb; pdb.set_trace()
    relation.to_pandas(context=relational_context)