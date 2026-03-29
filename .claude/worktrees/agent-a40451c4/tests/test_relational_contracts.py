"""Tests verifying Phase 1 foundational contracts for relational_models.

These tests check:
- Each Relation subclass has a ``column_format`` property returning the
  correct ``ColumnFormat`` value.
- Each Relation subclass implements ``output_column_names()`` and returns a
  list of strings.
- Specific column naming guarantees for EntityTable, RelationshipTable,
  Projection, Join, FilterRows, SelectColumns, and RelationIntersection.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import Variable
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    AttributeEqualsValue,
    ColumnFormat,
    Context,
    EntityMapping,
    EntityTable,
    FilterRows,
    Join,
    JoinType,
    Projection,
    Relation,
    RelationIntersection,
    RelationshipMapping,
    RelationshipTable,
    SelectColumns,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def people_df() -> pd.DataFrame:
    """Minimal Person DataFrame."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )


@pytest.fixture()
def knows_df() -> pd.DataFrame:
    """Minimal KNOWS DataFrame."""
    return pd.DataFrame(
        {
            ID_COLUMN: [10, 11],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2],
            RELATIONSHIP_TARGET_COLUMN: [2, 1],
        }
    )


@pytest.fixture()
def person_table(people_df: pd.DataFrame) -> EntityTable:
    return EntityTable(
        entity_type="Person",
        identifier="Person",
        source_obj=people_df,
        attribute_map={"name": "name", "age": "age"},
        source_obj_attribute_map={"name": "name", "age": "age"},
        column_names=[ID_COLUMN, "name", "age"],
    )


@pytest.fixture()
def knows_table(knows_df: pd.DataFrame) -> RelationshipTable:
    return RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        source_obj=knows_df,
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
        },
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
        },
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
    )


@pytest.fixture()
def relational_context(
    person_table: EntityTable,
    knows_table: RelationshipTable,
) -> Context:
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


# ---------------------------------------------------------------------------
# 1.1 / 1.2  column_format property
# ---------------------------------------------------------------------------


def test_entity_table_column_format(person_table: EntityTable) -> None:
    assert person_table.column_format == ColumnFormat.PREFIXED_ENTITY


def test_relationship_table_column_format(
    knows_table: RelationshipTable,
) -> None:
    assert knows_table.column_format == ColumnFormat.PREFIXED_ENTITY


def test_projection_column_format(person_table: EntityTable) -> None:
    import hashlib
    import random

    new_col = "abc123"
    proj = Projection(
        relation=person_table,
        projected_column_names={"Person____ID__": new_col},
        column_names=[new_col],
        variable_map={Variable(name="n"): new_col},
        variable_type_map={Variable(name="n"): "Person"},
    )
    assert proj.column_format == ColumnFormat.HASH_ID


def test_join_column_format(
    person_table: EntityTable, knows_table: RelationshipTable
) -> None:
    n_col = "col_n"
    r_col = "col_r"
    node_proj = Projection(
        relation=person_table,
        projected_column_names={"Person____ID__": n_col},
        column_names=[n_col],
        variable_map={Variable(name="n"): n_col},
        variable_type_map={Variable(name="n"): "Person"},
    )
    rel_proj = Projection(
        relation=knows_table,
        projected_column_names={
            "KNOWS____ID__": r_col,
            "KNOWS____SOURCE__": "src",
            "KNOWS____TARGET__": "tgt",
        },
        column_names=[r_col, "src", "tgt"],
        variable_map={Variable(name="r"): r_col},
        variable_type_map={Variable(name="r"): "KNOWS"},
    )
    join = Join(
        left=node_proj,
        right=rel_proj,
        on_left=[n_col],
        on_right=["src"],
        join_type=JoinType.INNER,
        variable_map={Variable(name="n"): n_col, Variable(name="r"): r_col},
        variable_type_map={
            Variable(name="n"): "Person",
            Variable(name="r"): "KNOWS",
        },
        column_names=[n_col, r_col],
    )
    assert join.column_format == ColumnFormat.HASH_ID


def test_filter_rows_column_format(person_table: EntityTable) -> None:
    person_table.variable_map = {Variable(name="n"): "Person____ID__"}
    person_table.variable_type_map = {Variable(name="n"): "Person"}
    fr = FilterRows(
        relation=person_table,
        condition=AttributeEqualsValue(left="age", right=30),
        variable_map={Variable(name="n"): "Person____ID__"},
        variable_type_map={Variable(name="n"): "Person"},
        column_names=["Person____ID__"],
    )
    assert fr.column_format == ColumnFormat.HASH_ID


def test_select_columns_column_format(person_table: EntityTable) -> None:
    sc = SelectColumns(
        relation=person_table,
        column_names=["Person____ID__"],
    )
    assert sc.column_format == ColumnFormat.HASH_ID


def test_relation_intersection_column_format(
    person_table: EntityTable,
) -> None:
    col = "col_x"
    proj = Projection(
        relation=person_table,
        projected_column_names={"Person____ID__": col},
        column_names=[col],
        variable_map={Variable(name="n"): col},
        variable_type_map={Variable(name="n"): "Person"},
    )
    ri = RelationIntersection(
        relation_list=[proj],
        column_names=[col],
    )
    assert ri.column_format == ColumnFormat.HASH_ID


# ---------------------------------------------------------------------------
# 1.4  output_column_names()
# ---------------------------------------------------------------------------


def test_output_column_names_returns_list_of_strings(
    person_table: EntityTable,
) -> None:
    names = person_table.output_column_names()
    assert isinstance(names, list)
    assert all(isinstance(n, str) for n in names)


def test_entity_table_output_column_names_includes_prefixed_id(
    person_table: EntityTable,
) -> None:
    names = person_table.output_column_names()
    assert "Person____ID__" in names


def test_entity_table_output_column_names_includes_prefixed_attributes(
    person_table: EntityTable,
) -> None:
    names = person_table.output_column_names()
    assert "Person__name" in names
    assert "Person__age" in names


def test_relationship_table_output_column_names(
    knows_table: RelationshipTable,
) -> None:
    names = knows_table.output_column_names()
    assert "KNOWS____ID__" in names
    assert "KNOWS____SOURCE__" in names
    assert "KNOWS____TARGET__" in names


def test_projection_output_column_names(person_table: EntityTable) -> None:
    target = "hashed_id_col"
    proj = Projection(
        relation=person_table,
        projected_column_names={"Person____ID__": target},
        column_names=[target],
        variable_map={Variable(name="n"): target},
        variable_type_map={Variable(name="n"): "Person"},
    )
    assert proj.output_column_names() == [target]


def test_filter_rows_output_column_names(person_table: EntityTable) -> None:
    person_table.variable_map = {Variable(name="n"): "Person____ID__"}
    person_table.variable_type_map = {Variable(name="n"): "Person"}
    fr = FilterRows(
        relation=person_table,
        condition=AttributeEqualsValue(left="age", right=30),
        variable_map={Variable(name="n"): "Person____ID__"},
        variable_type_map={Variable(name="n"): "Person"},
        column_names=["Person____ID__"],
    )
    names = fr.output_column_names()
    assert isinstance(names, list)
    assert all(isinstance(n, str) for n in names)
    assert "Person____ID__" in names


def test_join_output_column_names(
    person_table: EntityTable, knows_table: RelationshipTable
) -> None:
    n_col = "n_id_col"
    r_col = "r_id_col"
    node_proj = Projection(
        relation=person_table,
        projected_column_names={"Person____ID__": n_col},
        column_names=[n_col],
        variable_map={Variable(name="n"): n_col},
        variable_type_map={Variable(name="n"): "Person"},
    )
    rel_proj = Projection(
        relation=knows_table,
        projected_column_names={
            "KNOWS____ID__": r_col,
            "KNOWS____SOURCE__": "src",
            "KNOWS____TARGET__": "tgt",
        },
        column_names=[r_col, "src", "tgt"],
        variable_map={Variable(name="r"): r_col},
        variable_type_map={Variable(name="r"): "KNOWS"},
    )
    join = Join(
        left=node_proj,
        right=rel_proj,
        on_left=[n_col],
        on_right=["src"],
        join_type=JoinType.INNER,
        variable_map={Variable(name="n"): n_col, Variable(name="r"): r_col},
        variable_type_map={
            Variable(name="n"): "Person",
            Variable(name="r"): "KNOWS",
        },
        column_names=[n_col, r_col],
    )
    names = join.output_column_names()
    assert n_col in names
    assert r_col in names


def test_select_columns_output_column_names(person_table: EntityTable) -> None:
    sc = SelectColumns(
        relation=person_table,
        column_names=["Person____ID__", "Person__name"],
    )
    assert sc.output_column_names() == ["Person____ID__", "Person__name"]


def test_relation_intersection_output_column_names(
    person_table: EntityTable,
) -> None:
    col = "col_y"
    proj = Projection(
        relation=person_table,
        projected_column_names={"Person____ID__": col},
        column_names=[col],
        variable_map={Variable(name="n"): col},
        variable_type_map={Variable(name="n"): "Person"},
    )
    ri = RelationIntersection(
        relation_list=[proj],
        column_names=[col],
    )
    assert ri.output_column_names() == [col]


# ---------------------------------------------------------------------------
# 1.3  Type aliases exist and are importable
# ---------------------------------------------------------------------------


def test_match_alias_variable_map_importable() -> None:
    from pycypher.relational_models import (
        AliasVariableMap,
        MatchVariableMap,
    )

    assert MatchVariableMap is dict
    assert AliasVariableMap is dict


# ---------------------------------------------------------------------------
# 1.1  ColumnFormat enum has all required members
# ---------------------------------------------------------------------------


def test_column_format_members() -> None:
    members = {m.value for m in ColumnFormat}
    assert "prefixed_entity" in members
    assert "hash_id" in members
    assert "alias" in members
    assert "mixed" in members
