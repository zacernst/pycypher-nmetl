"""Tests for execute_query with multiple MATCH clauses.

Covers multi-MATCH query execution via the BindingFrame path.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    Match,
    NodePattern,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Variable,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture()
def person_df() -> pd.DataFrame:
    """Person entity data: Alice(1), Bob(2), Carol(3)."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 40, 25],
        }
    )


@pytest.fixture()
def knows_df() -> pd.DataFrame:
    """KNOWS relationship data (Person→Person)."""
    return pd.DataFrame(
        {
            ID_COLUMN: [100, 101, 102],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 1],
            "since": [2020, 2021, 2019],
        }
    )


@pytest.fixture()
def person_table(person_df: pd.DataFrame) -> EntityTable:
    return EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )


@pytest.fixture()
def knows_table(knows_df: pd.DataFrame) -> RelationshipTable:
    return RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
            "since",
        ],
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
        source_obj=knows_df,
    )


@pytest.fixture()
def person_only_context(
    person_table: EntityTable,
    knows_table: RelationshipTable,
) -> Context:
    """Minimal context with only Person and KNOWS."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


# ============================================================================
# execute_query — multiple MATCH clauses
# ============================================================================


class TestExecuteQueryMultiMatch:
    """Cover execute_query with multiple MATCH clauses."""

    def test_two_match_clauses(self, person_only_context: Context) -> None:
        """Two MATCH clauses are joined then RETURN projects from the combined relation."""
        star = Star(context=person_only_context)

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="p"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="k"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={},
                                    ),
                                    NodePattern(
                                        variable=Variable(name="q"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                ]
                            )
                        ]
                    )
                ),
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="q"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="m"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={},
                                    ),
                                    NodePattern(
                                        variable=Variable(name="r"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"), property="name"
                            ),
                            alias="p_name",
                        ),
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="r"), property="name"
                            ),
                            alias="r_name",
                        ),
                    ]
                ),
            ]
        )

        df = star.execute_query(query)
        assert "p_name" in df.columns
        assert "r_name" in df.columns
        assert len(df) > 0
