"""Hypothesis strategies for generating Cypher test data.

Provides reusable strategies for generating:
- Entity DataFrames with known ID ranges
- Relationship DataFrames with valid source/target references
- Star instances with generated graph data
- Scalar values for expression testing
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from hypothesis import strategies as st
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


@st.composite
def entity_dataframes(
    draw: Any,
    min_rows: int = 1,
    max_rows: int = 20,
) -> pd.DataFrame:
    """Generate a Person-like entity DataFrame."""
    n = draw(st.integers(min_value=min_rows, max_value=max_rows))
    ids = list(range(1, n + 1))
    names = draw(
        st.lists(
            st.text(
                alphabet=st.characters(whitelist_categories=("L",)),
                min_size=1,
                max_size=10,
            ),
            min_size=n,
            max_size=n,
        )
    )
    ages = draw(
        st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=n,
            max_size=n,
        )
    )
    return pd.DataFrame({ID_COLUMN: ids, "name": names, "age": ages})


@st.composite
def relationship_dataframes(
    draw: Any,
    entity_ids: list[int],
    min_edges: int = 0,
    max_edges: int = 15,
) -> pd.DataFrame:
    """Generate a relationship DataFrame with valid source/target refs."""
    if len(entity_ids) < 2:
        return pd.DataFrame(
            {
                ID_COLUMN: pd.Series(dtype=int),
                "__SOURCE__": pd.Series(dtype=int),
                "__TARGET__": pd.Series(dtype=int),
            }
        )
    n = draw(st.integers(min_value=min_edges, max_value=max_edges))
    sources = draw(
        st.lists(st.sampled_from(entity_ids), min_size=n, max_size=n)
    )
    targets = draw(
        st.lists(st.sampled_from(entity_ids), min_size=n, max_size=n)
    )
    return pd.DataFrame(
        {
            ID_COLUMN: list(range(101, 101 + n)),
            "__SOURCE__": sources,
            "__TARGET__": targets,
        }
    )


@st.composite
def social_stars(
    draw: Any,
    min_nodes: int = 2,
    max_nodes: int = 15,
) -> Star:
    """Generate a Star with Person entities and KNOWS relationships."""
    people_df = draw(entity_dataframes(min_rows=min_nodes, max_rows=max_nodes))
    entity_ids = list(people_df[ID_COLUMN])
    knows_df = draw(relationship_dataframes(entity_ids))

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )
    return Star(context=ctx)


# Scalar value strategies
cypher_integers = st.integers(min_value=-1_000_000, max_value=1_000_000)
cypher_floats = st.floats(
    min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False
)
cypher_strings = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=0,
    max_size=20,
)
cypher_booleans = st.booleans()
