"""Tests that aggregation functions in scalar expression context raise clear errors.

When a user writes ``WHERE count(p) > 1`` or uses an aggregation function in a
context where only scalar expressions are valid (e.g. WHERE filter, property
value), the engine should raise a ``WrongCypherTypeError`` with a helpful message
explaining *why* the function cannot be used there — not the confusing
"Unknown scalar function: count" that the raw scalar registry emits.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.exceptions import WrongCypherTypeError
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture()
def person_context() -> Context:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestAggregationInScalarContext:
    """Using aggregation functions in non-aggregation positions raises WrongCypherTypeError."""

    def test_count_in_where_raises_type_error(
        self, person_context: Context
    ) -> None:
        """count() in WHERE must raise WrongCypherTypeError, not crash with a confusing message."""
        star = Star(context=person_context)
        with pytest.raises(WrongCypherTypeError, match="aggregation"):
            star.execute_query(
                "MATCH (p:Person) WHERE count(p) > 1 RETURN p.name"
            )

    def test_sum_in_where_raises_type_error(
        self, person_context: Context
    ) -> None:
        """sum() in WHERE must raise WrongCypherTypeError."""
        star = Star(context=person_context)
        with pytest.raises(WrongCypherTypeError, match="aggregation"):
            star.execute_query(
                "MATCH (p:Person) WHERE sum(p.age) > 50 RETURN p.name"
            )

    def test_avg_in_where_raises_type_error(
        self, person_context: Context
    ) -> None:
        """avg() in WHERE must raise WrongCypherTypeError."""
        star = Star(context=person_context)
        with pytest.raises(WrongCypherTypeError, match="aggregation"):
            star.execute_query(
                "MATCH (p:Person) WHERE avg(p.age) > 25 RETURN p.name"
            )

    def test_error_message_names_the_function(
        self, person_context: Context
    ) -> None:
        """Error message must include the specific function name for debugging."""
        star = Star(context=person_context)
        with pytest.raises(WrongCypherTypeError, match="count"):
            star.execute_query(
                "MATCH (p:Person) WHERE count(p) > 1 RETURN p.name"
            )

    def test_count_star_in_return_still_works(
        self, person_context: Context
    ) -> None:
        """COUNT(*) in RETURN (valid aggregation context) must still work."""
        star = Star(context=person_context)
        result = star.execute_query("MATCH (p:Person) RETURN count(*) AS n")
        assert result["n"].iloc[0] == 3
