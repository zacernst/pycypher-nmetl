"""Tests for ContextBuilder.from_dict() convenience constructor."""

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.relational_models import ID_COLUMN, Context
from pycypher.star import Star


@pytest.fixture
def person_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )


@pytest.fixture
def product_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [10, 20],
            "title": ["Widget", "Gadget"],
            "price": [9.99, 49.99],
        }
    )


class TestContextBuilderFromDict:
    def test_returns_context(self, person_df: pd.DataFrame) -> None:
        """from_dict() returns a Context instance."""
        ctx = ContextBuilder.from_dict({"Person": person_df})
        assert isinstance(ctx, Context)

    def test_single_entity_type_queryable(
        self, person_df: pd.DataFrame
    ) -> None:
        """Entity registered via from_dict() is reachable in a query."""
        ctx = ContextBuilder.from_dict({"Person": person_df})
        star = Star(context=ctx)
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert set(result["name"].tolist()) == {"Alice", "Bob", "Carol"}

    def test_multiple_entity_types(
        self, person_df: pd.DataFrame, product_df: pd.DataFrame
    ) -> None:
        """Multiple entity types can be registered in one call."""
        ctx = ContextBuilder.from_dict(
            {"Person": person_df, "Product": product_df}
        )
        star = Star(context=ctx)
        persons = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        products = star.execute_query(
            "MATCH (pr:Product) RETURN pr.title AS title"
        )
        assert len(persons) == 3
        assert set(products["title"].tolist()) == {"Widget", "Gadget"}

    def test_empty_dict_returns_usable_context(self) -> None:
        """Empty dict produces a valid (empty) context."""
        ctx = ContextBuilder.from_dict({})
        assert isinstance(ctx, Context)
        assert ctx.entity_mapping.mapping == {}

    def test_custom_id_column(self) -> None:
        """id_column kwarg renames the source column to __ID__."""
        df = pd.DataFrame({"person_id": [1, 2], "name": ["Dave", "Eve"]})
        ctx = ContextBuilder.from_dict({"Person": df}, id_column="person_id")
        star = Star(context=ctx)
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert set(result["name"].tolist()) == {"Dave", "Eve"}

    def test_non_dataframe_raises_type_error(self) -> None:
        """Passing a non-DataFrame value raises TypeError."""
        with pytest.raises(TypeError):
            ContextBuilder.from_dict({"Person": [{"name": "Alice"}]})  # type: ignore[dict-item]

    def test_property_values_correct(self, person_df: pd.DataFrame) -> None:
        """Registered properties are returned with correct values."""
        ctx = ContextBuilder.from_dict({"Person": person_df})
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name"
        )
        assert set(result["name"].tolist()) == {"Alice", "Carol"}
