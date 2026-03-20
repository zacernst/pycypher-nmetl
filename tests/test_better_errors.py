"""Tests for improved error messages in BindingFrame.get_property()."""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture
def person_context() -> Context:
    df = pd.DataFrame(
        {ID_COLUMN: [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]}
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


class TestMissingPropertyError:
    def test_missing_property_returns_null(
        self, person_context: Context
    ) -> None:
        """Accessing a nonexistent property returns null per Cypher semantics."""
        import math

        import pandas as pd

        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.nonexistent AS x"
        )
        val = result["x"].iloc[0]
        assert (
            val is None
            or val is pd.NA
            or (isinstance(val, float) and math.isnan(val))
        )

    def test_missing_property_is_null_check(
        self, person_context: Context
    ) -> None:
        """IS NULL on a missing property returns True."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.salary IS NULL AS missing"
        )
        assert bool(result["missing"].iloc[0]) is True

    def test_unknown_entity_type_raises_value_error(
        self, person_context: Context
    ) -> None:
        """Querying an unknown entity type raises ValueError."""
        star = Star(context=person_context)
        with pytest.raises((ValueError, KeyError)):
            star.execute_query("MATCH (c:Company) RETURN c.name AS name")


class TestVariableNotInTypeRegistry:
    def test_unregistered_variable_error_message(
        self, person_context: Context
    ) -> None:
        """Variable in bindings but not type_registry gives a clear error."""
        from pycypher.binding_frame import BindingFrame

        # Build a frame where 'x' is a scalar column, not an entity
        df = pd.DataFrame({"x": [1, 2, 3]})
        frame = BindingFrame(
            bindings=df,
            type_registry={},  # 'x' not registered as entity type
            context=person_context,
        )
        with pytest.raises(ValueError, match="registered entity type"):
            frame.get_property("x", "name")

    def test_variable_not_in_bindings_error_message(
        self, person_context: Context
    ) -> None:
        """Variable absent from bindings gives a clear error."""
        from pycypher.binding_frame import BindingFrame

        df = pd.DataFrame({"p": [1, 2]})
        frame = BindingFrame(
            bindings=df,
            type_registry={"p": "Person"},
            context=person_context,
        )
        with pytest.raises(ValueError, match="is not defined"):
            frame.get_property("q", "name")
