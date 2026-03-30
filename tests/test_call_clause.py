"""Tests for CALL clause execution.

CALL procedureName(args) YIELD col1, col2 [WHERE pred]

The CALL clause invokes a registered procedure with the supplied arguments
and YIELDs the results as new columns in the binding frame.  When no YIELD
is present the procedure is still called but its output is discarded.

Built-in procedures under the ``db.*`` namespace:
  - db.labels()            → label (string)
  - db.relationshipTypes() → relationshipType (string)
  - db.propertyKeys()      → propertyKey (string)

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def social_context() -> Context:
    """Two entity types (Person, Company) and one relationship type (WORKS_AT)."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
        },
    )
    companies_df = pd.DataFrame(
        {
            ID_COLUMN: [10],
            "industry": ["Tech"],
        },
    )
    works_at_df = pd.DataFrame(
        {
            ID_COLUMN: [101],
            "__SOURCE__": [1],
            "__TARGET__": [10],
        },
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    companies_table = EntityTable(
        entity_type="Company",
        identifier="Company",
        column_names=[ID_COLUMN, "industry"],
        source_obj_attribute_map={"industry": "industry"},
        attribute_map={"industry": "industry"},
        source_obj=companies_df,
    )
    works_at_table = RelationshipTable(
        relationship_type="WORKS_AT",
        identifier="WORKS_AT",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=works_at_df,
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": people_table, "Company": companies_table},
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"WORKS_AT": works_at_table},
        ),
    )


@pytest.fixture
def empty_context() -> Context:
    """Completely empty context — no entities or relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ---------------------------------------------------------------------------
# db.labels() — returns one row per entity type registered in context
# ---------------------------------------------------------------------------


class TestDbLabels:
    """CALL db.labels() YIELD label returns all registered entity type names."""

    def test_db_labels_returns_entity_types(
        self,
        social_context: Context,
    ) -> None:
        """db.labels() returns a row for each entity type in the context."""
        star = Star(context=social_context)
        result = star.execute_query(
            "CALL db.labels() YIELD label RETURN label",
        )
        assert set(result["label"].tolist()) == {"Person", "Company"}

    def test_db_labels_row_count(self, social_context: Context) -> None:
        """db.labels() returns exactly as many rows as there are entity types."""
        star = Star(context=social_context)
        result = star.execute_query(
            "CALL db.labels() YIELD label RETURN label",
        )
        assert len(result) == 2  # Person and Company

    def test_db_labels_empty_context(self, empty_context: Context) -> None:
        """db.labels() on an empty context returns an empty result."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CALL db.labels() YIELD label RETURN label",
        )
        assert len(result) == 0

    def test_db_labels_single_entity_type(self) -> None:
        """db.labels() with one entity type returns exactly one row."""
        df = pd.DataFrame({ID_COLUMN: [1], "x": [42]})
        table = EntityTable(
            entity_type="Widget",
            identifier="Widget",
            column_names=[ID_COLUMN, "x"],
            source_obj_attribute_map={"x": "x"},
            attribute_map={"x": "x"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Widget": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=ctx)
        result = star.execute_query(
            "CALL db.labels() YIELD label RETURN label",
        )
        assert result["label"].tolist() == ["Widget"]

    def test_db_labels_does_not_raise_not_implemented(
        self,
        social_context: Context,
    ) -> None:
        """Regression: CALL db.labels() must not raise NotImplementedError."""
        star = Star(context=social_context)
        result = star.execute_query(
            "CALL db.labels() YIELD label RETURN label",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# db.relationshipTypes() — returns one row per relationship type
# ---------------------------------------------------------------------------


class TestDbRelationshipTypes:
    """CALL db.relationshipTypes() YIELD relationshipType."""

    def test_db_relationship_types_returns_types(
        self,
        social_context: Context,
    ) -> None:
        """db.relationshipTypes() returns all registered relationship types."""
        star = Star(context=social_context)
        result = star.execute_query(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
        )
        assert result["relationshipType"].tolist() == ["WORKS_AT"]

    def test_db_relationship_types_empty_context(
        self,
        empty_context: Context,
    ) -> None:
        """db.relationshipTypes() on an empty context returns an empty result."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# db.propertyKeys() — returns one row per unique property key across all types
# ---------------------------------------------------------------------------


class TestDbPropertyKeys:
    """CALL db.propertyKeys() YIELD propertyKey."""

    def test_db_property_keys_returns_keys(
        self,
        social_context: Context,
    ) -> None:
        """db.propertyKeys() returns all unique user-visible property keys."""
        star = Star(context=social_context)
        result = star.execute_query(
            "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
        )
        # Person has 'name', Company has 'industry'; __ID__, __SOURCE__, __TARGET__ excluded
        assert set(result["propertyKey"].tolist()) == {"name", "industry"}

    def test_db_property_keys_empty_context(
        self,
        empty_context: Context,
    ) -> None:
        """db.propertyKeys() on an empty context returns an empty result."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# CALL without YIELD — procedure is called but output is discarded
# ---------------------------------------------------------------------------


class TestCallWithoutYield:
    """CALL without YIELD does not raise and returns empty DataFrame."""

    def test_call_without_yield_does_not_raise(
        self,
        social_context: Context,
    ) -> None:
        """CALL db.labels() without YIELD completes without error."""
        star = Star(context=social_context)
        result = star.execute_query("CALL db.labels()")
        assert result is not None

    def test_call_without_yield_returns_empty_dataframe(
        self,
        social_context: Context,
    ) -> None:
        """CALL without YIELD returns an empty DataFrame (no RETURN clause)."""
        star = Star(context=social_context)
        result = star.execute_query("CALL db.labels()")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Unknown procedure — clear error message
# ---------------------------------------------------------------------------


class TestUnknownProcedure:
    """Calling an unknown procedure raises ValueError with the procedure name."""

    def test_unknown_procedure_raises_value_error(
        self,
        social_context: Context,
    ) -> None:
        """An unknown procedure name raises ValueError, not NotImplementedError."""
        star = Star(context=social_context)
        with pytest.raises(ValueError, match="no.such.procedure"):
            star.execute_query("CALL no.such.procedure() YIELD x RETURN x")

    def test_unknown_procedure_error_mentions_name(
        self,
        social_context: Context,
    ) -> None:
        """The error message includes the unknown procedure name."""
        star = Star(context=social_context)
        with pytest.raises(ValueError, match="custom.missing"):
            star.execute_query(
                "CALL custom.missing() YIELD result RETURN result",
            )
