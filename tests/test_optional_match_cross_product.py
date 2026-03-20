"""Tests for cross-product OPTIONAL MATCH (no shared variables).

When OPTIONAL MATCH introduces new variables that share none with the
preceding frame, the semantics are:
- If the optional pattern returns rows  → Cartesian product (cross join)
- If the optional pattern returns no rows → null columns for new variables
  (same as the entity-absent failure case)
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
)
from pycypher.star import Star


@pytest.fixture()
def two_entity_context() -> Context:
    """Two Persons and two Companies — unrelated entity types."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    company_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 20],
            "name": ["Acme", "Globex"],
        }
    )
    company_table = EntityTable(
        entity_type="Company",
        identifier="Company",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=company_df,
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": people_table, "Company": company_table}
        ),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture()
def person_only_context() -> Context:
    """Two Persons — no Company type in context."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestOptionalMatchCrossProduct:
    """OPTIONAL MATCH with no shared variables — cross-join or null-fill."""

    def test_cross_product_produces_cartesian_result(
        self, two_entity_context: Context
    ) -> None:
        """Two Persons × two Companies = four rows."""
        star = Star(context=two_entity_context)
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (c:Company) RETURN p.name, c.name"
        )
        assert len(result) == 4

    def test_cross_product_contains_all_combinations(
        self, two_entity_context: Context
    ) -> None:
        """Every (person, company) pair appears in the output."""
        star = Star(context=two_entity_context)
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (c:Company) RETURN p.name AS person, c.name AS company"
        )
        person_names = set(result["person"].tolist())
        company_names = set(result["company"].tolist())
        assert person_names == {"Alice", "Bob"}
        assert company_names == {"Acme", "Globex"}

    def test_cross_product_absent_entity_nulls_new_variable(
        self, person_only_context: Context
    ) -> None:
        """If Company doesn't exist, all Person rows survive with c=null."""
        star = Star(context=person_only_context)
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (c:Company) RETURN p.name AS person"
        )
        # Both persons survive even though Company doesn't exist
        assert len(result) == 2
        assert set(result["person"].tolist()) == {"Alice", "Bob"}

    def test_cross_product_absent_entity_new_var_is_null(
        self, person_only_context: Context
    ) -> None:
        """When Company is absent, c.name returns null for all rows."""
        star = Star(context=person_only_context)
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (c:Company) "
            "RETURN p.name AS person, c.name AS company"
        )
        assert len(result) == 2
        assert result["company"].isna().all()

    def test_cross_product_does_not_raise_not_implemented(
        self, two_entity_context: Context
    ) -> None:
        """Cross-product OPTIONAL MATCH must not raise NotImplementedError."""
        star = Star(context=two_entity_context)
        # Previously this raised NotImplementedError — must not do so now
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (c:Company) RETURN p.name"
        )
        assert result is not None
