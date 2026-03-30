"""Tests for cross-product MATCH support.

MATCH (a:A), (b:B) — when the two patterns share no common variables —
produces the Cartesian product of both entity scans.

TDD red phase → green phase.
"""

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
def people_products_context() -> Context:
    """3 people, 2 products — cross join should give 6 rows."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "budget": [100, 50, 200],
        },
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "budget"],
        source_obj_attribute_map={"name": "name", "budget": "budget"},
        attribute_map={"name": "name", "budget": "budget"},
        source_obj=people_df,
    )

    products_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 20],
            "title": ["Widget", "Gadget"],
            "price": [60, 150],
        },
    )
    products_table = EntityTable(
        entity_type="Product",
        identifier="Product",
        column_names=[ID_COLUMN, "title", "price"],
        source_obj_attribute_map={"title": "title", "price": "price"},
        attribute_map={"title": "title", "price": "price"},
        source_obj=products_df,
    )

    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": people_table, "Product": products_table},
        ),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def persons_context() -> Context:
    """3 people — same-type cross join gives 9 rows (3×3)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Basic cross-product MATCH
# ─────────────────────────────────────────────────────────────────────────────


class TestCrossProductBasic:
    def test_cross_product_two_types(
        self,
        people_products_context: Context,
    ) -> None:
        """MATCH (p:Person), (pr:Product) → 3 × 2 = 6 rows."""
        star = Star(context=people_products_context)
        result = star.execute_query(
            "MATCH (p:Person), (pr:Product) RETURN p.name AS person, pr.title AS product",
        )
        assert len(result) == 6
        assert "person" in result.columns
        assert "product" in result.columns

    def test_cross_product_all_combinations_present(
        self,
        people_products_context: Context,
    ) -> None:
        """Every (person, product) pair appears exactly once."""
        star = Star(context=people_products_context)
        result = star.execute_query(
            "MATCH (p:Person), (pr:Product) RETURN p.name AS person, pr.title AS product",
        )
        pairs = set(zip(result["person"].tolist(), result["product"].tolist()))
        assert ("Alice", "Widget") in pairs
        assert ("Alice", "Gadget") in pairs
        assert ("Bob", "Widget") in pairs
        assert ("Carol", "Gadget") in pairs

    def test_cross_product_with_where_filter(
        self,
        people_products_context: Context,
    ) -> None:
        """WHERE budget >= price filters the Cartesian product."""
        star = Star(context=people_products_context)
        result = star.execute_query(
            "MATCH (p:Person), (pr:Product) "
            "WHERE p.budget >= pr.price "
            "RETURN p.name AS person, pr.title AS product",
        )
        # Alice (100) can afford Widget (60) but not Gadget (150)
        # Bob (50) can afford neither
        # Carol (200) can afford both
        assert len(result) == 3
        pairs = set(zip(result["person"].tolist(), result["product"].tolist()))
        assert ("Alice", "Widget") in pairs
        assert ("Carol", "Widget") in pairs
        assert ("Carol", "Gadget") in pairs


# ─────────────────────────────────────────────────────────────────────────────
# Same-type cross-product MATCH
# ─────────────────────────────────────────────────────────────────────────────


class TestSameTypeCrossProduct:
    def test_self_cross_product_row_count(
        self,
        persons_context: Context,
    ) -> None:
        """MATCH (p:Person), (q:Person) → 3 × 3 = 9 rows."""
        star = Star(context=persons_context)
        result = star.execute_query(
            "MATCH (p:Person), (q:Person) RETURN p.name AS pname, q.name AS qname",
        )
        assert len(result) == 9

    def test_self_cross_product_filter_different(
        self,
        persons_context: Context,
    ) -> None:
        """WHERE p.name <> q.name keeps only the N²-N = 6 off-diagonal pairs."""
        star = Star(context=persons_context)
        result = star.execute_query(
            "MATCH (p:Person), (q:Person) "
            "WHERE p.name <> q.name "
            "RETURN p.name AS pname, q.name AS qname",
        )
        assert len(result) == 6
        # Diagonal (same name) should be absent
        for _, row in result.iterrows():
            assert row["pname"] != row["qname"]
