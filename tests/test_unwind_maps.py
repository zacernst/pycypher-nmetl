"""TDD tests for UNWIND of a list of maps.

In openCypher, UNWIND can unwrap a list of map literals into individual rows:

    UNWIND [{name: 'Alice', age: 30}, {name: 'Bob', age: 25}] AS row
    RETURN row.name, row.age

This is a very common ETL pattern used to insert or transform structured data.
pycypher currently raises ``TypeError: unhashable type: 'dict'`` for this case
because ``_infer_entity_type`` tries to call ``.unique()`` on the dict-valued
series, and ``_eval_property_lookup`` assumes the variable holds entity IDs.

All tests written before implementation (TDD step 1).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture
def empty_star() -> Star:
    return Star(context=Context(entity_mapping=EntityMapping(mapping={})))


@pytest.fixture
def person_star() -> Star:
    df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]})
    t = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Star(
        context=Context(entity_mapping=EntityMapping(mapping={"Person": t})),
    )


# ---------------------------------------------------------------------------
# Basic UNWIND of list of maps
# ---------------------------------------------------------------------------


class TestUnwindMaps:
    """UNWIND [{...}, {...}] AS row RETURN row.prop must work."""

    def test_unwind_maps_does_not_raise(self, empty_star: Star) -> None:
        """UNWIND of a map list must not raise TypeError."""
        empty_star.execute_query(
            "UNWIND [{name: 'Alice'}, {name: 'Bob'}] AS row RETURN row.name",
        )

    def test_unwind_maps_row_count(self, empty_star: Star) -> None:
        """UNWIND of a 2-element map list produces 2 rows."""
        r = empty_star.execute_query(
            "UNWIND [{name: 'Alice'}, {name: 'Bob'}] AS row RETURN row.name",
        )
        assert len(r) == 2

    def test_unwind_maps_values(self, empty_star: Star) -> None:
        """Row property values are correctly extracted."""
        r = empty_star.execute_query(
            "UNWIND [{name: 'Alice'}, {name: 'Bob'}] AS row RETURN row.name",
        )
        assert set(r["name"]) == {"Alice", "Bob"}

    def test_unwind_maps_multiple_properties(self, empty_star: Star) -> None:
        """Multiple properties from each map row are accessible."""
        r = empty_star.execute_query(
            "UNWIND [{name: 'Alice', age: 30}, {name: 'Bob', age: 25}] AS row "
            "RETURN row.name, row.age ORDER BY row.name",
        )
        assert list(r["name"]) == ["Alice", "Bob"]
        assert [int(v) for v in r["age"]] == [30, 25]

    def test_unwind_maps_with_where(self, empty_star: Star) -> None:
        """WHERE can filter on UNWIND map properties via a WITH clause."""
        r = empty_star.execute_query(
            "UNWIND [{name: 'Alice', age: 30}, {name: 'Bob', age: 25}] AS row "
            "WITH row WHERE row.age > 26 "
            "RETURN row.name",
        )
        assert list(r["name"]) == ["Alice"]

    def test_unwind_maps_missing_key_returns_null(
        self,
        empty_star: Star,
    ) -> None:
        """Accessing a key absent from a map returns null (not an error)."""
        r = empty_star.execute_query(
            "UNWIND [{name: 'Alice'}, {name: 'Bob', score: 99}] AS row "
            "RETURN row.name, row.score",
        )
        assert len(r) == 2
        # First row: score is missing → null
        assert r["score"].iloc[0] is None or pd.isna(r["score"].iloc[0])
        assert int(r["score"].iloc[1]) == 99

    def test_unwind_maps_with_aggregation(self, empty_star: Star) -> None:
        """Aggregation over UNWIND-ed maps works correctly."""
        r = empty_star.execute_query(
            "UNWIND [{val: 10}, {val: 20}, {val: 30}] AS row "
            "RETURN sum(row.val) AS total",
        )
        assert int(r["total"].iloc[0]) == 60


# ---------------------------------------------------------------------------
# UNWIND maps combined with MATCH
# ---------------------------------------------------------------------------


class TestUnwindMapsWithMatch:
    """UNWIND of maps combined with a MATCH works end-to-end."""

    def test_unwind_maps_then_match(self, person_star: Star) -> None:
        """UNWIND map list, then MATCH entity by name, return both."""
        r = person_star.execute_query(
            "UNWIND [{uname: 'Alice'}, {uname: 'Bob'}] AS row "
            "MATCH (p:Person) WHERE p.name = row.uname "
            "RETURN row.uname, p.name ORDER BY row.uname",
        )
        assert len(r) == 2
        # Both rows should resolve to the matching person
