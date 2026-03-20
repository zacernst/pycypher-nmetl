"""Tests for map projection expressions: node{.prop, key: expr}.

Map projection ``n{.name, .age}`` returns a dict with the selected
properties for each row.  Syntax variants:

* ``.name``          — copy property with same name as the key
* ``key: expression`` — computed property
* ``.*``             — include all entity properties
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "dept": ["eng", "sales", "eng"],
        }
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


class TestMapProjection:
    """n{.prop, key: expr} map projection."""

    def test_single_property_selector(self, star: Star) -> None:
        """n{.name} returns a dict with a 'name' key."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p{.name} AS m"
        )
        assert result["m"].iloc[0] == {"name": "Alice"}

    def test_two_property_selectors(self, star: Star) -> None:
        """n{.name, .age} returns a dict with both keys."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p{.name, .age} AS m"
        )
        row = result["m"].iloc[0]
        assert row["name"] == "Alice"
        assert row["age"] == 30

    def test_computed_property(self, star: Star) -> None:
        """n{double_age: n.age * 2} includes a computed key."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p{double_age: p.age * 2} AS m"
        )
        assert result["m"].iloc[0] == {"double_age": 60}

    def test_mixed_selector_and_computed(self, star: Star) -> None:
        """n{.name, senior: n.age > 30} mixes selector and computed."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p{.name, senior: p.age > 30} AS m"
        )
        row = result["m"].iloc[0]
        assert row["name"] == "Alice"
        assert row["senior"] is False or row["senior"] == False  # noqa: E712

    def test_all_rows(self, star: Star) -> None:
        """Map projection applied to all rows returns correct dicts."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p{.name} AS m ORDER BY p.name ASC"
        )
        names = [row["name"] for row in result["m"]]
        assert names == ["Alice", "Bob", "Carol"]

    def test_in_with_clause(self, star: Star) -> None:
        """Map projection works as a WITH expression alias."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH p{.name, .dept} AS info "
            "RETURN info"
        )
        row = result["info"].iloc[0]
        assert row["name"] == "Alice"
        assert row["dept"] == "eng"
