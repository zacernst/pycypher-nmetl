"""Tests for WITH * (pass-through) support.

WITH * preserves all variables from the preceding MATCH frame without
requiring explicit projection.
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
def person_context() -> Context:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "dept": ["eng", "hr", "eng"],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "dept"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
        },
        attribute_map={"name": "name", "age": "age", "dept": "dept"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestWithStar:
    def test_with_star_basic_return(self, person_context: Context) -> None:
        """WITH * passes all variables; RETURN can access them."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * RETURN p.name AS name"
        )
        assert set(result["name"].tolist()) == {"Alice", "Bob", "Carol"}

    def test_with_star_row_count_unchanged(
        self, person_context: Context
    ) -> None:
        """WITH * preserves the same number of rows as the MATCH."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * RETURN p.name AS name"
        )
        assert len(result) == 3

    def test_with_star_where_filter(self, person_context: Context) -> None:
        """WITH * WHERE filters rows using the passed-through variables."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * WHERE p.age > 28 RETURN p.name AS name"
        )
        assert set(result["name"].tolist()) == {"Alice", "Carol"}

    def test_with_star_multiple_properties(
        self, person_context: Context
    ) -> None:
        """WITH * allows RETURN to access multiple properties of passed variables."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * RETURN p.name AS name, p.age AS age"
        )
        assert len(result) == 3
        assert set(result.columns) == {"name", "age"}

    def test_with_star_then_aggregate(self, person_context: Context) -> None:
        """WITH * followed by aggregation in RETURN works correctly."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * RETURN count(p) AS n"
        )
        assert int(result["n"].iloc[0]) == 3

    def test_with_star_order_by(self, person_context: Context) -> None:
        """WITH * ORDER BY applies ordering to passed-through rows."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * ORDER BY p.age ASC RETURN p.name AS name"
        )
        assert result["name"].tolist() == ["Bob", "Alice", "Carol"]

    def test_with_star_limit(self, person_context: Context) -> None:
        """WITH * LIMIT caps the number of rows."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * ORDER BY p.name ASC LIMIT 2 RETURN p.name AS name"
        )
        assert len(result) == 2
        assert result["name"].tolist() == ["Alice", "Bob"]

    def test_with_star_chained(self, person_context: Context) -> None:
        """WITH * can appear in a chain: MATCH ... WITH * WITH ... RETURN."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH * WITH p.name AS name, p.dept AS dept RETURN name, dept"
        )
        assert len(result) == 3
        assert "name" in result.columns and "dept" in result.columns
