"""Tests for count(DISTINCT expr) aggregation.

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
def dept_context() -> Context:
    """5 people in 2 departments (some share dept)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "dept": ["eng", "eng", "hr", "hr", "eng"],
            "score": [90, 90, 80, 85, 90],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "dept", "score"],
        source_obj_attribute_map={
            "name": "name",
            "dept": "dept",
            "score": "score",
        },
        attribute_map={"name": "name", "dept": "dept", "score": "score"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestCountDistinct:
    def test_count_distinct_property(self, dept_context: Context) -> None:
        """count(DISTINCT p.dept) returns 2 (only 'eng' and 'hr' are unique)."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN count(DISTINCT p.dept) AS n"
        )
        assert result["n"].iloc[0] == 2

    def test_count_distinct_all_unique(self, dept_context: Context) -> None:
        """count(DISTINCT p.name) returns 5 (all names are unique)."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN count(DISTINCT p.name) AS n"
        )
        assert result["n"].iloc[0] == 5

    def test_count_distinct_score_duplicates(
        self, dept_context: Context
    ) -> None:
        """count(DISTINCT p.score) returns 3 (90, 80, 85) despite 3 people scoring 90."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN count(DISTINCT p.score) AS n"
        )
        assert result["n"].iloc[0] == 3

    def test_count_distinct_grouped(self, dept_context: Context) -> None:
        """count(DISTINCT p.score) grouped by dept."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores"
        )
        assert len(result) == 2
        eng_row = result[result["dept"] == "eng"]
        hr_row = result[result["dept"] == "hr"]
        # eng: Alice(90), Bob(90), Eve(90) — 1 unique score
        assert eng_row["unique_scores"].iloc[0] == 1
        # hr: Carol(80), Dave(85) — 2 unique scores
        assert hr_row["unique_scores"].iloc[0] == 2
