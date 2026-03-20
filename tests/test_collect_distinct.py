"""Tests for collect(DISTINCT ...) aggregation.

The DISTINCT modifier is already handled in evaluate_aggregation(); this
confirms it works for collect(), sum(), and avg() as well.
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


class TestCollectDistinct:
    def test_collect_distinct_depts(self, dept_context: Context) -> None:
        """collect(DISTINCT p.dept) returns only unique departments."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN collect(DISTINCT p.dept) AS depts"
        )
        depts = result["depts"].iloc[0]
        assert sorted(depts) == ["eng", "hr"]

    def test_collect_distinct_scores(self, dept_context: Context) -> None:
        """collect(DISTINCT p.score) deduplicates 90."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN collect(DISTINCT p.score) AS scores"
        )
        scores = result["scores"].iloc[0]
        assert sorted(scores) == [80, 85, 90]


class TestSumAvgDistinct:
    def test_sum_distinct_scores(self, dept_context: Context) -> None:
        """sum(DISTINCT p.score): 80 + 85 + 90 = 255 (not 80+85+90+90+90=435)."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN sum(DISTINCT p.score) AS total"
        )
        assert float(result["total"].iloc[0]) == 255.0

    def test_avg_distinct_scores(self, dept_context: Context) -> None:
        """avg(DISTINCT p.score): (80+85+90)/3 = 85.0."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN avg(DISTINCT p.score) AS mean"
        )
        assert abs(float(result["mean"].iloc[0]) - 85.0) < 0.01
