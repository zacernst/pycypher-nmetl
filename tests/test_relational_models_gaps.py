"""Tests for relational_models.py coverage gaps.

Covers:
- flatten() utility
- RelationIntersection.variables_in_common()
- SelectColumns.to_pandas()
- FilterRows error branches
- GroupedAggregation.to_pandas() single/multi column groupby
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from pycypher.ast_models import (
    CountStar,
    FunctionInvocation,
    PropertyLookup,
    Variable,
)
from pycypher.relational_models import (
    ID_COLUMN,
    AttributeEqualsValue,
    Context,
    EntityMapping,
    EntityTable,
    FilterRows,
    GroupedAggregation,
    Projection,
    RelationIntersection,
    SelectColumns,
    flatten,
)


# ---------------------------------------------------------------------------
# flatten()
# ---------------------------------------------------------------------------


class TestFlatten:
    """Tests for the flatten() utility."""

    def test_flat_list(self) -> None:
        assert flatten([1, 2, 3]) == [1, 2, 3]

    def test_nested_list(self) -> None:
        assert flatten([1, [2, 3], [4, [5, 6]]]) == [1, 2, 3, 4, 5, 6]

    def test_empty_list(self) -> None:
        assert flatten([]) == []

    def test_deeply_nested(self) -> None:
        assert flatten([[[["a"]]]]) == ["a"]


# ---------------------------------------------------------------------------
# RelationIntersection.variables_in_common()
# ---------------------------------------------------------------------------


class TestRelationIntersection:
    """Tests for RelationIntersection.variables_in_common()."""

    @staticmethod
    def _make_table(
        name: str, var_names: list[str], data: pd.DataFrame
    ) -> Projection:
        table = EntityTable(
            entity_type=name,
            identifier=name,
            column_names=[ID_COLUMN],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=data,
        )
        proj = Projection(
            relation=table,
            projected_column_names={
                f"{name}__{ID_COLUMN}": f"{name}__{ID_COLUMN}",
            },
        )
        proj.variable_map = {
            Variable(name=v): f"{name}__{ID_COLUMN}" for v in var_names
        }
        return proj

    def test_empty_relation_list(self) -> None:
        """Empty list → empty set."""
        ri = RelationIntersection(relation_list=[])
        assert ri.variables_in_common() == set()

    def test_single_relation(self) -> None:
        """Single relation → its own variables."""
        data = pd.DataFrame({ID_COLUMN: [1]})
        rel = self._make_table("A", ["a", "b"], data)
        ri = RelationIntersection(relation_list=[rel])
        result = ri.variables_in_common()
        assert {v.name for v in result} == {"a", "b"}

    def test_intersection_of_two(self) -> None:
        """Two relations sharing one variable."""
        data = pd.DataFrame({ID_COLUMN: [1]})
        r1 = self._make_table("A", ["a", "b"], data)
        r2 = self._make_table("B", ["b", "c"], data)
        ri = RelationIntersection(relation_list=[r1, r2])
        result = ri.variables_in_common()
        assert {v.name for v in result} == {"b"}


# ---------------------------------------------------------------------------
# SelectColumns.to_pandas()
# ---------------------------------------------------------------------------


class TestSelectColumns:
    """Tests for SelectColumns.to_pandas()."""

    def test_projects_subset(self) -> None:
        """Only selected columns appear in result."""
        data = pd.DataFrame(
            {ID_COLUMN: [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]}
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age"],
            source_obj_attribute_map={"name": "name", "age": "age"},
            attribute_map={"name": "name", "age": "age"},
            source_obj=data,
        )
        ctx = Context(entity_mapping=EntityMapping(mapping={"Person": table}))

        sc = SelectColumns(
            relation=table,
            column_names=[f"Person__{ID_COLUMN}", "Person__name"],
        )
        result = sc.to_pandas(context=ctx)
        assert list(result.columns) == [
            f"Person__{ID_COLUMN}",
            "Person__name",
        ]
        assert len(result) == 2


# ---------------------------------------------------------------------------
# FilterRows error branches
# ---------------------------------------------------------------------------


class TestFilterRowsErrors:
    """Cover error branches in FilterRows.to_pandas()."""

    def test_unsupported_condition_rejected_by_pydantic(self) -> None:
        """A non-BooleanCondition object is rejected by Pydantic validation."""
        data = pd.DataFrame({ID_COLUMN: [1, 2]})
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=data,
        )

        class FakeCondition:
            pass

        with pytest.raises(Exception):  # Pydantic ValidationError
            FilterRows(
                relation=table,
                condition=FakeCondition(),  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# GroupedAggregation.to_pandas()
# ---------------------------------------------------------------------------


class TestGroupedAggregation:
    """Cover GroupedAggregation single/multi column and error paths."""

    @staticmethod
    def _setup() -> tuple[Context, EntityTable, pd.DataFrame]:
        data = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "city": ["NY", "NY", "LA", "LA"],
                "dept": ["eng", "hr", "eng", "hr"],
                "salary": [100, 200, 150, 250],
            }
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "city", "dept", "salary"],
            source_obj_attribute_map={
                "city": "city",
                "dept": "dept",
                "salary": "salary",
            },
            attribute_map={
                "city": "city",
                "dept": "dept",
                "salary": "salary",
            },
            source_obj=data,
        )
        # Build a Projection so variable_map and variable_type_map are set
        proj = Projection(
            relation=table,
            projected_column_names={
                f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}",
            },
        )
        proj.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
        proj.variable_type_map = {Variable(name="p"): "Person"}

        ctx = Context(entity_mapping=EntityMapping(mapping={"Person": table}))
        return ctx, proj, data

    def test_single_grouping_column(self) -> None:
        """Group by one column with COUNT(*)."""
        ctx, proj, _ = self._setup()
        ga = GroupedAggregation(
            relation=proj,
            grouping_expressions={
                "city": PropertyLookup(
                    expression=Variable(name="p"), property="city"
                ),
            },
            aggregations={"cnt": CountStar()},
        )
        result = ga.to_pandas(context=ctx)
        assert len(result) == 2
        assert set(result["city"]) == {"NY", "LA"}
        assert list(result["cnt"].sort_values()) == [2, 2]

    def test_multiple_grouping_columns(self) -> None:
        """Group by two columns with COUNT(*)."""
        ctx, proj, _ = self._setup()
        ga = GroupedAggregation(
            relation=proj,
            grouping_expressions={
                "city": PropertyLookup(
                    expression=Variable(name="p"), property="city"
                ),
                "dept": PropertyLookup(
                    expression=Variable(name="p"), property="dept"
                ),
            },
            aggregations={"cnt": CountStar()},
        )
        result = ga.to_pandas(context=ctx)
        # 4 unique (city, dept) combinations
        assert len(result) == 4
        assert "city" in result.columns
        assert "dept" in result.columns
        assert "cnt" in result.columns

    def test_no_grouping_columns_raises(self) -> None:
        """Empty grouping expressions raises ValueError."""
        ctx, proj, _ = self._setup()
        ga = GroupedAggregation(
            relation=proj,
            grouping_expressions={},
            aggregations={"cnt": CountStar()},
        )
        with pytest.raises(ValueError, match="at least one grouping"):
            ga.to_pandas(context=ctx)

    def test_aggregation_with_sum(self) -> None:
        """Group-level sum aggregation."""
        ctx, proj, _ = self._setup()
        ga = GroupedAggregation(
            relation=proj,
            grouping_expressions={
                "city": PropertyLookup(
                    expression=Variable(name="p"), property="city"
                ),
            },
            aggregations={
                "total_salary": FunctionInvocation(
                    name="sum",
                    arguments={
                        "arguments": [
                            PropertyLookup(
                                expression=Variable(name="p"), property="salary"
                            ),
                        ]
                    },
                ),
            },
        )
        result = ga.to_pandas(context=ctx)
        totals = dict(zip(result["city"], result["total_salary"]))
        assert totals["NY"] == 300.0  # 100 + 200
        assert totals["LA"] == 400.0  # 150 + 250
