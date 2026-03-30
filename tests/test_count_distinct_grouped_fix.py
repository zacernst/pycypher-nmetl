"""TDD test for Testing Loop 282 - COUNT DISTINCT Grouped Aggregation Fix.

This module validates the fix for the COUNT DISTINCT grouped aggregation bug where
count(DISTINCT p.score) grouped by department was returning global count instead
of per-group distinct count.

Bug: AggregationExpressionEvaluator.evaluate_aggregation_grouped had special COUNT
handling that bypassed the general DISTINCT modifier logic, causing COUNT DISTINCT
to ignore grouping and return global distinct counts.

Fix: Modified COUNT handling in evaluate_aggregation_grouped to check for DISTINCT
modifier and implement proper per-group distinct counting logic.

Run with:
    uv run pytest tests/test_testing_loop_282_count_distinct_grouped_fix_tdd.py -v
"""

import pandas as pd
import pytest
from pycypher import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


class TestCountDistinctGroupedFix:
    """Test the COUNT DISTINCT grouped aggregation fix."""

    @pytest.fixture
    def count_distinct_context(self) -> Context:
        """Create test context with data designed to expose COUNT DISTINCT bug."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
                "dept": ["eng", "eng", "hr", "hr", "eng"],
                "score": [
                    90,
                    90,
                    80,
                    85,
                    90,
                ],  # eng has 1 unique (90), hr has 2 unique (80,85)
            },
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
            relationship_mapping={},
        )

    def test_count_distinct_grouped_per_department_correctness(
        self,
        count_distinct_context: Context,
    ) -> None:
        """COUNT DISTINCT grouped by department returns per-group distinct counts."""
        star = Star(context=count_distinct_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores",
        )

        # Should return 2 rows: one per department
        assert len(result) == 2

        # Engineering: Alice(90), Bob(90), Eve(90) -> 1 unique score
        eng_row = result[result["dept"] == "eng"]
        assert not eng_row.empty, "Should have engineering department row"
        assert eng_row["unique_scores"].iloc[0] == 1, (
            "Eng should have 1 unique score (90)"
        )

        # HR: Carol(80), Dave(85) -> 2 unique scores
        hr_row = result[result["dept"] == "hr"]
        assert not hr_row.empty, "Should have HR department row"
        assert hr_row["unique_scores"].iloc[0] == 2, (
            "HR should have 2 unique scores (80, 85)"
        )

    def test_count_distinct_global_vs_grouped_isolation(
        self,
        count_distinct_context: Context,
    ) -> None:
        """Global and grouped COUNT DISTINCT should return different results."""
        star = Star(context=count_distinct_context)

        # Global COUNT DISTINCT should count all unique values across all groups
        global_result = star.execute_query(
            "MATCH (p:Person) RETURN count(DISTINCT p.score) AS n",
        )
        assert global_result["n"].iloc[0] == 3, (
            "Global should see 3 unique scores (90, 80, 85)"
        )

        # Grouped COUNT DISTINCT should count per-group
        grouped_result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores",
        )

        # Sum of grouped counts may equal global count when data aligns that way
        # but the important thing is that per-group logic is different
        grouped_sum = grouped_result["unique_scores"].sum()
        global_count = global_result["n"].iloc[0]

        # In this specific case: eng(1) + hr(2) = 3, global = 3, so they're equal
        # But that's mathematically correct - the bug was eng returning 3 instead of 1

        # The key test: eng department should have 1 unique score, not 3
        eng_unique = grouped_result[grouped_result["dept"] == "eng"][
            "unique_scores"
        ].iloc[0]
        assert eng_unique == 1, "Eng should have 1 unique score (not global count of 3)"

        # Verify the logic is working correctly
        assert grouped_sum == 3, "Grouped sum should be 1+2=3"
        assert global_count == 3, "Global count should be 3 unique values (90,80,85)"

    def test_count_distinct_with_no_duplicates_within_groups(self) -> None:
        """COUNT DISTINCT grouped when each group has no internal duplicates."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "dept": ["a", "a", "b", "b"],
                "score": [10, 20, 30, 40],  # No duplicates within groups
            },
        )

        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "dept", "score"],
            source_obj_attribute_map={"dept": "dept", "score": "score"},
            attribute_map={"dept": "dept", "score": "score"},
            source_obj=df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping={},
        )

        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores",
        )

        # When no duplicates within groups, COUNT DISTINCT should equal COUNT
        regular_count_result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(p.score) AS regular_count",
        )

        # Both departments should have 2 items each
        assert len(result) == 2
        for _, row in result.iterrows():
            assert row["unique_scores"] == 2, (
                f"Dept {row['dept']} should have 2 unique scores"
            )

        # Should match regular count when no duplicates
        result_sorted = result.sort_values("dept").reset_index(drop=True)
        regular_sorted = regular_count_result.sort_values("dept").reset_index(
            drop=True,
        )

        assert (
            result_sorted["unique_scores"] == regular_sorted["regular_count"]
        ).all(), "COUNT DISTINCT should equal COUNT when no duplicates within groups"

    def test_count_distinct_with_all_duplicates_within_group(self) -> None:
        """COUNT DISTINCT grouped when a group has all duplicate values."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "dept": ["same", "same", "diff", "diff"],
                "score": [
                    100,
                    100,
                    200,
                    300,
                ],  # 'same' dept has all duplicates, 'diff' has unique
            },
        )

        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "dept", "score"],
            source_obj_attribute_map={"dept": "dept", "score": "score"},
            attribute_map={"dept": "dept", "score": "score"},
            source_obj=df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping={},
        )

        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores ORDER BY p.dept",
        )

        assert len(result) == 2

        # 'diff' department has 2 unique scores (200, 300)
        diff_row = result[result["dept"] == "diff"]
        assert diff_row["unique_scores"].iloc[0] == 2, (
            "Diff dept should have 2 unique scores"
        )

        # 'same' department has 1 unique score (100) despite having 2 rows
        same_row = result[result["dept"] == "same"]
        assert same_row["unique_scores"].iloc[0] == 1, (
            "Same dept should have 1 unique score"
        )

    def test_count_distinct_multiple_grouping_columns(self) -> None:
        """COUNT DISTINCT with multiple grouping columns."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5, 6],
                "dept": ["eng", "eng", "eng", "hr", "hr", "hr"],
                "level": [
                    "junior",
                    "junior",
                    "senior",
                    "junior",
                    "senior",
                    "senior",
                ],
                "score": [
                    90,
                    90,
                    95,
                    80,
                    85,
                    85,
                ],  # Different distinct counts per group combo
            },
        )

        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "dept", "level", "score"],
            source_obj_attribute_map={
                "dept": "dept",
                "level": "level",
                "score": "score",
            },
            attribute_map={"dept": "dept", "level": "level", "score": "score"},
            source_obj=df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping={},
        )

        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, p.level AS level, count(DISTINCT p.score) AS unique_scores",
        )

        # Should have 4 groups: (eng,junior), (eng,senior), (hr,junior), (hr,senior)
        assert len(result) == 4

        # Check specific group expectations:
        # eng+junior: [90, 90] -> 1 unique
        eng_junior = result[(result["dept"] == "eng") & (result["level"] == "junior")]
        assert eng_junior["unique_scores"].iloc[0] == 1, (
            "Eng+Junior should have 1 unique score"
        )

        # hr+senior: [85, 85] -> 1 unique
        hr_senior = result[(result["dept"] == "hr") & (result["level"] == "senior")]
        assert hr_senior["unique_scores"].iloc[0] == 1, (
            "HR+Senior should have 1 unique score"
        )

        # eng+senior: [95] -> 1 unique
        eng_senior = result[(result["dept"] == "eng") & (result["level"] == "senior")]
        assert eng_senior["unique_scores"].iloc[0] == 1, (
            "Eng+Senior should have 1 unique score"
        )

        # hr+junior: [80] -> 1 unique
        hr_junior = result[(result["dept"] == "hr") & (result["level"] == "junior")]
        assert hr_junior["unique_scores"].iloc[0] == 1, (
            "HR+Junior should have 1 unique score"
        )


class TestCountDistinctRegressionPrevention:
    """Test to prevent regression of the COUNT DISTINCT grouped bug."""

    def test_bug_reproduction_attempt(self) -> None:
        """Attempt to reproduce the original bug (should now be fixed)."""
        # This is the exact scenario that was failing before the fix
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
                "dept": ["eng", "eng", "hr", "hr", "eng"],
                "score": [90, 90, 80, 85, 90],
            },
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

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping={},
        )

        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores",
        )

        eng_row = result[result["dept"] == "eng"]
        hr_row = result[result["dept"] == "hr"]

        # Before fix: eng_row["unique_scores"] was 3 (global count)
        # After fix: eng_row["unique_scores"] should be 1 (per-group count)
        assert eng_row["unique_scores"].iloc[0] == 1, (
            "REGRESSION: eng should have 1 unique score, not global count"
        )

        assert hr_row["unique_scores"].iloc[0] == 2, (
            "REGRESSION: hr should have 2 unique scores"
        )

    def test_original_test_cases_still_pass(self) -> None:
        """Verify that the originally failing test cases now pass."""
        # This replicates the data from test_count_distinct.py::test_count_distinct_grouped
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
                "dept": ["eng", "eng", "hr", "hr", "eng"],
                "score": [90, 90, 80, 85, 90],
            },
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

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping={},
        )

        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(DISTINCT p.score) AS unique_scores",
        )

        assert len(result) == 2

        eng_row = result[result["dept"] == "eng"]
        hr_row = result[result["dept"] == "hr"]

        # These are the exact assertions from the original failing tests
        assert eng_row["unique_scores"].iloc[0] == 1, (
            "eng: Alice(90), Bob(90), Eve(90) — 1 unique score"
        )
        assert hr_row["unique_scores"].iloc[0] == 2, (
            "hr: Carol(80), Dave(85) — 2 unique scores"
        )
