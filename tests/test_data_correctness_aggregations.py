"""Critical data correctness tests for aggregation mathematical accuracy.
Aggregations are critical for ETL data quality and must be mathematically precise.
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
def aggregation_test_context():
    """Create context with specific data for testing aggregation accuracy."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5, 6, 7, 8],
            "name": [
                "Alice",
                "Bob",
                "Carol",
                "Dave",
                "Eve",
                "Frank",
                "Grace",
                "Henry",
            ],
            "age": [30, None, 25, 40, 35, None, 28, 32],  # Mix with nulls
            "salary": [
                100000,
                120000,
                None,
                110000,
                95000,
                105000,
                None,
                115000,
            ],  # Mix with nulls
            "score": [
                85.5,
                92.3,
                78.9,
                88.1,
                91.7,
                87.2,
                83.4,
                89.6,
            ],  # All valid floats
            "department": [
                "Eng",
                "Sales",
                "Eng",
                "Marketing",
                "Eng",
                "Sales",
                "Marketing",
                "Eng",
            ],
            "bonus": [
                5000,
                8000,
                3000,
                6000,
                4000,
                7000,
                5500,
                6500,
            ],  # All valid
            "years": [1, 5, 2, 10, 3, 7, 4, 8],  # All valid integers
            "rating": [
                4.2,
                3.8,
                4.5,
                4.1,
                3.9,
                4.3,
                4.0,
                4.4,
            ],  # Precise decimals
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[
            ID_COLUMN,
            "name",
            "age",
            "salary",
            "score",
            "department",
            "bonus",
            "years",
            "rating",
        ],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "score": "score",
            "department": "department",
            "bonus": "bonus",
            "years": "years",
            "rating": "rating",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "score": "score",
            "department": "department",
            "bonus": "bonus",
            "years": "years",
            "rating": "rating",
        },
        source_obj=person_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestBasicAggregationAccuracy:
    """Test mathematical accuracy of basic aggregation functions."""

    def test_sum_accuracy(self, aggregation_test_context):
        """Test that sum() produces mathematically correct results."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.bonus) AS total_bonus RETURN total_bonus AS total_bonus",
        )

        # Bonuses: [5000, 8000, 3000, 6000, 4000, 7000, 5500, 6500]
        # Sum: 45000
        total = result["total_bonus"].iloc[0]
        assert total == 45000

    def test_avg_accuracy_with_nulls(self, aggregation_test_context):
        """Test that avg() correctly ignores nulls and computes accurate average."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH avg(p.age) AS avg_age RETURN avg_age AS avg_age",
        )

        # Ages: [30, null, 25, 40, 35, null, 28, 32] -> [30, 25, 40, 35, 28, 32]
        # Average: (30 + 25 + 40 + 35 + 28 + 32) / 6 = 190 / 6 = 31.666...
        avg_age = result["avg_age"].iloc[0]
        assert abs(avg_age - 31.666666666666668) < 0.000001

    def test_count_vs_count_field_accuracy(self, aggregation_test_context):
        """Test count(*) vs count(field) with nulls."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH count(*) AS total_rows, count(p.age) AS non_null_ages, count(p.salary) AS non_null_salaries RETURN total_rows AS total_rows, non_null_ages AS non_null_ages, non_null_salaries AS non_null_salaries",
        )

        assert result["total_rows"].iloc[0] == 8  # All rows
        assert result["non_null_ages"].iloc[0] == 6  # 2 nulls in age
        assert result["non_null_salaries"].iloc[0] == 6  # 2 nulls in salary

    def test_min_max_accuracy(self, aggregation_test_context):
        """Test min/max ignore nulls and find correct extremes."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH min(p.score) AS min_score, max(p.score) AS max_score RETURN min_score AS min_score, max_score AS max_score",
        )

        # Scores: [85.5, 92.3, 78.9, 88.1, 91.7, 87.2, 83.4, 89.6]
        # Min: 78.9, Max: 92.3
        assert result["min_score"].iloc[0] == 78.9
        assert result["max_score"].iloc[0] == 92.3

    def test_collect_preserves_values(self, aggregation_test_context):
        """Test that collect() preserves all values including nulls."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH collect(p.years) AS all_years RETURN all_years AS all_years",
        )

        # Years: [1, 5, 2, 10, 3, 7, 4, 8] - no nulls, should collect all
        collected = result["all_years"].iloc[0]
        assert len(collected) == 8
        assert set(collected) == {1, 2, 3, 4, 5, 7, 8, 10}


class TestGroupedAggregationAccuracy:
    """Test grouped aggregations produce correct results per group."""

    def test_grouped_sum_by_department(self, aggregation_test_context):
        """Test sum() grouped by department."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.department AS dept, sum(p.bonus) AS dept_total_bonus RETURN dept AS dept, dept_total_bonus AS dept_total_bonus",
        )

        # Group bonuses by department:
        # Eng: Alice(5000) + Carol(3000) + Eve(4000) + Henry(6500) = 18500
        # Sales: Bob(8000) + Frank(7000) = 15000
        # Marketing: Dave(6000) + Grace(5500) = 11500

        dept_totals = {}
        for _, row in result.iterrows():
            dept_totals[row["dept"]] = row["dept_total_bonus"]

        assert dept_totals["Eng"] == 18500
        assert dept_totals["Sales"] == 15000
        assert dept_totals["Marketing"] == 11500

    def test_grouped_avg_accuracy(self, aggregation_test_context):
        """Test avg() grouped with null handling."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.department AS dept, avg(p.age) AS avg_age RETURN dept AS dept, avg_age AS avg_age",
        )

        # Ages by department (ignoring nulls):
        # Eng: [30, 25, 35, 32] -> avg = 30.5
        # Sales: [null, null] -> but Bob is null, Frank is null -> should handle gracefully
        # Marketing: [40, 28] -> avg = 34.0

        dept_avgs = {}
        for _, row in result.iterrows():
            if not pd.isna(row["avg_age"]):
                dept_avgs[row["dept"]] = row["avg_age"]

        assert abs(dept_avgs["Eng"] - 30.5) < 0.001
        assert abs(dept_avgs["Marketing"] - 34.0) < 0.001
        # Sales might have null avg if all ages are null

    def test_grouped_count_accuracy(self, aggregation_test_context):
        """Test count per group accuracy."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.department AS dept, count(*) AS people_count, count(p.salary) AS salary_count RETURN dept AS dept, people_count AS people_count, salary_count AS salary_count",
        )

        # People per department: Eng=4, Sales=2, Marketing=2
        # Non-null salaries: need to check data
        dept_counts = {}
        for _, row in result.iterrows():
            dept_counts[row["dept"]] = {
                "people": row["people_count"],
                "salaries": row["salary_count"],
            }

        assert dept_counts["Eng"]["people"] == 4
        assert dept_counts["Sales"]["people"] == 2
        assert dept_counts["Marketing"]["people"] == 2


class TestAggregationFloatingPointPrecision:
    """Test aggregations maintain floating point precision."""

    def test_sum_floating_point_precision(self, aggregation_test_context):
        """Test sum of floating point numbers maintains precision."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.rating) AS total_rating RETURN total_rating AS total_rating",
        )

        # Ratings: [4.2, 3.8, 4.5, 4.1, 3.9, 4.3, 4.0, 4.4]
        # Sum: 33.2
        total_rating = result["total_rating"].iloc[0]
        assert abs(total_rating - 33.2) < 0.000001

    def test_avg_floating_point_precision(self, aggregation_test_context):
        """Test average of floating point numbers."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH avg(p.rating) AS avg_rating RETURN avg_rating AS avg_rating",
        )

        # Average: 33.2 / 8 = 4.15
        avg_rating = result["avg_rating"].iloc[0]
        assert abs(avg_rating - 4.15) < 0.000001

    def test_decimal_precision_in_grouped_avg(self, aggregation_test_context):
        """Test decimal precision preserved in grouped averages."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.department AS dept, avg(p.rating) AS avg_rating RETURN dept AS dept, avg_rating AS avg_rating",
        )

        # Should maintain decimal precision for each group
        for _, row in result.iterrows():
            avg_rating = row["avg_rating"]
            if not pd.isna(avg_rating):
                assert isinstance(avg_rating, float)
                assert avg_rating > 0  # Sanity check


class TestAggregationEdgeCases:
    """Test edge cases in aggregation behavior."""

    def test_aggregation_on_all_nulls(self, aggregation_test_context):
        """Test aggregations when all values are null."""
        # Create a temporary context with all-null column
        person_df_nulls = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "all_null": [None, None, None],
            },
        )

        person_table_nulls = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "all_null"],
            source_obj_attribute_map={"all_null": "all_null"},
            attribute_map={"all_null": "all_null"},
            source_obj=person_df_nulls,
        )

        context_nulls = Context(
            entity_mapping=EntityMapping(
                mapping={"Person": person_table_nulls},
            ),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

        star = Star(context=context_nulls)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.all_null) AS sum_nulls, avg(p.all_null) AS avg_nulls, count(p.all_null) AS count_nulls RETURN sum_nulls AS sum_nulls, avg_nulls AS avg_nulls, count_nulls AS count_nulls",
        )

        # All aggregations on all-null column should return null (except count which should be 0)
        assert pd.isna(result["sum_nulls"].iloc[0])  # sum of nulls = null
        assert pd.isna(result["avg_nulls"].iloc[0])  # avg of nulls = null
        assert result["count_nulls"].iloc[0] == 0  # count of nulls = 0

    def test_empty_group_aggregation(self, aggregation_test_context):
        """Test aggregation on empty result set."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 1000 WITH count(*) AS impossible_count, sum(p.salary) AS impossible_sum RETURN impossible_count AS impossible_count, impossible_sum AS impossible_sum",
        )

        # Should return one row with count=0 and sum=null
        assert len(result) == 1
        assert result["impossible_count"].iloc[0] == 0
        assert pd.isna(result["impossible_sum"].iloc[0])

    def test_single_value_aggregations(self, aggregation_test_context):
        """Test aggregations on single-value groups."""
        star = Star(context=aggregation_test_context)

        # Filter to single person and aggregate
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' WITH sum(p.bonus) AS single_sum, avg(p.bonus) AS single_avg, count(*) AS single_count RETURN single_sum AS single_sum, single_avg AS single_avg, single_count AS single_count",
        )

        # Alice's bonus: 5000
        assert result["single_sum"].iloc[0] == 5000
        assert result["single_avg"].iloc[0] == 5000.0  # avg of single value = value
        assert result["single_count"].iloc[0] == 1


class TestComplexAggregationExpressions:
    """Test aggregations of computed expressions."""

    def test_aggregation_of_arithmetic_expression(
        self,
        aggregation_test_context,
    ):
        """Test aggregating computed values."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.bonus * 2) AS doubled_bonus_sum RETURN doubled_bonus_sum AS doubled_bonus_sum",
        )

        # Sum of (bonus * 2) = (sum of bonus) * 2 = 45000 * 2 = 90000
        doubled_sum = result["doubled_bonus_sum"].iloc[0]
        assert doubled_sum == 90000

    def test_avg_of_complex_expression(self, aggregation_test_context):
        """Test average of complex computed expression."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH avg(p.bonus / 1000.0) AS avg_bonus_k RETURN avg_bonus_k AS avg_bonus_k",
        )

        # Average of bonuses in thousands: 45000 / 8 / 1000 = 5.625
        avg_bonus_k = result["avg_bonus_k"].iloc[0]
        assert abs(avg_bonus_k - 5.625) < 0.000001

    def test_nested_aggregation_expressions(self, aggregation_test_context):
        """Test aggregations with function calls."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(toInteger(p.rating)) AS sum_rounded_ratings RETURN sum_rounded_ratings AS sum_rounded_ratings",
        )

        # toInteger on ratings: [4, 3, 4, 4, 3, 4, 4, 4] (truncated)
        # Sum: 30
        sum_rounded = result["sum_rounded_ratings"].iloc[0]
        assert sum_rounded == 30  # Sum of truncated integer ratings


class TestAggregationConsistency:
    """Test that aggregations are consistent and deterministic."""

    def test_aggregation_determinism(self, aggregation_test_context):
        """Test that same aggregation query gives same results."""
        star = Star(context=aggregation_test_context)

        query = "MATCH (p:Person) WITH avg(p.score) AS avg_score, sum(p.bonus) AS total_bonus RETURN avg_score AS avg_score, total_bonus AS total_bonus"

        result1 = star.execute_query(query)
        result2 = star.execute_query(query)

        # Results should be identical
        assert (
            abs(result1["avg_score"].iloc[0] - result2["avg_score"].iloc[0]) < 0.000001
        )
        assert result1["total_bonus"].iloc[0] == result2["total_bonus"].iloc[0]

    def test_mathematical_relationships(self, aggregation_test_context):
        """Test mathematical relationships between aggregations."""
        star = Star(context=aggregation_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.bonus) AS total, avg(p.bonus) AS average, count(*) AS count RETURN total AS total, average AS average, count AS count",
        )

        total = result["total"].iloc[0]
        average = result["average"].iloc[0]
        count = result["count"].iloc[0]

        # Mathematical relationship: sum = avg * count
        assert abs(total - (average * count)) < 0.000001
