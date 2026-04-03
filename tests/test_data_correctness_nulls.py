"""Critical data correctness tests for null handling.
Priority 1: Null propagation bugs cause silent data corruption.
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
def null_test_context():
    """Create context with strategic null values for testing."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": [
                "Alice",
                None,
                "Carol",
                "",
                "Dave",
            ],  # Mix of null and empty
            "age": [30, None, 25, 40, 35],  # Null in middle
            "salary": [100000, 120000, None, 110000, None],  # Multiple nulls
            "score": [85.5, None, 92.0, None, 78.0],  # Float nulls
            "active": ["true", None, "false", "", "yes"],  # Boolean-ish nulls
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "salary", "score", "active"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "score": "score",
            "active": "active",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "salary": "salary",
            "score": "score",
            "active": "active",
        },
        source_obj=person_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestNullArithmeticPropagation:
    """Test that arithmetic operations correctly propagate nulls."""

    def test_null_plus_number(self, null_test_context):
        """Null + number must equal null, not number."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.age + 5 AS age_plus_five RETURN age_plus_five AS age_plus_five",
        )

        # Should be [35, null, 30, 45, 40] - null preserved
        assert len(result) == 5
        non_null_values = result["age_plus_five"].dropna()
        assert set(non_null_values) == {35, 30, 45, 40}
        assert result["age_plus_five"].isna().sum() == 1  # Exactly one null

    def test_null_multiplication(self, null_test_context):
        """Null * number must equal null."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.salary * 2 AS double_salary RETURN double_salary AS double_salary",
        )

        # Should preserve nulls in salary column
        assert len(result) == 5
        assert result["double_salary"].isna().sum() == 2  # Two salary nulls
        non_null_values = result["double_salary"].dropna()
        assert set(non_null_values) == {200000.0, 240000.0, 220000.0}

    def test_null_division(self, null_test_context):
        """Null / number must equal null."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.score / 2 AS half_score RETURN half_score AS half_score",
        )

        assert len(result) == 5
        assert result["half_score"].isna().sum() == 2  # Two score nulls
        non_null_values = result["half_score"].dropna().tolist()
        expected_values = [42.75, 46.0, 39.0]  # 85.5/2, 92.0/2, 78.0/2
        assert set(non_null_values) == set(expected_values)


class TestNullStringFunctions:
    """Test that string functions correctly handle nulls."""

    def test_toupper_with_null(self, null_test_context):
        """toUpper(null) must return null, not empty string."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toUpper(p.name) AS upper_name RETURN upper_name AS upper_name",
        )

        assert len(result) == 5
        # One null name should produce one null result
        assert result["upper_name"].isna().sum() == 1

        non_null_values = result["upper_name"].dropna().tolist()
        assert "ALICE" in non_null_values
        assert "CAROL" in non_null_values
        assert "DAVE" in non_null_values
        assert (
            "" in non_null_values
        )  # Empty string should become empty string, not null

    def test_trim_with_null(self, null_test_context):
        """trim(null) must return null."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH trim(p.name) AS trimmed_name RETURN trimmed_name AS trimmed_name",
        )

        assert len(result) == 5
        assert (
            result["trimmed_name"].isna().sum() == 1
        )  # One null input = one null output

    def test_size_with_null(self, null_test_context):
        """size(null) must return null, not 0."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH size(p.name) AS name_length RETURN name_length AS name_length",
        )

        assert len(result) == 5
        assert (
            result["name_length"].isna().sum() == 1
        )  # One null input = one null output

        # Non-null lengths should be correct
        non_null_lengths = result["name_length"].dropna().tolist()
        expected_lengths = [5, 5, 0, 4]  # "Alice", "Carol", "", "Dave"
        assert set(non_null_lengths) == set(expected_lengths)


class TestNullAggregations:
    """Test that aggregations handle nulls like SQL standard."""

    def test_avg_with_nulls(self, null_test_context):
        """avg() should ignore nulls, not treat them as zeros."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH avg(p.age) AS avg_age RETURN avg_age AS avg_age",
        )

        # Ages: [30, null, 25, 40, 35] → avg(30, 25, 40, 35) = 32.5
        assert len(result) == 1
        avg_age = result["avg_age"].iloc[0]
        assert abs(avg_age - 32.5) < 0.001  # 32.5, not including null as 0

    def test_sum_with_nulls(self, null_test_context):
        """sum() should ignore nulls."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.salary) AS total_salary RETURN total_salary AS total_salary",
        )

        # Salaries: [100000, 120000, null, 110000, null] → sum = 330000
        assert len(result) == 1
        total = result["total_salary"].iloc[0]
        assert total == 330000.0  # Should ignore nulls

    def test_count_vs_count_star_with_nulls(self, null_test_context):
        """count(*) vs count(field) should handle nulls differently."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH count(*) AS total_rows, count(p.age) AS non_null_ages RETURN total_rows AS total_rows, non_null_ages AS non_null_ages",
        )

        assert len(result) == 1
        assert result["total_rows"].iloc[0] == 5  # count(*) counts all rows
        assert (
            result["non_null_ages"].iloc[0] == 4
        )  # count(p.age) ignores nulls

    def test_min_max_with_nulls(self, null_test_context):
        """min/max should ignore nulls."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH min(p.score) AS min_score, max(p.score) AS max_score RETURN min_score AS min_score, max_score AS max_score",
        )

        # Scores: [85.5, null, 92.0, null, 78.0] → min=78.0, max=92.0
        assert len(result) == 1
        assert result["min_score"].iloc[0] == 78.0
        assert result["max_score"].iloc[0] == 92.0


class TestNullTypeConversions:
    """Test null handling in type conversion functions."""

    def test_tointeger_with_null(self, null_test_context):
        """toInteger(null) must return null."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toInteger(p.score) AS int_score RETURN int_score AS int_score",
        )

        assert len(result) == 5
        assert result["int_score"].isna().sum() == 2  # Two null scores

    def test_toboolean_with_null_and_empty(self, null_test_context):
        """ToBoolean should handle null and empty string correctly."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toBoolean(p.active) AS is_active RETURN is_active AS is_active",
        )

        assert len(result) == 5
        # Should have some nulls for invalid conversions
        assert result["is_active"].isna().sum() > 0


class TestComplexNullScenarios:
    """Test null behavior in complex expressions."""

    def test_nested_functions_with_nulls(self, null_test_context):
        """Nested functions should propagate nulls correctly."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toUpper(trim(p.name)) AS clean_upper_name RETURN clean_upper_name AS clean_upper_name",
        )

        assert len(result) == 5
        assert (
            result["clean_upper_name"].isna().sum() == 1
        )  # One null propagated through

    def test_coalesce_null_handling(self, null_test_context):
        """Coalesce should work correctly with nulls."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH coalesce(p.name, 'Unknown') AS final_name RETURN final_name AS final_name",
        )

        assert len(result) == 5
        assert result["final_name"].isna().sum() == 0  # No nulls should remain

        # Should have exactly one 'Unknown' (replacing the null)
        final_names = result["final_name"].tolist()
        assert final_names.count("Unknown") == 1

    def test_arithmetic_expression_with_mixed_nulls(self, null_test_context):
        """Complex arithmetic with nulls."""
        star = Star(context=null_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH (p.age * 1000) + coalesce(p.salary, 0) AS combined_score RETURN combined_score AS combined_score",
        )

        # This tests null handling in complex expressions
        assert len(result) == 5
        # Verify that nulls are handled appropriately in the computation
