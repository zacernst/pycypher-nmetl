"""
Critical data correctness tests for complex expressions and mathematical edge cases.
Priority 3: Complex expression evaluation and mathematical accuracy.
"""

import pandas as pd
import pytest
import numpy as np
import math

from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    RelationshipMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture
def expression_test_context():
    """Create context for testing complex expressions."""
    person_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5],
        "name": ["Alice Smith", "  Bob  ", "CAROL", "", "dave@company.com"],
        "age": [30, 40, 25, 35, 28],
        "salary": [100000, 120000, 90000, 110000, 95000],
        "bonus": [5000, 8000, 3000, 6000, 4000],
        "score": [85.5, 92.3, 78.9, 88.1, 91.7],
        "active": [True, False, True, True, False],
        "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales"],
        "years": [5, 10, 2, 7, 4],
        "rating": [4.2, 3.8, 4.5, 4.1, 3.9],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "salary", "bonus", "score", "active", "department", "years", "rating"],
        source_obj_attribute_map={
            "name": "name", "age": "age", "salary": "salary", "bonus": "bonus",
            "score": "score", "active": "active", "department": "department",
            "years": "years", "rating": "rating"
        },
        attribute_map={
            "name": "name", "age": "age", "salary": "salary", "bonus": "bonus",
            "score": "score", "active": "active", "department": "department",
            "years": "years", "rating": "rating"
        },
        source_obj=person_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestComplexArithmeticExpressions:
    """Test complex nested arithmetic expressions."""

    def test_nested_arithmetic_precedence(self, expression_test_context):
        """Test operator precedence in nested expressions."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH (p.salary + p.bonus) * p.years / 100 AS complex_calc RETURN complex_calc AS complex_calc"
        )

        # Should be: (salary + bonus) * years / 100
        # First row: (100000 + 5000) * 5 / 100 = 5250.0
        calc_results = result["complex_calc"].tolist()
        assert abs(calc_results[0] - 5250.0) < 0.001

        # Second row: (120000 + 8000) * 10 / 100 = 12800.0
        assert abs(calc_results[1] - 12800.0) < 0.001

    def test_deeply_nested_expressions(self, expression_test_context):
        """Test very deep nesting of arithmetic operations."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH ((p.age * 2) + (p.years * 3)) - ((p.rating * 10) / 2) AS deep_calc RETURN deep_calc AS deep_calc"
        )

        # First row: ((30 * 2) + (5 * 3)) - ((4.2 * 10) / 2) = (60 + 15) - (42 / 2) = 75 - 21 = 54
        deep_results = result["deep_calc"].tolist()
        assert abs(deep_results[0] - 54.0) < 0.001

    def test_mixed_operations_with_parentheses(self, expression_test_context):
        """Test that parentheses correctly override precedence."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.age + p.years * 2 AS without_parens, (p.age + p.years) * 2 AS with_parens RETURN without_parens AS without_parens, with_parens AS with_parens"
        )

        # First row: age=30, years=5
        # without_parens: 30 + (5 * 2) = 40
        # with_parens: (30 + 5) * 2 = 70
        assert abs(result["without_parens"].iloc[0] - 40) < 0.001
        assert abs(result["with_parens"].iloc[0] - 70) < 0.001


class TestComplexStringExpressions:
    """Test complex string manipulation expressions."""

    def test_nested_string_functions(self, expression_test_context):
        """Test nested string function calls."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toUpper(trim(p.name)) AS clean_upper RETURN clean_upper AS clean_upper"
        )

        clean_names = result["clean_upper"].tolist()

        # Should process: "  Bob  " -> "Bob" -> "BOB"
        assert "BOB" in clean_names
        assert "ALICE SMITH" in clean_names
        assert "CAROL" in clean_names
        assert "" in clean_names  # Empty string stays empty

    def test_string_function_composition(self, expression_test_context):
        """Test complex string function combinations."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH substring(toLower(p.name), 0, 3) AS first_three_lower RETURN first_three_lower AS first_three_lower"
        )

        # Should process: "Alice Smith" -> "alice smith" -> "ali"
        first_three = result["first_three_lower"].tolist()
        assert "ali" in first_three  # From "Alice Smith"
        assert "  b" in first_three  # From "  Bob  " -> "  bob  " -> "  b" (first 3 chars including spaces)
        assert "car" in first_three  # From "CAROL"

    def test_string_arithmetic_combination(self, expression_test_context):
        """Test combining string functions with arithmetic."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH size(p.name) + p.age AS name_age_sum RETURN name_age_sum AS name_age_sum"
        )

        # First row: size("Alice Smith") + 30 = 11 + 30 = 41
        sums = result["name_age_sum"].tolist()
        assert 41 in sums


class TestBooleanLogicExpressions:
    """Test complex boolean logic and conditional expressions."""

    @pytest.mark.skip(reason="WHERE clause filtering not implemented yet (Phase 4)")
    def test_and_or_precedence(self, expression_test_context):
        """Test AND/OR precedence in boolean expressions."""
        star = Star(context=expression_test_context)

        # Test if AND has higher precedence than OR
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 25 AND p.salary > 100000 OR p.department = 'Sales' RETURN p.name AS name"
        )

        # Should be: (age > 25 AND salary > 100000) OR (department = 'Sales')
        # This tests boolean logic evaluation accuracy
        names = result["name"].tolist()
        assert len(names) > 0  # Should match some people

    @pytest.mark.skip(reason="Comparison expressions not implemented yet")
    def test_boolean_with_arithmetic(self, expression_test_context):
        """Test boolean results used in arithmetic context."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH (p.age > 30) AS is_senior RETURN is_senior AS is_senior"
        )

        senior_flags = result["is_senior"].tolist()
        assert True in senior_flags
        assert False in senior_flags
        assert all(isinstance(flag, bool) for flag in senior_flags)


class TestCoalesceAndNullHandling:
    """Test coalesce and complex null handling scenarios."""

    def test_coalesce_with_expressions(self, expression_test_context):
        """Test coalesce with computed expressions."""
        star = Star(context=expression_test_context)

        # Add some null data for testing
        result = star.execute_query(
            "MATCH (p:Person) WITH coalesce(p.bonus / 1000, p.rating, 0) AS fallback_score RETURN fallback_score AS fallback_score"
        )

        # Should use bonus/1000 for most cases since bonus is not null
        # First row: 5000/1000 = 5.0
        fallback_scores = result["fallback_score"].tolist()
        assert 5.0 in fallback_scores

    @pytest.mark.skip(reason="Null literals not implemented yet")
    def test_nested_coalesce(self, expression_test_context):
        """Test nested coalesce expressions."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH coalesce(null, coalesce(null, p.rating, 1.0), 0.0) AS nested_fallback RETURN nested_fallback AS nested_fallback"
        )

        # Should resolve to p.rating values since inner coalesce finds rating first
        nested_results = result["nested_fallback"].tolist()
        assert 4.2 in nested_results
        assert 3.8 in nested_results


class TestAggregationComplexity:
    """Test complex aggregation scenarios."""

    def test_aggregation_with_expressions(self, expression_test_context):
        """Test aggregations of computed expressions."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH avg(p.salary + p.bonus) AS avg_total_comp RETURN avg_total_comp AS avg_total_comp"
        )

        # Should compute total comp for each person, then average
        # Totals: 105000, 128000, 93000, 116000, 99000
        # Average: (105000 + 128000 + 93000 + 116000 + 99000) / 5 = 108200
        avg_comp = result["avg_total_comp"].iloc[0]
        assert abs(avg_comp - 108200.0) < 0.001

    @pytest.mark.skip(reason="CASE expressions not implemented yet")
    def test_conditional_aggregation(self, expression_test_context):
        """Test aggregations with conditional logic."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH sum(CASE WHEN p.department = 'Engineering' THEN p.salary ELSE 0 END) AS eng_total_salary RETURN eng_total_salary AS eng_total_salary"
        )

        # This tests CASE expressions if supported
        # Engineering salaries: 100000 + 90000 = 190000
        # Note: CASE might not be implemented yet, so this could fail expectedly


class TestExpressionEdgeCases:
    """Test edge cases in expression evaluation."""

    def test_expression_with_empty_string(self, expression_test_context):
        """Test expressions involving empty strings."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH size(p.name) * 2 AS double_name_length RETURN double_name_length AS double_name_length"
        )

        # Empty string should give size 0, so 0 * 2 = 0
        double_lengths = result["double_name_length"].tolist()
        assert 0 in double_lengths  # From empty string

    def test_very_long_expression_chain(self, expression_test_context):
        """Test very long expression chains."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.age AS age,
                 p.salary AS salary,
                 p.bonus AS bonus,
                 p.years AS years
            WITH age + salary / 1000 + bonus / 1000 + years * 10 AS mega_score
            WITH mega_score * 1.5 AS final_score
            RETURN final_score AS final_score"""
        )

        # Test that complex multi-stage WITH clauses work correctly
        final_scores = result["final_score"].tolist()
        assert len(final_scores) == 5
        assert all(isinstance(score, (int, float)) for score in final_scores)

        # First row calculation:
        # age=30, salary=100000, bonus=5000, years=5
        # mega_score = 30 + 100 + 5 + 50 = 185
        # final_score = 185 * 1.5 = 277.5
        assert abs(final_scores[0] - 277.5) < 0.001


class TestDataIntegrityThroughTransformations:
    """Test that data maintains integrity through complex transformations."""

    def test_data_consistency_multi_stage(self, expression_test_context):
        """Test data consistency through multiple WITH stages."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.name AS original_name, toUpper(p.name) AS upper_name
            WITH original_name AS orig, toLower(upper_name) AS round_trip
            RETURN orig AS orig, round_trip AS round_trip"""
        )

        # Round-trip: name -> toUpper -> toLower should preserve case changes
        for _, row in result.iterrows():
            orig = row["orig"]
            round_trip = row["round_trip"]

            if orig:  # Skip empty strings
                assert orig.lower() == round_trip

    @pytest.mark.skip(reason="Multi-stage WITH clauses variable scoping not implemented yet")
    def test_mathematical_consistency(self, expression_test_context):
        """Test mathematical operations maintain consistency."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.age AS age, p.salary AS salary
            WITH age * 1000 AS age_thousands, salary / 1000 AS salary_thousands
            WITH age_thousands / 1000 AS age_restored, salary_thousands * 1000 AS salary_restored
            RETURN age AS original_age, age_restored AS age_restored, salary AS original_salary, salary_restored AS salary_restored"""
        )

        # Mathematical round-trip should preserve values
        for _, row in result.iterrows():
            assert abs(row["original_age"] - row["age_restored"]) < 0.001
            assert abs(row["original_salary"] - row["salary_restored"]) < 0.001

    def test_type_preservation_through_pipeline(self, expression_test_context):
        """Test that types are preserved correctly through processing pipeline."""
        star = Star(context=expression_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH toString(p.age) AS age_str, toFloat(p.rating) AS rating_float
            WITH toInteger(age_str) AS age_back, rating_float * 1.0 AS rating_preserved
            RETURN age_back AS age_back, rating_preserved AS rating_preserved"""
        )

        # Type conversions should work correctly
        for _, row in result.iterrows():
            assert isinstance(row["age_back"], (int, np.integer))
            assert isinstance(row["rating_preserved"], float)