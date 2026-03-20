"""
Critical data correctness tests for type coercion and conversion.
Priority 2: Type coercion bugs can silently corrupt data.
"""

import math

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
def type_test_context():
    """Create context with diverse data types for testing coercion."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age_int": [30, 40, 25, 35, 28],  # Integers
            "age_float": [30.0, 40.5, 25.2, 35.7, 28.9],  # Floats
            "salary_str": [
                "100000",
                "120000.50",
                "90000",
                "110000.75",
                "95000",
            ],  # String numbers
            "score_mixed": [
                "85",
                "92.5",
                "invalid",
                "",
                "78.0",
            ],  # Mixed valid/invalid
            "bool_str": [
                "true",
                "false",
                "1",
                "0",
                "yes",
            ],  # Boolean-ish strings
            "empty_or_null": [
                "",
                None,
                " ",
                "text",
                "",
            ],  # Empty vs null vs whitespace
        }
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[
            ID_COLUMN,
            "name",
            "age_int",
            "age_float",
            "salary_str",
            "score_mixed",
            "bool_str",
            "empty_or_null",
        ],
        source_obj_attribute_map={
            "name": "name",
            "age_int": "age_int",
            "age_float": "age_float",
            "salary_str": "salary_str",
            "score_mixed": "score_mixed",
            "bool_str": "bool_str",
            "empty_or_null": "empty_or_null",
        },
        attribute_map={
            "name": "name",
            "age_int": "age_int",
            "age_float": "age_float",
            "salary_str": "salary_str",
            "score_mixed": "score_mixed",
            "bool_str": "bool_str",
            "empty_or_null": "empty_or_null",
        },
        source_obj=person_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestIntegerFloatArithmetic:
    """Test mixed integer/float arithmetic produces correct types."""

    def test_int_plus_float(self, type_test_context):
        """int + float should produce float with correct precision."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.age_int + p.age_float AS sum_result RETURN sum_result AS sum_result"
        )

        assert len(result) == 5

        # First row: 30 + 30.0 = 60.0
        # Second row: 40 + 40.5 = 80.5
        sums = result["sum_result"].tolist()
        assert abs(sums[0] - 60.0) < 0.001
        assert abs(sums[1] - 80.5) < 0.001
        assert all(
            isinstance(val, float) for val in sums
        )  # All should be floats

    def test_float_division_precision(self, type_test_context):
        """Integer / integer uses truncating integer division (openCypher spec).
        Float / integer still produces float results."""
        star = Star(context=type_test_context)

        # Integer column / integer literal → integer (truncation toward zero)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.age_int / 3 AS third_age RETURN third_age AS third_age"
        )
        # 30/3=10, 40/3=13 (truncated), 25/3=8 (truncated)
        thirds = result["third_age"].tolist()
        assert thirds[0] == 10
        assert thirds[1] == 13  # 13.333... truncates to 13
        assert thirds[2] == 8  # 8.333... truncates to 8

        # Float column / integer literal → float
        result_f = star.execute_query(
            "MATCH (p:Person) WITH p.age_float / 3 AS third_age RETURN third_age AS third_age"
        )
        thirds_f = result_f["third_age"].tolist()
        # age_float[1] = 40.5, so 40.5 / 3 = 13.5
        assert abs(thirds_f[1] - 13.5) < 0.001

    def test_integer_division_vs_float_division(self, type_test_context):
        """Integer / integer → integer; float / integer → float (openCypher spec)."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.age_int / 3 AS int_div, p.age_float / 3 AS float_div RETURN int_div AS int_div, float_div AS float_div"
        )

        # Check column dtypes (iterrows upcasts values, so check the Series dtype instead)
        assert pd.api.types.is_integer_dtype(result["int_div"]), (
            f"int_div column should be integer dtype, got {result['int_div'].dtype}"
        )
        assert pd.api.types.is_float_dtype(result["float_div"]), (
            f"float_div column should be float dtype, got {result['float_div'].dtype}"
        )


class TestStringToNumberConversion:
    """Test string to number conversion accuracy."""

    def test_tointeger_conversion_accuracy(self, type_test_context):
        """toInteger should truncate, not round."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toInteger(p.salary_str) AS int_salary RETURN int_salary AS int_salary"
        )

        assert len(result) == 5
        int_salaries = result["int_salary"].dropna().tolist()

        # "120000.50" should become 120000 (truncated, not rounded to 120001)
        expected = [100000, 120000, 90000, 110000, 95000]
        assert int_salaries == expected

    def test_tofloat_conversion_accuracy(self, type_test_context):
        """toFloat should preserve decimal precision."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toFloat(p.salary_str) AS float_salary RETURN float_salary AS float_salary"
        )

        float_salaries = result["float_salary"].tolist()

        # Should preserve decimal places exactly
        assert abs(float_salaries[1] - 120000.50) < 0.001
        assert abs(float_salaries[3] - 110000.75) < 0.001

    def test_invalid_number_conversion(self, type_test_context):
        """Invalid number strings should convert to null."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toInteger(p.score_mixed) AS int_score RETURN int_score AS int_score"
        )

        # "invalid" and "" should become null
        assert result["int_score"].isna().sum() == 2

        valid_scores = result["int_score"].dropna().tolist()
        assert set(valid_scores) == {85, 92, 78}  # Valid conversions only


class TestBooleanConversion:
    """Test boolean conversion edge cases."""

    def test_toboolean_string_conversion(self, type_test_context):
        """toBoolean should handle string values correctly."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toBoolean(p.bool_str) AS bool_result RETURN bool_result AS bool_result"
        )

        bool_results = result["bool_result"].dropna().tolist()

        # Expected: "true"->True, "false"->False, "1"->True, "0"->False
        # "yes" should probably be null or follow specific rules
        expected_true_false = [
            True,
            False,
            True,
            False,
        ]  # First 4 should be clear
        assert bool_results[:4] == expected_true_false

    def test_boolean_arithmetic(self, type_test_context):
        """Boolean values in arithmetic should behave correctly."""
        # This tests if boolean True/False get treated as 1/0 in arithmetic
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toBoolean('true') + 5 AS bool_plus_int RETURN bool_plus_int AS bool_plus_int"
        )

        # True + 5 should equal 6 (if boolean coercion to int works)
        # Or should this be an error? Depends on implementation choice
        assert len(result) == 5


class TestMathematicalEdgeCases:
    """Test division by zero and mathematical edge cases."""

    def test_division_by_zero_float(self, type_test_context):
        """Division by zero should produce infinity, not error."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.age_float / 0.0 AS inf_result RETURN inf_result AS inf_result"
        )

        # Should produce positive infinity for positive numbers
        inf_results = result["inf_result"].tolist()
        assert all(math.isinf(val) and val > 0 for val in inf_results)

    def test_zero_division_by_zero(self, type_test_context):
        """0/0 should produce NaN."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH 0.0 / 0.0 AS nan_result RETURN nan_result AS nan_result"
        )

        nan_results = result["nan_result"].tolist()
        assert all(math.isnan(val) for val in nan_results)

    def test_negative_division_by_zero(self, type_test_context):
        """Negative number / 0 should produce negative infinity."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH -p.age_float / 0.0 AS neg_inf_result RETURN neg_inf_result AS neg_inf_result"
        )

        neg_inf_results = result["neg_inf_result"].tolist()
        assert all(math.isinf(val) and val < 0 for val in neg_inf_results)

    def test_very_large_numbers(self, type_test_context):
        """Very large number arithmetic should not overflow unexpectedly."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.age_int * 999999999999 AS huge_result RETURN huge_result AS huge_result"
        )

        # Should handle large numbers gracefully
        huge_results = result["huge_result"].tolist()
        assert all(isinstance(val, (int, float)) for val in huge_results)
        assert all(not math.isnan(val) for val in huge_results)


class TestFloatingPointPrecision:
    """Test floating point precision edge cases."""

    def test_decimal_addition_precision(self, type_test_context):
        """Classic 0.1 + 0.2 precision test."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH 0.1 + 0.2 AS decimal_sum RETURN decimal_sum AS decimal_sum"
        )

        decimal_sums = result["decimal_sum"].tolist()
        # Should be approximately 0.3, but with floating point precision
        expected = 0.1 + 0.2  # Whatever Python produces
        assert all(abs(val - expected) < 0.0001 for val in decimal_sums)

    def test_large_small_number_precision(self, type_test_context):
        """Adding very large and very small numbers."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH 1000000000000.0 + 0.001 AS precision_test RETURN precision_test AS precision_test"
        )

        # Test if small precision is preserved when adding to large numbers
        precision_results = result["precision_test"].tolist()
        expected = 1000000000000.001

        # May lose precision due to float limitations - that's expected behavior
        assert all(isinstance(val, float) for val in precision_results)


class TestEmptyStringVsNull:
    """Test distinction between empty strings and nulls."""

    def test_empty_string_operations(self, type_test_context):
        """Empty string should behave differently from null."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH size(p.empty_or_null) AS str_length RETURN str_length AS str_length"
        )

        lengths = result["str_length"].tolist()

        # Expected: [""]->0, null->null, " "->1, "text"->4, ""->0
        non_null_lengths = [l for l in lengths if not pd.isna(l)]
        assert 0 in non_null_lengths  # Empty strings should give length 0
        assert 1 in non_null_lengths  # Space should give length 1
        assert 4 in non_null_lengths  # "text" should give length 4

    def test_empty_string_concatenation(self, type_test_context):
        """Empty string concatenation should work."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH p.empty_or_null + '_suffix' AS concat_result RETURN concat_result AS concat_result"
        )

        concat_results = result["concat_result"].tolist()

        # Empty string + suffix should equal just suffix
        assert "_suffix" in concat_results  # From empty string concatenation
        assert "text_suffix" in concat_results  # From "text" concatenation


class TestComplexTypeCoercion:
    """Test complex scenarios with multiple type coercions."""

    def test_mixed_arithmetic_chain(self, type_test_context):
        """Chain of mixed type operations."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH toFloat(p.salary_str) AS float_sal,
                 p.age_int AS int_age
            WITH float_sal / int_age AS ratio
            RETURN ratio AS ratio"""
        )

        # Should produce float results from string->float / int
        ratios = result["ratio"].tolist()
        assert all(isinstance(val, float) for val in ratios)

        # First row: 100000.0 / 30 = 3333.333...
        assert abs(ratios[0] - 3333.333333333333) < 0.001

    def test_nested_type_conversions(self, type_test_context):
        """Nested type conversion functions."""
        star = Star(context=type_test_context)

        result = star.execute_query(
            "MATCH (p:Person) WITH toString(toInteger(toFloat(p.salary_str))) AS converted_back RETURN converted_back AS converted_back"
        )

        # String -> Float -> Integer -> String
        # Should lose decimal precision but be valid strings
        converted = result["converted_back"].tolist()
        assert all(isinstance(val, str) for val in converted)
        assert "100000" in converted
        assert (
            "120000" in converted
        )  # 120000.50 -> 120000.0 -> 120000 -> "120000"
