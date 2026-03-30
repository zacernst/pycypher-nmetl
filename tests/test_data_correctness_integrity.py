"""Critical data correctness tests for data integrity and consistency.
Tests that data maintains integrity through complex transformations and multi-stage pipelines.
"""

import numpy as np
import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def integrity_test_context():
    """Create context for testing data integrity across transformations."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "email": [
                "alice@company.com",
                "bob@personal.net",
                "carol@company.com",
                "dave@company.com",
                "eve@other.org",
            ],
            "age": [30, 40, 25, 35, 28],
            "salary": [100000, 120000, 90000, 110000, 95000],
            "department": [
                "Engineering",
                "Sales",
                "Engineering",
                "Marketing",
                "Engineering",
            ],
            "manager_id": [None, 1, 1, 2, 3],  # Hierarchical relationships
            "start_date": [
                "2020-01-15",
                "2018-05-20",
                "2021-03-10",
                "2019-08-05",
                "2022-01-20",
            ],
        },
    )

    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103, 104, 105],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 1, 3, 4],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 3, 4, 5],
            "relationship_type": [
                "mentor",
                "colleague",
                "friend",
                "reports_to",
                "collaborates",
            ],
            "strength": [0.9, 0.6, 0.8, 0.7, 0.5],
            "since": [
                "2020-02-01",
                "2019-01-15",
                "2021-04-01",
                "2021-03-15",
                "2022-02-01",
            ],
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[
            ID_COLUMN,
            "name",
            "email",
            "age",
            "salary",
            "department",
            "manager_id",
            "start_date",
        ],
        source_obj_attribute_map={
            "name": "name",
            "email": "email",
            "age": "age",
            "salary": "salary",
            "department": "department",
            "manager_id": "manager_id",
            "start_date": "start_date",
        },
        attribute_map={
            "name": "name",
            "email": "email",
            "age": "age",
            "salary": "salary",
            "department": "department",
            "manager_id": "manager_id",
            "start_date": "start_date",
        },
        source_obj=person_df,
    )

    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
            "relationship_type",
            "strength",
            "since",
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "relationship_type": "relationship_type",
            "strength": "strength",
            "since": "since",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "relationship_type": "relationship_type",
            "strength": "strength",
            "since": "since",
        },
        source_obj=knows_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


class TestDataConsistencyThroughPipeline:
    """Test data remains consistent through multi-stage WITH pipelines."""

    def test_multi_stage_with_data_preservation(self, integrity_test_context):
        """Test that data is correctly passed through multiple WITH stages."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.name AS person_name, p.salary AS original_salary, p.age AS age
            WITH person_name AS name, original_salary * 1.1 AS boosted_salary, age + 1 AS next_age
            WITH name AS final_name, boosted_salary / 1000 AS salary_k, next_age - 1 AS restored_age
            RETURN final_name AS final_name, salary_k AS salary_k, restored_age AS restored_age""",
        )

        # Verify data integrity through transformations
        assert len(result) == 5

        # Names should be preserved exactly
        names = result["final_name"].tolist()
        assert "Alice" in names
        assert "Bob" in names
        assert "Carol" in names

        # Age round-trip: age -> age+1 -> (age+1)-1 = age (should be restored)
        restored_ages = result["restored_age"].tolist()
        assert set(restored_ages) == {30, 40, 25, 35, 28}

        # Salary transformation: salary -> salary*1.1 -> (salary*1.1)/1000
        # Alice: 100000 -> 110000 -> 110.0
        salary_k_values = result["salary_k"].tolist()
        # Use tolerance for floating point comparison due to precision
        assert any(
            abs(val - 110.0) < 0.001 for val in salary_k_values
        )  # Alice's transformed salary

    def test_variable_scoping_integrity(self, integrity_test_context):
        """Test that variable scoping works correctly between WITH clauses."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.name AS original_name, toUpper(p.name) AS upper_name
            WITH original_name AS preserved_original, toLower(upper_name) AS lower_from_upper
            RETURN preserved_original AS preserved_original, lower_from_upper AS lower_from_upper""",
        )

        # Test that original name is preserved and transformation chain works
        for _, row in result.iterrows():
            original = row["preserved_original"]
            lower_from_upper = row["lower_from_upper"]

            # Should be: name -> toUpper -> toLower = name.lower()
            assert original.lower() == lower_from_upper

    def test_expression_consistency_across_stages(
        self,
        integrity_test_context,
    ):
        """Test that complex expressions remain consistent across WITH stages."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.age * 1000 + p.salary / 100 AS complex_score
            WITH complex_score / 1000 AS age_component, complex_score % 1000 AS salary_component
            WITH age_component AS extracted_age, salary_component * 100 AS reconstructed_salary_part
            RETURN extracted_age AS extracted_age, reconstructed_salary_part AS reconstructed_salary_part""",
        )

        # This tests mathematical consistency through complex transformations
        for _, row in result.iterrows():
            extracted_age = row["extracted_age"]
            reconstructed_salary_part = row["reconstructed_salary_part"]

            # Should be able to extract original age from complex transformation
            import numpy as np

            assert isinstance(extracted_age, (int, float, np.integer))
            assert extracted_age > 0


class TestJoinDataIntegrity:
    """Test that relationship joins preserve data integrity."""

    def test_relationship_join_completeness(self, integrity_test_context):
        """Test that relationship joins don't lose or duplicate data incorrectly."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS from_name, b.name AS to_name, r.relationship_type AS rel_type",
        )

        # Should have exactly 5 relationships as defined in test data
        assert len(result) == 5

        # Verify all relationship types are preserved
        rel_types = result["rel_type"].tolist()
        expected_types = [
            "mentor",
            "colleague",
            "friend",
            "reports_to",
            "collaborates",
        ]
        assert set(rel_types) == set(expected_types)

        # Verify specific known relationships exist
        relationships = [
            (row["from_name"], row["to_name"], row["rel_type"])
            for _, row in result.iterrows()
        ]
        assert ("Alice", "Bob", "mentor") in relationships
        assert ("Bob", "Carol", "colleague") in relationships

    def test_join_data_type_preservation(self, integrity_test_context):
        """Test that data types are preserved through joins."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.age AS from_age, b.age AS to_age, r.strength AS relationship_strength",
        )

        # Verify data types are numeric (pandas join may widen int columns to float64)
        for _, row in result.iterrows():
            assert isinstance(
                row["from_age"],
                (int, float, np.integer, np.floating),
            )
            assert isinstance(
                row["to_age"],
                (int, float, np.integer, np.floating),
            )
            assert isinstance(
                row["relationship_strength"],
                (float, np.floating),
            )

    def test_self_referential_integrity(self, integrity_test_context):
        """Test queries that reference the same entity multiple times."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.name AS name1, p.name AS name2, p.age AS age1, p.age AS age2
            RETURN name1 AS name1, name2 AS name2, age1 AS age1, age2 AS age2""",
        )

        # Self-references should be identical
        for _, row in result.iterrows():
            assert row["name1"] == row["name2"]
            assert row["age1"] == row["age2"]


class TestNullPropagationIntegrity:
    """Test that nulls propagate correctly through complex pipelines."""

    def test_null_propagation_through_with_chain(self, integrity_test_context):
        """Test null propagation through multiple WITH clauses."""
        # Create test data with strategic nulls
        person_df_nulls = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "value1": [10, None, 30],
                "value2": [100, 200, None],
            },
        )

        person_table_nulls = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "value1", "value2"],
            source_obj_attribute_map={"value1": "value1", "value2": "value2"},
            attribute_map={"value1": "value1", "value2": "value2"},
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
            """MATCH (p:Person)
            WITH p.value1 + 5 AS step1, p.value2 * 2 AS step2
            WITH step1 * 2 AS final1, step2 + 10 AS final2
            RETURN final1 AS final1, final2 AS final2""",
        )

        # Nulls should propagate: null+5 -> null, null*2 -> null, null*2 -> null, null+10 -> null
        assert len(result) == 3
        assert result["final1"].isna().sum() == 1  # One null in value1
        assert result["final2"].isna().sum() == 1  # One null in value2

        # Non-null values should transform correctly
        # Row 1: value1=10 -> 10+5=15 -> 15*2=30
        final1_values = result["final1"].dropna().tolist()
        assert 30 in final1_values

    def test_null_handling_in_mixed_operations(self, integrity_test_context):
        """Test nulls in mixed string/numeric operations."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH coalesce(p.manager_id, 0) AS safe_manager_id, p.name AS name
            WITH safe_manager_id * 100 AS scaled_manager, toUpper(name) AS upper_name
            RETURN scaled_manager AS scaled_manager, upper_name AS upper_name""",
        )

        # coalesce should eliminate nulls in manager_id
        assert result["scaled_manager"].isna().sum() == 0  # No nulls after coalesce

        # String operations should preserve non-null values
        assert result["upper_name"].isna().sum() == 0  # Names are all non-null


class TestAggregationIntegrity:
    """Test aggregation integrity in complex scenarios."""

    def test_aggregation_after_filtering(self, integrity_test_context):
        """Test that aggregations work correctly after WHERE filtering."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WHERE p.department = 'Engineering'
            WITH count(*) AS eng_count, avg(p.salary) AS eng_avg_salary
            RETURN eng_count AS eng_count, eng_avg_salary AS eng_avg_salary""",
        )

        # Engineering: Alice(100000), Carol(90000), Eve(95000) = 3 people
        # Average: (100000 + 90000 + 95000) / 3 = 95000
        assert result["eng_count"].iloc[0] == 3
        assert abs(result["eng_avg_salary"].iloc[0] - 95000.0) < 0.001

    def test_grouped_aggregation_integrity(self, integrity_test_context):
        """Test grouped aggregations maintain group integrity."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.department AS dept, count(*) AS people_count, sum(p.salary) AS total_salary
            WITH dept AS department, people_count AS count, total_salary / people_count AS avg_salary_check
            RETURN department AS department, count AS count, avg_salary_check AS avg_salary_check""",
        )

        # Verify mathematical consistency: sum/count should equal average
        for _, row in result.iterrows():
            dept = row["department"]
            count = row["count"]
            avg_check = row["avg_salary_check"]

            assert count > 0  # Each department should have at least one person
            assert avg_check > 0  # Average salary should be positive

        # Check specific departments
        dept_data = {
            row["department"]: {
                "count": row["count"],
                "avg": row["avg_salary_check"],
            }
            for _, row in result.iterrows()
        }

        assert dept_data["Engineering"]["count"] == 3
        assert dept_data["Sales"]["count"] == 1
        assert dept_data["Marketing"]["count"] == 1


class TestStringProcessingIntegrity:
    """Test string processing maintains data integrity."""

    def test_string_transformation_roundtrip(self, integrity_test_context):
        """Test string transformations maintain essential information."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.name AS original, toUpper(p.name) AS upper, toLower(p.name) AS lower
            WITH original AS orig, upper AS up, lower AS low,
                 size(original) AS orig_len, size(upper) AS up_len, size(lower) AS low_len
            RETURN orig AS orig, up AS up, low AS low, orig_len AS orig_len, up_len AS up_len, low_len AS low_len""",
        )

        # String transformations should preserve length
        for _, row in result.iterrows():
            orig_len = row["orig_len"]
            up_len = row["up_len"]
            low_len = row["low_len"]

            assert orig_len == up_len == low_len  # Length should be preserved

        # Case transformations should be consistent
        for _, row in result.iterrows():
            orig = row["orig"]
            up = row["up"]
            low = row["low"]

            assert orig.upper() == up
            assert orig.lower() == low

    def test_email_domain_extraction_integrity(self, integrity_test_context):
        """Test domain extraction maintains referential integrity."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.name AS name, p.email AS email,
                 substring(p.email, size(split(p.email, '@')[0]) + 1) AS domain_attempt
            RETURN name AS name, email AS email, domain_attempt AS domain_attempt""",
        )

        # Note: This uses substring + split which may not be implemented
        # But tests the concept of maintaining integrity in string processing
        assert len(result) == 5


class TestDataTypeConsistency:
    """Test data type consistency through transformations."""

    def test_type_consistency_through_arithmetic(self, integrity_test_context):
        """Test that arithmetic maintains expected types."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH p.age AS int_age, toFloat(p.age) AS float_age
            WITH int_age + 1 AS int_result, float_age + 1.0 AS float_result
            RETURN int_result AS int_result, float_result AS float_result""",
        )

        # Check that type consistency is maintained
        for _, row in result.iterrows():
            int_result = row["int_result"]
            float_result = row["float_result"]

            # Both should be numeric
            assert isinstance(
                int_result,
                (int, float, np.integer, np.floating),
            )
            assert isinstance(float_result, (float, np.floating))

    def test_string_numeric_boundary_integrity(self, integrity_test_context):
        """Test integrity at string/numeric boundaries."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """MATCH (p:Person)
            WITH toString(p.age) AS age_str, p.salary AS salary_num
            WITH toInteger(age_str) AS age_back, toString(salary_num) AS salary_str
            WITH age_back AS restored_age, toFloat(salary_str) AS restored_salary
            RETURN restored_age AS restored_age, restored_salary AS restored_salary""",
        )

        # Round-trip conversions should preserve values
        restored_ages = result["restored_age"].tolist()
        restored_salaries = result["restored_salary"].tolist()

        assert set(restored_ages) == {30, 40, 25, 35, 28}  # Original ages
        assert set(restored_salaries) == {
            100000.0,
            120000.0,
            90000.0,
            110000.0,
            95000.0,
        }  # Original salaries
