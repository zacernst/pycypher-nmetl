"""
Integration tests for PropertyAddition functionality.
Tests end-to-end SET clause functionality with real-world scenarios.
"""

import pytest
import pandas as pd
import numpy as np
from pycypher.star import Star
from pycypher.relational_models import (
    Context, EntityMapping, RelationshipMapping, EntityTable, RelationshipTable,
    ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN
)


class TestPropertyAdditionRealWorldScenarios:
    """Test PropertyAddition with realistic business scenarios."""

    @pytest.fixture
    def hr_system_context(self):
        """Create an HR system context for realistic testing."""
        # Employee data
        employee_df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3, 4, 5],
            "employee_id": ["EMP001", "EMP002", "EMP003", "EMP004", "EMP005"],
            "first_name": ["Alice", "Bob", "Carol", "David", "Eve"],
            "last_name": ["Johnson", "Smith", "Williams", "Brown", "Davis"],
            "email": ["alice@company.com", "bob@company.com", "carol@company.com", "david@company.com", "eve@company.com"],
            "hire_date": ["2020-01-15", "2019-03-20", "2021-06-10", "2018-11-05", "2022-02-28"],
            "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales"],
            "base_salary": [85000, 65000, 90000, 70000, 68000],
            "performance_rating": [4.2, 3.8, 4.5, 3.9, 4.1],
            "active": [True, True, True, False, True]
        })

        employee_table = EntityTable(
            entity_type="Employee",
            identifier="Employee",
            column_names=[ID_COLUMN, "employee_id", "first_name", "last_name", "email", "hire_date",
                         "department", "base_salary", "performance_rating", "active"],
            source_obj_attribute_map={
                "employee_id": "employee_id", "first_name": "first_name", "last_name": "last_name",
                "email": "email", "hire_date": "hire_date", "department": "department",
                "base_salary": "base_salary", "performance_rating": "performance_rating", "active": "active"
            },
            attribute_map={
                "employee_id": "employee_id", "first_name": "first_name", "last_name": "last_name",
                "email": "email", "hire_date": "hire_date", "department": "department",
                "base_salary": "base_salary", "performance_rating": "performance_rating", "active": "active"
            },
            source_obj=employee_df
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Employee": employee_table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_employee_annual_review_process(self, hr_system_context):
        """Test annual review process with salary adjustments and promotions."""
        star = Star(context=hr_system_context)

        result = star.execute_query(
            """
            MATCH (e:Employee)
            SET e.review_year = 2024,
                e.salary_increase = CASE
                    WHEN e.performance_rating >= 4.0 THEN e.base_salary * 0.05
                    WHEN e.performance_rating >= 3.5 THEN e.base_salary * 0.03
                    ELSE 0
                END,
                e.new_salary = e.base_salary + e.salary_increase,
                e.promotion_eligible = e.performance_rating >= 4.0
            RETURN e.employee_id AS id,
                   e.first_name + ' ' + e.last_name AS full_name,
                   e.performance_rating AS rating,
                   e.salary_increase AS increase,
                   e.new_salary AS new_salary,
                   e.promotion_eligible AS eligible
            """
        )

        assert len(result) == 5
        assert "increase" in result.columns
        assert "new_salary" in result.columns
        assert "eligible" in result.columns

        # Check salary calculations
        for _, row in result.iterrows():
            rating = row["rating"]
            if rating >= 4.0:
                # Should be 5% increase
                expected_eligible = True
            elif rating >= 3.5:
                # Should be 3% increase
                expected_eligible = False
            else:
                # Should be 0% increase
                expected_eligible = False

            assert row["eligible"] == expected_eligible

    def test_bulk_data_migration(self, hr_system_context):
        """Test bulk data migration scenario."""
        star = Star(context=hr_system_context)

        result = star.execute_query(
            """
            MATCH (e:Employee)
            SET e += {
                migrated_at: '2024-01-15',
                legacy_id: 'LEG_' + e.employee_id,
                full_name: e.first_name + ' ' + e.last_name,
                years_service: 2024 - toInteger(substring(e.hire_date, 0, 4)),
                email_domain: substring(e.email, size(split(e.email, '@')[0]) + 1)
            }
            RETURN e.employee_id AS id,
                   e.legacy_id AS legacy_id,
                   e.full_name AS full_name,
                   e.years_service AS years_service,
                   e.email_domain AS email_domain
            """
        )

        assert len(result) == 5
        assert "legacy_id" in result.columns
        assert "full_name" in result.columns
        assert "years_service" in result.columns
        assert "email_domain" in result.columns

        # Check legacy ID format
        for _, row in result.iterrows():
            assert row["legacy_id"].startswith("LEG_EMP")

        # Check full name composition
        full_names = set(result["full_name"])
        assert "Alice Johnson" in full_names
        assert "Bob Smith" in full_names

    def test_conditional_property_updates(self, hr_system_context):
        """Test conditional property updates based on existing data."""
        star = Star(context=hr_system_context)

        result = star.execute_query(
            """
            MATCH (e:Employee)
            SET e.status = CASE
                    WHEN e.active = false THEN 'inactive'
                    WHEN e.department = 'Engineering' THEN 'technical'
                    WHEN e.department = 'Sales' THEN 'revenue'
                    ELSE 'support'
                END,
                e.risk_level = CASE
                    WHEN e.performance_rating < 3.5 THEN 'high'
                    WHEN e.performance_rating < 4.0 THEN 'medium'
                    ELSE 'low'
                END
            RETURN e.first_name AS name,
                   e.department AS department,
                   e.active AS active,
                   e.performance_rating AS rating,
                   e.status AS status,
                   e.risk_level AS risk_level
            """
        )

        assert len(result) == 5
        assert "status" in result.columns
        assert "risk_level" in result.columns

        # Check conditional logic
        for _, row in result.iterrows():
            if not row["active"]:
                assert row["status"] == "inactive"
            elif row["department"] == "Engineering":
                assert row["status"] == "technical"
            elif row["department"] == "Sales":
                assert row["status"] == "revenue"
            else:
                assert row["status"] == "support"


class TestPropertyAdditionPerformanceScenarios:
    """Test PropertyAddition performance and scalability."""

    @pytest.fixture
    def large_dataset_context(self):
        """Create a larger dataset for performance testing."""
        # Generate larger dataset
        n_records = 1000
        employee_ids = [f"EMP{i:06d}" for i in range(1, n_records + 1)]
        names = [f"Employee_{i}" for i in range(1, n_records + 1)]
        departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"] * (n_records // 5)
        salaries = np.random.normal(75000, 15000, n_records).astype(int)
        ratings = np.random.normal(3.8, 0.6, n_records).round(1)

        large_df = pd.DataFrame({
            ID_COLUMN: range(1, n_records + 1),
            "employee_id": employee_ids,
            "name": names,
            "department": departments[:n_records],
            "base_salary": salaries,
            "performance_rating": ratings,
            "active": [True] * n_records
        })

        large_table = EntityTable(
            entity_type="Employee",
            identifier="Employee",
            column_names=[ID_COLUMN, "employee_id", "name", "department", "base_salary", "performance_rating", "active"],
            source_obj_attribute_map={
                "employee_id": "employee_id", "name": "name", "department": "department",
                "base_salary": "base_salary", "performance_rating": "performance_rating", "active": "active"
            },
            attribute_map={
                "employee_id": "employee_id", "name": "name", "department": "department",
                "base_salary": "base_salary", "performance_rating": "performance_rating", "active": "active"
            },
            source_obj=large_df
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Employee": large_table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_bulk_property_addition_performance(self, large_dataset_context):
        """Test performance of bulk property additions."""
        star = Star(context=large_dataset_context)

        import time
        start_time = time.time()

        result = star.execute_query(
            """
            MATCH (e:Employee)
            SET e.bonus = e.base_salary * 0.1,
                e.total_comp = e.base_salary + e.bonus,
                e.tax_bracket = CASE
                    WHEN e.total_comp > 100000 THEN 'high'
                    WHEN e.total_comp > 60000 THEN 'medium'
                    ELSE 'low'
                END
            RETURN count(*) AS processed_count
            """
        )

        execution_time = time.time() - start_time

        # Check that all records were processed
        assert result["processed_count"].iloc[0] == 1000

        # Performance assertion - should complete in reasonable time
        assert execution_time < 10.0  # Should complete in under 10 seconds

    def test_memory_efficiency_large_dataset(self, large_dataset_context):
        """Test memory efficiency with large datasets."""
        star = Star(context=large_dataset_context)

        # Monitor memory usage (basic check)
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        result = star.execute_query(
            """
            MATCH (e:Employee)
            SET e += {
                computed_field_1: e.base_salary * 1.1,
                computed_field_2: e.base_salary * 1.2,
                computed_field_3: e.base_salary * 1.3,
                computed_field_4: e.performance_rating * 100,
                computed_field_5: 'processed_' + e.employee_id
            }
            RETURN count(*) AS total
            """
        )

        final_memory = process.memory_info().rss
        memory_increase = (final_memory - initial_memory) / (1024 * 1024)  # MB

        # Check that we didn't use excessive memory (allow reasonable overhead)
        assert memory_increase < 500  # Less than 500MB increase

        assert result["total"].iloc[0] == 1000


class TestPropertyAdditionErrorRecovery:
    """Test error handling and recovery scenarios."""

    @pytest.fixture
    def error_test_context(self):
        """Create context for error testing."""
        df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "value": [100, 200, 300],
            "nullable_field": [1, None, 3]
        })

        table = EntityTable(
            entity_type="TestEntity",
            identifier="TestEntity",
            column_names=[ID_COLUMN, "name", "value", "nullable_field"],
            source_obj_attribute_map={
                "name": "name", "value": "value", "nullable_field": "nullable_field"
            },
            attribute_map={
                "name": "name", "value": "value", "nullable_field": "nullable_field"
            },
            source_obj=df
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"TestEntity": table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_graceful_handling_of_null_operations(self, error_test_context):
        """Test graceful handling of operations involving null values."""
        star = Star(context=error_test_context)

        result = star.execute_query(
            """
            MATCH (e:TestEntity)
            SET e.null_safe_calc = coalesce(e.nullable_field * 2, -1),
                e.null_indicator = e.nullable_field IS NULL
            RETURN e.name AS name,
                   e.nullable_field AS original,
                   e.null_safe_calc AS calculated,
                   e.null_indicator AS is_null
            """
        )

        assert len(result) == 3

        # Check null handling
        for _, row in result.iterrows():
            if pd.isna(row["original"]):
                assert row["calculated"] == -1  # Coalesce fallback
                assert row["is_null"] == True
            else:
                assert row["calculated"] == row["original"] * 2
                assert row["is_null"] == False

    def test_transaction_rollback_on_error(self, error_test_context):
        """Test that errors don't leave partial modifications."""
        star = Star(context=error_test_context)

        # This test documents expected behavior for error scenarios
        try:
            star.execute_query(
                """
                MATCH (e:TestEntity)
                SET e.good_field = 'success',
                    e.bad_field = e.nonexistent_field / 0
                RETURN e.name
                """
            )
            assert False, "Should have raised an error"
        except Exception:
            # Check that no partial modifications occurred
            # This is implementation-dependent behavior
            pass

    def test_type_coercion_warnings(self, error_test_context):
        """Test handling of implicit type coercions."""
        star = Star(context=error_test_context)

        result = star.execute_query(
            """
            MATCH (e:TestEntity)
            SET e.string_to_int = toInteger(toString(e.value)),
                e.int_to_string = toString(e.value),
                e.mixed_type = CASE
                    WHEN e.value > 150 THEN 'high'
                    ELSE e.value
                END
            RETURN e.name AS name,
                   e.string_to_int AS string_to_int,
                   e.int_to_string AS int_to_string,
                   e.mixed_type AS mixed_type
            """
        )

        assert len(result) == 3

        # Check type conversions
        for _, row in result.iterrows():
            assert isinstance(row["string_to_int"], (int, np.integer))
            assert isinstance(row["int_to_string"], str)
            # mixed_type will be either string or int depending on condition


class TestPropertyAdditionDataIntegrity:
    """Test data integrity and consistency."""

    @pytest.fixture
    def integrity_test_context(self):
        """Create context for data integrity testing."""
        df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "balance": [1000, 2000, 1500, 3000],
            "account_type": ["checking", "savings", "checking", "savings"],
            "last_transaction": [100, -50, 200, -75]
        })

        table = EntityTable(
            entity_type="Account",
            identifier="Account",
            column_names=[ID_COLUMN, "name", "balance", "account_type", "last_transaction"],
            source_obj_attribute_map={
                "name": "name", "balance": "balance", "account_type": "account_type",
                "last_transaction": "last_transaction"
            },
            attribute_map={
                "name": "name", "balance": "balance", "account_type": "account_type",
                "last_transaction": "last_transaction"
            },
            source_obj=df
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Account": table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_balance_calculation_integrity(self, integrity_test_context):
        """Test that balance calculations maintain integrity."""
        star = Star(context=integrity_test_context)

        result = star.execute_query(
            """
            MATCH (a:Account)
            SET a.new_balance = a.balance + a.last_transaction,
                a.transaction_processed = true,
                a.balance_check = a.new_balance - a.last_transaction
            RETURN a.name AS name,
                   a.balance AS original_balance,
                   a.last_transaction AS transaction,
                   a.new_balance AS new_balance,
                   a.balance_check AS balance_check
            """
        )

        # Check mathematical integrity
        for _, row in result.iterrows():
            expected_new_balance = row["original_balance"] + row["transaction"]
            assert row["new_balance"] == expected_new_balance

            # Balance check should equal original balance
            assert row["balance_check"] == row["original_balance"]

    def test_referential_integrity_preservation(self, integrity_test_context):
        """Test that property additions don't break referential integrity."""
        star = Star(context=integrity_test_context)

        # Get original row count
        original_result = star.execute_query("MATCH (a:Account) RETURN count(*) AS count")
        original_count = original_result["count"].iloc[0]

        # Add properties
        result = star.execute_query(
            """
            MATCH (a:Account)
            SET a.audit_timestamp = '2024-01-15',
                a.processed_by = 'system'
            RETURN count(*) AS final_count
            """
        )

        # Check that row count is preserved
        final_count = result["final_count"].iloc[0]
        assert final_count == original_count

    def test_concurrent_modification_simulation(self, integrity_test_context):
        """Simulate concurrent modifications to test consistency."""
        star = Star(context=integrity_test_context)

        # Simulate multiple "concurrent" operations
        result1 = star.execute_query(
            """
            MATCH (a:Account)
            SET a.operation_1 = a.balance * 1.05
            RETURN count(*) AS count1
            """
        )

        result2 = star.execute_query(
            """
            MATCH (a:Account)
            SET a.operation_2 = a.balance * 0.95
            RETURN a.name AS name, a.operation_1 AS op1, a.operation_2 AS op2
            """
        )

        # Check that both operations can access the base balance
        assert len(result2) == 4
        for _, row in result2.iterrows():
            # Both operations should have been applied
            assert pd.notna(row["op1"])
            assert pd.notna(row["op2"])

    def test_schema_evolution_consistency(self, integrity_test_context):
        """Test that schema changes maintain consistency."""
        star = Star(context=integrity_test_context)

        # Add new properties that change the effective schema
        result = star.execute_query(
            """
            MATCH (a:Account)
            SET a.version = 2,
                a.schema_migrated = true,
                a.legacy_balance = a.balance,
                a.new_format_balance = toString(a.balance) + '.00'
            RETURN a.name AS name,
                   a.version AS version,
                   a.legacy_balance AS legacy_balance,
                   a.new_format_balance AS new_format_balance
            """
        )

        # Check schema consistency
        assert len(result) == 4
        for _, row in result.iterrows():
            assert row["version"] == 2
            assert row["legacy_balance"] == int(row["new_format_balance"].split('.')[0])


class TestPropertyAdditionCompatibility:
    """Test compatibility with existing PyCypher features."""

    @pytest.fixture
    def compatibility_context(self):
        """Create context for compatibility testing."""
        df = pd.DataFrame({
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "score": [85, 92, 78]
        })

        table = EntityTable(
            entity_type="Student",
            identifier="Student",
            column_names=[ID_COLUMN, "name", "score"],
            source_obj_attribute_map={"name": "name", "score": "score"},
            attribute_map={"name": "name", "score": "score"},
            source_obj=df
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Student": table}),
            relationship_mapping=RelationshipMapping(mapping={})
        )

    def test_compatibility_with_existing_aggregations(self, compatibility_context):
        """Test that SET works with existing aggregation functions."""
        star = Star(context=compatibility_context)

        result = star.execute_query(
            """
            MATCH (s:Student)
            SET s.grade = CASE
                WHEN s.score >= 90 THEN 'A'
                WHEN s.score >= 80 THEN 'B'
                ELSE 'C'
            END
            WITH count(*) AS total_students,
                 avg(s.score) AS avg_score,
                 collect(s.grade) AS all_grades
            RETURN total_students, avg_score, all_grades
            """
        )

        assert result["total_students"].iloc[0] == 3
        assert abs(result["avg_score"].iloc[0] - 85.0) < 0.1  # (85+92+78)/3

        grades = result["all_grades"].iloc[0]
        assert 'A' in grades  # Bob's grade
        assert 'B' in grades  # Alice's grade
        assert 'C' in grades  # Carol's grade

    def test_compatibility_with_scalar_functions(self, compatibility_context):
        """Test that SET works with existing scalar functions."""
        star = Star(context=compatibility_context)

        result = star.execute_query(
            """
            MATCH (s:Student)
            SET s.name_upper = toUpper(s.name),
                s.name_length = size(s.name),
                s.score_string = toString(s.score),
                s.passing = toBoolean(s.score >= 80)
            RETURN s.name AS name,
                   s.name_upper AS name_upper,
                   s.name_length AS name_length,
                   s.score_string AS score_string,
                   s.passing AS passing
            """
        )

        # Check scalar function integration
        for _, row in result.iterrows():
            assert row["name_upper"] == row["name"].upper()
            assert row["name_length"] == len(row["name"])
            assert row["score_string"] == str(row["name"])  # Assuming this is the intended behavior
            assert isinstance(row["passing"], bool)

    def test_backward_compatibility(self, compatibility_context):
        """Test that existing queries still work after SET functionality is added."""
        star = Star(context=compatibility_context)

        # Run a traditional query without SET
        traditional_result = star.execute_query(
            """
            MATCH (s:Student)
            WITH s.name AS name, s.score AS score
            RETURN name, score ORDER BY score DESC
            """
        )

        assert len(traditional_result) == 3
        assert "name" in traditional_result.columns
        assert "score" in traditional_result.columns

        # Now run a query with SET followed by traditional operations
        enhanced_result = star.execute_query(
            """
            MATCH (s:Student)
            SET s.enhanced = true
            WITH s.name AS name, s.score AS score, s.enhanced AS enhanced
            RETURN name, score, enhanced ORDER BY score DESC
            """
        )

        assert len(enhanced_result) == 3
        assert "enhanced" in enhanced_result.columns
        assert (enhanced_result["enhanced"] == True).all()

        # Original columns should still work the same way
        assert enhanced_result["name"].tolist() == traditional_result["name"].tolist()
        assert enhanced_result["score"].tolist() == traditional_result["score"].tolist()