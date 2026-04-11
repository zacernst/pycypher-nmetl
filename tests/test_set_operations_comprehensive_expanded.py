"""Comprehensive test suite for SET operations in PyCypher.
This test suite validates all aspects of SET clause functionality with proper handling
of current system limitations and establishes performance baselines.
"""

import time

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


class TestSetOperationsCore:
    """Core SET operation functionality tests."""

    @pytest.fixture
    def basic_person_context(self):
        """Basic Person context for testing."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
                "department": ["Engineering", "Sales", "Engineering"],
                "salary": [75000, 65000, 80000],
            },
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age", "department", "salary"],
            source_obj_attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
                "salary": "salary",
            },
            attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
                "salary": "salary",
            },
            source_obj=person_df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        return context

    def test_set_single_property_literal(self, basic_person_context):
        """Test setting a single property to a literal value."""
        star = Star(context=basic_person_context)

        result = star.execute_query(
            "MATCH (p:Person) SET p.status = 'active' RETURN p.name AS name, p.status AS status",
        )

        assert len(result) == 3
        assert (result["status"] == "active").all()
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}

    def test_set_multiple_properties_literals(self, basic_person_context):
        """Test setting multiple properties to literal values."""
        star = Star(context=basic_person_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.status = 'employed', p.level = 3
               RETURN p.name AS name, p.status AS status, p.level AS level""",
        )

        assert len(result) == 3
        assert (result["status"] == "employed").all()
        assert (result["level"] == 3).all()

    def test_set_expression_based_properties(self, basic_person_context):
        """Test setting properties using expressions."""
        star = Star(context=basic_person_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.bonus = p.salary * 0.15, p.total_comp = p.salary + p.bonus
               RETURN p.name AS name, p.salary AS salary, p.bonus AS bonus, p.total_comp AS total""",
        )

        assert len(result) == 3

        # Verify calculations
        for _, row in result.iterrows():
            expected_bonus = row["salary"] * 0.15
            expected_total = row["salary"] + expected_bonus
            assert abs(row["bonus"] - expected_bonus) < 0.01
            assert abs(row["total"] - expected_total) < 0.01

    def test_set_modify_existing_property(self, basic_person_context):
        """Test modifying existing property values."""
        star = Star(context=basic_person_context)

        # First, verify original ages
        original = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
        )
        assert set(original["age"]) == {25, 30, 35}

        # Modify ages
        result = star.execute_query(
            "MATCH (p:Person) SET p.age = p.age + 5 RETURN p.name AS name, p.age AS age",
        )

        assert len(result) == 3
        assert set(result["age"]) == {30, 35, 40}  # All ages increased by 5


class TestSetOperationsAdvanced:
    """Advanced SET operation functionality tests."""

    @pytest.fixture
    def extended_person_context(self):
        """Extended Person context with more diverse data."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Carol", "David", "Eve"],
                "age": [25, 30, 35, 28, 32],
                "department": [
                    "Engineering",
                    "Sales",
                    "Engineering",
                    "Marketing",
                    "Sales",
                ],
                "salary": [75000.0, 65000.5, 80000.0, 70000.0, 68000.0],
                "performance": [4.2, 3.8, 4.5, 4.0, 4.1],
            },
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[
                ID_COLUMN,
                "name",
                "age",
                "department",
                "salary",
                "performance",
            ],
            source_obj_attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
                "salary": "salary",
                "performance": "performance",
            },
            attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
                "salary": "salary",
                "performance": "performance",
            },
            source_obj=person_df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        return context

    def test_set_complex_mathematical_expressions(
        self,
        extended_person_context,
    ):
        """Test SET operations with complex mathematical expressions."""
        star = Star(context=extended_person_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.perf_bonus = p.salary * p.performance / 10,
                   p.age_salary_ratio = p.salary / p.age,
                   p.compound = (p.salary + 1000) * 1.05
               RETURN p.name AS name, p.perf_bonus AS bonus, p.age_salary_ratio AS ratio, p.compound AS compound""",
        )

        assert len(result) == 5

        # Validate calculations for first row
        alice_row = result[result["name"] == "Alice"].iloc[0]
        expected_bonus = 75000.0 * 4.2 / 10
        expected_ratio = 75000.0 / 25
        expected_compound = (75000.0 + 1000) * 1.05

        assert abs(alice_row["bonus"] - expected_bonus) < 0.01
        assert abs(alice_row["ratio"] - expected_ratio) < 0.01
        assert abs(alice_row["compound"] - expected_compound) < 0.01

    def test_set_sequential_dependencies(self, extended_person_context):
        """Test SET operations where later properties depend on earlier ones."""
        star = Star(context=extended_person_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.base_bonus = p.salary * 0.1,
                   p.performance_multiplier = p.performance,
                   p.total_bonus = p.base_bonus * p.performance_multiplier,
                   p.final_salary = p.salary + p.total_bonus
               RETURN p.name AS name, p.salary AS orig_salary, p.final_salary AS final""",
        )

        assert len(result) == 5

        # Verify that final salary > original salary for all employees
        assert (result["final"] > result["orig_salary"]).all()

    def test_set_property_persistence_complex(self, extended_person_context):
        """Test complex property persistence across multiple operations."""
        star = Star(context=extended_person_context)

        # Step 1: Add review properties
        star.execute_query(
            """MATCH (p:Person)
               SET p.review_date = '2024-01-15', p.review_status = 'pending'
               RETURN p.name AS name""",
        )

        # Step 2: Add calculated properties
        star.execute_query(
            """MATCH (p:Person)
               SET p.review_score = p.performance * 20, p.review_status = 'scored'
               RETURN p.name AS name""",
        )

        # Step 3: Verify all properties persist
        result = star.execute_query(
            """MATCH (p:Person)
               RETURN p.name AS name,
                      p.review_date AS date,
                      p.review_status AS status,
                      p.review_score AS score,
                      p.performance AS orig_perf""",
        )

        assert len(result) == 5
        assert (result["status"] == "scored").all()
        assert result["date"].notna().all()
        assert (result["score"] == result["orig_perf"] * 20).all()


class TestSetOperationsPerformanceBaselines:
    """Performance testing and baseline establishment for SET operations."""

    def test_set_operations_small_dataset_baseline(self):
        """Test SET operations on small dataset (100 rows) - performance baseline."""
        # Create small test dataset
        small_df = pd.DataFrame(
            {
                ID_COLUMN: range(100),
                "name": [f"Person_{i}" for i in range(100)],
                "value": [100 + i for i in range(100)],
            },
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "value"],
            source_obj_attribute_map={"name": "name", "value": "value"},
            attribute_map={"name": "name", "value": "value"},
            source_obj=small_df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        star = Star(context=context)

        start_time = time.time()

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.doubled = p.value * 2, p.status = 'processed'
               RETURN p.name AS name, p.doubled AS doubled""",
        )

        execution_time = time.time() - start_time

        # Validate results
        assert len(result) == 100
        assert result["doubled"].min() == 200  # 100 * 2
        assert result["doubled"].max() == 398  # 199 * 2

        # Performance baseline
        print(
            f"Pandas SET operations on 100 rows: {execution_time:.3f} seconds",
        )
        assert execution_time < perf_threshold(2.0), (
            f"Small dataset took {execution_time:.3f}s, expected < 2.0s"
        )

    def test_set_operations_medium_dataset_baseline(self):
        """Test SET operations on medium dataset (1000 rows) - baseline for PySpark comparison."""
        # Create medium test dataset
        medium_df = pd.DataFrame(
            {
                ID_COLUMN: range(1000),
                "name": [f"Person_{i}" for i in range(1000)],
                "salary": [50000 + (i % 50) * 1000 for i in range(1000)],
                "performance": [3.0 + (i % 20) * 0.1 for i in range(1000)],
            },
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "salary", "performance"],
            source_obj_attribute_map={
                "name": "name",
                "salary": "salary",
                "performance": "performance",
            },
            attribute_map={
                "name": "name",
                "salary": "salary",
                "performance": "performance",
            },
            source_obj=medium_df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        star = Star(context=context)

        start_time = time.time()

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.bonus = p.salary * 0.1,
                   p.total_comp = p.salary + p.bonus,
                   p.perf_category = p.performance * 10
               RETURN p.name AS name, p.total_comp AS total""",
        )

        execution_time = time.time() - start_time

        # Validate results
        assert len(result) == 1000
        assert result["total"].min() > 50000  # Should be salary + bonus

        # Performance baseline for PySpark comparison
        print(
            f"Pandas SET operations on 1000 rows: {execution_time:.3f} seconds",
        )
        assert execution_time < perf_threshold(5.0), (
            f"Medium dataset took {execution_time:.3f}s, expected < 5.0s"
        )

    def test_set_multiple_properties_performance_baseline(self):
        """Test performance of setting many properties simultaneously - baseline."""
        # Create test dataset
        test_df = pd.DataFrame(
            {ID_COLUMN: range(500), "base": [100 + i for i in range(500)]},
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "base"],
            source_obj_attribute_map={"base": "base"},
            attribute_map={"base": "base"},
            source_obj=test_df,
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        star = Star(context=context)

        start_time = time.time()

        # Set 10 different properties
        result = star.execute_query(
            """MATCH (p:Person)
               SET p.calc1 = p.base * 1.1,
                   p.calc2 = p.base * 1.2,
                   p.calc3 = p.base * 1.3,
                   p.calc4 = p.base * 1.4,
                   p.calc5 = p.base * 1.5,
                   p.calc6 = p.base * 1.6,
                   p.calc7 = p.base * 1.7,
                   p.calc8 = p.base * 1.8,
                   p.calc9 = p.base * 1.9,
                   p.calc10 = p.base * 2.0
               RETURN p.base AS base, p.calc10 AS calc10""",
        )

        execution_time = time.time() - start_time

        # Validate
        assert len(result) == 500
        assert len(result.columns) == 2  # base + calc10

        # Performance baseline
        print(
            f"Multiple property SET (10 props, 500 rows): {execution_time:.3f} seconds",
        )
        assert execution_time < perf_threshold(3.0), (
            f"Multi-property SET took {execution_time:.3f}s, expected < 3.0s"
        )
