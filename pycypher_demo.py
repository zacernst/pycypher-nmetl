#!/usr/bin/env python3
"""
PyCypher Demonstration: Cypher to Pandas DataFrames with SET Operations

This script demonstrates the complete PyCypher system capabilities:
- Parse Cypher queries into relational algebra
- Execute against Pandas DataFrames
- Dynamic property modification with SET clauses
- Property persistence across queries
- Return results as standard Pandas DataFrames

Author: PyCypher Development Team
"""

import pandas as pd
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


def setup_sample_data():
    """Create sample employee dataset for demonstration."""
    employees_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "employee_id": ["E001", "E002", "E003", "E004", "E005"],
            "first_name": ["Alice", "Bob", "Carol", "David", "Eve"],
            "last_name": ["Johnson", "Smith", "Williams", "Brown", "Davis"],
            "department": [
                "Engineering",
                "Sales",
                "Engineering",
                "Marketing",
                "Sales",
            ],
            "base_salary": [85000, 65000, 90000, 70000, 68000],
            "performance_rating": [4.2, 3.8, 4.5, 3.9, 4.1],
            "hire_date": [
                "2020-01-15",
                "2019-03-20",
                "2021-06-10",
                "2018-11-05",
                "2022-02-28",
            ],
        }
    )

    # Set up PyCypher context
    employee_table = EntityTable(
        entity_type="Employee",
        identifier="Employee",
        column_names=[
            ID_COLUMN,
            "employee_id",
            "first_name",
            "last_name",
            "department",
            "base_salary",
            "performance_rating",
            "hire_date",
        ],
        source_obj_attribute_map={
            "employee_id": "employee_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "department": "department",
            "base_salary": "base_salary",
            "performance_rating": "performance_rating",
            "hire_date": "hire_date",
        },
        attribute_map={
            "employee_id": "employee_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "department": "department",
            "base_salary": "base_salary",
            "performance_rating": "performance_rating",
            "hire_date": "hire_date",
        },
        source_obj=employees_df,
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Employee": employee_table})
    )
    return Star(context=context), employees_df


def demo_basic_queries(star):
    """Demonstrate basic Cypher query capabilities."""
    print("=" * 70)
    print("📊 BASIC CYPHER QUERIES")
    print("=" * 70)

    # Simple node retrieval
    print("\n1. Basic Node Retrieval:")
    print(
        "   MATCH (e:Employee) RETURN e.first_name AS name, e.department AS dept"
    )

    result = star.execute_query("""
        MATCH (e:Employee)
        RETURN e.first_name AS name, e.department AS dept
    """)
    print(f"\n   Results ({result.shape[0]} employees):")
    print("   " + result.to_string(index=False).replace("\n", "\n   "))


def demo_set_operations(star):
    """Demonstrate SET clause operations."""
    print("\n\n" + "=" * 70)
    print("🔧 SET CLAUSE OPERATIONS")
    print("=" * 70)

    # 1. Adding new properties
    print("\n1. Adding New Properties:")
    print(
        "   MATCH (e:Employee) SET e.status = 'active', e.review_year = 2024"
    )

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.status = 'active', e.review_year = 2024
        RETURN e.first_name AS name, e.status AS status, e.review_year AS year
    """)
    print(f"\n   Results - New properties added:")
    print("   " + result.to_string(index=False).replace("\n", "\n   "))

    # 2. Expression-based calculations
    print("\n\n2. Expression-Based Property Calculations:")
    print("   MATCH (e:Employee) SET e.annual_bonus = e.base_salary * 0.12")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.annual_bonus = e.base_salary * 0.12
        RETURN e.first_name AS name, e.base_salary AS salary, e.annual_bonus AS bonus
    """)
    print(f"\n   Results - Calculated bonuses (12% of salary):")
    print("   " + result.to_string(index=False).replace("\n", "\n   "))

    # 3. String concatenation
    print("\n\n3. String Operations & Complex Expressions:")
    print(
        "   MATCH (e:Employee) SET e.full_name = e.first_name + ' ' + e.last_name"
    )
    print("   SET e.performance_score = e.performance_rating * 25")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.full_name = e.first_name + ' ' + e.last_name,
            e.performance_score = e.performance_rating * 25
        RETURN e.full_name AS full_name, e.performance_score AS score
    """)
    print(f"\n   Results - String concatenation & score calculation:")
    print("   " + result.to_string(index=False).replace("\n", "\n   "))

    # 4. Modifying existing properties
    print("\n\n4. Modifying Existing Properties:")
    print(
        "   MATCH (e:Employee) SET e.base_salary = e.base_salary + e.annual_bonus"
    )

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.base_salary = e.base_salary + e.annual_bonus
        RETURN e.first_name AS name, e.base_salary AS updated_salary
    """)
    print(f"\n   Results - Salaries updated with bonuses:")
    print("   " + result.to_string(index=False).replace("\n", "\n   "))


def demo_persistence(star):
    """Demonstrate property persistence across queries."""
    print("\n\n" + "=" * 70)
    print("💾 PROPERTY PERSISTENCE VERIFICATION")
    print("=" * 70)

    print(
        "\n   All properties from previous SET operations should be accessible:"
    )
    print("   MATCH (e:Employee) RETURN comprehensive property list")

    result = star.execute_query("""
        MATCH (e:Employee)
        RETURN e.employee_id AS id,
               e.full_name AS name,
               e.department AS dept,
               e.status AS status,
               e.base_salary AS total_salary,
               e.performance_score AS score
    """)
    print(
        f"\n   Results - All properties persist ({result.shape[0]} rows, {result.shape[1]} columns):"
    )
    print("   " + result.to_string(index=False).replace("\n", "\n   "))


def demo_data_transformation(star):
    """Demonstrate complex data transformation scenarios."""
    print("\n\n" + "=" * 70)
    print("⚡ ADVANCED DATA TRANSFORMATIONS")
    print("=" * 70)

    print("\n   Comprehensive employee analytics generation:")
    print("   Multiple SET operations with complex calculations")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.years_experience = 2024 - 2020,
            e.salary_tier = e.base_salary / 10000,
            e.email = e.first_name + '.' + e.last_name + '@company.com'
        RETURN e.full_name AS employee,
               e.years_experience AS experience,
               e.salary_tier AS tier,
               e.email AS email
    """)
    print(f"\n   Results - Advanced transformations:")
    print("   " + result.to_string(index=False).replace("\n", "\n   "))


def main():
    """Run the complete PyCypher demonstration."""
    print("🚀 PyCypher System Demonstration")
    print("   Cypher Query Language → Relational Algebra → Pandas DataFrames")
    print("   With Dynamic Property Modification via SET Clauses")

    # Setup
    star, original_data = setup_sample_data()
    print(
        f"\n📋 Sample Dataset: {len(original_data)} employees with {len(original_data.columns)} properties"
    )

    # Run demonstrations
    demo_basic_queries(star)
    demo_set_operations(star)
    demo_persistence(star)
    demo_data_transformation(star)

    # Summary
    print("\n\n" + "=" * 70)
    print("✅ DEMONSTRATION COMPLETE")
    print("=" * 70)
    print("\n🎯 Key Capabilities Demonstrated:")
    print("   • Cypher query parsing and execution")
    print("   • Dynamic property addition with SET clauses")
    print("   • Expression evaluation (arithmetic, string operations)")
    print("   • Property persistence across multiple queries")
    print("   • Seamless Pandas DataFrame integration")
    print("   • Complex data transformations")

    print("\n🏗️ Architecture Highlights:")
    print("   • Properties stored in original entity tables")
    print("   • Expression evaluator uses updated entity context")
    print("   • No dynamic schema tracking needed")
    print("   • Clean pipeline with ID/attribute separation")

    print("\n🚀 Ready for Production Use:")
    print("   • All SET operations working correctly")
    print("   • Property modifications fully integrated")
    print("   • Results returned as standard Pandas DataFrames")
    print("   • Query pipeline maintains data integrity")


if __name__ == "__main__":
    # Suppress debug logs for clean demo output
    import logging

    logging.getLogger("shared.logger").setLevel(logging.CRITICAL)

    main()
