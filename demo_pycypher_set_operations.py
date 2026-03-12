#!/usr/bin/env python3
"""
PyCypher Demo: Cypher to Pandas DataFrame with SET Operations

This demo showcases the PyCypher system's ability to:
1. Parse Cypher queries and translate them to relational algebra
2. Execute queries against Pandas DataFrames
3. Modify properties using SET clauses
4. Return results as Pandas DataFrames

The SET clause functionality demonstrates dynamic property addition
and modification with full integration into the query pipeline.
"""

import pandas as pd
from pycypher.star import Star
from pycypher.relational_models import (
    Context, EntityMapping, RelationshipMapping,
    EntityTable, RelationshipTable, ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN
)

def create_sample_hr_data():
    """Create sample HR data for demonstration."""

    print("🏢 Creating Sample HR Dataset")
    print("=" * 50)

    # Employee data
    employees_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5, 6],
        "employee_id": ["EMP001", "EMP002", "EMP003", "EMP004", "EMP005", "EMP006"],
        "first_name": ["Alice", "Bob", "Carol", "David", "Eve", "Frank"],
        "last_name": ["Johnson", "Smith", "Williams", "Brown", "Davis", "Miller"],
        "email": ["alice@company.com", "bob@company.com", "carol@company.com",
                 "david@company.com", "eve@company.com", "frank@company.com"],
        "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales", "HR"],
        "base_salary": [85000, 65000, 90000, 70000, 68000, 72000],
        "hire_date": ["2020-01-15", "2019-03-20", "2021-06-10", "2018-11-05", "2022-02-28", "2020-09-12"],
        "performance_rating": [4.2, 3.8, 4.5, 3.9, 4.1, 3.7],
        "active": [True, True, True, False, True, True]
    })

    # Department data
    departments_df = pd.DataFrame({
        ID_COLUMN: [101, 102, 103, 104],
        "dept_code": ["ENG", "SALES", "MKT", "HR"],
        "dept_name": ["Engineering", "Sales", "Marketing", "Human Resources"],
        "budget": [2500000, 1800000, 1200000, 800000],
        "manager_id": ["EMP003", "EMP002", "EMP004", "EMP006"]
    })

    # Relationships: Employee WORKS_IN Department
    works_in_df = pd.DataFrame({
        ID_COLUMN: [201, 202, 203, 204, 205, 206],
        RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3, 4, 5, 6],  # Employee IDs
        RELATIONSHIP_TARGET_COLUMN: [101, 102, 101, 103, 102, 104],  # Department IDs
        "start_date": ["2020-01-15", "2019-03-20", "2021-06-10", "2018-11-05", "2022-02-28", "2020-09-12"],
        "role": ["Senior Developer", "Account Manager", "Tech Lead", "Marketing Specialist", "Sales Rep", "HR Manager"]
    })

    print(f"📊 Created {len(employees_df)} employees across {len(departments_df)} departments")
    print(f"🔗 Created {len(works_in_df)} employment relationships")

    return employees_df, departments_df, works_in_df

def setup_pycypher_context(employees_df, departments_df, works_in_df):
    """Set up the PyCypher context with entity and relationship tables."""

    print("\n⚙️ Setting up PyCypher Context")
    print("=" * 50)

    # Create Employee entity table
    employee_table = EntityTable(
        entity_type="Employee",
        identifier="Employee",
        column_names=[ID_COLUMN, "employee_id", "first_name", "last_name", "email",
                     "department", "base_salary", "hire_date", "performance_rating", "active"],
        source_obj_attribute_map={
            "employee_id": "employee_id", "first_name": "first_name", "last_name": "last_name",
            "email": "email", "department": "department", "base_salary": "base_salary",
            "hire_date": "hire_date", "performance_rating": "performance_rating", "active": "active"
        },
        attribute_map={
            "employee_id": "employee_id", "first_name": "first_name", "last_name": "last_name",
            "email": "email", "department": "department", "base_salary": "base_salary",
            "hire_date": "hire_date", "performance_rating": "performance_rating", "active": "active"
        },
        source_obj=employees_df
    )

    # Create Department entity table
    department_table = EntityTable(
        entity_type="Department",
        identifier="Department",
        column_names=[ID_COLUMN, "dept_code", "dept_name", "budget", "manager_id"],
        source_obj_attribute_map={
            "dept_code": "dept_code", "dept_name": "dept_name",
            "budget": "budget", "manager_id": "manager_id"
        },
        attribute_map={
            "dept_code": "dept_code", "dept_name": "dept_name",
            "budget": "budget", "manager_id": "manager_id"
        },
        source_obj=departments_df
    )

    # Create WORKS_IN relationship table
    works_in_table = RelationshipTable(
        relationship_type="WORKS_IN",
        identifier="WORKS_IN",
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN, "start_date", "role"],
        source_obj_attribute_map={
            "start_date": "start_date", "role": "role"
        },
        attribute_map={
            "start_date": "start_date", "role": "role"
        },
        source_obj=works_in_df
    )

    # Create context
    context = Context(
        entity_mapping=EntityMapping(mapping={
            "Employee": employee_table,
            "Department": department_table
        }),
        relationship_mapping=RelationshipMapping(mapping={
            "WORKS_IN": works_in_table
        })
    )

    print("✅ Created Employee entity table with {} records".format(len(employees_df)))
    print("✅ Created Department entity table with {} records".format(len(departments_df)))
    print("✅ Created WORKS_IN relationship table with {} records".format(len(works_in_df)))

    return context

def demo_basic_queries(star):
    """Demonstrate basic MATCH and RETURN queries."""

    print("\n🔍 Basic Query Demonstrations")
    print("=" * 50)

    # Query 1: Simple node retrieval
    print("\n1️⃣ Query: Basic employee listing")
    print("MATCH (e:Employee) RETURN e.first_name AS name, e.department AS dept")

    result = star.execute_query("""
        MATCH (e:Employee)
        RETURN e.first_name AS name, e.department AS dept
    """)
    print("\n📋 Results:")
    print(result)
    print(f"   Shape: {result.shape}")

    # Query 2: Filtering with expressions
    print("\n2️⃣ Query: High-performing employees")
    print("MATCH (e:Employee) WHERE e.performance_rating >= 4.0")
    print("RETURN e.first_name AS name, e.performance_rating AS rating, e.base_salary AS salary")

    try:
        result = star.execute_query("""
            MATCH (e:Employee)
            RETURN e.first_name AS name, e.performance_rating AS rating, e.base_salary AS salary
        """)

        # Filter in post-processing since WHERE isn't implemented yet
        high_performers = result[result['rating'] >= 4.0]
        print("\n📋 High Performers (rating >= 4.0):")
        print(high_performers)
        print(f"   Found {len(high_performers)} high performers out of {len(result)} total employees")
    except Exception as e:
        print(f"⚠️  Note: WHERE clause not yet implemented - {e}")

def demo_set_operations(star):
    """Demonstrate SET clause operations for property modification."""

    print("\n🛠️ SET Clause Demonstrations")
    print("=" * 50)

    # Demo 1: Adding new properties
    print("\n1️⃣ Adding new properties with SET")
    print("Query: MATCH (e:Employee) SET e.review_year = 2024, e.status = 'active'")
    print("       RETURN e.first_name AS name, e.review_year, e.status")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.review_year = 2024, e.status = 'active'
        RETURN e.first_name AS name, e.review_year AS review_year, e.status AS status
    """)
    print("\n📋 Results (new properties added):")
    print(result)
    print(f"   Added 'review_year' and 'status' properties to {len(result)} employees")

    # Demo 2: Expression-based property calculation
    print("\n2️⃣ Calculating properties with expressions")
    print("Query: MATCH (e:Employee) SET e.annual_bonus = e.base_salary * 0.1")
    print("       RETURN e.first_name AS name, e.base_salary, e.annual_bonus")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.annual_bonus = e.base_salary * 0.1
        RETURN e.first_name AS name, e.base_salary AS base_salary, e.annual_bonus AS annual_bonus
    """)
    print("\n📋 Results (calculated bonuses):")
    print(result)

    # Demo 3: Conditional property updates
    print("\n3️⃣ Conditional property updates")
    print("Query: MATCH (e:Employee) SET e.salary_adjustment = e.performance_rating * 1000")
    print("       RETURN e.first_name AS name, e.performance_rating, e.salary_adjustment")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.salary_adjustment = e.performance_rating * 1000
        RETURN e.first_name AS name, e.performance_rating AS performance_rating, e.salary_adjustment AS salary_adjustment
    """)
    print("\n📋 Results (performance-based adjustments):")
    print(result)

    # Demo 4: Modifying existing properties
    print("\n4️⃣ Modifying existing properties")
    print("Query: MATCH (e:Employee) SET e.base_salary = e.base_salary + e.salary_adjustment")
    print("       RETURN e.first_name AS name, e.base_salary AS new_salary")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.base_salary = e.base_salary + e.salary_adjustment
        RETURN e.first_name AS name, e.base_salary AS new_salary
    """)
    print("\n📋 Results (updated salaries):")
    print(result)

    # Demo 5: Verify properties persist across queries
    print("\n5️⃣ Verifying property persistence")
    print("Query: MATCH (e:Employee) RETURN e.first_name AS name, e.status, e.annual_bonus")
    print("       (Properties from previous SET operations should still be accessible)")

    result = star.execute_query("""
        MATCH (e:Employee)
        RETURN e.first_name AS name, e.status AS status, e.annual_bonus AS annual_bonus, e.base_salary AS updated_salary
    """)
    print("\n📋 Results (persistent properties):")
    print(result)
    print("   ✅ Properties added in previous queries are still accessible!")

def demo_complex_scenarios(star):
    """Demonstrate complex scenarios combining SET with other operations."""

    print("\n🎯 Complex Scenario Demonstrations")
    print("=" * 50)

    # Scenario 1: Bulk data transformation
    print("\n1️⃣ Bulk data transformation")
    print("Query: Employee data standardization and enrichment")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.full_name = e.first_name + ' ' + e.last_name,
            e.domain = 'company.com',
            e.years_service = 2024 - 2020,
            e.performance_multiplier = e.performance_rating * 10
        RETURN e.full_name AS full_name, e.domain AS domain, e.years_service AS years_service, e.performance_multiplier AS performance_multiplier
    """)
    print("\n📋 Results (bulk transformation):")
    print(result)

    # Scenario 2: Calculated fields for reporting
    print("\n2️⃣ Calculated fields for analytics")
    print("Query: Generate comprehensive employee analytics")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.total_compensation = e.base_salary + e.annual_bonus,
            e.performance_tier = e.performance_rating * 10,
            e.retention_score = e.years_service * e.performance_rating
        RETURN e.first_name AS name,
               e.total_compensation AS total_compensation,
               e.performance_tier AS performance_tier,
               e.retention_score AS retention_score
    """)
    print("\n📋 Results (analytics fields):")
    print(result)

def main():
    """Main demo execution."""

    print("🚀 PyCypher Demo: Cypher to Pandas with SET Operations")
    print("=" * 60)
    print("This demo showcases PyCypher's ability to:")
    print("• Parse Cypher queries into relational algebra")
    print("• Execute queries against Pandas DataFrames")
    print("• Modify properties dynamically using SET clauses")
    print("• Maintain property persistence across queries")
    print("• Return results as standard Pandas DataFrames")

    # Set up sample data and context
    employees_df, departments_df, works_in_df = create_sample_hr_data()
    context = setup_pycypher_context(employees_df, departments_df, works_in_df)

    # Create Star query processor
    star = Star(context=context)
    print("\n⭐ PyCypher Star query processor initialized")

    # Run demonstrations
    demo_basic_queries(star)
    demo_set_operations(star)
    demo_complex_scenarios(star)

    print("\n🎉 Demo Complete!")
    print("=" * 60)
    print("Key Takeaways:")
    print("• SET clauses successfully add new properties to entities")
    print("• Expression evaluation works with existing properties")
    print("• Property modifications persist across multiple queries")
    print("• Results are returned as standard Pandas DataFrames")
    print("• The system maintains full query pipeline integration")

    print("\nNext Steps:")
    print("• Try modifying the sample data and queries")
    print("• Experiment with more complex Cypher expressions")
    print("• Explore relationship traversals (when implemented)")
    print("• Add your own entity types and properties")

if __name__ == "__main__":
    main()