#!/usr/bin/env python3
"""
PyCypher Demo: Clean demonstration of SET operations
"""

import logging
import pandas as pd
from pycypher.star import Star
from pycypher.relational_models import (
    Context, EntityMapping, RelationshipMapping,
    EntityTable, RelationshipTable, ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN
)

# Disable debug logging for clean output
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger('shared.logger').setLevel(logging.CRITICAL)
logging.getLogger('pycypher').setLevel(logging.CRITICAL)

def main():
    print("🚀 PyCypher Demo: Cypher to Pandas with SET Operations")
    print("=" * 60)

    # Create sample employee data
    employees_df = pd.DataFrame({
        ID_COLUMN: [1, 2, 3, 4, 5],
        "first_name": ["Alice", "Bob", "Carol", "David", "Eve"],
        "last_name": ["Johnson", "Smith", "Williams", "Brown", "Davis"],
        "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales"],
        "base_salary": [85000, 65000, 90000, 70000, 68000],
        "performance_rating": [4.2, 3.8, 4.5, 3.9, 4.1]
    })

    # Set up PyCypher context
    employee_table = EntityTable(
        entity_type="Employee",
        identifier="Employee",
        column_names=[ID_COLUMN, "first_name", "last_name", "department", "base_salary", "performance_rating"],
        source_obj_attribute_map={
            "first_name": "first_name", "last_name": "last_name", "department": "department",
            "base_salary": "base_salary", "performance_rating": "performance_rating"
        },
        attribute_map={
            "first_name": "first_name", "last_name": "last_name", "department": "department",
            "base_salary": "base_salary", "performance_rating": "performance_rating"
        },
        source_obj=employees_df
    )

    context = Context(entity_mapping=EntityMapping(mapping={"Employee": employee_table}))
    star = Star(context=context)

    print(f"📊 Sample data: {len(employees_df)} employees")
    print("\n" + "=" * 60)

    # Demo 1: Basic query
    print("1️⃣ Basic Query")
    print("MATCH (e:Employee) RETURN e.first_name AS name, e.department AS dept")

    result = star.execute_query("""
        MATCH (e:Employee)
        RETURN e.first_name AS name, e.department AS dept
    """)
    print(f"\n📋 Results ({result.shape[0]} rows):")
    print(result.to_string(index=False))

    # Demo 2: SET - Add new properties
    print(f"\n{'-' * 60}")
    print("2️⃣ SET: Adding New Properties")
    print("MATCH (e:Employee) SET e.status = 'active', e.review_year = 2024")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.status = 'active', e.review_year = 2024
        RETURN e.first_name AS name, e.status AS status, e.review_year AS year
    """)
    print(f"\n📋 Results with new properties ({result.shape[0]} rows):")
    print(result.to_string(index=False))

    # Demo 3: SET - Expression-based calculations
    print(f"\n{'-' * 60}")
    print("3️⃣ SET: Expression-Based Calculations")
    print("MATCH (e:Employee) SET e.annual_bonus = e.base_salary * 0.1")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.annual_bonus = e.base_salary * 0.1
        RETURN e.first_name AS name, e.base_salary AS salary, e.annual_bonus AS bonus
    """)
    print(f"\n📋 Results with calculated bonuses ({result.shape[0]} rows):")
    print(result.to_string(index=False))

    # Demo 4: SET - Modify existing properties
    print(f"\n{'-' * 60}")
    print("4️⃣ SET: Modifying Existing Properties")
    print("MATCH (e:Employee) SET e.base_salary = e.base_salary + e.annual_bonus")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.base_salary = e.base_salary + e.annual_bonus
        RETURN e.first_name AS name, e.base_salary AS updated_salary
    """)
    print(f"\n📋 Results with updated salaries ({result.shape[0]} rows):")
    print(result.to_string(index=False))

    # Demo 5: SET - Complex expressions
    print(f"\n{'-' * 60}")
    print("5️⃣ SET: Complex Expressions")
    print("MATCH (e:Employee) SET e.full_name = e.first_name + ' ' + e.last_name")

    result = star.execute_query("""
        MATCH (e:Employee)
        SET e.full_name = e.first_name + ' ' + e.last_name,
            e.performance_score = e.performance_rating * 100
        RETURN e.full_name AS name, e.performance_score AS score
    """)
    print(f"\n📋 Results with computed fields ({result.shape[0]} rows):")
    print(result.to_string(index=False))

    # Demo 6: Verify persistence
    print(f"\n{'-' * 60}")
    print("6️⃣ Property Persistence Verification")
    print("MATCH (e:Employee) RETURN all previously SET properties")

    result = star.execute_query("""
        MATCH (e:Employee)
        RETURN e.first_name AS name,
               e.status AS status,
               e.annual_bonus AS bonus,
               e.full_name AS full_name
    """)
    print(f"\n📋 All properties persist across queries ({result.shape[0]} rows):")
    print(result.to_string(index=False))

    print(f"\n{'=' * 60}")
    print("🎉 Demo Complete!")
    print("\n✅ Key Features Demonstrated:")
    print("• Cypher queries parsed and executed against Pandas DataFrames")
    print("• SET clauses add new properties dynamically")
    print("• Expression evaluation with existing properties")
    print("• Property modifications persist across queries")
    print("• Results returned as standard Pandas DataFrames")
    print("\n🔧 Architecture Benefits:")
    print("• Properties are stored in original entity tables")
    print("• Expression evaluator works with updated entity context")
    print("• No need to track dynamic schemas across pipeline")
    print("• Clean separation between ID tracking and attribute access")

if __name__ == "__main__":
    main()