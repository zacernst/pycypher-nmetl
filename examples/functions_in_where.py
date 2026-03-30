"""Examples: using registered scalar functions in WHERE clauses.

All built-in scalar functions (toUpper, toLower, trim, size, abs, toInteger, etc.)
can be used directly inside WHERE predicates in MATCH and WITH...WHERE clauses.
Property lookups inside function calls are resolved by joining against the entity
table — the WHERE predicate always operates in the pre-projection scope where
original entity variables are available.
"""

from __future__ import annotations

import pandas as pd
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


def main() -> None:
    # ---------------------------------------------------------------------------
    # Sample data
    # ---------------------------------------------------------------------------

    people = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["  Alice  ", "BOB", "carol", "Dave Smith", None],
            "age": [30, 40, 25, 35, None],
            "score": [85.5, 92.0, 78.0, 60.0, None],
            "department": [
                "Engineering",
                "Sales",
                "Engineering",
                "Marketing",
                "Sales",
            ],
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "score", "department"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
            "department": "department",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
            "department": "department",
        },
        source_obj=people,
    )

    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )

    star = Star(context=ctx)

    # ---------------------------------------------------------------------------
    # Example 1: String function in WHERE
    # ---------------------------------------------------------------------------

    print("=== Example 1: toUpper in WHERE ===")
    result = star.execute_query(
        "MATCH (p:Person) WHERE toUpper(trim(p.name)) = 'ALICE' RETURN p.name AS name",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 2: size() to filter by string length
    # ---------------------------------------------------------------------------

    print("\n=== Example 2: size() in WHERE ===")
    result = star.execute_query(
        "MATCH (p:Person) WHERE size(trim(p.name)) > 4 RETURN p.name AS name, p.age AS age",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 3: Math function — abs() for range filter
    # ---------------------------------------------------------------------------

    print("\n=== Example 3: abs() in WHERE ===")
    result = star.execute_query(
        "MATCH (p:Person) WHERE abs(p.score) > 80.0 RETURN p.name AS name, p.score AS score",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 4: Boolean combination with function
    # ---------------------------------------------------------------------------

    print("\n=== Example 4: function inside AND ===")
    result = star.execute_query(
        "MATCH (p:Person) "
        "WHERE toUpper(p.department) = 'ENGINEERING' AND p.age < 30 "
        "RETURN p.name AS name, p.age AS age",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 5: NOT with a function predicate
    # ---------------------------------------------------------------------------

    print("\n=== Example 5: NOT with function ===")
    result = star.execute_query(
        "MATCH (p:Person) "
        "WHERE NOT toLower(p.department) = 'sales' "
        "RETURN p.name AS name, p.department AS department",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 6: WITH...WHERE — function on original entity property
    # ---------------------------------------------------------------------------

    print(
        "\n=== Example 6: WITH...WHERE with function on original property ===",
    )
    result = star.execute_query(
        "MATCH (p:Person) "
        "WITH p.name AS name, p.score AS score "
        "WHERE toUpper(p.name) = 'BOB' "
        "RETURN name AS name, score AS score",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 7: Custom registered function in WHERE
    # ---------------------------------------------------------------------------

    print("\n=== Example 7: custom function in WHERE ===")

    registry = ScalarFunctionRegistry.get_instance()
    registry.register_function(
        name="is_long_name",
        callable=lambda s: s.str.strip().str.len() > 5,
        min_args=1,
        max_args=1,
        description="True if name (after trim) has more than 5 characters",
        example="is_long_name('  Alice  ') → False, is_long_name('Dave Smith') → True",
    )

    result = star.execute_query(
        "MATCH (p:Person) WHERE is_long_name(p.name) RETURN p.name AS name",
    )
    print(result)

    # ---------------------------------------------------------------------------
    # Example 8: toInteger() for type conversion in WHERE
    # ---------------------------------------------------------------------------

    print("\n=== Example 8: toInteger() conversion in WHERE ===")
    result = star.execute_query(
        "MATCH (p:Person) WHERE toInteger(p.age) > 30 RETURN p.name AS name, p.age AS age",
    )
    print(result)


if __name__ == "__main__":
    main()
