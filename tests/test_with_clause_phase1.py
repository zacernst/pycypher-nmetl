"""Unit tests for WITH clause execution via BindingFrame path.

Tests scalar functions in WITH...WHERE predicates.
"""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture
def person_context():
    """Create a test context with Person entities."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 40, 25],
            "city": ["NYC", "LA", "SF"],
        }
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "city"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        source_obj=person_df,
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table})
    )

    return context


class TestWithWhereScalarFunctions:
    """Tests for scalar functions used in WITH...WHERE predicates.

    The WITH...WHERE predicate is evaluated BEFORE projection (pre-projection scope).
    This means the WHERE expression has access to original entity variables (e.g. p.name)
    but NOT to projected aliases (e.g. 'name' from WITH p.name AS name).

    The pre-projection scope is the correct Cypher contract here: in standard Cypher,
    WITH ... WHERE filters rows using the same scope as the MATCH (or preceding WITH).
    """

    def test_with_where_toupper_on_original_property(self, person_context):
        """WHERE toUpper(p.name) = 'CAROL' — function on original entity property."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.age AS age "
            "WHERE toUpper(p.name) = 'CAROL' RETURN age AS age"
        )
        # Only Carol (age=25) matches
        assert len(result) == 1
        assert result["age"].iloc[0] == 25

    def test_with_where_tolower_on_original_property(self, person_context):
        """WHERE toLower(p.name) = 'alice' — toLower on original entity property."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name "
            "WHERE toLower(p.name) = 'alice' RETURN name AS name"
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Alice"

    def test_with_where_size_on_original_property(self, person_context):
        """WHERE size(p.name) > 4 — size() on original entity property."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name "
            "WHERE size(p.name) > 4 RETURN name AS name"
        )
        # "Alice"=5, "Bob"=3, "Carol"=5 → Alice and Carol (both > 4)
        assert len(result) == 2
        assert set(result["name"].tolist()) == {"Alice", "Carol"}

    def test_with_where_function_and_comparison(self, person_context):
        """WHERE toUpper(p.name) = 'BOB' AND p.age > 30 — function combined with comparison."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.age AS age "
            "WHERE toUpper(p.name) = 'BOB' AND p.age > 30 RETURN name AS name, age AS age"
        )
        # Bob (40) satisfies both
        assert len(result) == 1
        assert result["name"].iloc[0] == "Bob"
        assert result["age"].iloc[0] == 40

    def test_with_where_function_in_or(self, person_context):
        """WHERE toLower(p.name) = 'alice' OR p.age > 35 — function in OR."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.age AS age "
            "WHERE toLower(p.name) = 'alice' OR p.age > 35 RETURN name AS name"
        )
        # Alice (name matches) + Bob (age 40 > 35) → 2 rows
        assert len(result) == 2

    def test_with_where_nested_function_on_original_property(
        self, person_context
    ):
        """WHERE size(toUpper(p.name)) > 4 — nested function on original property."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name "
            "WHERE size(toUpper(p.name)) > 4 RETURN name AS name"
        )
        # toUpper: ALICE=5, BOB=3, CAROL=5 — > 4: Alice, Carol
        assert len(result) == 2

    def test_with_where_function_not(self, person_context):
        """WHERE NOT toUpper(p.name) = 'BOB' — NOT wrapping a function predicate."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name "
            "WHERE NOT toUpper(p.name) = 'BOB' RETURN name AS name"
        )
        # Everyone except Bob → 2 rows
        assert len(result) == 2
        assert "Bob" not in result["name"].tolist()

    def test_with_where_projected_alias(self, person_context):
        """WHERE references a projected alias from WITH clause."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name WHERE size(name) > 4 RETURN name AS name"
        )
        assert len(result) == 2  # Alice (5), Carol (5)

    def test_with_function_result_aliased_then_where(self, person_context):
        """WHERE filters on an alias produced by a function in WITH clause."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH toUpper(p.name) AS upper WHERE upper = 'CAROL' RETURN upper AS upper"
        )
        assert len(result) == 1
        assert result["upper"].iloc[0] == "CAROL"
