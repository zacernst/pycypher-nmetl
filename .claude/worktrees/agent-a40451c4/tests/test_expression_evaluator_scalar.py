"""TDD tests for ExpressionEvaluator.evaluate_scalar().

These tests are written BEFORE the implementation so that they initially
fail, then pass once Phase 2.1 is implemented.

Covers:
- evaluate_scalar with a Literal expression returns the literal value
- evaluate_scalar with PropertyLookup finds the property by bare name
- evaluate_scalar with PropertyLookup finds the property when the column
  has an entity prefix (e.g. "Person__age")
- evaluate_scalar with an Arithmetic expression evaluates correctly
- evaluate_scalar raises KeyError when property is missing
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    Arithmetic,
    IntegerLiteral,
    PropertyLookup,
    StringLiteral,
    Variable,
)
from pycypher.expression_evaluator import ExpressionEvaluator
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )


@pytest.fixture()
def person_table(people_df: pd.DataFrame) -> EntityTable:
    return EntityTable(
        entity_type="Person",
        identifier="Person",
        source_obj=people_df,
        attribute_map={"name": "name", "age": "age"},
        source_obj_attribute_map={"name": "name", "age": "age"},
        column_names=[ID_COLUMN, "name", "age"],
    )


@pytest.fixture()
def relational_context(person_table: EntityTable) -> Context:
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
    )


@pytest.fixture()
def evaluator(
    person_table: EntityTable, relational_context: Context
) -> ExpressionEvaluator:
    """ExpressionEvaluator bound to the person_table relation."""
    return ExpressionEvaluator(
        context=relational_context,
        relation=person_table,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_evaluate_scalar_integer_literal(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar on an IntegerLiteral returns the integer value."""
    row = pd.Series({"age": 30, "name": "Alice"})
    result = evaluator.evaluate_scalar(IntegerLiteral(value=42), row)
    assert result == 42


def test_evaluate_scalar_string_literal(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar on a StringLiteral returns the string value."""
    row = pd.Series({"age": 30, "name": "Alice"})
    result = evaluator.evaluate_scalar(StringLiteral(value="hello"), row)
    assert result == "hello"


def test_evaluate_scalar_property_lookup_bare_name(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar finds a property by bare column name."""
    row = pd.Series({"age": 30, "name": "Alice"})
    expr = PropertyLookup(variable=Variable(name="n"), property="age")
    result = evaluator.evaluate_scalar(expr, row)
    assert result == 30


def test_evaluate_scalar_property_lookup_prefixed_name(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar finds a property when the row uses entity-prefixed column names."""
    row = pd.Series({"Person__age": 30, "Person__name": "Alice"})
    expr = PropertyLookup(variable=Variable(name="n"), property="age")
    result = evaluator.evaluate_scalar(expr, row)
    assert result == 30


def test_evaluate_scalar_arithmetic_addition(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar correctly evaluates an addition expression."""
    row = pd.Series({"age": 30, "name": "Alice"})
    expr = Arithmetic(
        left=PropertyLookup(variable=Variable(name="n"), property="age"),
        right=IntegerLiteral(value=5),
        operator="+",
    )
    result = evaluator.evaluate_scalar(expr, row)
    assert result == 35


def test_evaluate_scalar_arithmetic_subtraction(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar correctly evaluates a subtraction expression."""
    row = pd.Series({"age": 30})
    expr = Arithmetic(
        left=PropertyLookup(variable=Variable(name="n"), property="age"),
        right=IntegerLiteral(value=10),
        operator="-",
    )
    result = evaluator.evaluate_scalar(expr, row)
    assert result == 20


def test_evaluate_scalar_arithmetic_multiplication(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar correctly evaluates a multiplication expression."""
    row = pd.Series({"age": 5})
    expr = Arithmetic(
        left=PropertyLookup(variable=Variable(name="n"), property="age"),
        right=IntegerLiteral(value=3),
        operator="*",
    )
    result = evaluator.evaluate_scalar(expr, row)
    assert result == 15


def test_evaluate_scalar_missing_property_raises_key_error(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar raises KeyError when the property is not in the row."""
    row = pd.Series({"age": 30})
    expr = PropertyLookup(variable=Variable(name="n"), property="nonexistent")
    with pytest.raises(KeyError):
        evaluator.evaluate_scalar(expr, row)


def test_evaluate_scalar_nested_arithmetic(
    evaluator: ExpressionEvaluator,
) -> None:
    """evaluate_scalar correctly recurses through nested arithmetic."""
    row = pd.Series({"age": 10})
    # (age + 5) * 2
    inner = Arithmetic(
        left=PropertyLookup(variable=Variable(name="n"), property="age"),
        right=IntegerLiteral(value=5),
        operator="+",
    )
    outer = Arithmetic(
        left=inner,
        right=IntegerLiteral(value=2),
        operator="*",
    )
    result = evaluator.evaluate_scalar(outer, row)
    assert result == 30
