"""Integration tests for scalar functions in WITH clauses and expressions.

Tests the full integration of scalar functions with:
- ExpressionEvaluator
- WITH clause processing
- Star query execution
- Mixed scalar and aggregation functions
"""

import pandas as pd
import pytest

from pycypher.ast_models import (
    FunctionInvocation,
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
    Projection,
)


# ---------------------------------------------------------------------------
# Shared fixtures following test_arithmetic_operations.py patterns
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_data() -> pd.DataFrame:
    """Raw Person entity data."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["alice", "BOB", "Charlie"],
            "age": [25, 30, 35],
        }
    )


@pytest.fixture()
def person_context(person_data: pd.DataFrame) -> Context:
    """Context with Person entities."""
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_data,
    )
    entity_mapping = EntityMapping(mapping={"Person": person_table})
    return Context(entity_mapping=entity_mapping)


@pytest.fixture()
def person_relation(person_context: Context) -> Projection:
    """Projection relation with variable 'p' pointing at Person."""
    person_table = person_context.entity_mapping["Person"]
    relation = Projection(
        relation=person_table,
        projected_column_names={
            f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}",
        },
    )
    relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
    relation.variable_type_map = {Variable(name="p"): "Person"}
    return relation


# ---------------------------------------------------------------------------
# ExpressionEvaluator tests
# ---------------------------------------------------------------------------


class TestScalarFunctionsInExpressionEvaluator:
    """Test scalar functions through ExpressionEvaluator."""

    def test_toupper_function_invocation(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Test toUpper through ExpressionEvaluator."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        func_expr = FunctionInvocation(
            name="toUpper",
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="name"),
                ]
            },
        )

        result_series, col_name = evaluator.evaluate(func_expr, df)

        assert result_series.tolist() == ["ALICE", "BOB", "CHARLIE"]
        assert col_name == "toUpper(...)"

    def test_tolower_with_property_lookup(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Test toLower on property access."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        func_expr = FunctionInvocation(
            name="toLower",
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="name"),
                ]
            },
        )

        result_series, _ = evaluator.evaluate(func_expr, df)
        assert result_series.tolist() == ["alice", "bob", "charlie"]

    def test_tostring_on_integer_property(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Test toString on integer property."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        func_expr = FunctionInvocation(
            name="toString",
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="age"),
                ]
            },
        )

        result_series, _ = evaluator.evaluate(func_expr, df)
        assert result_series.tolist() == ["25", "30", "35"]

    def test_tointeger_on_string_literal(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Test toInteger on string literal."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        func_expr = FunctionInvocation(
            name="toInteger",
            arguments={"arguments": [StringLiteral(value="42")]},
        )

        result_series, _ = evaluator.evaluate(func_expr, df)
        # Should return 42 for every row
        assert result_series.tolist() == [42, 42, 42]

    def test_coalesce_with_null(self) -> None:
        """Test coalesce handling null values."""
        # Build dedicated data with nulls and a nickname column
        data = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["alice", None, "charlie"],
                "nickname": [None, "bobby", None],
            }
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "nickname"],
            source_obj_attribute_map={"name": "name", "nickname": "nickname"},
            attribute_map={"name": "name", "nickname": "nickname"},
            source_obj=data,
        )
        entity_mapping = EntityMapping(mapping={"Person": person_table})
        context = Context(entity_mapping=entity_mapping)

        relation = Projection(
            relation=person_table,
            projected_column_names={
                f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}",
            },
        )
        relation.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
        relation.variable_type_map = {Variable(name="p"): "Person"}

        evaluator = ExpressionEvaluator(context=context, relation=relation)
        df = relation.to_pandas(context=context)

        # coalesce(p.nickname, p.name, "Unknown")
        func_expr = FunctionInvocation(
            name="coalesce",
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="nickname"),
                    PropertyLookup(expression=Variable(name="p"), property="name"),
                    StringLiteral(value="Unknown"),
                ]
            },
        )

        result_series, _ = evaluator.evaluate(func_expr, df)

        # Row 0: nickname null -> name "alice"
        # Row 1: nickname "bobby"
        # Row 2: nickname null -> name "charlie"
        assert result_series.iloc[0] == "alice"
        assert result_series.iloc[1] == "bobby"
        assert result_series.iloc[2] == "charlie"

    def test_nested_function_calls(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Test nested function calls: toUpper(toString(p.age))."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        inner_func = FunctionInvocation(
            name="toString",
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="age"),
                ]
            },
        )
        outer_func = FunctionInvocation(
            name="toUpper",
            arguments={"arguments": [inner_func]},
        )

        result_series, _ = evaluator.evaluate(outer_func, df)
        # toUpper on numeric strings is a no-op for digits
        assert result_series.tolist() == ["25", "30", "35"]

    def test_substring_with_literals(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Test substring with integer literals."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        # substring(p.name, 0, 3)
        func_expr = FunctionInvocation(
            name="substring",
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="name"),
                    IntegerLiteral(value=0),
                    IntegerLiteral(value=3),
                ]
            },
        )

        result_series, _ = evaluator.evaluate(func_expr, df)
        assert result_series.tolist() == ["ali", "BOB", "Cha"]


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


class TestScalarFunctionErrors:
    """Test error handling for scalar functions in evaluator."""

    def test_unknown_function_error(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Unknown function produces clear error."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        func_expr = FunctionInvocation(
            name="unknownFunction",
            arguments={"arguments": [StringLiteral(value="test")]},
        )

        with pytest.raises(ValueError, match="Unknown scalar function"):
            evaluator.evaluate(func_expr, df)

    def test_wrong_argument_count_error(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Wrong argument count produces clear error."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        # toUpper expects 1 argument, give it 2
        func_expr = FunctionInvocation(
            name="toUpper",
            arguments={
                "arguments": [
                    StringLiteral(value="hello"),
                    StringLiteral(value="world"),
                ]
            },
        )

        with pytest.raises(ValueError, match="at most 1 argument"):
            evaluator.evaluate(func_expr, df)


# ---------------------------------------------------------------------------
# Scalar vs aggregation distinction (Star._contains_aggregation)
# ---------------------------------------------------------------------------


class TestScalarVsAggregationDistinction:
    """Test that scalar functions are not treated as aggregations."""

    def test_contains_aggregation_scalar_function(self) -> None:
        """Scalar functions should NOT be classified as aggregations."""
        from pycypher.star import Star

        star = Star(Context())

        func_expr = FunctionInvocation(
            name="toUpper",
            arguments={"arguments": [StringLiteral(value="test")]},
        )
        assert not star._contains_aggregation(func_expr)

    def test_contains_aggregation_agg_function(self) -> None:
        """Aggregation functions should be classified as aggregations."""
        from pycypher.star import Star

        star = Star(Context())

        func_expr = FunctionInvocation(
            name="count",
            arguments={"arguments": [StringLiteral(value="test")]},
        )
        assert star._contains_aggregation(func_expr)

    def test_contains_aggregation_all_agg_functions(self) -> None:
        """All known aggregation functions are recognized."""
        from pycypher.star import Star

        star = Star(Context())
        agg_functions = ["collect", "count", "sum", "avg", "min", "max"]

        for func_name in agg_functions:
            func_expr = FunctionInvocation(
                name=func_name,
                arguments={"arguments": [IntegerLiteral(value=1)]},
            )
            assert star._contains_aggregation(func_expr), (
                f"{func_name} should be classified as aggregation"
            )

    def test_contains_aggregation_all_scalar_functions(self) -> None:
        """Known scalar functions are NOT classified as aggregations."""
        from pycypher.star import Star

        star = Star(Context())
        scalar_functions = [
            "toUpper",
            "toLower",
            "trim",
            "toString",
            "toInteger",
            "toFloat",
            "toBoolean",
            "substring",
            "size",
            "coalesce",
        ]

        for func_name in scalar_functions:
            func_expr = FunctionInvocation(
                name=func_name,
                arguments={"arguments": [StringLiteral(value="test")]},
            )
            assert not star._contains_aggregation(func_expr), (
                f"{func_name} should NOT be classified as aggregation"
            )
